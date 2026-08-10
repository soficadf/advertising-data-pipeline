import json
import math
import random
import logging
import copy
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
import os
import pyodbc
from config.adls_client import upload_to_adls

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_db_connection():
    conn_string = os.getenv("AZURE_SQL_CONNECTION_STRING")
    if not conn_string:
        raise ValueError("AZURE_SQL_CONNECTION_STRING no encontrado en .env")
    return pyodbc.connect(conn_string)


def get_daily_spend(target_date: date) -> float:
    """Consulta el gasto publicitario total de un día concreto."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COALESCE(SUM(daily_spend), 0)
            FROM daily_spend
            WHERE date = ?
        """, target_date)
        result = cursor.fetchone()[0]
        conn.close()
        return float(result)
    except Exception as e:
        logger.warning(f"No se pudo consultar el gasto para {target_date}: {e}")
        return 100.0


def get_today_spend() -> float:
    """Consulta el gasto de hoy como referencia."""
    return get_daily_spend(datetime.now(timezone.utc).date())


def load_today_ads() -> list:
    """Carga los anuncios reales de hoy desde landing en ADLS."""
    from config.adls_client import get_adls_client
    
    container = os.getenv("ADLS_CONTAINER_NAME")
    client = get_adls_client()
    filesystem = client.get_file_system_client(container)

    paths = list(filesystem.get_paths(path="landing/meta_ads", recursive=True))
    json_files = sorted([p.name for p in paths if p.name.endswith(".json")])

    if not json_files:
        raise FileNotFoundError("No hay ficheros de Meta en landing/meta_ads/")

    latest = json_files[-1]
    logger.info(f"Usando como base: {latest}")

    file_client = filesystem.get_file_client(latest)
    content = file_client.download_file().readall()
    data = json.loads(content)
    return data["ads"]


def scale_reach_breakdown(breakdown: list, factor: float) -> list:
    """
    Escala los valores numéricos del desglose demográfico por un factor.
    Mantiene al menos 1 si el valor original era > 0.
    """
    scaled = copy.deepcopy(breakdown)
    for country in scaled:
        for age_gender in country.get("age_gender_breakdowns", []):
            for gender in ["male", "female", "unknown"]:
                if gender in age_gender:
                    new_val = int(age_gender[gender] * factor * random.uniform(0.9, 1.1))
                    age_gender[gender] = max(1, new_val)
    return scaled


def generate_historical_ad(ad: dict, factor: float, target_date: date) -> dict:
    """
    Genera una versión histórica de un anuncio escalando el alcance
    proporcionalmente al factor gasto_dia / gasto_hoy.
    """
    historical = copy.deepcopy(ad)

    # Escala el alcance total
    original_reach = ad.get("eu_total_reach", 0)
    historical["eu_total_reach"] = max(1, int(original_reach * factor * random.uniform(0.9, 1.1)))

    # Escala el desglose demográfico
    if "age_country_gender_reach_breakdown" in ad:
        historical["age_country_gender_reach_breakdown"] = scale_reach_breakdown(
            ad["age_country_gender_reach_breakdown"], factor
        )

    # Actualiza la fecha de extracción
    historical["extraction_date"] = target_date.isoformat()

    return historical


def run_meta_backfill(days: int = 30):
    """Genera ficheros históricos de Meta para los últimos N días."""
    today_ads = load_today_ads()
    today_spend = get_today_spend()
    today = datetime.now(timezone.utc).date()

    logger.info(f"Gasto de hoy como referencia: {today_spend}€")
    logger.info(f"Anuncios base: {len(today_ads)}")

    for i in range(days, 0, -1):
        target_date = today - timedelta(days=i)
        daily_spend = get_daily_spend(target_date)

        # Factor de escala: ratio entre gasto del día y gasto de hoy
        factor = (daily_spend / today_spend) if today_spend > 0 else 0.5
        # Añade degradación temporal: anuncios más antiguos tienen menos alcance acumulado
        temporal_factor = (days - i + 1) / days
        combined_factor = factor * temporal_factor

        logger.info(f"{target_date} — Gasto: {daily_spend:.2f}€ — Factor: {combined_factor:.2f}")

        historical_ads = [
            generate_historical_ad(ad, combined_factor, target_date)
            for ad in today_ads
        ]

        filename = f"meta_ads_backfill_{target_date.strftime('%Y%m%d')}.json"
        content = json.dumps({
            "extraction_timestamp": datetime.combine(
                target_date, datetime.min.time()
            ).replace(tzinfo=timezone.utc).isoformat(),
            "extraction_date": target_date.isoformat(),
            "total_ads": len(historical_ads),
            "is_synthetic": True,
            "ads": historical_ads
        }, ensure_ascii=False, indent=2)

        upload_to_adls(
            content=content,
            layer="landing",
            folder="meta_ads",
            filename=filename,
            target_date=target_date
        )

    logger.info(f"Meta backfill completado: {days} días generados")


if __name__ == "__main__":
    run_meta_backfill(days=30)