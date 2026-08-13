import copy
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

from dotenv import load_dotenv

from config.adls_client import get_adls_client, upload_to_adls
from procesamiento.model import DatasetsLanding, Layers

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_daily_spend(target_date: date) -> float:
    """Consulta el gasto publicitario total de un día en Azure SQL."""
    import pyodbc

    conn_string = os.getenv("AZURE_SQL_CONNECTION_STRING")
    if not conn_string:
        raise ValueError("AZURE_SQL_CONNECTION_STRING no encontrado en .env")

    try:
        conn = pyodbc.connect(conn_string)
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
        return 0.0


def load_today_ads() -> list:
    """Carga los anuncios del último fichero de Meta disponible en Landing."""
    container = os.getenv("ADLS_CONTAINER_NAME")
    if not container:
        raise ValueError("ADLS_CONTAINER_NAME no encontrado en .env")

    client = get_adls_client()
    filesystem = client.get_file_system_client(container)

    meta_path = f"{Layers.LANDING.value}/{DatasetsLanding.META.value}"
    paths = list(filesystem.get_paths(path=meta_path, recursive=True))
    json_files = sorted(p.name for p in paths if p.name.endswith(".json"))

    if not json_files:
        raise FileNotFoundError("No hay ficheros de Meta en landing")

    latest_file = json_files[-1]
    logger.info(f"Usando fichero de Meta como base: {latest_file}")

    file_client = filesystem.get_file_client(latest_file)
    content = file_client.download_file().readall()
    data = json.loads(content)

    ads = data.get("ads", [])
    logger.info(f"Anuncios encontrados en el fichero base: {len(ads)}")

    return ads


def parse_ad_start_date(ad: dict) -> date:
    """Obtiene la fecha real de publicación del anuncio."""
    value = ad.get("ad_delivery_start_time")

    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        ).date()
    except ValueError:
        try:
            return datetime.strptime(
                value[:10],
                "%Y-%m-%d"
            ).date()
        except ValueError:
            logger.warning(
                f"No se pudo interpretar la fecha del anuncio {ad.get('id')}: {value}"
            )
            return None


def scale_reach_breakdown(breakdown: list, factor: float) -> list:
    """Escala el desglose demográfico manteniendo la proporción."""
    scaled = copy.deepcopy(breakdown)

    for country in scaled:
        for age_gender in country.get("age_gender_breakdowns", []):
            for gender in ["male", "female", "unknown"]:
                if gender in age_gender:
                    age_gender[gender] = max(
                        0,
                        int(age_gender[gender] * factor)
                    )

    return scaled


def generate_historical_ad(
    ad: dict,
    cumulative_factor: float,
    target_date: date
) -> dict:
    """
    Genera el snapshot histórico de un anuncio.

    cumulative_factor representa la proporción del alcance acumulado
    que tenía el anuncio en la fecha indicada respecto al alcance actual.
    """
    historical = copy.deepcopy(ad)

    original_reach = ad.get("eu_total_reach", 0)
    historical_reach = max(
        1,
        int(original_reach * cumulative_factor)
    )

    historical["eu_total_reach"] = historical_reach

    breakdown = ad.get("age_country_gender_reach_breakdown")
    if breakdown:
        historical["age_country_gender_reach_breakdown"] = (
            scale_reach_breakdown(
                breakdown,
                cumulative_factor
            )
        )

    historical["extraction_date"] = target_date.isoformat()
    historical["extraction_timestamp"] = (
        datetime.combine(
            target_date,
            datetime.min.time()
        )
        .replace(tzinfo=timezone.utc)
        .isoformat()
    )

    return historical


def run_meta_backfill(days: int = 30):
    """
    Reconstruye los snapshots de Meta de los últimos N días.

    Un anuncio solo aparece desde su fecha real de publicación.
    El alcance es acumulado y nunca disminuye.
    """

    today = datetime.now(timezone.utc).date()
    first_date = today - timedelta(days=days - 1)

    today_ads = load_today_ads()

    if not today_ads:
        raise ValueError("No se han encontrado anuncios.")

    # Guardamos la fecha REAL de publicación de cada anuncio.
    ads_with_start = []

    for ad in today_ads:
        start_date = parse_ad_start_date(ad)

        if start_date is None:
            logger.warning(
                f"Anuncio {ad.get('id')} sin fecha de publicación. Se ignora."
            )
            continue

        ads_with_start.append((ad, start_date))

    logger.info(
        f"Anuncios válidos con fecha de publicación: "
        f"{len(ads_with_start)}/{len(today_ads)}"
    )

    # Gasto total del periodo
    daily_spends = {}
    for i in range(days):
        target_date = first_date + timedelta(days=i)
        daily_spends[target_date] = get_daily_spend(target_date)

    total_spend = sum(daily_spends.values())

    if total_spend <= 0:
        total_spend = 1.0

    logger.info(
        f"Gasto total del periodo: {total_spend:.2f}€"
    )

    total_files = 0

    for i in range(days):
        target_date = first_date + timedelta(days=i)
        daily_spend = daily_spends[target_date]

        # Gasto acumulado desde el inicio del periodo.
        cumulative_spend = sum(
            daily_spends[first_date + timedelta(days=j)]
            for j in range(i + 1)
        )

        # Proporción del alcance acumulado.
        cumulative_factor = min(
            1.0,
            cumulative_spend / total_spend
        )

        # Solo anuncios que ya existían en esa fecha.
        active_ads = [
            (ad, start_date)
            for ad, start_date in ads_with_start
            if start_date <= target_date
        ]

        historical_ads = [
            generate_historical_ad(
                ad=ad,
                cumulative_factor=cumulative_factor,
                target_date=target_date
            )
            for ad, _ in active_ads
        ]

        logger.info(
            f"{target_date} | "
            f"Gasto: {daily_spend:.2f}€ | "
            f"Factor acumulado: {cumulative_factor:.2f} | "
            f"Anuncios activos: {len(historical_ads)}"
        )

        filename = (
            f"meta_ads_backfill_{target_date.strftime('%Y%m%d')}.json"
        )

        content = json.dumps(
            {
                "extraction_timestamp": datetime.combine(
                    target_date,
                    datetime.min.time()
                ).replace(tzinfo=timezone.utc).isoformat(),
                "extraction_date": target_date.isoformat(),
                "total_ads": len(historical_ads),
                "is_synthetic": True,
                "ads": historical_ads
            },
            ensure_ascii=False,
            indent=2
        )

        upload_to_adls(
            content=content,
            layer=Layers.LANDING.value,
            folder=DatasetsLanding.META.value,
            filename=filename,
            target_date=target_date
        )

        total_files += 1

    logger.info(
        f"Meta backfill completado. {total_files} ficheros generados."
    )


if __name__ == "__main__":
    run_meta_backfill(days=30)