import copy
import json
import logging
import os
import random
from datetime import date, datetime, timedelta, timezone

from dotenv import load_dotenv

from config.adls_client import get_adls_client, upload_to_adls
from procesamiento.model import DatasetsLanding, Layers

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_daily_spend(target_date: date) -> float:
    """Consulta el gasto publicitario total de un día en Azure SQL."""
    import pyodbc

    conn_string = os.getenv("AZURE_SQL_CONNECTION_STRING")

    if not conn_string:
        raise ValueError(
            "AZURE_SQL_CONNECTION_STRING no encontrado en .env"
        )

    try:
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COALESCE(SUM(daily_spend), 0)
            FROM daily_spend
            WHERE date = ?
            """,
            target_date
        )

        result = cursor.fetchone()[0]

        conn.close()

        return float(result)

    except Exception as e:
        logger.warning(
            f"No se pudo consultar el gasto para "
            f"{target_date}: {e}"
        )
        return 100.0


def load_today_ads() -> list:
    """
    Carga los anuncios del último fichero de Meta disponible
    en Landing.
    """

    container = os.getenv("ADLS_CONTAINER_NAME")

    if not container:
        raise ValueError(
            "ADLS_CONTAINER_NAME no encontrado en .env"
        )

    client = get_adls_client()
    filesystem = client.get_file_system_client(container)

    meta_path = (
        f"{Layers.LANDING.value}/"
        f"{DatasetsLanding.META.value}"
    )

    paths = list(
        filesystem.get_paths(
            path=meta_path,
            recursive=True
        )
    )

    json_files = sorted(
        p.name
        for p in paths
        if p.name.endswith(".json")
    )

    if not json_files:
        raise FileNotFoundError(
            "No hay ficheros de Meta en landing"
        )

    latest_file = json_files[-1]

    logger.info(
        f"Usando fichero de Meta como base: {latest_file}"
    )

    file_client = filesystem.get_file_client(latest_file)

    content = (
        file_client
        .download_file()
        .readall()
    )

    data = json.loads(content)

    ads = data.get("ads", [])

    logger.info(
        f"Anuncios encontrados en el fichero base: {len(ads)}"
    )

    return ads


def scale_reach_breakdown(
    breakdown: list,
    factor: float
) -> list:
    """
    Escala el desglose demográfico del anuncio.
    """

    scaled = copy.deepcopy(breakdown)

    for country in scaled:

        for age_gender in country.get(
            "age_gender_breakdowns",
            []
        ):

            for gender in [
                "male",
                "female",
                "unknown"
            ]:

                if gender in age_gender:

                    original_value = age_gender[gender]

                    variation = random.uniform(
                        0.90,
                        1.10
                    )

                    new_value = int(
                        original_value
                        * factor
                        * variation
                    )

                    age_gender[gender] = max(
                        1,
                        new_value
                    )

    return scaled


def generate_historical_ad(
    ad: dict,
    factor: float,
    target_date: date
) -> dict:
    """
    Genera una versión histórica de un anuncio.

    El alcance se escala según el gasto del día.
    """

    historical = copy.deepcopy(ad)

    # Alcance acumulado
    original_reach = ad.get(
        "eu_total_reach",
        0
    )

    variation = random.uniform(
        0.90,
        1.10
    )

    historical["eu_total_reach"] = max(
        1,
        int(
            original_reach
            * factor
            * variation
        )
    )

    # Desglose demográfico
    breakdown = ad.get(
        "age_country_gender_reach_breakdown"
    )

    if breakdown:
        historical[
            "age_country_gender_reach_breakdown"
        ] = scale_reach_breakdown(
            breakdown,
            factor
        )

    # Fecha de extracción
    historical["extraction_date"] = (
        target_date.isoformat()
    )

    historical["extraction_timestamp"] = (
        datetime.combine(
            target_date,
            datetime.min.time()
        )
        .replace(tzinfo=timezone.utc)
        .isoformat()
    )

    return historical


def generate_ad_start_dates(
    ads: list,
    today: date,
    days: int
) -> dict:
    """
    Asigna a cada anuncio una fecha de inicio
    aleatoria dentro del periodo generado.
    """

    start_dates = {}

    for ad in ads:

        ad_id = ad["id"]

        start_dates[ad_id] = (
            today
            - timedelta(
                days=random.randint(
                    0,
                    days - 1
                )
            )
        )

    return start_dates


def run_meta_backfill(
    days: int = 30
):
    """
    Genera datos históricos sintéticos de Meta
    para los últimos N días.
    """

    today = datetime.now(
        timezone.utc
    ).date()

    today_ads = load_today_ads()

    if not today_ads:
        raise ValueError(
            "No se han encontrado anuncios."
        )

    logger.info(
        f"Generando histórico de {days} días."
    )

    logger.info(
        f"Total anuncios disponibles: "
        f"{len(today_ads)}"
    )

    # Cada anuncio tiene su propia fecha de inicio.
    ad_start_dates = generate_ad_start_dates(
        today_ads,
        today,
        days
    )

    logger.info(
        "Fechas de inicio de anuncios generadas."
    )

    # Gasto de hoy como referencia
    today_spend = get_daily_spend(today)

    if today_spend <= 0:
        today_spend = 1.0

    logger.info(
        f"Gasto de hoy: {today_spend:.2f}€"
    )

    total_files = 0

    for i in range(
        days - 1,
        -1,
        -1
    ):

        target_date = (
            today
            - timedelta(days=i)
        )

        daily_spend = get_daily_spend(
            target_date
        )

        # Relación entre gasto del día y gasto actual.
        spend_factor = (
            daily_spend
            / today_spend
        )

        # Los anuncios acumulan alcance con el tiempo.
        elapsed_days = (
            target_date
            - (today - timedelta(days=days))
        ).days + 1

        temporal_factor = (
            elapsed_days
            / days
        )

        combined_factor = max(
            0.05,
            spend_factor
            * temporal_factor
        )

        # Solo anuncios que ya estaban activos
        active_ads = [
            ad
            for ad in today_ads
            if ad_start_dates[
                ad["id"]
            ] <= target_date
        ]

        historical_ads = [
            generate_historical_ad(
                ad=ad,
                factor=combined_factor,
                target_date=target_date
            )
            for ad in active_ads
        ]

        logger.info(
            f"{target_date} | "
            f"Gasto: {daily_spend:.2f}€ | "
            f"Factor: {combined_factor:.2f} | "
            f"Anuncios activos: "
            f"{len(historical_ads)}/{len(today_ads)}"
        )

        filename = (
            f"meta_ads_backfill_"
            f"{target_date.strftime('%Y%m%d')}.json"
        )

        content = json.dumps(
            {
                "extraction_timestamp": (
                    datetime.combine(
                        target_date,
                        datetime.min.time()
                    )
                    .replace(
                        tzinfo=timezone.utc
                    )
                    .isoformat()
                ),
                "extraction_date": (
                    target_date.isoformat()
                ),
                "total_ads": len(
                    historical_ads
                ),
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
        f"Meta backfill completado. "
        f"{total_files} ficheros generados."
    )


if __name__ == "__main__":
    run_meta_backfill(days=30)