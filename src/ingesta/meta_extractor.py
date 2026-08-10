import requests
import json
import logging
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Permite importar módulos del repo tanto en local como en Databricks
try:
    sys.path.append("/Workspace/Repos/tu-usuario/ad-performance-pipeline/src")
    from config.adls_client import upload_to_adls
except ImportError:
    from config.adls_client import upload_to_adls

BASE_URL = "https://graph.facebook.com/v25.0/ads_archive"

FIELDS = ",".join([
    "id",
    "ad_delivery_start_time",
    "ad_delivery_stop_time",
    "impressions",
    "publisher_platforms",
    "demographic_distribution",
    "age_country_gender_reach_breakdown",
    "eu_total_reach",
    "target_ages",
    "target_gender",
    "target_locations"
])


def get_meta_token() -> str:
    """Lee el token de Meta del entorno disponible."""
    if os.getenv("ENV", "prod") == "local":
        from dotenv import load_dotenv
        load_dotenv()
        token = os.getenv("META_ACCESS_TOKEN")
        logger.info("Token leído desde .env")
    else:
        token = dbutils.secrets.get(scope="ad-pipeline", key="meta_access_token")
        logger.info("Token leído desde Databricks Secrets")

    if not token:
        raise ValueError("META_ACCESS_TOKEN no encontrado")
    return token


def fetch_ads(search_terms: str, country: str = "ES") -> list:
    """Extrae todos los anuncios de una marca de la Meta Ad Library."""
    token = get_meta_token()

    params = {
        "search_terms": search_terms,
        "ad_reached_countries": country,
        "ad_active_status": "active",
        "ad_type": "ALL",
        "fields": FIELDS,
        "access_token": token,
        "limit": 100
    }

    all_ads = []
    page = 1

    while True:
        logger.info(f"Extrayendo página {page}...")
        response = requests.get(BASE_URL, params=params)

        if response.status_code != 200:
            logger.error(f"Error en la API: {response.text}")
            break

        data = response.json()
        ads = data.get("data", [])
        all_ads.extend(ads)
        logger.info(f"Página {page}: {len(ads)} anuncios extraídos")

        cursor_after = data.get("paging", {}).get("cursors", {}).get("after")
        if not cursor_after:
            break

        params["after"] = cursor_after
        page += 1

    logger.info(f"Total anuncios extraídos: {len(all_ads)}")
    return all_ads


def save_to_landing(ads: list) -> str:
    """Guarda los anuncios extraídos en la capa landing del Data Lake."""
    output = {
        "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
        "extraction_date": datetime.now(timezone.utc).date().isoformat(),
        "total_ads": len(ads),
        "ads": ads
    }

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"meta_ads_{timestamp}.json"

    path = upload_to_adls(
        content=json.dumps(output, ensure_ascii=False, indent=2),
        layer="landing",
        folder="meta_ads",
        filename=filename
    )

    return path


def main():
    logger.info("Iniciando extracción de Meta Ad Library")
    ads = fetch_ads(search_terms="nude project", country="ES")
    path = save_to_landing(ads)
    logger.info(f"Extracción completada. Fichero en: {path}")


if __name__ == "__main__":
    main()