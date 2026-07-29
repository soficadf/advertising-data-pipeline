import requests
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from config.adls_client import upload_to_adls

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

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


def fetch_ads(search_terms: str, country: str = "ES") -> list:
    """Extrae todos los anuncios de una marca de la Meta Ad Library."""
    
    token = os.getenv("META_ACCESS_TOKEN")
    if not token:
        raise ValueError("META_ACCESS_TOKEN no encontrado en .env")

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
    import json
    
    output = {
        "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
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


if __name__ == "__main__":
    ads = fetch_ads(search_terms="nude project", country="ES")
    path = save_to_landing(ads)
    logger.info(f"Extracción completada. Fichero en: {path}")