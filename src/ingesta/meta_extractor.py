import json
import logging
from datetime import datetime, timezone

import requests

from config.adls_client import upload_to_adls
from config.settings import get_secret
from procesamiento.model import DatasetsLanding, Layers


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)



BASE_URL = (
    "https://graph.facebook.com/v25.0/ads_archive"
)

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

REQUEST_TIMEOUT = 30
PAGE_SIZE = 100


def fetch_ads(search_terms: str,country: str = "ES") -> list:
    """
    Extrae todos los anuncios activos de una marca
    de la Meta Ad Library.
    """

    token = get_secret(key="META_ACCESS_TOKEN")
    if not token:
        raise ValueError("META_ACCESS_TOKEN no configurado")

    params = {
        "search_terms": search_terms,
        "ad_reached_countries": country,
        "ad_active_status": "active",
        "ad_type": "ALL",
        "fields": FIELDS,
        "access_token": token,
        "limit": PAGE_SIZE
    }

    all_ads = []
    page = 1

    with requests.Session() as session:
        while True:

            logger.info("Extrayendo página %s...",page)

            response = session.get(
                BASE_URL,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            try:
                response.raise_for_status()
            except requests.HTTPError:
                logger.error("Error en Meta Ad Library API: %s",response.text)
                raise

            data = response.json()
            ads = data.get("data", [])
            all_ads.extend(ads)

            logger.info("Página %s: %s anuncios extraídos",page,len(ads))

            cursor_after = (
                data
                .get("paging", {})
                .get("cursors", {})
                .get("after")
            )

            if not cursor_after:
                break

            params["after"] = cursor_after
            page += 1

    logger.info("Total anuncios extraídos: %s",len(all_ads))
    return all_ads


def save_to_landing(ads: list) -> str:
    """
    Guarda los anuncios extraídos en la capa
    landing del Data Lake.
    """

    now = datetime.now(timezone.utc)
    output = {
        "extraction_timestamp": now.isoformat(),
        "extraction_date": now.date().isoformat(),
        "total_ads": len(ads),
        "ads": ads
    }

    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = (f"meta_ads_{timestamp}.json")

    path = upload_to_adls(
        content=json.dumps(
            output,
            ensure_ascii=False,
            indent=2
        ),
        layer=Layers.LANDING.value,
        folder=DatasetsLanding.META.value,
        filename=filename
    )

    logger.info("Datos guardados en landing: %s",path)

    return path


def main():

    logger.info(
        "Iniciando extracción de Meta Ad Library"
    )

    ads = fetch_ads(
        search_terms="nude project",
        country="ES"
    )

    path = save_to_landing(ads)
    logger.info("Extracción completada. ""Fichero en: %s",path)


if __name__ == "__main__":
    main()
