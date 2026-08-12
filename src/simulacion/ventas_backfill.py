import json
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from procesamiento.model import DatasetsLanding, Layers
from simulacion.ventas_utils import (
    get_product_weights, get_daily_spend,
    ventas_por_dia, generate_sale_event
)
from config.adls_client import upload_to_adls

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_backfill(days: int = 30):
    """Genera eventos históricos de ventas para los últimos N días."""
    products, weights = get_product_weights()
    today = datetime.now(timezone.utc).date()
    total_events = 0

    for i in range(days, 0, -1):
        target_date = today - timedelta(days=i)
        gasto = get_daily_spend(target_date)
        num_ventas = ventas_por_dia(gasto)

        logger.info(f"{target_date} — Gasto: {gasto:.2f}€ → {num_ventas} ventas")

        events = sorted([
            generate_sale_event(products, weights, target_date)
            for _ in range(num_ventas)
        ], key=lambda e: e["timestamp"])

        filename = f"ventas_backfill_{target_date.strftime('%Y%m%d')}.json"
        content = json.dumps({
            "batch_date": target_date.isoformat(),
            "total_events": len(events),
            "events": events
        }, ensure_ascii=False, indent=2)

        upload_to_adls(
            content=content,
            layer={Layers.LANDING.value},
            folder={DatasetsLanding.VENTAS.value},
            filename=filename,
            target_date=target_date
        )

        total_events += num_ventas

    logger.info(f"Backfill completado. Total eventos: {total_events}")


if __name__ == "__main__":
    run_backfill(days=30)