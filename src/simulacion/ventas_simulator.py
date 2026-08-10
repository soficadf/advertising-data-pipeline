import json
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from azure.eventhub import EventHubProducerClient, EventData, TransportType
from simulacion.ventas_utils import (
    get_product_weights, get_daily_spend,
    ventas_por_hora, generate_sale_event
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def simulate_sales(duration_minutes: int = None):
    """Emite eventos de ventas a Azure Event Hubs ajustando el ritmo según el gasto."""
    connection_string = os.getenv("EVENT_HUBS_CONNECTION_STRING")
    eventhub_name = os.getenv("EVENT_HUBS_NAME")

    if not connection_string or not eventhub_name:
        raise ValueError("EVENT_HUBS_CONNECTION_STRING o EVENT_HUBS_NAME no encontrados en .env")

    products, weights = get_product_weights()
    gasto_diario = get_daily_spend(datetime.now(timezone.utc).date())
    eventos_por_hora = ventas_por_hora(gasto_diario)
    interval = 3600 / eventos_por_hora

    logger.info(f"Gasto publicitario hoy: {gasto_diario}€")
    logger.info(f"Ritmo: {eventos_por_hora} eventos/hora ({interval:.1f}s entre eventos)")

    total_events = 0
    start_time = datetime.now(timezone.utc)

    try:
        while True:
            if duration_minutes:
                elapsed = (datetime.now(timezone.utc) - start_time).seconds / 60
                if elapsed >= duration_minutes:
                    logger.info(f"Duración completada. Total eventos: {total_events}")
                    break

            with EventHubProducerClient.from_connection_string(
                conn_str=connection_string,
                eventhub_name=eventhub_name,
                uamqp_transport=False,
                transport_type=TransportType.AmqpOverWebsocket
            ) as client:
                event = generate_sale_event(products, weights)
                batch = client.create_batch()
                batch.add(EventData(json.dumps(event)))
                client.send_batch(batch)

            total_events += 1
            logger.info(f"Evento #{total_events}: {event['product_name']} - {event['total_amount']}€")
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info(f"Simulador detenido. Total eventos: {total_events}")


if __name__ == "__main__":
    simulate_sales(duration_minutes=5)