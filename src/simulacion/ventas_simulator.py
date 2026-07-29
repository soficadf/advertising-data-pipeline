import json
import random
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from azure.eventhub import EventHubProducerClient, EventData

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Productos de Nude Project con precio medio estimado
PRODUCTS = [
    {"id": "NP001", "name": "Hoodie Basic", "price": 79.90},
    {"id": "NP002", "name": "Camiseta Oversize", "price": 39.90},
    {"id": "NP003", "name": "Jogger", "price": 69.90},
    {"id": "NP004", "name": "Crop Top", "price": 34.90},
    {"id": "NP005", "name": "Chaqueta", "price": 119.90},
]

REGIONS = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao"]


def generate_sale_event() -> dict:
    """Genera un evento de venta sintético."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)
    
    return {
        "event_id": f"sale_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(product["price"] * quantity, 2),
        "region": random.choice(REGIONS),
    }


def simulate_sales(events_per_minute: int = 10, duration_minutes: int = None):
    """
    Emite eventos de ventas a Azure Event Hubs.
    
    Args:
        events_per_minute: Número de eventos por minuto
        duration_minutes: Duración en minutos. None para correr indefinidamente.
    """
    connection_string = os.getenv("EVENT_HUBS_CONNECTION_STRING")
    eventhub_name = os.getenv("EVENT_HUBS_NAME")

    if not connection_string or not eventhub_name:
        raise ValueError("EVENT_HUBS_CONNECTION_STRING o EVENT_HUBS_NAME no encontrados en .env")

    client = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=eventhub_name
    )

    interval = 60 / events_per_minute
    total_events = 0
    start_time = datetime.now(timezone.utc)

    logger.info(f"Simulador iniciado: {events_per_minute} eventos/minuto")

    try:
        while True:
            if duration_minutes:
                elapsed = (datetime.now(timezone.utc) - start_time).seconds / 60
                if elapsed >= duration_minutes:
                    logger.info(f"Duración completada. Total eventos emitidos: {total_events}")
                    break


            event = generate_sale_event()
            
            with client:
                batch = client.create_batch()
                batch.add(EventData(json.dumps(event)))
                client.send_batch(batch)

            total_events += 1
            logger.info(f"Evento emitido #{total_events}: {event['product_name']} - {event['total_amount']}€")
            
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info(f"Simulador detenido manualmente. Total eventos emitidos: {total_events}")


if __name__ == "__main__":
    simulate_sales(events_per_minute=10, duration_minutes=5)