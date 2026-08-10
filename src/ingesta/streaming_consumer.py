import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from azure.eventhub import EventHubConsumerClient, TransportType
from config.adls_client import upload_to_adls

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

buffer = []
BUFFER_SIZE = 10  # Escribe en el Data Lake cada 10 eventos


def on_event(partition_context, event):
    """Callback que se ejecuta cada vez que llega un evento de Event Hubs."""
    global buffer

    data = json.loads(event.body_as_str())
    buffer.append(data)
    logger.info(f"Evento recibido: {data['product_name']} - {data['total_amount']}€")

    if len(buffer) >= BUFFER_SIZE:
        flush_buffer()
        partition_context.update_checkpoint(event)


def flush_buffer():
    """Escribe el buffer acumulado en la capa landing del Data Lake."""
    global buffer

    if not buffer:
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S%f")
    filename = f"ventas_{timestamp}.json"

    content = json.dumps({
        "batch_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_events": len(buffer),
        "events": buffer,
        "batch_date": datetime.now(timezone.utc).date().isoformat(),
    }, ensure_ascii=False, indent=2)

    upload_to_adls(
        content=content,
        layer="landing",
        folder="ventas",
        filename=filename
    )

    logger.info(f"Buffer de {len(buffer)} eventos escrito en landing/ventas/")
    buffer = []


def start_consumer():
    """Inicia el consumidor de Event Hubs."""
    connection_string = os.getenv("EVENT_HUBS_CONNECTION_STRING")
    eventhub_name = os.getenv("EVENT_HUBS_NAME")

    if not connection_string or not eventhub_name:
        raise ValueError("EVENT_HUBS_CONNECTION_STRING o EVENT_HUBS_NAME no encontrados en .env")

    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_string,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        uamqp_transport=False,  # Fuerza el transporte Python puro
        transport_type=TransportType.AmqpOverWebsocket
)

    logger.info("Consumer iniciado, esperando eventos...")

    try:
        with client:
            client.receive(
                on_event=on_event,
                starting_position="-1"
            )
    except KeyboardInterrupt:
        flush_buffer()
        logger.info("Consumer detenido manualmente.")


if __name__ == "__main__":
    start_consumer()