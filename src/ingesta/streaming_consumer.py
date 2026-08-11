
import json
import logging
import re
import time
from datetime import datetime, timezone

from config.adls_client import upload_to_adls
from config.settings import get_secret, is_local

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# CREDENCIALES
# ─────────────────────────────────────────

def get_eventhubs_credentials() -> tuple:
    """Obtiene las credenciales de Azure Event Hubs."""
    connection_string = get_secret(key="EVENT_HUBS_CONNECTION_STRING")
    eventhub_name = get_secret(key="EVENT_HUBS_NAME")
    return connection_string, eventhub_name


# ─────────────────────────────────────────
# PARSEAR CONNECTION STRING
# ─────────────────────────────────────────

def parse_eventhubs_connection_string(connection_string: str) -> tuple:
    """Extrae el namespace y el Event Hub de la connection string."""

    endpoint_match = re.search(
        r"Endpoint=sb://([^/]+)",
        connection_string
    )

    if not endpoint_match:
        raise ValueError(
            "No se pudo obtener el namespace de la connection string."
        )

    namespace = endpoint_match.group(1)

    entity_path_match = re.search(
        r"(?:^|;)EntityPath=([^;]+)",
        connection_string
    )

    eventhub_name = (
        entity_path_match.group(1)
        if entity_path_match
        else None
    )

    return namespace, eventhub_name


# ─────────────────────────────────────────
# ESCRITURA DE EVENTOS
# ─────────────────────────────────────────

def save_event(event: dict):
    """Guarda un evento individual en la capa Landing."""

    now = datetime.now(timezone.utc)

    event_id = event.get("event_id", "unknown")

    filename = (
        f"venta_{now.strftime('%Y%m%d_%H%M%S_%f')}"
        f"_{event_id}.json"
    )

    upload_to_adls(
        content=json.dumps(
            event,
            ensure_ascii=False,
            indent=2
        ),
        layer="landing",
        folder="ventas",
        filename=filename,
        target_date=now
    )

    logger.info(
        "Evento %s escrito en landing/ventas/%s",
        event_id,
        filename
    )


# ─────────────────────────────────────────
# MODO LOCAL — EVENT HUBS SDK
# ─────────────────────────────────────────

def on_event(partition_context, event):
    """Procesa y almacena un evento recibido desde Event Hubs."""

    try:
        data = json.loads(event.body_as_str())

        logger.info(
            "Evento recibido: %s - %s€",
            data.get("product_name"),
            data.get("total_amount")
        )

        save_event(data)

        partition_context.update_checkpoint(event)

    except Exception:
        logger.exception("Error procesando evento de Event Hubs.")
        raise


def start_consumer_local(
    connection_string: str,
    eventhub_name: str
):
    """Inicia el consumidor local usando EventHubConsumerClient."""

    from azure.eventhub import (
        EventHubConsumerClient,
        TransportType
    )

    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_string,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        uamqp_transport=False,
        transport_type=TransportType.AmqpOverWebsocket
    )

    logger.info(
        "Consumer local iniciado. Esperando eventos..."
    )

    try:
        with client:
            client.receive(
                on_event=on_event,
                starting_position="-1"
            )

    except KeyboardInterrupt:
        logger.info("Consumer detenido manualmente.")


# ─────────────────────────────────────────
# CONFIGURACIÓN KAFKA → EVENT HUBS
# ─────────────────────────────────────────

def get_eventhubs_kafka_config(
    connection_string: str,
    eventhub_name: str
) -> dict:
    """Genera la configuración Kafka para Azure Event Hubs."""

    namespace, entity_path = parse_eventhubs_connection_string(
        connection_string
    )

    kafka_topic = entity_path or eventhub_name

    if not kafka_topic:
        raise ValueError(
            "No se ha podido determinar el nombre del Event Hub."
        )

    logger.info(
        "Configurando conexión Kafka con Event Hub '%s'.",
        kafka_topic
    )

    return {
        "kafka.bootstrap.servers": f"{namespace}:9093",
        "subscribe": kafka_topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain."
            "PlainLoginModule required "
            'username="$ConnectionString" '
            f'password="{connection_string}";'
        ),
        "startingOffsets": "earliest",
        "failOnDataLoss": "false"
    }


# ─────────────────────────────────────────
# MODO PROD — DATABRICKS STRUCTURED STREAMING
# ─────────────────────────────────────────

def start_consumer_spark(
    connection_string: str,
    eventhub_name: str,
    base_path: str
):
    """
    Inicia el consumidor en Databricks usando Structured Streaming.

    Cada evento recibido se almacena como un JSON independiente
    en la capa Landing.
    """

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    spark = SparkSession.builder.getOrCreate()

    kafka_options = get_eventhubs_kafka_config(
        connection_string=connection_string,
        eventhub_name=eventhub_name
    )

    logger.info(
        "Iniciando lectura de Event Hubs mediante Kafka..."
    )

    df_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    df_events = (
        df_stream
        .select(
            col("value").cast("string").alias("value")
        )
    )

    output_path = f"{base_path}/landing/ventas/"

    checkpoint_path = (
        f"{base_path}/checkpoints/ventas_streaming"
    )

    logger.info(
        "Escribiendo eventos en: %s",
        output_path
    )

    query = (
        df_events
        .writeStream
        .format("json")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )

    logger.info(
        "Consumer Spark iniciado correctamente."
    )

    while query.isActive:
        progress = query.lastProgress

        if progress:
            logger.info(
                "Eventos procesados en último batch: %s",
                progress.get("numInputRows", 0)
            )

        time.sleep(10)

    query.awaitTermination()


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main():

    connection_string, eventhub_name = (
        get_eventhubs_credentials()
    )

    if is_local():

        start_consumer_local(
            connection_string=connection_string,
            eventhub_name=eventhub_name
        )

        return

    storage_account = get_secret(
        key="ADLS_ACCOUNT_NAME"
    )

    container = get_secret(
        key="ADLS_CONTAINER_NAME"
    )

    account_key = get_secret(
        key="ADLS_ACCOUNT_KEY"
    )

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    spark.conf.set(
        f"fs.azure.account.key."
        f"{storage_account}.dfs.core.windows.net",
        account_key
    )

    base_path = (
        f"abfss://{container}"
        f"@{storage_account}.dfs.core.windows.net"
    )

    start_consumer_spark(
        connection_string=connection_string,
        eventhub_name=eventhub_name,
        base_path=base_path
    )


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────

if __name__ == "__main__":
    main()

