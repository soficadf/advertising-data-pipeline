import json
import logging
import os
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

BUFFER_SIZE = 10
buffer = []


def get_eventhubs_credentials() -> tuple:
    """Obtiene las credenciales de Azure Event Hubs."""
    connection_string = get_secret(key="EVENT_HUBS_CONNECTION_STRING")
    eventhub_name = get_secret(key="EVENT_HUBS_NAME")
    return connection_string, eventhub_name


def parse_eventhubs_connection_string(connection_string: str) -> tuple:
    """Extrae el namespace y el Event Hub de la connection string."""
    endpoint_match = re.search(r"Endpoint=sb://([^/]+)", connection_string)
    if not endpoint_match:
        raise ValueError("No se pudo obtener el namespace de la connection string.")

    namespace = endpoint_match.group(1)
    entity_path_match = re.search(r"(?:^|;)EntityPath=([^;]+)", connection_string)
    eventhub_name = entity_path_match.group(1) if entity_path_match else None

    return namespace, eventhub_name


# ─────────────────────────────────────────
# MODO LOCAL — EVENT HUBS SDK
# ─────────────────────────────────────────

def on_event(partition_context, event):
    """Callback ejecutado cada vez que llega un evento en modo local."""
    global buffer

    data = json.loads(event.body_as_str())
    buffer.append(data)
    logger.info("Evento recibido: %s - %s€", data.get("product_name"), data.get("total_amount"))

    if len(buffer) >= BUFFER_SIZE:
        flush_buffer()
        partition_context.update_checkpoint(event)


def flush_buffer():
    """Escribe el buffer acumulado en la capa landing del Data Lake."""
    global buffer

    if not buffer:
        return

    now = datetime.now(timezone.utc)
    filename = f"ventas_{now.strftime('%Y%m%d_%H%M%S%f')}.json"

    content = json.dumps({
        "batch_date": now.date().isoformat(),
        "batch_timestamp": now.isoformat(),
        "total_events": len(buffer),
        "events": buffer
    }, ensure_ascii=False, indent=2)

    upload_to_adls(content=content, layer="landing", folder="ventas", filename=filename)
    logger.info("Buffer de %s eventos escrito en landing/ventas/", len(buffer))
    buffer = []


def start_consumer_local(connection_string: str, eventhub_name: str):
    """Inicia el consumidor en modo local usando EventHubConsumerClient."""
    from azure.eventhub import EventHubConsumerClient, TransportType

    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_string,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        uamqp_transport=False,
        transport_type=TransportType.AmqpOverWebsocket
    )

    logger.info("Consumer local iniciado. Esperando eventos...")

    try:
        with client:
            client.receive(on_event=on_event, starting_position="-1")
    except KeyboardInterrupt:
        flush_buffer()
        logger.info("Consumer detenido manualmente.")


# ─────────────────────────────────────────
# MODO PROD — DATABRICKS STRUCTURED STREAMING
# ─────────────────────────────────────────

def get_eventhubs_kafka_config(connection_string: str, eventhub_name: str) -> dict:
    """Genera la configuración Kafka para conectarse a Azure Event Hubs."""
    namespace, entity_path = parse_eventhubs_connection_string(connection_string)
    kafka_topic = entity_path or eventhub_name

    if not kafka_topic:
        raise ValueError("No se ha podido determinar el nombre del Event Hub.")

    logger.info("Configurando conexión Kafka con Event Hub '%s'.", kafka_topic)

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


def write_batch(df_batch, batch_id):
    """
    Escribe cada micro-batch en formato compatible con landing_bronze.
    Mismo formato que el consumer local: batch_date, batch_timestamp,
    total_events y events como array.
    """
    if df_batch.isEmpty():
        logger.info("Batch %s vacío, nada que escribir.", batch_id)
        return

    events = [row.asDict() for row in df_batch.collect()]
    now = datetime.now(timezone.utc)
    filename = f"ventas_{now.strftime('%Y%m%d_%H%M%S%f')}.json"

    content = json.dumps({
        "batch_date": now.date().isoformat(),
        "batch_timestamp": now.isoformat(),
        "total_events": len(events),
        "events": events
    }, ensure_ascii=False, indent=2)

    upload_to_adls(content=content, layer="landing", folder="ventas", filename=filename)
    logger.info("Batch %s: %s eventos escritos en landing/ventas/", batch_id, len(events))


def start_consumer_spark(connection_string: str, eventhub_name: str, base_path: str):
    """
    Inicia el consumidor en Databricks usando Structured Streaming
    y el endpoint Kafka de Azure Event Hubs.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json
    from schemas.ventas import get_ventas_schema

    spark = SparkSession.builder.getOrCreate()

    kafka_options = get_eventhubs_kafka_config(
        connection_string=connection_string,
        eventhub_name=eventhub_name
    )

    logger.info("Iniciando lectura de Event Hubs mediante Kafka...")

    df_stream = spark.readStream.format("kafka").options(**kafka_options).load()

    schema = get_ventas_schema()

    df_parsed = df_stream.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("enqueued_time")
    ).select(
        "data.*",
        "enqueued_time"
    )

    checkpoint_path = f"{base_path}/checkpoints/ventas_streaming"

    query = df_parsed.writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Consumer Spark iniciado correctamente.")

    # Monitoriza el stream en tiempo real
    while query.isActive:
        progress = query.lastProgress
        if progress:
            logger.info("Eventos procesados en último batch: %s",
                        progress.get("numInputRows", 0))
        time.sleep(10)

    query.awaitTermination()


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main():
    connection_string, eventhub_name = get_eventhubs_credentials()
   
    if is_local():
        start_consumer_local(
            connection_string=connection_string,
            eventhub_name=eventhub_name
        )
        return

    storage_account = get_secret(key="ADLS_ACCOUNT_NAME")
    container = get_secret(key="ADLS_CONTAINER_NAME")
    account_key = get_secret(key="ADLS_ACCOUNT_KEY")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        account_key
    )

    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

    start_consumer_spark(
        connection_string=connection_string,
        eventhub_name=eventhub_name,
        base_path=base_path
    )


if __name__ == "__main__":
    main()