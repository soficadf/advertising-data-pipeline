import json
import os
import logging
from datetime import datetime, timezone
from azure.eventhub import EventHubConsumerClient, TransportType
from config.adls_client import upload_to_adls

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

buffer = []
BUFFER_SIZE = 10


def get_credentials() -> tuple:
    """Lee las credenciales del entorno disponible."""
    if os.getenv("ENV", "prod") == "local":
        from dotenv import load_dotenv
        load_dotenv()
        connection_string = os.getenv("EVENT_HUBS_CONNECTION_STRING")
        eventhub_name = os.getenv("EVENT_HUBS_NAME")
        logger.info("Credenciales leídas desde .env")
    else:
        connection_string = dbutils.secrets.get(scope="ad-pipeline", key="event_hubs_connection_string")
        eventhub_name = dbutils.secrets.get(scope="ad-pipeline", key="event_hubs_name")
        logger.info("Credenciales leídas desde Databricks Secrets")

    if not connection_string or not eventhub_name:
        raise ValueError("Credenciales de Event Hubs no encontradas")

    return connection_string, eventhub_name


# ─────────────────────────────────────────
# MODO LOCAL — EventHubConsumerClient
# ─────────────────────────────────────────

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
        "batch_date": datetime.now(timezone.utc).date().isoformat(),
        "batch_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_events": len(buffer),
        "events": buffer
    }, ensure_ascii=False, indent=2)

    upload_to_adls(
        content=content,
        layer="landing",
        folder="ventas",
        filename=filename
    )

    logger.info(f"Buffer de {len(buffer)} eventos escrito en landing/ventas/")
    buffer = []


def start_consumer_local(connection_string: str, eventhub_name: str):
    """
    Inicia el consumidor en modo local usando EventHubConsumerClient.
    Útil para desarrollo y pruebas desde la máquina local.
    """
    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_string,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
        uamqp_transport=False,
        transport_type=TransportType.AmqpOverWebsocket
    )

    logger.info("Consumer local iniciado, esperando eventos...")

    try:
        with client:
            client.receive(
                on_event=on_event,
                starting_position="-1"
            )
    except KeyboardInterrupt:
        flush_buffer()
        logger.info("Consumer detenido manualmente.")


# ─────────────────────────────────────────
# MODO PROD — Databricks Structured Streaming
# ─────────────────────────────────────────

def get_ventas_schema():
    """Schema de los eventos de ventas."""
    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
    return StructType([
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("quantity", LongType()),
        StructField("unit_price", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("region", StringType()),
        StructField("channel", StringType())
    ])


def start_consumer_spark(connection_string: str, eventhub_name: str, base_path: str):
    """
    Inicia el consumidor en modo producción usando Databricks Structured Streaming.
    Lee de Event Hubs y escribe directamente en landing/ventas/ en el Data Lake.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col, lit, current_date, current_timestamp

    spark = SparkSession.builder.getOrCreate()

    # Configura la conexión a Event Hubs
    eh_conf = {
        "eventhubs.connectionString": spark._jvm.org.apache.spark.eventhubs \
            .EventHubsUtils.encrypt(connection_string),
        "eventhubs.consumerGroup": "$Default"
    }

    # Lee el stream de Event Hubs
    df_stream = spark.readStream \
        .format("eventhubs") \
        .options(**eh_conf) \
        .load()

    # Parsea el body del evento
    schema = get_ventas_schema()
    df_parsed = df_stream.select(
        from_json(col("body").cast("string"), schema).alias("data"),
        col("enqueuedTime").alias("enqueued_time")
    ).select(
        "data.*",
        current_date().alias("batch_date"),
        current_timestamp().alias("batch_timestamp")
    )

    # Escribe en landing/ventas/ particionado por fecha
    query = df_parsed.writeStream \
        .format("json") \
        .option("path", f"{base_path}/landing/ventas/") \
        .option("checkpointLocation", f"{base_path}/checkpoints/ventas_streaming") \
        .trigger(processingTime="1 minute") \
        .start()

    logger.info("Consumer Spark iniciado, consumiendo eventos de Event Hubs...")
    query.awaitTermination()


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main():
    connection_string, eventhub_name = get_credentials()

    if os.getenv("ENV", "prod") == "local":
        start_consumer_local(connection_string, eventhub_name)
    else:
        storage_account = dbutils.secrets.get(scope="ad-pipeline", key="adls_account_name")
        container = dbutils.secrets.get(scope="ad-pipeline", key="adls_container_name")
        account_key = dbutils.secrets.get(scope="ad-pipeline", key="adls_account_key")

        spark = SparkSession.builder.getOrCreate()
        spark.conf.set(
            f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
            account_key
        )

        base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
        start_consumer_spark(connection_string, eventhub_name, base_path)


if __name__ == "__main__":
    main()