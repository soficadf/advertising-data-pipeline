
import json
import logging
import os
import re
from datetime import datetime, timezone

from config.adls_client import upload_to_adls
from config.settings import get_secret
from schemas.ventas import get_ventas_schema


# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────

BUFFER_SIZE = 10


# ─────────────────────────────────────────
# CREDENCIALES
# ─────────────────────────────────────────

def get_eventhubs_credentials() -> tuple[str, str]:
    """
    Obtiene las credenciales de Azure Event Hubs.

    Local:
        EVENT_HUBS_CONNECTION_STRING y EVENT_HUBS_NAME
        desde .env.

    Databricks:
        event_hubs_connection_string y event_hubs_name
        desde Databricks Secrets.
    """

    connection_string = get_secret(
        key="EVENT_HUBS_CONNECTION_STRING"
    )

    eventhub_name = get_secret(
        key="EVENT_HUBS_NAME"
    )

    return connection_string, eventhub_name


# ─────────────────────────────────────────
# PARSEAR CONNECTION STRING
# ─────────────────────────────────────────

def parse_eventhubs_connection_string(
    connection_string: str
) -> tuple[str, str | None]:
    """
    Extrae el namespace y el Event Hub de la
    connection string de Azure Event Hubs.

    Ejemplo:

        Endpoint=sb://namespace.servicebus.windows.net/;
        SharedAccessKeyName=...;
        SharedAccessKey=...;
        EntityPath=sales-events
    """

    endpoint_match = re.search(
        r"Endpoint=sb://([^/]+)",
        connection_string
    )

    if not endpoint_match:
        raise ValueError(
            "No se pudo obtener el namespace "
            "de la connection string de Event Hubs."
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
# MODO LOCAL — EVENT HUBS SDK
# ─────────────────────────────────────────

def on_event(partition_context, event):
    """
    Callback ejecutado cada vez que llega un evento
    de Event Hubs en modo local.
    """

    data = json.loads(
        event.body_as_str()
    )

    buffer.append(data)

    logger.info(
        "Evento recibido: %s - %s€",
        data.get("product_name"),
        data.get("total_amount")
    )

    if len(buffer) >= BUFFER_SIZE:

        flush_buffer()

        partition_context.update_checkpoint(
            event
        )


def flush_buffer():
    """
    Escribe el buffer acumulado en la capa
    landing del Data Lake.
    """

    global buffer

    if not buffer:
        return

    now = datetime.now(timezone.utc)

    filename = (
        f"ventas_{now.strftime('%Y%m%d_%H%M%S%f')}.json"
    )

    content = json.dumps(
        {
            "batch_date": now.date().isoformat(),
            "batch_timestamp": now.isoformat(),
            "total_events": len(buffer),
            "events": buffer
        },
        ensure_ascii=False,
        indent=2
    )

    upload_to_adls(
        content=content,
        layer="landing",
        folder="ventas",
        filename=filename
    )

    logger.info(
        "Buffer de %s eventos escrito en landing/ventas/",
        len(buffer)
    )

    buffer = []


def start_consumer_local(
    connection_string: str,
    eventhub_name: str
):
    """
    Inicia el consumidor en modo local utilizando
    EventHubConsumerClient.

    Este consumidor se utiliza únicamente para
    desarrollo y pruebas locales.
    """

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
        "Consumer local iniciado. "
        "Esperando eventos..."
    )

    try:

        with client:

            client.receive(
                on_event=on_event,
                starting_position="-1"
            )

    except KeyboardInterrupt:

        flush_buffer()

        logger.info(
            "Consumer detenido manualmente."
        )


# ─────────────────────────────────────────
# KAFKA → EVENT HUBS
# ─────────────────────────────────────────

def get_eventhubs_kafka_config(
    connection_string: str,
    eventhub_name: str
) -> dict:
    """
    Genera la configuración Kafka necesaria
    para conectarse a Azure Event Hubs.

    Event Hubs expone un endpoint compatible
    con Kafka en el puerto 9093.
    """

    namespace, entity_path = (
        parse_eventhubs_connection_string(
            connection_string
        )
    )

    kafka_topic = (
        entity_path
        or eventhub_name
    )

    if not kafka_topic:
        raise ValueError(
            "No se ha podido determinar el nombre "
            "del Event Hub."
        )

    logger.info(
        "Configurando conexión Kafka con Event Hub '%s'.",
        kafka_topic
    )

    return {
        "kafka.bootstrap.servers": (
            f"{namespace}:9093"
        ),

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
# DATABRICKS STRUCTURED STREAMING
# ─────────────────────────────────────────

def start_consumer_spark(
    connection_string: str,
    eventhub_name: str,
    base_path: str
):
    """
    Inicia el consumidor en Databricks utilizando
    Structured Streaming y el endpoint Kafka de
    Azure Event Hubs.
    """

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        current_date,
        current_timestamp,
        from_json
    )

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    kafka_options = get_eventhubs_kafka_config(
        connection_string=connection_string,
        eventhub_name=eventhub_name
    )

    logger.info(
        "Iniciando lectura de Event Hubs "
        "mediante Kafka..."
    )

    # ─────────────────────────────────────
    # LECTURA STREAMING
    # ─────────────────────────────────────

    df_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    # ─────────────────────────────────────
    # PARSEO DE EVENTOS
    # ─────────────────────────────────────

    schema = get_ventas_schema()

    df_parsed = (
        df_stream
        .select(
            from_json(
                col("value").cast("string"),
                schema
            ).alias("data"),

            col("timestamp").alias(
                "enqueued_time"
            )
        )
        .select(
            "data.*",
            "enqueued_time",
            current_date().alias(
                "batch_date"
            ),
            current_timestamp().alias(
                "batch_timestamp"
            )
        )
    )

    # ─────────────────────────────────────
    # ESCRITURA EN LANDING
    # ─────────────────────────────────────

    output_path = (
        f"{base_path}/landing/ventas/"
    )

    checkpoint_path = (
        f"{base_path}/checkpoints/"
        f"ventas_streaming"
    )

    logger.info(
        "Escribiendo eventos en: %s",
        output_path
    )

    query = (
        df_parsed
        .writeStream
        .format("json")
        .option(
            "path",
            output_path
        )
        .option(
            "checkpointLocation",
            checkpoint_path
        )
        .trigger(
            processingTime="1 minute"
        )
        .start()
    )

    logger.info(
        "Consumer Spark iniciado correctamente."
    )

    query.awaitTermination()


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────

def main():

    connection_string, eventhub_name = (
        get_eventhubs_credentials()
    )

    # ─────────────────────────────────────
    # LOCAL
    # ─────────────────────────────────────

    if os.getenv("ENV", "prod") == "local":

        start_consumer_local(
            connection_string=connection_string,
            eventhub_name=eventhub_name
        )

        return

    # ─────────────────────────────────────
    # DATABRICKS
    # ─────────────────────────────────────

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

    spark = (
        SparkSession
        .builder
        .getOrCreate()
    )

    # Configuración de acceso a ADLS Gen2
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
