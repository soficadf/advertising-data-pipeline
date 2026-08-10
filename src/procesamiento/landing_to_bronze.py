import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode, col, to_date, to_timestamp, coalesce, regexp_extract, from_json
from pyspark.sql.types import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_credentials(spark: SparkSession) -> tuple:
    """Lee las credenciales del entorno disponible."""
    if os.getenv("ENV", "prod") == "local":
        from dotenv import load_dotenv
        load_dotenv()
        storage_account = os.getenv("ADLS_ACCOUNT_NAME")
        account_key = os.getenv("ADLS_ACCOUNT_KEY")
        container = os.getenv("ADLS_CONTAINER_NAME")
        logger.info("Credenciales leídas desde .env")
    else:
        storage_account = dbutils.secrets.get(scope="ad-pipeline", key="adls_account_name")
        account_key = dbutils.secrets.get(scope="ad-pipeline", key="adls_account_key")
        container = dbutils.secrets.get(scope="ad-pipeline", key="adls_container_name")
        logger.info("Credenciales leídas desde Databricks Secrets")

    return storage_account, account_key, container


def get_meta_schema() -> StructType:
    return StructType([
        StructField("extraction_date", StringType()),
        StructField("extraction_timestamp", StringType()),
        StructField("total_ads", LongType()),
        StructField("ads", ArrayType(StructType([
            StructField("id", StringType()),
            StructField("eu_total_reach", LongType()),
            StructField("ad_delivery_start_time", StringType()),
            StructField("target_gender", StringType()),
            StructField("target_ages", ArrayType(StringType())),
            StructField("publisher_platforms", ArrayType(StringType())),
            StructField("age_country_gender_reach_breakdown", ArrayType(StructType([
                StructField("country", StringType()),
                StructField("age_gender_breakdowns", ArrayType(StructType([
                    StructField("age_range", StringType()),
                    StructField("female", LongType()),
                    StructField("male", LongType()),
                    StructField("unknown", LongType())
                ])))
            ])))
        ])))
    ])


def get_ventas_schema() -> StructType:
    return StructType([
        StructField("batch_date", StringType()),
        StructField("batch_timestamp", StringType()),
        StructField("total_events", LongType()),
        StructField("events", ArrayType(StructType([
            StructField("event_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("quantity", LongType()),
            StructField("unit_price", DoubleType()),
            StructField("total_amount", DoubleType()),
            StructField("region", StringType()),
            StructField("channel", StringType())
        ])))
    ])


def read_from_landing(spark: SparkSession, base_path: str, source: str, schema: StructType):
    """Lee ficheros desde landing usando Auto Loader."""
    return spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "text") \
        .option("wholetext", "true") \
        .load(f"{base_path}/landing/{source}/") \
        .withColumn("file_path", col("_metadata.file_path")) \
        .withColumn("parsed", from_json(col("value"), schema)) \
        .select("file_path", "parsed.*")


def transform_meta(df_raw) -> DataFrame:
    """Transforma datos raw de Meta a Bronze."""
    return df_raw.select(
        coalesce(
            to_date(col("extraction_date")),
            to_date(regexp_extract(col("file_path"), r"(\d{4}/\d{2}/\d{2})", 1), "yyyy/MM/dd")
        ).alias("fecha"),
        explode(col("ads")).alias("ad")
    ).select(
        col("fecha"),
        col("ad.id").alias("ad_id"),
        col("ad.eu_total_reach").alias("eu_total_reach"),
        to_date(col("ad.ad_delivery_start_time")).alias("ad_delivery_start_time"),
        col("ad.target_gender").alias("target_gender"),
        col("ad.target_ages").alias("target_ages"),
        col("ad.publisher_platforms").alias("publisher_platforms"),
        col("ad.age_country_gender_reach_breakdown").alias("age_country_gender_reach_breakdown")
    )


def transform_ventas(df_raw) -> DataFrame:
    """Transforma datos raw de ventas a Bronze."""
    return df_raw.select(
        to_date(col("batch_date")).alias("fecha"),
        explode(col("events")).alias("event")
    ).select(
        col("fecha"),
        col("event.event_id").alias("event_id"),
        to_timestamp(col("event.timestamp")).alias("timestamp"),
        col("event.product_id").alias("product_id"),
        col("event.product_name").alias("product_name"),
        col("event.quantity").alias("quantity"),
        col("event.unit_price").alias("unit_price"),
        col("event.total_amount").alias("total_amount"),
        col("event.region").alias("region"),
        col("event.channel").alias("channel")
    )


def write_to_bronze(df_bronze, base_path: str, source: str):
    """Escribe el dataframe en Bronze en formato Delta."""
    query = df_bronze.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{base_path}/checkpoints/{source}_bronze") \
        .partitionBy("fecha") \
        .trigger(availableNow=True) \
        .start(f"{base_path}/bronze/{source}/")

    query.awaitTermination()
    logger.info(f"Bronze {source} escrito correctamente")


def process_meta(spark: SparkSession, base_path: str):
    """Proceso completo landing → bronze para Meta Ads."""
    logger.info("Procesando Meta Ads: landing → bronze")
    df_raw = read_from_landing(spark, base_path, "meta_ads", get_meta_schema())
    write_to_bronze(transform_meta(df_raw), base_path, "meta_ads")


def process_ventas(spark: SparkSession, base_path: str):
    """Proceso completo landing → bronze para Ventas."""
    logger.info("Procesando Ventas: landing → bronze")
    df_raw = read_from_landing(spark, base_path, "ventas", get_ventas_schema())
    write_to_bronze(transform_ventas(df_raw), base_path, "ventas")


def main():
    spark = SparkSession.builder.getOrCreate()

    storage_account, account_key, container = get_credentials(spark)

    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        account_key
    )

    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

    logger.info("Iniciando proceso landing → bronze")
    process_meta(spark, base_path)
    process_ventas(spark, base_path)
    logger.info("Proceso landing → bronze completado")


if __name__ == "__main__":
    main()