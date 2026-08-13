
import logging
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    explode,
    from_json,
    regexp_extract,
    to_date,
    to_timestamp,
    year,
    month,
    dayofmonth

)
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession

from config.settings import  get_spark_session
from schemas.meta import get_meta_schema
from schemas.ventas import get_ventas_schema
from config.adls_client import get_adls_base_path, configure_spark_adls
from model import Layers, DatasetsLanding



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def read_from_landing(spark: SparkSession,base_path: str,source: str,schema: StructType) -> DataFrame:
    """
    Lee ficheros JSON desde Landing utilizando Auto Loader.
    Los ficheros se leen como texto completo y posteriormente
    se parsean utilizando el schema proporcionado.
    """

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .load(f"{base_path}/{Layers.LANDING.value}/{source}/")
        .withColumn("file_path",col("_metadata.file_path"))
        .withColumn("parsed",from_json(col("value"),schema) )
        .select("file_path","parsed.*")
    )



def transform_meta(df_raw: DataFrame) -> DataFrame:
    """Transforma los datos de Meta Ads de Landing al modelo de Bronze."""
    return (
        df_raw
        .select(
            coalesce(
                to_date(col("extraction_date")),
                to_date(regexp_extract(col("file_path"), r"(\d{4}/\d{2}/\d{2})", 1), "yyyy/MM/dd")
            ).alias("fecha"),
            explode(col("ads")).alias("ad")
        )
        .select(
            col("fecha"),
            col("ad.id").alias("ad_id"),
            col("ad.eu_total_reach").alias("eu_total_reach"),
            to_date(col("ad.ad_delivery_start_time")).alias("ad_delivery_start_time"),
            col("ad.target_gender").alias("target_gender"),
            col("ad.target_ages").alias("target_ages"),
            col("ad.publisher_platforms").alias("publisher_platforms"),
            col("ad.age_country_gender_reach_breakdown").alias("age_country_gender_reach_breakdown")
        )
)


def transform_ventas(df_raw: DataFrame) -> DataFrame:
    """
    Transforma los datos de ventas de Landing
    al modelo de Bronze.
    """

    return (
        df_raw
        .select(
            to_date(col("batch_date")).alias("fecha"),
            explode(col("events")).alias("event")
        )
        .select(
            col("fecha"),
            col("event.event_id").alias("event_id"),
            to_timestamp(col("event.timestamp")).alias("timestamp"),
            col("event.product_id").alias("product_id"),
            col("event.product_name").alias("product_name" ),
            col("event.quantity").alias("quantity"),
            col("event.unit_price").alias("unit_price"),
            col("event.total_amount").alias("total_amount"),
            col("event.region").alias("region"),
            col("event.channel").alias("channel")
        )
    )


def write_to_bronze(df_bronze: DataFrame,base_path: str,source: str) -> None:
    """
    Escribe un DataFrame en Bronze en formato Delta.

    El proceso utiliza availableNow para que pueda
    ejecutarse como un Job y termine cuando haya
    procesado todos los datos disponibles.
    """

    output_path = (f"{base_path}/{Layers.BRONZE.value}/{source}/")
    checkpoint_path = (f"{base_path}/checkpoints/{source}_{Layers.BRONZE.value}")
    df_bronze = (
        df_bronze
        .withColumn("anio", year("fecha"))
        .withColumn("mes", month("fecha"))
        .withColumn("dia", dayofmonth("fecha"))
    )

    logger.info("Escribiendo Bronze para '%s' en %s",source,output_path)

    query = (
        df_bronze
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation",checkpoint_path)
        .partitionBy("anio", "mes", "dia")
        .trigger(availableNow=True)
        .start(output_path)
    )

    query.awaitTermination()
    logger.info("Bronze '%s' escrito correctamente",source)



def process(spark: SparkSession,base_path: str,source:str,transform: Callable,schema=None) -> None:
    """
    Ejecuta el proceso Landing → Bronze
    """
    logger.info("Procesando: Landing → Bronze", source)

    df_raw = read_from_landing(
        spark=spark,
        base_path=base_path,
        source=source,
        schema=schema
    )

    df_bronze = transform(df_raw)

    write_to_bronze(
        df_bronze=df_bronze,
        base_path=base_path,
        source=source
    )


def main() -> None:
    logger.info("Iniciando proceso Landing → Bronze")

    spark = get_spark_session()
    base_path = get_adls_base_path()
    configure_spark_adls(spark)

    process(
        spark=spark,
        base_path=base_path,
        source=DatasetsLanding.META.value,
        schema=get_meta_schema(),
        transform=transform_meta
    )

    process(
        spark=spark,
        base_path=base_path,
        source=DatasetsLanding.VENTAS.value,
        schema= get_ventas_schema(),
        transform=transform_ventas
    )

    logger.info("Proceso Landing → Bronze completado")


if __name__ == "__main__":
    main()
