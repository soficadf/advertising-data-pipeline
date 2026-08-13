import logging

from config.adls_client import configure_spark_adls, get_adls_base_path
from pyspark.sql import DataFrame
from pyspark.sql.functions import  sum, round, col, explode,coalesce,lit,year, month, dayofmonth

from config.settings import get_secret, get_spark_session
from model import DatasetsSilver, Layers,DatasetsBronze, TablesSQl


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def read_sql_tables(spark, jdbc_url: str, jdbc_properties: dict) -> tuple:
    """Lee las tablas de Azure SQL."""

    df_spend = spark.read.jdbc(
        url=jdbc_url,
        table=TablesSQl.SPEND.value,
        properties=jdbc_properties
    )

    df_mapping = spark.read.jdbc(
        url=jdbc_url,
        table=TablesSQl.PRODUCT_MAP.value,
        properties=jdbc_properties
    )

    df_products = spark.read.jdbc(
        url=jdbc_url,
        table=TablesSQl.PRODUCTS.value,
        properties=jdbc_properties
    )

    logger.info(f"SQL leído: {df_spend.count()} spend, {df_mapping.count()} mappings, {df_products.count()} products")
    return df_spend, df_mapping, df_products

def transform_ventas(spark, base_path: str) -> DataFrame:
    """Lee Bronze ventas y agrega por producto y fecha."""

    return spark.read.format("delta").load(f"{base_path}/{Layers.BRONZE.value}/{DatasetsBronze.VENTAS.value}/") \
        .filter(col("fecha").isNotNull()) \
        .groupBy("fecha", "product_id", "product_name") \
        .agg(
            sum("quantity").alias("ventas_unidades"),
            round(sum("total_amount"), 2).alias("ventas_importe")
        ) \
        .orderBy("fecha", "product_id")

def transform_spend(df_spend: DataFrame,df_mapping: DataFrame,df_products: DataFrame) -> DataFrame:
    """Une las tablas de SQL y genera Silver Ad Spend."""

    return df_spend \
        .join(df_mapping, "ad_id", "left") \
        .join(df_products, "product_id", "left") \
        .select(
            col("date").alias("fecha"),
            col("ad_id"),
            col("product_id"),
            col("product_name"),
            col("price"),
            col("daily_spend")
        ) \
        .orderBy("fecha", "ad_id")


def transform_meta(spark, base_path: str) -> DataFrame:
    """Lee Bronze Meta y explota el desglose demográfico."""

    df_bronze = spark.read.format("delta").load(f"{base_path}/{Layers.BRONZE.value}/{DatasetsBronze.META.value}/")

    return df_bronze.select(
        col("fecha"), col("ad_id"), col("eu_total_reach"),
        col("ad_delivery_start_time"), col("target_gender"),
        col("target_ages"), col("publisher_platforms"),
        explode(col("age_country_gender_reach_breakdown")).alias("country_data")
    ).select(
        col("fecha"), col("ad_id"), col("eu_total_reach"),
        col("ad_delivery_start_time"), col("target_gender"),
        col("target_ages"), col("publisher_platforms"),
        col("country_data.country").alias("country"),
        explode(col("country_data.age_gender_breakdowns")).alias("age_gender")
    ).select(
        col("fecha"), col("ad_id"), col("eu_total_reach"),
        col("ad_delivery_start_time"), col("target_gender"),
        col("target_ages"), col("publisher_platforms"),
        col("country"),
        col("age_gender.age_range").alias("age_range"),
        coalesce(col("age_gender.female"), lit(0)).alias("female"),
        coalesce(col("age_gender.male"), lit(0)).alias("male"),
        (
            coalesce(col("age_gender.female"), lit(0)) +
            coalesce(col("age_gender.male"), lit(0))
        ).alias("reach")
    )


def write_to_silver(df: DataFrame, base_path: str, name:str):
    df = (
        df
        .withColumn("anio", year("fecha"))
        .withColumn("mes", month("fecha"))
        .withColumn("dia", dayofmonth("fecha"))
    )

    df.write.format("delta").mode("overwrite").partitionBy("anio", "mes", "dia") \
        .save(f"{base_path}/silver/{name}/")

    logger.info(f"Silver {name} escrito: {df.count()} registros")

def main():
    spark = get_spark_session()
    base_path = get_adls_base_path()
    configure_spark_adls(spark)
    
    sql_password = get_secret("AZURE_SQL_PASSWORD")

    jdbc_url ="jdbc:sqlserver://ad-pipeline-server.database.windows.net:1433;database=ad-pipeline-db;encrypt=true;trustServerCertificate=true;"
    
    jdbc_properties = {
        "user": "admin_ad_pipeline",
        "password": sql_password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    logger.info("Iniciando proceso bronze → silver")

    logger.info("Procesando Ventas...")
    write_to_silver(
        transform_ventas(spark, base_path),
        base_path,
        DatasetsSilver.VENTAS.value
    )

    logger.info("Procesando Ad Spend...")
    df_spend, df_mapping, df_products = read_sql_tables(spark,jdbc_url,jdbc_properties)

    write_to_silver(
        transform_spend(df_spend,df_mapping,df_products),
        base_path,
        DatasetsSilver.SPEND.value
    )

    logger.info("Procesando Meta Ads...")
    write_to_silver(
        transform_meta(spark, base_path),
        base_path,
        DatasetsSilver.META.value
    )

    logger.info("Proceso bronze → silver completado")


if __name__ == "__main__":
    main()

