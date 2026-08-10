import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum, round, explode, coalesce, lit

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
        sql_password = os.getenv("AZURE_SQL_PASSWORD")
        logger.info("Credenciales leídas desde .env")
    else:
        storage_account = dbutils.secrets.get(scope="ad-pipeline", key="adls_account_name")
        account_key = dbutils.secrets.get(scope="ad-pipeline", key="adls_account_key")
        container = dbutils.secrets.get(scope="ad-pipeline", key="adls_container_name")
        sql_password = dbutils.secrets.get(scope="ad-pipeline", key="sql_password")
        logger.info("Credenciales leídas desde Databricks Secrets")

    return storage_account, account_key, container, sql_password


def transform_ventas_to_silver(spark: SparkSession, base_path: str) -> DataFrame:
    """Lee Bronze ventas y agrega por producto y fecha."""
    return spark.read.format("delta").load(f"{base_path}/bronze/ventas/") \
        .filter(col("fecha").isNotNull()) \
        .groupBy("fecha", "product_id", "product_name") \
        .agg(
            sum("quantity").alias("ventas_unidades"),
            round(sum("total_amount"), 2).alias("ventas_importe")
        ) \
        .orderBy("fecha", "product_id")


def write_silver_ventas(df: DataFrame, base_path: str):
    """Escribe Silver ventas en Delta."""
    df.write.format("delta").mode("overwrite").partitionBy("fecha") \
        .save(f"{base_path}/silver/ventas_diarias/")
    logger.info(f"Silver Ventas escrito: {df.count()} registros")


def read_sql_tables(spark: SparkSession, jdbc_url: str, jdbc_properties: dict) -> tuple:
    """Lee las tablas de Azure SQL."""
    df_spend = spark.read.jdbc(url=jdbc_url, table="daily_spend", properties=jdbc_properties)
    df_mapping = spark.read.jdbc(url=jdbc_url, table="ad_product_mapping", properties=jdbc_properties)
    df_products = spark.read.jdbc(url=jdbc_url, table="products", properties=jdbc_properties)
    logger.info(f"SQL leído: {df_spend.count()} spend, {df_mapping.count()} mappings, {df_products.count()} productos")
    return df_spend, df_mapping, df_products


def transform_spend_to_silver(df_spend: DataFrame, df_mapping: DataFrame, df_products: DataFrame) -> DataFrame:
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


def write_silver_spend(df: DataFrame, base_path: str):
    """Escribe Silver Ad Spend en Delta."""
    df.write.format("delta").mode("overwrite").partitionBy("fecha") \
        .save(f"{base_path}/silver/ad_spend/")
    logger.info(f"Silver Ad Spend escrito: {df.count()} registros")


def transform_meta_to_silver(spark: SparkSession, base_path: str) -> DataFrame:
    """Lee Bronze Meta y explota el desglose demográfico."""
    df_bronze = spark.read.format("delta").load(f"{base_path}/bronze/meta_ads/")

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
        (coalesce(col("age_gender.female"), lit(0)) +
         coalesce(col("age_gender.male"), lit(0))).alias("reach")
    )


def write_silver_meta(df: DataFrame, base_path: str):
    """Escribe Silver Meta en Delta."""
    df.write.format("delta").mode("overwrite").partitionBy("fecha") \
        .save(f"{base_path}/silver/meta_ads/")
    logger.info(f"Silver Meta escrito: {df.count()} registros")


def main():
    spark = SparkSession.builder.getOrCreate()

    storage_account, account_key, container, sql_password = get_credentials(spark)

    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        account_key
    )

    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

    jdbc_url = "jdbc:sqlserver://ad-pipeline-server.database.windows.net:1433;database=ad-pipeline-db;encrypt=true;trustServerCertificate=true;"
    jdbc_properties = {
        "user": "admin_ad_pipeline",
        "password": sql_password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    logger.info("Iniciando proceso bronze → silver")

    logger.info("Procesando Ventas...")
    write_silver_ventas(transform_ventas_to_silver(spark, base_path), base_path)

    logger.info("Procesando Ad Spend...")
    df_spend, df_mapping, df_products = read_sql_tables(spark, jdbc_url, jdbc_properties)
    write_silver_spend(transform_spend_to_silver(df_spend, df_mapping, df_products), base_path)

    logger.info("Procesando Meta Ads...")
    write_silver_meta(transform_meta_to_silver(spark, base_path), base_path)

    logger.info("Proceso bronze → silver completado")


if __name__ == "__main__":
    main()