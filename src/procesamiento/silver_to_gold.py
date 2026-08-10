import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum, min, round, explode, coalesce, lit, when, datediff

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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


def read_silver_tables(spark: SparkSession, base_path: str) -> tuple:
    """Lee las tablas Silver necesarias para generar Gold."""
    df_ventas = spark.read.format("delta").load(f"{base_path}/silver/ventas_diarias/")
    df_spend = spark.read.format("delta").load(f"{base_path}/silver/ad_spend/")
    df_meta = spark.read.format("delta").load(f"{base_path}/silver/meta_ads/")
    return df_ventas, df_spend, df_meta


def transform_demographic_reach(df_spend: DataFrame, df_meta: DataFrame) -> DataFrame:
    """
    Genera Gold Demographic Reach.
    Pregunta: ¿A quién está llegando cada anuncio?
    Granularidad: día + anuncio + país + edad + género
    """
    df_ads = df_spend.select("ad_id", "product_id", "product_name").dropDuplicates(["ad_id"])
    df = df_meta.join(df_ads, on="ad_id", how="left")

    df_female = df.select("fecha", "ad_id", "product_name", "country", "age_range",
                          lit("female").alias("gender"), col("female").alias("reach"))
    df_male = df.select("fecha", "ad_id", "product_name", "country", "age_range",
                        lit("male").alias("gender"), col("male").alias("reach"))

    return df_female.unionByName(df_male) \
        .filter(col("fecha").isNotNull()) \
        .filter(col("ad_id").isNotNull()) \
        .filter(col("reach") > 0) \
        .select("fecha", "ad_id", "product_name", "country", "age_range", "gender", "reach")


def transform_ad_daily_metrics(df_ventas: DataFrame, df_spend: DataFrame, df_meta: DataFrame) -> DataFrame:
    """
    Genera Gold Ad Daily Metrics.
    Pregunta: ¿Cuánto rinde cada anuncio? ¿Cuándo se ve el efecto en ventas?
    """
    df_spend_daily = df_spend.groupBy("fecha", "ad_id", "product_id", "product_name") \
        .agg(round(sum("daily_spend"), 2).alias("gasto"))

    df_reach_daily = df_meta.groupBy("fecha", "ad_id") \
        .agg(sum("reach").alias("alcance"))

    df_ad_start = df_meta.groupBy("ad_id") \
        .agg(min("ad_delivery_start_time").alias("fecha_inicio_anuncio"))

    df_sales_daily = df_ventas.groupBy("fecha", "product_id", "product_name") \
        .agg(sum("ventas_unidades").alias("ventas_unidades"),
             round(sum("ventas_importe"), 2).alias("ventas_importe"))

    return df_spend_daily \
        .join(df_reach_daily, on=["fecha", "ad_id"], how="left") \
        .join(df_ad_start, on="ad_id", how="left") \
        .join(df_sales_daily, on=["fecha", "product_id", "product_name"], how="left") \
        .select(
            col("fecha"), col("ad_id"), col("product_name"), col("gasto"),
            coalesce(col("alcance"), lit(0)).alias("alcance"),
            coalesce(col("ventas_unidades"), lit(0)).alias("ventas_unidades"),
            coalesce(col("ventas_importe"), lit(0)).alias("ventas_importe"),
            col("fecha_inicio_anuncio")
        ) \
        .withColumn("roas", when(col("gasto") > 0,
                                 round(col("ventas_importe") / col("gasto"), 2)).otherwise(lit(0))) \
        .withColumn("dias_desde_inicio", when(col("fecha_inicio_anuncio").isNotNull(),
                                              datediff(col("fecha"), col("fecha_inicio_anuncio")))) \
        .select("fecha", "ad_id", "product_name", "gasto", "alcance",
                "ventas_unidades", "ventas_importe", "roas", "dias_desde_inicio") \
        .orderBy("fecha", "ad_id")


def transform_saturation_curve(df_ad_daily_metrics: DataFrame) -> DataFrame:
    """
    Genera Gold Saturation Curve.
    Pregunta: ¿Cuál es el presupuesto óptimo?
    """
    return df_ad_daily_metrics.groupBy("fecha") \
        .agg(
            round(sum("gasto"), 2).alias("gasto_total_dia"),
            sum("ventas_unidades").alias("ventas_unidades_total_dia"),
            round(sum("ventas_importe"), 2).alias("ventas_total_dia")
        ) \
        .withColumn("roas_global", when(col("gasto_total_dia") > 0,
                                        round(col("ventas_total_dia") / col("gasto_total_dia"), 2))
                    .otherwise(lit(0))) \
        .select("fecha", "gasto_total_dia", "ventas_unidades_total_dia", "ventas_total_dia", "roas_global") \
        .orderBy("fecha")


def write_gold(df: DataFrame, spark: SparkSession, catalog: str, schema: str, table: str):
    """Escribe una tabla Gold directamente en Unity Catalog."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.{table}")
    logger.info(f"Tabla {catalog}.{schema}.{table} escrita: {df.count()} registros")


def main():
    spark = SparkSession.builder.getOrCreate()

    storage_account, account_key, container = get_credentials(spark)

    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        account_key
    )

    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    catalog = "masterscf002dbr"
    schema = "ad_pipeline"

    logger.info("Iniciando proceso silver → gold")

    df_ventas, df_spend, df_meta = read_silver_tables(spark, base_path)

    logger.info("Procesando Gold Demographic Reach...")
    write_gold(transform_demographic_reach(df_spend, df_meta), spark, catalog, schema, "gold_demographic_reach")

    logger.info("Procesando Gold Ad Daily Metrics...")
    df_ad_daily = transform_ad_daily_metrics(df_ventas, df_spend, df_meta)
    write_gold(df_ad_daily, spark, catalog, schema, "gold_ad_daily_metrics")

    logger.info("Procesando Gold Saturation Curve...")
    write_gold(transform_saturation_curve(df_ad_daily), spark, catalog, schema, "gold_saturation_curve")

    logger.info("Proceso silver → gold completado")


if __name__ == "__main__":
    main()