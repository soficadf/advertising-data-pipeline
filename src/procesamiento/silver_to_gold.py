
import logging

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, min, round, coalesce, lit, when, datediff, lag,window,greatest

from config.settings import get_secret, get_spark_session
from config.adls_client import get_adls_base_path, configure_spark_adls
from model import Layers,DatasetsSilver,DatasetsGold


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)



def transform_demographic_reach(df_spend: DataFrame, df_meta: DataFrame) -> DataFrame:
    """
    Genera Gold Demographic Reach.
    """
    df_ads = df_spend.select("ad_id", "product_id", "product_name").dropDuplicates(["ad_id"])
    df = df_meta.join(df_ads, on="ad_id", how="left")

    df_female = df.select(
        "fecha", "ad_id", "product_id", "product_name", "country", "age_range",
        lit("female").alias("gender"), col("female").alias("reach")
    )
    df_male = df.select(
        "fecha", "ad_id",  "product_id","product_name", "country", "age_range",
        lit("male").alias("gender"), col("male").alias("reach")
    )

    return df_female.unionByName(df_male) \
        .filter(col("fecha").isNotNull()) \
        .filter(col("ad_id").isNotNull()) \
        .filter(col("reach") > 0) \
        .select("fecha", "ad_id", "product_id","product_name", "country", "age_range", "gender", "reach")


def transform_ad_daily_metrics(df_ventas: DataFrame, df_spend: DataFrame, df_meta: DataFrame) -> DataFrame:
    """
    Genera Gold Ad Daily Metrics.
    """
    df_spend_daily = df_spend.groupBy("fecha", "ad_id", "product_id", "product_name") \
        .agg(round(sum("daily_spend"), 2).alias("gasto"))

    window = Window.partitionBy("ad_id").orderBy("fecha")
    df_reach_daily = (
            df_meta.groupBy("fecha", "ad_id")
            .agg(sum("reach").alias("alcance_acumulado"))
            .withColumn("alcance_anterior", lag("alcance_acumulado").over(window))
            .withColumn(
                "alcance",
                when(
                    col("alcance_anterior").isNull(),
                    col("alcance_acumulado")
                ).otherwise(
                    greatest(col("alcance_acumulado") - col("alcance_anterior"), lit(0))
                )
            )
            .select("fecha", "ad_id", "alcance")
        )

    df_ad_start = df_meta.groupBy("ad_id") \
        .agg(min("ad_delivery_start_time").alias("fecha_inicio_anuncio"))

    df_sales_daily = df_ventas.groupBy("fecha", "product_id", "product_name") \
        .agg(
            sum("ventas_unidades").alias("ventas_unidades"),
            round(sum("ventas_importe"), 2).alias("ventas_importe")
        )

    return df_spend_daily \
        .join(df_reach_daily, on=["fecha", "ad_id"], how="left") \
        .join(df_ad_start, on="ad_id", how="left") \
        .join(df_sales_daily, on=["fecha", "product_id", "product_name"], how="left") \
        .select(
            col("fecha"), col("ad_id"),col( "product_id"), col("product_name"), col("gasto"),
            coalesce(col("alcance"), lit(0)).alias("alcance"),
            coalesce(col("ventas_unidades"), lit(0)).alias("ventas_unidades"),
            coalesce(col("ventas_importe"), lit(0)).alias("ventas_importe"),
            col("fecha_inicio_anuncio")
        ) \
        .withColumn(
            "roas",
            when(
                col("gasto") > 0,
                round(col("ventas_importe") / col("gasto"), 2)
            ).otherwise(lit(0))
        ) \
        .withColumn(
            "dias_desde_inicio",
            when(
                col("fecha_inicio_anuncio").isNotNull(),
                datediff(col("fecha"), col("fecha_inicio_anuncio"))
            )
        ) \
        .select(
            "fecha", "ad_id","product_id", "product_name", "gasto", "alcance",
            "ventas_unidades", "ventas_importe", "roas", "dias_desde_inicio"
        ) \
        .orderBy("fecha", "ad_id")

def transform_ad_total_metrics(df_ad_daily_metrics: DataFrame) -> DataFrame:
    """
    Genera Gold Ad Total Metrics.
    """
    return df_ad_daily_metrics.groupBy("ad_id", "product_id", "product_name") \
        .agg(
            round(sum("gasto"), 2).alias("gasto_total"),
            sum("alcance").alias("alcance_total"),
            sum("ventas_unidades").alias("ventas_unidades_total"),
            round(sum("ventas_importe"), 2).alias("ventas_importe_total")
        ) \
        .withColumn(
            "roas_total",
            when(
                col("gasto_total") > 0,
                round(col("ventas_importe_total") / col("gasto_total"), 2)
            ).otherwise(lit(0))
        ) \
        .select(
            "ad_id", "product_id", "product_name",
            "gasto_total", "alcance_total",
            "ventas_unidades_total", "ventas_importe_total",
            "roas_total"
        ) \
        .orderBy("ad_id")

def transform_saturation_curve(df_ad_daily_metrics: DataFrame) -> DataFrame:
    """
    Genera Gold Saturation Curve.
    """
    return df_ad_daily_metrics.groupBy("fecha") \
        .agg(
            round(sum("gasto"), 2).alias("gasto_total_dia"),
            sum("ventas_unidades").alias("ventas_unidades_total_dia"),
            round(sum("ventas_importe"), 2).alias("ventas_total_dia")
        ) \
        .withColumn(
            "roas_global",
            when(
                col("gasto_total_dia") > 0,
                round(col("ventas_total_dia") / col("gasto_total_dia"), 2)
            ).otherwise(lit(0))
        ) \
        .select(
            "fecha", "gasto_total_dia", "ventas_unidades_total_dia",
            "ventas_total_dia", "roas_global"
        ) \
        .orderBy("fecha")


def write_gold(df: DataFrame, spark, catalog: str, schema: str, table: str):
    """Escribe una tabla Gold directamente en Unity Catalog."""

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.{table}")
    logger.info(f"Tabla {table} escrita: {df.count()} registros")


def main():
    spark = get_spark_session()
    base_path = get_adls_base_path()
    configure_spark_adls(spark)

    catalog = get_secret("UNITY_CATALOG")
    schema =get_secret("UNITY_SCHEMA")

    logger.info("Iniciando proceso silver → gold")

    df_ventas = spark.read.format("delta").load(f"{base_path}/{Layers.SILVER.value}/{DatasetsSilver.VENTAS.value}/")
    df_spend = spark.read.format("delta").load(f"{base_path}/{Layers.SILVER.value}/{DatasetsSilver.SPEND.value}/")
    df_meta = spark.read.format("delta").load(f"{base_path}/{Layers.SILVER.value}/{DatasetsSilver.META.value}/")

    logger.info("Procesando Gold Demographic Reach...")
    write_gold(
        transform_demographic_reach(df_spend, df_meta),
        spark,
        catalog,
        schema,
        DatasetsGold.REACH.value
    )

    logger.info("Procesando Gold Ad Daily Metrics...")
    df_ad_daily = transform_ad_daily_metrics(df_ventas, df_spend, df_meta)
    write_gold(
        df_ad_daily,
        spark,
        catalog,
        schema,
         DatasetsGold.DAILY_METRICS.value
    )

    logger.info("Procesando Gold Ad Total Metrics...")
    write_gold(
        transform_ad_total_metrics(df_ad_daily),
        spark,
        catalog,
        schema,
        DatasetsGold.TOTAL_METRICS.value
    )

    logger.info("Procesando Gold Saturation Curve...")
    write_gold(
        transform_saturation_curve(df_ad_daily),
        spark,
        catalog,
        schema,
        DatasetsGold.SATURATION.value
    )

    logger.info("Proceso silver → gold completado")


if __name__ == "__main__":
    main()
