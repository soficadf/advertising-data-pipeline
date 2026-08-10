from pyspark.sql.types import (StructType,StructField,StringType,LongType,DoubleType)


def get_ventas_schema() -> StructType:
    """Schema de los eventos de ventas."""

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