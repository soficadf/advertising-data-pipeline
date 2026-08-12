from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType


def get_meta_schema() -> StructType:
    """Schema de los datos de Meta Ads almacenados en Landing."""
    return StructType([
        StructField("extraction_date", StringType()),
        StructField("extraction_timestamp", StringType()),
        StructField("total_ads", LongType()),
        StructField("ads", ArrayType(StructType([
            StructField("id", StringType()),
            StructField("eu_total_reach",  LongType()),
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