"""Build quality reports without filtering or changing the input DataFrame."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def completeness_quality(df: DataFrame) -> DataFrame:
    return df.agg(
        F.count("*").alias("total_rows"),
        F.count(F.when(F.col("pm25").isNull(), 1)).alias("missing_pm25"),
    )


def temporal_quality(df: DataFrame) -> DataFrame:
    return df.agg(
        F.count(F.when(F.col("event_time_utc").isNull(), 1)).alias("invalid_utc"),
        F.count(F.when(F.col("local_date").isNull(), 1)).alias("invalid_date"),
        F.count(
            F.when(
                F.col("local_hour").isNull()
                | ~F.col("local_hour").between(0, 23),
                1,
            )
        ).alias("invalid_hour"),
    )


def date_range_quality(df: DataFrame, expected_year: int = 2024) -> DataFrame:
    return df.agg(
        F.min("local_date").alias("earliest_date"),
        F.max("local_date").alias("latest_date"),
        F.count(
            F.when(F.year("local_date") != expected_year, 1)
        ).alias(f"outside_{expected_year}"),
    )


def geographic_quality(df: DataFrame) -> DataFrame:
    return df.agg(
        F.count(
            F.when(
                F.col("latitude").isNull()
                | ~F.col("latitude").between(-90, 90),
                1,
            )
        ).alias("invalid_latitude"),
        F.count(
            F.when(
                F.col("longitude").isNull()
                | ~F.col("longitude").between(-180, 180),
                1,
            )
        ).alias("invalid_longitude"),
        F.count(
            F.when(
                F.col("state_code").isNull() | F.col("county_code").isNull(),
                1,
            )
        ).alias("missing_geographic_codes"),
    )


def duplicate_quality(df: DataFrame) -> DataFrame:
    measurement_key = [
        "state_code",
        "county_code",
        "site_num",
        "parameter_code",
        "poc",
        "event_time_utc",
    ]
    duplicate_groups = (
        df.groupBy(*measurement_key)
        .count()
        .filter(F.col("count") > 1)
    )
    return duplicate_groups.agg(
        F.count("*").alias("duplicate_keys"),
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("extra_rows"),
    )


def measurement_quality(df: DataFrame) -> DataFrame:
    return df.agg(
        F.min("pm25").alias("min_pm25"),
        F.max("pm25").alias("max_pm25"),
        F.avg("pm25").alias("avg_pm25"),
        F.count(F.when(F.col("pm25") < 0, 1)).alias("negative_pm25"),
        F.count(F.when(F.isnan("pm25"), 1)).alias("nan_pm25"),
    )
