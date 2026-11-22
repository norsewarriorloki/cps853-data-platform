"""Build DataFrames for interactive inspection of the source and Silver data."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def raw_sample(df: DataFrame, limit: int = 3) -> DataFrame:
    return df.limit(limit)


def highest_measurements(df: DataFrame, limit: int = 10) -> DataFrame:
    return (
        df.select(
            "state_name",
            "county_name",
            "event_time_utc",
            "pm25",
            "qualifier",
            "method_code",
        )
        .orderBy(F.col("pm25").desc())
        .limit(limit)
    )
