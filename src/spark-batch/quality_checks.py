"""Validate raw measurements, split rejected rows, and build quality reports."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

REQUIRED_FIELDS = [
    "State Code",
    "County Code",
    "Site Num",
    "POC",
    "Parameter Code",
    "Sample Measurement"
]

MEASUREMENT_KEY = [
    "State Code",
    "County Code",
    "Site Num",
    "Parameter Code",
    "POC",
    "Date GMT",
    "Time GMT"
]

def has_invalid_required_fields() -> Column:
    invalid = F.lit(False)

    for field in REQUIRED_FIELDS:
        column = F.col(field)

        invalid = (
            invalid
            | column.isNull()
            | (F.trim(column) == "")
        )

    invalid = invalid | F.col("POC").try_cast("int").isNull()
    pm25 = F.col("Sample Measurement").try_cast("double")
    invalid = invalid | pm25.isNull() | F.isnan(pm25)

    return invalid

def timestamp_violations() -> tuple[Column, Column]:
    """Return predicates for malformed timestamps and dates outside 2024."""
    utc_timestamp = F.try_to_timestamp(
        F.concat_ws(
            " ",
            F.col("Date GMT"),
            F.col("Time GMT")
        ),
        F.lit("yyyy-MM-dd HH:mm")
    )

    local_date = F.try_to_date(
        F.col("Date Local"),
        "yyyy-MM-dd"
    )

    valid_local_time = F.col("Time Local").rlike(
        r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$"
    )

    invalid_timestamp = (
        utc_timestamp.isNull()
        | local_date.isNull()
        | ~F.coalesce(valid_local_time, F.lit(False))
    )

    outside_date_range = (
        local_date.isNotNull()
        & (F.year(local_date) != 2024)
    )

    return invalid_timestamp, outside_date_range

def flag_duplicate_measurements(df: DataFrame) -> DataFrame:
    """Flag every row belonging to a repeated measurement key."""
    window = Window.partitionBy(*MEASUREMENT_KEY)

    return df.withColumn(
        "is_duplicate",
        F.count("*").over(window) > 1
    )


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid, invalid) raw rows, retaining flags and rejection reasons.

    Every row in a duplicate group is rejected, matching the existing policy.
    Both returned DataFrames are lazy; the caller decides when to run actions.
    """
    flagged_df = flag_duplicate_measurements(df)
    invalid_timestamp, outside_date_range = timestamp_violations()
    checked_df = flagged_df.withColumn(
        "rejection_reasons",
        F.filter(
            F.array(
                F.when(has_invalid_required_fields(), "MISSING_REQUIRED"),
                F.when(invalid_timestamp, "INVALID_TIMESTAMP"),
                F.when(outside_date_range, "OUTSIDE_DATE_RANGE"),
                F.when(F.col("is_duplicate"), "DUPLICATE"),
            ),
            lambda reason: reason.isNotNull(),
        ),
    )
    valid_df = checked_df.filter(F.size("rejection_reasons") == 0)
    invalid_df = checked_df.filter(F.size("rejection_reasons") > 0)
    return valid_df, invalid_df


def measurement_quality(df: DataFrame) -> DataFrame:
    return df.agg(
        F.min("pm25").alias("min_pm25"),
        F.max("pm25").alias("max_pm25"),
        F.avg("pm25").alias("avg_pm25"),
        F.count(F.when(F.col("pm25") < 0, 1)).alias("negative_pm25"),
        F.count(F.when(F.isnan("pm25"), 1)).alias("nan_pm25"),
    )
