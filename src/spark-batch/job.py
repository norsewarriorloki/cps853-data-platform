from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from quality_checks import split_valid_invalid


RAW_MEASUREMENTS_PATH = "/app/data/raw/hourly_88101_2024/hourly_88101_2024.csv"
EXCLUDED_STATE_CODES = ("72", "80")


def show_quality_summary(valid_df: DataFrame, invalid_df: DataFrame) -> None:
    valid_df.unionByName(invalid_df).groupBy("rejection_reasons").count().show(
        truncate=False
    )


def run_sample_quality_checks(raw_measurements_df: DataFrame) -> None:
    """Exercise validation on one source row and an intentionally invalid copy."""
    source_sample_df = raw_measurements_df.limit(1)
    invalid_sample_df = (
        source_sample_df
        .withColumn("Sample Measurement", F.lit(None).cast("string"))
        .withColumn("Date Local", F.lit("2023-01-02"))
    )
    # The copied measurement key also exercises duplicate rejection.
    sample_measurements_df = source_sample_df.unionByName(invalid_sample_df)
    valid_sample_df, invalid_sample_df = split_valid_invalid(sample_measurements_df)

    print("Sample quality-check results")
    show_quality_summary(valid_sample_df, invalid_sample_df)


def build_silver_dataframe(valid_measurements_df: DataFrame) -> DataFrame:
    """Select and type accepted measurements for the Silver layer."""
    silver_df = valid_measurements_df.select(
        F.col("State Code").alias("state_code"),
        F.col("County Code").alias("county_code"),
        F.col("Site Num").alias("site_num"),
        F.col("POC").cast("int").alias("poc"),
        F.col("State Name").alias("state_name"),
        F.col("County Name").alias("county_name"),
        F.col("Latitude").cast("double").alias("latitude"),
        F.col("Longitude").cast("double").alias("longitude"),
        F.col("Parameter Code").alias("parameter_code"),

        F.to_timestamp(
            F.concat_ws(" ", F.col("Date GMT"), F.col("Time GMT")),
            "yyyy-MM-dd HH:mm"
        ).alias("event_time_utc"),

        F.to_date(
            F.col("Date Local"),
            "yyyy-MM-dd"
        ).alias("local_date"),

        F.substring(F.col("Time Local"), 1, 2).cast("int").alias("local_hour"),

        F.col("Sample Measurement").cast("double").alias("pm25"),

        F.col("Qualifier").alias("qualifier"),

        F.col("MDL").cast("double").alias("mdl"),

        F.col("Method Code").alias("method_code")
    )
    return silver_df.filter(~F.col("state_code").isin(*EXCLUDED_STATE_CODES))


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("PM25-Silver-Pipeline")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("ERROR")
        spark.conf.set("spark.sql.session.timeZone", "UTC")

        raw_df = (
            spark.read.option("header", "true").csv(RAW_MEASUREMENTS_PATH)
        )

        # Complete the sample report before validating the full dataset.
        run_sample_quality_checks(raw_df)

        valid_df, invalid_df = split_valid_invalid(
            raw_df
        )
        print("Full-dataset quality-check results")
        show_quality_summary(valid_df, invalid_df)

        # Quarantine invalid rows
        quarantine_df = invalid_df.select(
            *raw_df.columns,
            "rejection_reasons"
        )

        silver_df = build_silver_dataframe(valid_df)

        # Write the silver df to parquet files
        silver_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet("/app/data/silver/pm25_hourly")

        # Write the quarantined records
        quarantine_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet("/app/data/rejected/pm25_hourly")
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
