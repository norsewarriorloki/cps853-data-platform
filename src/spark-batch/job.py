from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from quality_checks import split_valid_invalid


RAW_MEASUREMENTS_PATH = "/app/data/raw/hourly_88101_2024/hourly_88101_2024.csv"
EXCLUDED_STATE_CODES = ("72", "80")


def show_quality_summary(valid_df: DataFrame, invalid_df: DataFrame) -> None:
    counts = (
        valid_df.unionByName(invalid_df)
        .groupBy("rejection_reasons")
        .count()
        .collect()
    )
    accepted = sum(row["count"] for row in counts if not row["rejection_reasons"])
    rejected = sum(row["count"] for row in counts if row["rejection_reasons"])
    print("[silver] Validation report", flush=True)
    print(f"[silver] Validation input: {accepted + rejected:,} | Valid: {accepted:,} | Rejected: {rejected:,}")
    reasons = {}
    for row in counts:
        for reason in row["rejection_reasons"]:
            reasons[reason] = reasons.get(reason, 0) + row["count"]
    for reason, count in sorted(reasons.items()):
        print(f"[silver]   {reason:<24} {count:>12,}")
    print("[silver] Reason counts can overlap when a row violates multiple rules.", flush=True)


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
    print(f"[silver] Input: raw hourly PM2.5 CSV -> {RAW_MEASUREMENTS_PATH}", flush=True)
    print("[silver] Process: validate required fields, numbers, timestamps, 2024 dates, and duplicate keys", flush=True)
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

        valid_df, invalid_df = split_valid_invalid(
            raw_df
        )
        show_quality_summary(valid_df, invalid_df)

        # Quarantine invalid rows
        quarantine_df = invalid_df.select(
            *raw_df.columns,
            "rejection_reasons"
        )

        silver_df = build_silver_dataframe(valid_df)

        print("[silver] Process: type valid observations; exclude state codes 72 and 80 after validation", flush=True)
        print("[silver] Writing monitor-hour observations -> /app/data/silver/pm25_hourly", flush=True)
        # Write the silver df to parquet files
        silver_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet("/app/data/silver/pm25_hourly")

        print("[silver] Writing raw rejected rows + rejection reasons -> /app/data/rejected/pm25_hourly", flush=True)
        # Write the quarantined records
        quarantine_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet("/app/data/rejected/pm25_hourly")
        print("[silver] Output: Silver Parquet saved (location IDs, UTC timestamp, local date/hour, PM2.5, metadata)", flush=True)
        print("[silver] Output: rejected Parquet saved for quality auditing", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
