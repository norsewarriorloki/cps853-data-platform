from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import exploratory
import quality_checks


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
            spark.read
            .option("header", "true")
            .csv("/app/data/raw/hourly_88101_2024/hourly_88101_2024.csv") # docker mount location
        )

        # Optional source inspection:
        # raw_df.printSchema()
        # exploratory.raw_sample(raw_df).show(truncate=40, vertical=True)

        silver_df = raw_df.select(
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

        # Enable additional reports as needed; each action scans the input.
        # quality_checks.completeness_quality(silver_df).show()
        # quality_checks.temporal_quality(silver_df).show()
        # quality_checks.date_range_quality(silver_df, expected_year=2024).show()
        # quality_checks.geographic_quality(silver_df).show()
        # quality_checks.duplicate_quality(silver_df).show()
        #quality_checks.measurement_quality(silver_df).show()

        exploratory.highest_measurements(silver_df).show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
