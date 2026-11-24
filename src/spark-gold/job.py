from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import psycopg
import os 

print(f"[gold] Input: Silver Parquet -> {os.environ['SILVER_PARQUET_PATH']}", flush=True)
print("[gold] Process: average monitor-hour PM2.5 by state/year and county/local day", flush=True)
spark = SparkSession.builder.appName("PM25-Gold").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

silver_df = spark.read.parquet(
    os.environ["SILVER_PARQUET_PATH"]
)

# Question 1 which US states had the highest average pm25 concentrations in 2024?

state_annual_df = silver_df.groupBy(
    "state_code",
    "state_name"
).agg(
    F.avg("pm25").alias("avg_pm25"),
    F.count("*").alias("measurement_count")
).withColumn("year", F.lit(2024))

# Question 2: What were the the worst days of air pollution in the United States in 2024 and what regions did they belong to?

county_daily_df = silver_df.groupBy(
    "state_code",
    "state_name",
    "county_code",
    "county_name",
    "local_date"
).agg(
    F.avg("pm25").alias("avg_pm25"),
    F.count("*").alias("measurement_count")
)

# Load results into database for persistent storage 

annual_db_df = state_annual_df.select(
    "state_code",
    "year",
    "avg_pm25",
    "measurement_count"
)

daily_db_df = county_daily_df.select(
    "state_code",
    "county_code",
    "local_date",
    "avg_pm25",
    "measurement_count"
)

jdbc_url = (
    f"jdbc:postgresql://{os.environ['PGHOST']}:"
    f"{os.environ['PGPORT']}/{os.environ['PGDATABASE']}"
)

jdbc_properties = {
    "user": os.environ["PGUSER"],
    "password": os.environ["PGPASSWORD"],
    "driver": "org.postgresql.Driver",
    "batchsize": "1000"
}

print(f"[gold] Writing aggregates -> PostgreSQL {os.environ['PGDATABASE']}: stg_state_annual_pm25, stg_county_daily_pm25", flush=True)

# WRITE RESULTS TO A STAGING TABLE FIRST 

annual_db_df.write.jdbc(
    url=jdbc_url,
    table="stg_state_annual_pm25",
    mode="append",
    properties=jdbc_properties
)

daily_db_df.write.jdbc(
    url=jdbc_url,
    table="stg_county_daily_pm25",
    mode="append",
    properties=jdbc_properties
)

print("[gold] Publishing batch aggregates", flush=True)

with psycopg.connect(
    host=os.environ["PGHOST"],
    port=os.environ["PGPORT"],
    dbname=os.environ["PGDATABASE"],
    user=os.environ["PGUSER"],
    password=os.environ["PGPASSWORD"]
) as conn:

    with conn.cursor() as cursor:
        cursor.execute("""
            TRUNCATE TABLE state_annual_pm25, county_daily_pm25
        """)

        cursor.execute("""
            INSERT INTO state_annual_pm25 (
                state_code, year, avg_pm25, measurement_count
            )
            SELECT state_code, year, avg_pm25, measurement_count
            FROM stg_state_annual_pm25
        """)
        state_rows = cursor.rowcount

        cursor.execute("""
            INSERT INTO county_daily_pm25 (
                state_code, county_code, local_date,
                avg_pm25, measurement_count
            )
            SELECT state_code, county_code, local_date,
                   avg_pm25, measurement_count
            FROM stg_county_daily_pm25
        """)
        county_rows = cursor.rowcount

print(f"[gold] Output: state_annual_pm25 -> {state_rows:,} state-year rows", flush=True)
print(f"[gold] Output: county_daily_pm25 -> {county_rows:,} county-day rows (average PM2.5 + measurement count)", flush=True)
spark.stop()
