import os
from operator import or_
from functools import reduce
import psycopg

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ENABLE_ROLLING = os.getenv("ENABLE_ROLLING", "true").lower() == "true"
PUBLISH_GOLD = os.getenv("PUBLISH_GOLD", "true").lower() == "true"

EVENT_SCHEMA = """
    state_code STRING,
    county_code STRING,
    site_num STRING,
    poc INT,
    event_time_utc STRING,
    pm25 DOUBLE
"""

REQUIRED_FIELDS = {
    "state_code",
    "county_code",
    "site_num",
    "poc",
    "event_time_utc",
    "pm25"
}

def validate_event(df):
    # create a list of booleans corresponding to if a field exists or not
    missing_fields = [
        F.col(f"event.{field}").isNull()
        for field in REQUIRED_FIELDS
    ]

    invalid_condition = (
        F.col("event").isNull()
        | reduce(or_, missing_fields)
    )

    return df.withColumn(
        "is_invalid",
        invalid_condition
    )

def write_invalid_batch(df, batch_id):

    if df.isEmpty():
        return

    query = """
        INSERT INTO invalid_kafka_events (
            topic,
            partition_id,
            message_offset,
            raw_message,
            rejection_reasons
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (topic, partition_id, message_offset)
        DO NOTHING;
    """

    rows = (
        (
            row["topic"],
            row["partition_id"],
            row["message_offset"],
            bytes(row["raw_message"]) if row["raw_message"] is not None else None,
            row["rejection_reasons"]
        )
        for row in df.toLocalIterator()
    )

    with psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"]
    ) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)

    print(f"[Quarantine] Batch {batch_id} committed successfully", flush=True)

def write_rolling_batch(df, batch_id):

    if df.isEmpty():
        return

    query = """
        INSERT INTO stg_county_rolling_24h (
            source_topic,
            state_code,
            county_code,
            window_end_utc,
            avg_pm25,
            measurement_count
        )
        VALUES (%s, %s, %s, %s, %s, %s)

        ON CONFLICT (
            source_topic,
            state_code,
            county_code,
            window_end_utc
        )
        DO UPDATE SET
            avg_pm25 = EXCLUDED.avg_pm25,
            measurement_count = EXCLUDED.measurement_count;
    """

    rows = (
        (
            row["source_topic"],
            row["state_code"],
            row["county_code"],
            row["window_end_utc"],
            row["avg_pm25"],
            row["measurement_count"]
        )
        for row in df.toLocalIterator()
    )

    with psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"]
    ) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(query, rows)

    print(f"[Rolling] Batch {batch_id} committed", flush=True)

def publish_rolling_max():

    topic = os.environ["KAFKA_TOPIC"]

    if topic != "pm25-hourly":
        raise ValueError("Gold publication requires the production Kafka topic")

    query = """
        INSERT INTO county_rolling_24h_max (
            state_code,
            county_code,
            window_end_utc,
            max_avg_pm25,
            measurement_count
        )
        SELECT
            state_code,
            county_code,
            window_end_utc,
            avg_pm25,
            measurement_count
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY state_code, county_code
                    ORDER BY avg_pm25 DESC, window_end_utc DESC
                ) AS rank
            FROM stg_county_rolling_24h
            WHERE source_topic = %s
        ) ranked
        WHERE rank = 1;
    """

    with psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"]
    ) as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM stg_county_rolling_24h
                WHERE source_topic = %s
                """,
                (topic,)
            )

            if cursor.fetchone()[0] == 0:
                raise RuntimeError("No rolling windows available to publish")

            cursor.execute("TRUNCATE TABLE county_rolling_24h_max")
            cursor.execute(query, (topic,))

            published = cursor.rowcount

    print(f"[Gold] Published {published} county maxima", flush=True)

spark = (
    SparkSession.builder
    .appName("PM25-Streaming")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    .option("subscribe", os.environ["KAFKA_TOPIC"])
    .option("startingOffsets", "earliest")
    .option(
        "maxOffsetsPerTrigger",
        int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000"))
    )
    .load()
)

parsed_df = kafka_df.select(
    "topic",
    "partition",
    "offset",
    F.col("key").cast("string").alias("kafka_key"),
    F.col("value").alias("raw_message"),
    F.from_json(
        F.col("value").cast("string"),
        EVENT_SCHEMA
    ).alias("event")
)

validated_df = validate_event(parsed_df)

valid_df = validated_df.filter(
    ~F.col("is_invalid")
)

invalid_df = (
    validated_df
    .filter(F.col("is_invalid"))
    .withColumn("rejection_reason", F.lit("INVALID_SCHEMA"))
)

quarantine_df = invalid_df.select(
    F.col("topic"),
    F.col("partition").alias("partition_id"),
    F.col("offset").alias("message_offset"),
    F.col("raw_message"),
    F.array(F.col("rejection_reason")).alias("rejection_reasons")
)

valid_events_df = valid_df.select(
    F.col("topic").alias("source_topic"),
    F.col("event.state_code").alias("state_code"),
    F.col("event.county_code").alias("county_code"),
    F.col("event.event_time_utc").cast("timestamp").alias("event_time_utc"),
    F.col("event.pm25").alias("pm25")
)

FULL_DAY_MASK = (1 << 24) - 1

rolling_df = (
    valid_events_df
    .groupBy(
        "source_topic",
        "state_code",
        "county_code",
        F.window(
            "event_time_utc",
            "24 hours",
            "1 hour"
        ).alias("window")
    )
    .agg(
        F.avg("pm25").alias("avg_pm25"),
        F.count("pm25").alias("measurement_count"),
        F.bit_or(
            F.expr("1 << hour(event_time_utc)")
        ).alias("hour_mask")
    )
    .filter(F.col("hour_mask") == FULL_DAY_MASK)
    .select(
        "source_topic",
        "state_code",
        "county_code",
        F.col("window.end").alias("window_end_utc"),
        "avg_pm25",
        "measurement_count"
    )
)

invalid_query = (
    quarantine_df.writeStream
    .foreachBatch(write_invalid_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        os.getenv(
            "CHECKPOINT_LOCATION",
            "/app/checkpoints/invalid_events"
        )
    )
    .trigger(availableNow=True)
    .start()
)

rolling_query = None

if ENABLE_ROLLING:
    rolling_query = (
        rolling_df.writeStream
        .foreachBatch(write_rolling_batch)
        .outputMode("update")
        .option(
            "checkpointLocation",
            os.getenv(
                "ROLLING_CHECKPOINT_LOCATION",
                "/app/checkpoints/rolling_24h_v2"
            )
        )
        .trigger(availableNow=True)
        .start()
    )

invalid_query.awaitTermination()

if rolling_query is not None:
    rolling_query.awaitTermination()

if PUBLISH_GOLD:
    if not ENABLE_ROLLING:
        raise ValueError("Cannot publish Gold with rolling aggregation disabled")

    publish_rolling_max()

spark.stop()