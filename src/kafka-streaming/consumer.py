import os
from operator import or_
from functools import reduce
import psycopg

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

    

spark = (
    SparkSession.builder
    .appName("PM25-Streaming")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    .option("subscribe", os.environ["KAFKA_TOPIC"])
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
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

invalid_query = (
    quarantine_df.writeStream
    .foreachBatch(write_invalid_batch)
    .outputMode("append")
    .option("checkpointLocation", "/app/checkpoints/invalid_events")
    .trigger(availableNow=True)
    .start()
)
invalid_query.awaitTermination()