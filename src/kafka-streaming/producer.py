import os
from datetime import timezone
from confluent_kafka import Producer
import json
import duckdb

PARQUET_GLOB = os.environ["PARQUET_GLOB"]
EVENT_COLUMNS = [
    "state_code",
    "county_code",
    "site_num",
    "poc",
    "event_time_utc",
    "pm25"
]
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "1000"))

def delivery_report(err, msg):
    global failed_count
    global delivered_count
    if err is not None:
        failed_count += 1
        print(f"Delivery failed: {err}")
    else:
        delivered_count += 1

conn = duckdb.connect()
producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]
})

columns = ", ".join(EVENT_COLUMNS)
limit_clause = f"LIMIT {MAX_EVENTS}" if MAX_EVENTS > 0 else ""
result = conn.execute(
    f"""
    SELECT {columns}
    FROM read_parquet(?)
    ORDER BY event_time_utc ASC
    {limit_clause}
    """,
    [PARQUET_GLOB]
)

delivered_count = 0
failed_count = 0
submitted_count = 0
while True:
    rows = result.fetchmany(1000)

    if not rows:
        break

    if submitted_count % 100000 == 0:
        print(f"Submitted {submitted_count:,} messages")

    for row in rows:
        event = dict(zip(EVENT_COLUMNS,row))
        event["event_time_utc"] = (
            event["event_time_utc"]
            .replace(tzinfo=timezone.utc)
            .isoformat()
        )

        # Construct kafka key and message from event data
        message = json.dumps(event).encode("utf-8")
        key = f'{event["state_code"]}:{event["county_code"]}'
        while True:
            try:
                # Add a message to the broker 
                producer.produce(
                    topic=os.environ["KAFKA_TOPIC"],
                    key=key.encode("utf-8"),
                    value=message,
                    on_delivery=delivery_report
                )

                submitted_count +=1
                break

            except BufferError:
                producer.poll(1)

        producer.poll(0)

remaining = producer.flush(60)

if remaining > 0 or failed_count > 0 or delivered_count != submitted_count:
    raise RuntimeError("Kafka producer did not deliver all messages")

print(f"Delivered messages: {delivered_count}")
print(f"Submitted messages: {submitted_count}")
print(f"Failed messages: {failed_count}")
print(f"Undelivered messages: {remaining}")
