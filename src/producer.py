import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

measurements = [
    {"station_id": "NYC_001", "pm25": 42.5, "hour": 12},
    {"station_id": "LA_001", "pm25": 18.2, "hour": 12},
    {"station_id": "NYC_001", "pm25": 44.1, "hour": 13},
    {"station_id": "LA_001", "pm25": 19.6, "hour": 13},
]

def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Key: {message.key().decode()}, "
            f"Partition: {message.partition()}, "
            f"Offset: {message.offset()}"
        )

for measurement in measurements:
    producer.produce(
        topic="air_quality",
        key=measurement["station_id"],
        value=json.dumps(measurement),
        on_delivery=delivery_report
    )

producer.flush()

