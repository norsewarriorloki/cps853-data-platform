import json
import time
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers":"localhost:9092",
    "group.id": "python-crash-demo",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

# Subscribe to our topic
consumer.subscribe(["air_quality"])

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Kafka error: {message.error()}")
            continue

        reading = json.loads(message.value().decode("utf-8"))
        station = (
            message.key().decode("utf-8")
            if message.key()
            else reading.get("station_id", "unknown")
        )

        print(f"{station}: PM2.5 = {reading["pm25"]} offset = {message.offset()}")
        
        if reading['pm25'] > 35:
            print(f"ALERT: High pollution at {station}")

        time.sleep(15)

        consumer.commit(message=message, asynchronous=False)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()

