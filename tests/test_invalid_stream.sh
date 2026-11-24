#!/usr/bin/env bash
set -euo pipefail

TOPIC="pm25-validation-test"
CHECKPOINT="/app/checkpoints/invalid_events_test"

# Unique identifier for this test run
RUN_ID=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')

echo "[1/6] Starting Kafka and PostgreSQL"
docker compose up -d --wait kafka postgres

echo "[2/6] Initializing quarantine table"
docker compose run --rm --no-deps -T \
    --entrypoint psql db-init \
    -X -v ON_ERROR_STOP=1 \
    -f /sql/003_create_invalid_events.sql

echo "[3/6] Preparing Spark checkpoint storage"
docker compose run --rm --no-deps spark-checkpoint-init

echo "[4/6] Creating Kafka test topic"
docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1

echo "[5/6] Publishing invalid test event"

printf '{"state_code":"06","county_code":"037","site_num":"1103","poc":1,"event_time_utc":"2024-08-01T00:00:00Z","pm25":"invalid","test_run_id":"%s"}\n' "$RUN_ID" |
    docker compose exec -T kafka \
        /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 \
        --topic "$TOPIC"

echo "[6/6] Running Spark consumer"

docker compose run --rm --no-deps -T \
    -e KAFKA_TOPIC="$TOPIC" \
    -e CHECKPOINT_LOCATION="$CHECKPOINT" \
    consumer

echo "Verifying quarantine result"

COUNT=$(docker compose run --rm --no-deps -T \
    --entrypoint psql db-init \
    -X -At -c "
        SELECT COUNT(*)
        FROM invalid_kafka_events
        WHERE topic = '$TOPIC'
          AND rejection_reasons @> ARRAY['INVALID_SCHEMA']::TEXT[]
          AND position('$RUN_ID' in convert_from(raw_message, 'UTF8')) > 0;
    ")

if [[ "$COUNT" == "1" ]]; then
    echo "PASS: Invalid event was quarantined correctly."
else
    echo "FAIL: Expected 1 quarantined event, found $COUNT."
    exit 1
fi