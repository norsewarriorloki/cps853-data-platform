

#!/usr/bin/env bash
set -euo pipefail

# Unique topic and checkpoints for every test run
RUN_ID=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')
TOPIC="pm25-rolling-test-${RUN_ID}"

echo "[1/6] Starting infrastructure"
docker compose up -d --wait kafka postgres

echo "[2/6] Initializing database tables"
docker compose run --rm -T db-init

echo "[3/6] Preparing Spark checkpoints"
docker compose run --rm --no-deps -T spark-checkpoint-init

echo "[4/6] Creating Kafka test topic"
docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1

echo "[5/6] Publishing controlled measurements"

python3 - <<'PY' | docker compose exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$TOPIC" \
    --property parse.key=true \
    --property 'key.separator=|'

import json
from datetime import datetime, timedelta, timezone

start = datetime(2024, 1, 1, tzinfo=timezone.utc)

# County 037: 25 hours of data
# County 059: 23 hours of data (incomplete)
for county, hours in [("037", 25), ("059", 23)]:
    for hour in range(hours):

        pm25 = (
            34.0 if county == "037" and hour == 24
            else 20.0 if county == "059"
            else 10.0
        )

        event = {
            "state_code": "06",
            "county_code": county,
            "site_num": "1103",
            "poc": 1,
            "event_time_utc": (
                start + timedelta(hours=hour)
            ).isoformat(),
            "pm25": pm25
        }

        print(f"06:{county}|{json.dumps(event)}")

# Additional measurement for County 037 at hour 24.
# This should update an existing rolling window.
extra_event = {
    "state_code": "06",
    "county_code": "037",
    "site_num": "1104",
    "poc": 1,
    "event_time_utc": (
        start + timedelta(hours=24)
    ).isoformat(),
    "pm25": 58.0
}

print(f"06:037|{json.dumps(extra_event)}")
PY

echo "[6/6] Running Spark Structured Streaming"

# Record production Gold table count before processing
GOLD_BEFORE=$(docker compose run --rm --no-deps -T \
    --entrypoint psql db-init \
    -X -A -t \
    -c "SELECT COUNT(*) FROM county_rolling_24h_max;")

# Process test topic using small micro-batches
python3 scripts/run_stage.py validation-consumer docker compose run --rm --no-deps -T \
    -e KAFKA_TOPIC="$TOPIC" \
    -e ENABLE_ROLLING=true \
    -e PUBLISH_GOLD=false \
    -e MAX_OFFSETS_PER_TRIGGER=5 \
    -e CHECKPOINT_LOCATION="/app/checkpoints/invalid_${RUN_ID}" \
    -e ROLLING_CHECKPOINT_LOCATION="/app/checkpoints/rolling_${RUN_ID}" \
    consumer

echo "Verifying rolling-window calculations"

RESULT=$(docker compose run --rm --no-deps -T \
    --entrypoint psql db-init \
    -X -A -t \
    -c "
    SELECT CASE WHEN
        COUNT(*) = 2

        AND COUNT(*) FILTER (
            WHERE state_code = '06'
              AND county_code = '037'
              AND window_end_utc = TIMESTAMP '2024-01-02 00:00:00'
              AND ABS(avg_pm25 - 10.0) < 0.000001
              AND measurement_count = 24
        ) = 1

        AND COUNT(*) FILTER (
            WHERE state_code = '06'
              AND county_code = '037'
              AND window_end_utc = TIMESTAMP '2024-01-02 01:00:00'
              AND ABS(avg_pm25 - 12.88) < 0.000001
              AND measurement_count = 25
        ) = 1

    THEN 'PASS' ELSE 'FAIL' END
    FROM stg_county_rolling_24h
    WHERE source_topic = '$TOPIC';
    ")

# Verify production Gold was not modified
GOLD_AFTER=$(docker compose run --rm --no-deps -T \
    --entrypoint psql db-init \
    -X -A -t \
    -c "SELECT COUNT(*) FROM county_rolling_24h_max;")

# Display the actual PostgreSQL results
echo ""
echo "========================================="
echo "Rolling 24-hour aggregation results"
echo "========================================="

docker compose run --rm --no-deps -T \
    --entrypoint psql db-init \
    -X \
    -c "
    SELECT
        state_code,
        county_code,
        window_end_utc,
        ROUND(avg_pm25::numeric, 2) AS avg_pm25,
        measurement_count
    FROM stg_county_rolling_24h
    WHERE source_topic = '$TOPIC'
    ORDER BY county_code, window_end_utc;
    "

echo ""
echo "========================================="
echo "Integration test summary"
echo "========================================="

if [[ "$RESULT" == "PASS" && "$GOLD_BEFORE" == "$GOLD_AFTER" ]]; then
    echo "PASS: Rolling 24-hour averages are correct."
    echo "PASS: Existing windows update across micro-batches."
    echo "PASS: Incomplete windows were excluded."
    echo "PASS: Production Gold row count is unchanged."
else
    echo "FAIL: Rolling-window integration test failed."
    echo "Validation result: $RESULT"
    echo "Gold count before: $GOLD_BEFORE"
    echo "Gold count after:  $GOLD_AFTER"
    echo "Test topic: $TOPIC"
    exit 1
fi

echo ""
echo ""
echo "========================================="
echo "Testing final Gold publication"
echo "========================================="

docker compose run --rm --no-deps -T \
    --entrypoint psql db-init \
    -X -v ON_ERROR_STOP=1 \
    -v "test_topic=$TOPIC" <<'SQL'

BEGIN;

-- Make the test topic available to SQL assertions
SELECT set_config('app.test_topic', :'test_topic', true);

-- Check that the test county exists in our dimensions
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM dim_county
        WHERE state_code = '06'
          AND county_code = '037'
    ) THEN
        RAISE EXCEPTION 'Missing test county in dim_county';
    END IF;
END $$;

-- Temporarily replace the Gold results
TRUNCATE TABLE county_rolling_24h_max;

-- Execute the same publication logic used by consumer.py
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
        ) AS rn
    FROM stg_county_rolling_24h
    WHERE source_topic = current_setting('app.test_topic')
) ranked
WHERE rn = 1;

-- Assert the exact expected Gold result
DO $$
DECLARE
    result RECORD;
    result_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO result_count
    FROM county_rolling_24h_max;

    IF result_count <> 1 THEN
        RAISE EXCEPTION
            'Expected 1 Gold row, found %',
            result_count;
    END IF;

    SELECT *
    INTO result
    FROM county_rolling_24h_max;

    IF result.state_code <> '06'
       OR result.county_code <> '037'
       OR result.window_end_utc <> TIMESTAMP '2024-01-02 01:00:00'
       OR ABS(result.max_avg_pm25 - 12.88) > 0.000001
       OR result.measurement_count <> 25
    THEN
        RAISE EXCEPTION 'Incorrect Gold maximum result';
    END IF;
END $$;

-- Display the result before rolling back
SELECT
    state_code,
    county_code,
    window_end_utc,
    ROUND(max_avg_pm25::numeric, 2) AS max_avg_pm25,
    measurement_count
FROM county_rolling_24h_max;

-- Restore production Gold data
ROLLBACK;

SQL

echo "PASS: Final Gold publication selected the correct maximum."
echo "PASS: Publication transaction rolled back."
echo "Test topic: $TOPIC"
