#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
RUN_ID=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')
TOPIC="pm25-validation-test-${RUN_ID}"
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
trap 'echo "FAIL: Streaming validation stopped at line $LINENO (topic: $TOPIC)" >&2' ERR

echo "[1/7] Starting Kafka and PostgreSQL"
docker compose up -d --wait kafka postgres

echo "[2/7] Initializing quarantine table and checkpoint storage"
docker compose run --rm --no-deps -T --entrypoint psql db-init \
    -X -v ON_ERROR_STOP=1 -f /sql/003_create_invalid_events.sql
docker compose run --rm --no-deps -T spark-checkpoint-init

echo "[3/7] Creating isolated topic: $TOPIC"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --topic "$TOPIC" \
    --partitions 1 --replication-factor 1

python3 - "$WORK_DIR" <<'PY'
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
base = dict(state_code="06", county_code="037", site_num="1103", poc=1,
            event_time_utc="2024-08-01T00:00:00Z", pm25=12.5)
cases = []
def add(name, event, rejected=True, raw=False):
    cases.append(dict(name=name, payload=event if raw else json.dumps(event), rejected=rejected))

add("valid observation", base, rejected=False)
add("zero concentration is valid", dict(base, pm25=0), rejected=False)
add("extra metadata is allowed", dict(base, test_metadata="control"), rejected=False)
add("malformed JSON", '{"state_code":', raw=True)
add("JSON null", "null", raw=True)
for field in base:
    missing = dict(base)
    del missing[field]
    add(f"missing required field: {field}", missing)
    add(f"null required field: {field}", dict(base, **{field: None}))
add("nonnumeric PM2.5", dict(base, pm25="invalid"))
add("nonnumeric POC", dict(base, poc="invalid"))
add("object instead of PM2.5", dict(base, pm25={"value": 12}))
(root / "expected.json").write_text(json.dumps(cases))
(root / "events.jsonl").write_text("".join(case["payload"] + "\n" for case in cases))
print(f"Prepared {len(cases)} named streaming cases")
PY

echo "[4/7] Publishing cases in known offset order"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic "$TOPIC" \
    --producer-property acks=all < "$WORK_DIR/events.jsonl"

consume() {
    python3 scripts/run_stage.py validation-consumer docker compose run --rm --no-deps -T \
        -e KAFKA_TOPIC="$TOPIC" \
        -e ENABLE_ROLLING=false -e PUBLISH_GOLD=false \
        -e MAX_OFFSETS_PER_TRIGGER=5 \
        -e CHECKPOINT_LOCATION="/app/checkpoints/validation_${RUN_ID}_$1" \
        consumer
}

snapshot() {
    docker compose run --rm --no-deps -T --entrypoint psql db-init \
        -X -v ON_ERROR_STOP=1 -At -c "
        SELECT row_to_json(result) FROM (
            SELECT partition_id, message_offset,
                   encode(raw_message, 'hex') AS payload_hex,
                   rejection_reasons, rejected_at
            FROM invalid_kafka_events
            WHERE topic = '$TOPIC'
            ORDER BY partition_id, message_offset
        ) result;" > "$WORK_DIR/$1.jsonl"
}

echo "[5/7] Processing small micro-batches (Gold publication disabled)"
consume initial
snapshot initial

echo "[6/7] Replaying from a fresh checkpoint to test quarantine idempotence"
consume replay
snapshot replay

echo "[7/7] Checking exact quarantine records and preserved payloads"
python3 - "$WORK_DIR" <<'PY'
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
cases = json.loads((root / "expected.json").read_text())
failures = 0
checks = 0

def check(name, passed, expected, actual):
    global failures, checks
    checks += 1
    print(f"{'PASS' if passed else 'FAIL'}  {name}")
    if not passed:
        failures += 1
        print(f"      Expected: {expected}\n      Actual:   {actual}")

snapshots = {}
for phase in ["initial", "replay"]:
    rows = [json.loads(line) for line in (root / f"{phase}.jsonl").read_text().splitlines()]
    snapshots[phase] = rows
    expected_offsets = {offset for offset, case in enumerate(cases) if case["rejected"]}
    actual_offsets = [row["message_offset"] for row in rows]
    check(f"{phase}: exact rejected offsets and row count",
          len(rows) == len(expected_offsets) and set(actual_offsets) == expected_offsets,
          sorted(expected_offsets), actual_offsets)
    for offset, case in enumerate(cases):
        found = [row for row in rows if row["message_offset"] == offset]
        expected = [] if not case["rejected"] else [{
            "partition_id": 0, "message_offset": offset,
            "payload_hex": case["payload"].encode("utf-8").hex(),
            "rejection_reasons": ["INVALID_SCHEMA"],
        }]
        actual = [{key: row[key] for key in ["partition_id", "message_offset", "payload_hex", "rejection_reasons"]}
                  for row in found]
        check(f"{phase}: {case['name']}", actual == expected, expected, actual)

check("replay preserves existing records and rejection timestamps",
      snapshots["initial"] == snapshots["replay"], snapshots["initial"], snapshots["replay"])
print(f"\nStreaming validation: {checks - failures} passed, {failures} failed")
sys.exit(1 if failures else 0)
PY

echo "Test artifacts retained for inspection: topic $TOPIC; checkpoints validation_${RUN_ID}_{initial,replay}"
