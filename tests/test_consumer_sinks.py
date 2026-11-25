"""Exercise consumer COPY transactions against an isolated PostgreSQL schema."""

from datetime import datetime
import importlib.util
import io
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("consumer", ROOT / "src/kafka-streaming/consumer.py")
consumer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(consumer)
CONNECT = psycopg.connect


class Batch:
    """A bounded batch that can fail during iteration, after COPY has started."""

    def __init__(self, rows, fail_during_copy=False):
        self.rows = rows
        self.fail_during_copy = fail_during_copy
        self.cached = False
        self.released = False

    def persist(self):
        self.cached = True
        return self

    def isEmpty(self):
        assert self.cached, "Emptiness check must use the persisted batch"
        return not self.rows

    def toLocalIterator(self):
        assert self.cached, "COPY must read the persisted batch"
        for row in self.rows:
            yield row
            if self.fail_during_copy:
                raise RuntimeError("Injected COPY input failure")

    def unpersist(self):
        self.cached = False
        self.released = True


class ConsumerSinkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = f"sink_test_{uuid4().hex}"
        cls.config = dict(
            host=os.environ["PGHOST"], port=os.getenv("PGPORT", "5432"),
            dbname=os.environ["PGDATABASE"], user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
        )
        with CONNECT(**cls.config, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(cls.schema)))
        cls.addClassCleanup(cls.drop_schema)
        with cls.connect() as conn:
            conn.execute("""
                CREATE TABLE invalid_kafka_events (
                    topic VARCHAR(100) NOT NULL, partition_id INTEGER NOT NULL,
                    message_offset BIGINT NOT NULL, raw_message BYTEA,
                    rejection_reasons TEXT[] NOT NULL,
                    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (topic, partition_id, message_offset)
                )
            """)
            conn.execute("""
                CREATE TABLE stg_county_rolling_24h (
                    source_topic TEXT NOT NULL, state_code VARCHAR(2) NOT NULL,
                    county_code VARCHAR(3) NOT NULL, window_end_utc TIMESTAMP NOT NULL,
                    avg_pm25 DOUBLE PRECISION NOT NULL, measurement_count BIGINT NOT NULL,
                    PRIMARY KEY (source_topic, state_code, county_code, window_end_utc)
                )
            """)

    @classmethod
    def connect(cls, **kwargs):
        # Force every callback connection into the test schema, never production.
        return CONNECT(**(kwargs or cls.config), options=f"-c search_path={cls.schema}")

    @classmethod
    def drop_schema(cls):
        with CONNECT(**cls.config, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(cls.schema)))

    def setUp(self):
        self.topic = f"sink-{uuid4().hex}"

    def invalid(self, offset=1, **changes):
        return dict(topic=self.topic, partition_id=0, message_offset=offset,
                    raw_message=bytearray(b'\x00\xff\t\n\\"raw"'),
                    rejection_reasons=["INVALID_SCHEMA", "unicode: \u00e9"], **changes)

    def rolling(self, **changes):
        return {
            "source_topic": self.topic, "state_code": "06", "county_code": "037",
            "window_end_utc": datetime(2024, 1, 2), "avg_pm25": 10.0,
            "measurement_count": 24, **changes,
        }

    def write(self, sink, batch):
        with patch.object(consumer.psycopg, "connect", side_effect=self.connect):
            sink(batch, 1)
        self.assertTrue(batch.released)
        self.assertFalse(batch.cached)

    def records(self, rolling=False):
        table = "stg_county_rolling_24h" if rolling else "invalid_kafka_events"
        topic_column = "source_topic" if rolling else "topic"
        with self.connect() as conn:
            return conn.execute(sql.SQL("SELECT * FROM {} WHERE {} = %s ORDER BY 1, 2, 3").format(
                sql.Identifier(table), sql.Identifier(topic_column)), (self.topic,)).fetchall()

    def test_empty_batches_release_cache_without_connecting(self):
        for sink in [consumer.write_invalid_batch, consumer.write_rolling_batch]:
            with self.subTest(sink=sink.__name__):
                batch = Batch([])
                with patch.object(consumer.psycopg, "connect", side_effect=AssertionError("Unexpected connection")):
                    sink(batch, 0)
                self.assertTrue(batch.released)
                self.assertFalse(batch.cached)

    def test_quarantine_copy_preserves_payload_arrays_and_replay_timestamp(self):
        first = self.invalid()
        self.write(consumer.write_invalid_batch, Batch([first, self.invalid(offset=2)]))
        before = self.records()
        self.assertEqual(len(before), 2)
        self.assertTrue(all(row[3] == bytes(first["raw_message"]) for row in before))
        self.assertTrue(all(row[4] == first["rejection_reasons"] for row in before))
        replay = {**first, "raw_message": bytearray(b"changed")}
        self.write(consumer.write_invalid_batch, Batch([replay, self.invalid(offset=2)]))
        self.assertEqual(self.records(), before)

    def test_rolling_copy_updates_existing_window_and_replays_safely(self):
        self.write(consumer.write_rolling_batch, Batch([self.rolling()]))
        updated = self.rolling(avg_pm25=12.88, measurement_count=25)
        self.write(consumer.write_rolling_batch, Batch([updated]))
        before = self.records(rolling=True)
        self.assertEqual(len(before), 1)
        self.assertAlmostEqual(before[0][4], 12.88)
        self.assertEqual(before[0][5], 25)
        self.write(consumer.write_rolling_batch, Batch([updated]))
        self.assertEqual(self.records(rolling=True), before)

    def test_copy_failure_rolls_back_and_releases_cache_for_both_sinks(self):
        for rolling in [False, True]:
            with self.subTest(rolling=rolling):
                sink = consumer.write_rolling_batch if rolling else consumer.write_invalid_batch
                row = self.rolling() if rolling else self.invalid()
                batch = Batch([row], fail_during_copy=True)
                with self.assertRaisesRegex(RuntimeError, "Injected COPY input failure"):
                    self.write(sink, batch)
                self.assertTrue(batch.released)
                self.assertFalse(batch.cached)
                self.assertEqual(self.records(rolling), [])
                self.write(sink, Batch([row]))
                self.assertEqual(len(self.records(rolling)), 1)

    def test_merge_failure_rolls_back_entire_batch_and_allows_retry(self):
        for rolling in [False, True]:
            with self.subTest(rolling=rolling):
                sink = consumer.write_rolling_batch if rolling else consumer.write_invalid_batch
                first = self.rolling() if rolling else self.invalid()
                invalid = (
                    self.rolling(county_code="059", measurement_count=None) if rolling
                    else {**self.invalid(offset=2), "rejection_reasons": None}
                )
                batch = Batch([first, invalid])
                with self.assertRaises(psycopg.errors.NotNullViolation):
                    self.write(sink, batch)
                self.assertTrue(batch.released)
                self.assertFalse(batch.cached)
                self.assertEqual(self.records(rolling), [])
                self.write(sink, Batch([first]))
                self.assertEqual(len(self.records(rolling)), 1)


if __name__ == "__main__":
    output = io.StringIO()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(ConsumerSinkTests)
    result = unittest.TextTestRunner(verbosity=2, stream=output).run(suite)
    for line in output.getvalue().splitlines():
        print(f"[validation] {line}", flush=True)
    sys.exit(0 if result.wasSuccessful() else 1)
