"""Synthetic tests of the real batch validator; run with make test-validation."""

from collections import Counter
from pathlib import Path
import sys
import tempfile
import unittest

from pyspark.sql import SparkSession, functions as F
from table_output import print_table

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/spark-batch"))
from quality_checks import REQUIRED_FIELDS, split_valid_invalid


BASE = {
    "State Code": "06", "County Code": "037", "Site Num": "1103",
    "POC": "1", "Parameter Code": "88101", "Sample Measurement": "12.5",
    "Date GMT": "2024-06-01", "Time GMT": "12:00",
    "Date Local": "2024-06-01", "Time Local": "05:00",
    "Qualifier": None, "Uncertainty": None, "MDL": "20",
}


def build_cases():
    cases = []

    def add(name, changes=None, reasons=()):
        # Independent cases must not accidentally share a duplicate key.
        row = {**BASE, "Site Num": f"{len(cases) + 1:04d}", **(changes or {})}
        cases.append((name, row, set(reasons)))

    add("valid observation with leading-zero IDs")
    add("optional metadata may be null", {"Qualifier": None, "Uncertainty": None})
    add("measurement below MDL is accepted", {"Sample Measurement": "1.0"})
    add("zero concentration is accepted", {"Sample Measurement": "0"})
    # Current policy reports negative concentrations but does not reject them.
    add("negative concentration follows current acceptance policy", {"Sample Measurement": "-1"})
    for name, date, time in [
        ("start of year", "2024-01-01", "00:00"),
        ("end of year", "2024-12-31", "23:59"),
        ("leap day", "2024-02-29", "12:00"),
    ]:
        add(name, {"Date Local": date, "Date GMT": date, "Time Local": time, "Time GMT": time})
    for field in REQUIRED_FIELDS:
        for label, value in [("null", None), ("empty", ""), ("whitespace", "   ")]:
            add(f"{label} required field: {field}", {field: value}, ["MISSING_REQUIRED"])
    for field, values in [
        ("POC", ["abc", "1.5", "2147483648"]),
        ("Sample Measurement", ["abc", "NaN"]),
    ]:
        for value in values:
            add(f"invalid numeric {field}: {value}", {field: value}, ["MISSING_REQUIRED"])
    for field, value in [
        ("Date GMT", "not-a-date"), ("Date GMT", "2024-02-30"),
        ("Time GMT", "25:00"), ("Date Local", "2024-02-30"),
        ("Time Local", "24:00"), ("Time Local", "12:60"), ("Time Local", "1:00"),
    ]:
        add(f"malformed {field}: {value}", {field: value}, ["INVALID_TIMESTAMP"])
    for field in ["Date GMT", "Time GMT", "Date Local", "Time Local"]:
        for label, value in [("null", None), ("empty", "")]:
            add(f"{label} timestamp component: {field}", {field: value}, ["INVALID_TIMESTAMP"])
    for date in ["2023-12-31", "2025-01-01"]:
        add(f"local date outside 2024: {date}", {"Date Local": date}, ["OUTSIDE_DATE_RANGE"])
    add("UTC may cross year boundary when local date is in 2024", {"Date GMT": "2025-01-01"})
    add("multiple independent violations", {
        "Sample Measurement": None, "Time Local": "25:00", "Date Local": "2023-01-01",
    }, ["MISSING_REQUIRED", "INVALID_TIMESTAMP", "OUTSIDE_DATE_RANGE"])
    add("duplicate group first member", {"Site Num": "9000"}, ["DUPLICATE"])
    add("duplicate group second member", {"Site Num": "9000", "Sample Measurement": "18"}, ["DUPLICATE"])
    add("duplicate group third member with another violation", {
        "Site Num": "9000", "Sample Measurement": None,
    }, ["DUPLICATE", "MISSING_REQUIRED"])
    for field, value in [
        ("State Code", "07"), ("County Code", "059"), ("Site Num", "9001"),
        ("Parameter Code", "88102"), ("POC", "2"),
        ("Date GMT", "2024-06-02"), ("Time GMT", "13:00"),
    ]:
        add(f"different key component remains distinct: {field}", {"Site Num": "9000", field: value})
    return cases


CASES = build_cases()


class BatchValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("BatchValidationTests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.sparkContext.setLogLevel("ERROR")
        columns = ["case_name", *BASE]
        schema = ", ".join(f"`{column}` STRING" for column in columns)
        rows = [(name, *(record[column] for column in BASE)) for name, record, _ in CASES]
        source = cls.spark.createDataFrame(rows, schema)
        valid, invalid = split_valid_invalid(source)
        cls.quarantine = invalid.select("case_name", *BASE, "rejection_reasons")
        cls.results = (
            valid.withColumn("accepted", F.lit(True))
            .unionByName(invalid.withColumn("accepted", F.lit(False)))
            .select("case_name", "accepted", "rejection_reasons")
            .collect()
        )

    def test_partition_integrity(self):
        "Every input appears exactly once across accepted and rejected outputs."
        self.assertEqual(Counter(row.case_name for row in self.results), Counter(name for name, _, _ in CASES))

    def test_quarantine_parquet_round_trip(self):
        "Rejected rows retain raw fields and exact reasons after a Parquet round trip."
        with tempfile.TemporaryDirectory(prefix="quarantine-validation-") as directory:
            path = str(Path(directory) / "rejected")
            self.quarantine.write.mode("overwrite").parquet(path)
            restored = self.spark.read.parquet(path).collect()
            expected = {
                name: (record, reasons) for name, record, reasons in CASES if reasons
            }
            self.assertEqual(Counter(row.case_name for row in restored), Counter(expected.keys()))
            for row in restored:
                record, reasons = expected[row.case_name]
                self.assertEqual({field: row[field] for field in BASE}, record)
                self.assertEqual(set(row.rejection_reasons), reasons)

            print(f"[validation] TEST quarantine Parquet: {len(restored)} rejected synthetic rows read back", flush=True)
            print(f"[validation] Source: {path} (temporary test output; removed after the test)", flush=True)
            examples = [
                "null required field: Sample Measurement",
                "invalid numeric Sample Measurement: abc",
                "malformed Time Local: 24:00",
                "local date outside 2024: 2023-12-31",
                "duplicate group first member",
                "multiple independent violations",
            ]
            by_name = {row.case_name: row for row in restored}
            print_quarantine_preview([by_name[name] for name in examples], synthetic=True)

        pipeline_path = Path("/app/data/rejected/pm25_hourly")
        if pipeline_path.is_dir():
            # Preview only a bounded sample; never rewrite the pipeline artifact.
            fields = ["State Code", "County Code", "Site Num", "Sample Measurement",
                      "Date Local", "Time Local", "rejection_reasons"]
            sample = self.spark.read.parquet(str(pipeline_path)).select(*fields).limit(5).collect()
            print(f"[validation] PIPELINE quarantine Parquet: {pipeline_path} (up to 5 existing rows)", flush=True)
            print_quarantine_preview(sample)
        else:
            print("[validation] PIPELINE quarantine preview skipped: run make silver to create data/rejected/pm25_hourly", flush=True)


def print_quarantine_preview(rows, synthetic=False):
    if not rows:
        print("[validation] No rejected rows found in this Parquet dataset", flush=True)
    print_table(
        ["Test case" if synthetic else "Row", "State", "County", "Site", "PM2.5",
         "Local date", "Time", "Rejection reasons"],
        [[row["case_name"] if synthetic else index,
          row["State Code"], row["County Code"], row["Site Num"],
          row["Sample Measurement"], row["Date Local"], row["Time Local"],
          row["rejection_reasons"]] for index, row in enumerate(rows, 1)],
        [24, 5, 6, 5, 8, 10, 5, 22],
    )
    print("[validation] <NULL> = missing value; <EMPTY> = empty string; quoted spaces = whitespace-only value", flush=True)


def case_test(name, expected):
    def test(self):
        matches = [row for row in self.results if row.case_name == name]
        self.assertEqual(len(matches), 1, f"Expected exactly one output for {name}")
        row = matches[0]
        self.assertEqual(
            set(row.rejection_reasons), expected,
            f"{name}\nExpected: {sorted(expected) or ['ACCEPTED']}"
            f"\nActual:   {sorted(row.rejection_reasons) or ['ACCEPTED']}",
        )
        self.assertEqual(row.accepted, not expected, "Record routed to the wrong output")
    test.__doc__ = f"{name} -> {', '.join(sorted(expected)) or 'ACCEPTED'}"
    return test


for index, (name, _, expected) in enumerate(CASES):
    setattr(BatchValidationTests, f"test_case_{index:03d}", case_test(name, expected))


class TaggedOutput:
    """Keep unittest diagnostics visible through the quiet pipeline runner."""

    def __init__(self):
        self.pending = ""

    def write(self, text):
        self.pending += text
        while "\n" in self.pending:
            line, self.pending = self.pending.split("\n", 1)
            print(f"[validation] {line}", flush=True)

    def flush(self):
        sys.stdout.flush()


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(BatchValidationTests)
    result = unittest.TextTestRunner(verbosity=2, stream=TaggedOutput()).run(suite)
    failed = len(result.failures) + len(result.errors)
    print(f"[validation] Batch validation: {result.testsRun} run, {failed} failures/errors", flush=True)
    sys.exit(0 if result.wasSuccessful() else 1)
