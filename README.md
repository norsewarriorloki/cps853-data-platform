# CPS853 Data Platform Project

Run commands from the repository root with Docker Compose and GNU Make installed.
The streaming integration tests also require host Python 3. Place the source CSV at
`data/raw/hourly_88101_2024/hourly_88101_2024.csv` before running the batch pipeline.

Use `make` or `make help` to list commands. Typical workflows:

```sh
make build                  # Build pipeline images
make pipeline               # Run batch processing, then Kafka replay and consumption
make answers                # Display the analytical results
make down                   # Stop services while keeping data
```

Make commands show application messages and stage start/completion times. Spark's
startup, dependency, scheduler, and progress output stays off the console. Complete
command output is retained under `.logs/` (ignored by Git). Failures return a
nonzero status and show the failed stage, a short Python error when available, and
the diagnostic log path. Use `PIPELINE_VERBOSE=1 make <target>` to stream raw
command output for troubleshooting. `make logs` still shows service logs directly.

Run `make build` once after these logging changes to update the Spark images.
The image config logs Spark errors only and disables console progress bars; the
Make runner captures remaining JVM and dependency messages. Application logs use
stage labels such as `[silver]`, `[producer]`, and `[rolling]`. They report stage
boundaries, committed micro-batches, and producer progress every 100,000 submitted
events. Logging adds no Spark counts or actions. Gold's former preview tables and
display-only count have been removed; use `make answers` to inspect results.
Host Python 3 is required for the Make command runner. Diagnostic logs remain
after `make clean` so failures can still be investigated.

For a project recording, run `make pipeline`, then `make answers`. Each processing
stage names its **Input**, **Process**, and **Output** or destination, including
Parquet paths, Kafka topic, and PostgreSQL tables. The producer prints one actual
JSON event and its state/county key to illustrate the message format. No extra
Spark scans, preview queries, or per-event logging are used for this presentation.
Paths beginning with `/app/data` refer to the repository's `data/` directory mounted
inside containers.

Silver's valid/rejected counts describe validation before the state-code filter;
they are not a claim about the final Silver row count. Streaming quarantine counts
are newly inserted records (zero on a replay of already quarantined events).
Rolling counts are inserted/updated county-window rows, rather than raw event
counts. Gold counts describe the rows actually published to each results table.
Checkpoints are shown so a viewer can understand how processing resumes across runs.

Each processing stage can also run separately using existing upstream data:

```sh
make infra                  # Start Kafka and PostgreSQL and wait for readiness
make init                   # Apply schemas, create the topic, prepare checkpoints
make silver                 # Raw CSV -> Silver Parquet and rejected rows
make dimensions             # Existing Silver -> database dimensions
make gold                   # Existing Silver -> batch Gold (dimensions required)
make produce MAX_EVENTS=1000 # Replay a limited number of Silver rows into Kafka
make consume                # Process available Kafka events, publish results, exit
```

`make batch` runs `silver`, `dimensions`, and `gold` in order. `make stream` runs
initialization, replay, and consumption using existing Silver data. Load dimensions
first when publishing rolling Gold results. Individual targets prepare the services
they need without rerunning upstream processing jobs. The consumer uses persistent
checkpoints to resume processing; replaying the producer adds more Kafka messages.
Stages run sequentially even with `make -j`.

Consumer options can be passed as Make variables:

```sh
make consume ENABLE_ROLLING=false PUBLISH_GOLD=false
make consume MAX_OFFSETS_PER_TRIGGER=5000
make status
make logs SERVICE=consumer
make test-invalid
make test-rolling           # Requires loaded dimensions, including county 06/037
```

Run the validation suites independently of the full pipeline:

```sh
make test-validation        # Synthetic batch cases inside the existing Spark image
make test-invalid           # Kafka -> consumer -> PostgreSQL quarantine assertions
make test-quality           # Both validation suites, stopping if a suite fails
```

The batch suite uses Python's standard-library test runner, so no additional test
packages are needed in the Spark image. It requires no source CSV, Kafka, or
PostgreSQL. Named cases cover null/blank required fields, numeric parsing,
timestamps, year boundaries, leap day, duplicate groups, distinct measurement keys,
and combined violations. Every case checks its exact rejection reasons and output
partition; a separate assertion checks that no input was lost or duplicated.
Successful cases print `ok`; failures show actual and expected results, followed by
a suite summary. Any failure or setup error exits with a nonzero status.

Tests preserve the current validation policy: negative concentrations and values
below MDL are accepted, optional metadata may be null, and the date-range rule uses
the local date. The batch rejection label `MISSING_REQUIRED` also covers invalid
numeric values. Batch processing now prints a full-dataset report with total,
accepted, rejected, and per-reason counts instead of the old two-row sample.
Per-reason counts can overlap for rows with multiple violations.

The streaming suite needs host Python 3 and starts Kafka and PostgreSQL itself.
It tests valid controls, malformed JSON, each missing/null required field, and
incorrect numeric types. It verifies exact quarantined offsets, reasons, and raw
payload bytes, then replays the same topic from a fresh checkpoint and verifies
that records and rejection timestamps are unchanged. Every assertion prints
`PASS` or `FAIL`, with expected and actual values on failure. Rolling aggregation
and Gold publication are disabled for both consumer runs.

Each streaming run uses a unique topic and checkpoint paths, so it does not need
dimensions or a prior pipeline run. Test topics, checkpoints, and quarantine rows
are retained for inspection; the script prints the topic name. `make clean` removes
them along with all other pipeline data as described below. Docker may build or
download missing images and Spark connector dependencies on the first run.

`make clean` **permanently deletes all Kafka messages and topics, PostgreSQL tables
and contents, Spark checkpoints, and generated files under `data/silver`,
`data/rejected`, and `data/checkpoints`**. It stops the entire Compose stack and
removes its volumes, including integration-test topics and checkpoints. The cleanup
uses an Alpine container to remove generated files even if containers own them.
It preserves `data/raw`, source code, SQL, and built images so the pipeline can run
again:

```sh
make clean
make pipeline
make answers
```

## Dataset Schema



