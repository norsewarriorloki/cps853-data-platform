# CPS853 Data Platform

A PM2.5 pipeline using Spark, Kafka, and PostgreSQL.

Requires Docker Compose, GNU Make, and Python 3. Run commands from the repository root.
Place the input CSV at `data/raw/hourly_88101_2024/hourly_88101_2024.csv`.

## Run

```sh
make build       # Build the Docker images
make pipeline    # Run the full batch and streaming pipeline
make answers     # Show the database answers and query response times
make down        # Stop services and keep the data
```

## Commands

Individual stages use existing upstream data. Run them in this order:
`silver` → `dimensions` → `gold` → `produce` → `consume`.

| Command | What it does |
| --- | --- |
| `make help` | List commands and options. Also shown by running `make`. |
| `make build` | Build pipeline Docker images. |
| `make pipeline` | Run all batch and streaming stages in order. |
| `make batch` | Run Silver processing, dimension loading, and batch Gold. |
| `make stream` | Replay existing Silver data into Kafka, then consume it. Requires loaded dimensions for Gold publication. |
| `make silver` | Validate the CSV; save accepted and rejected rows as Parquet. |
| `make dimensions` | Load state and county lookup tables from Silver. |
| `make gold` | Show the two DataFrame answers and publish batch aggregates. Requires Silver and dimensions. |
| `make produce` | Replay Silver observations into Kafka in event-time order. |
| `make consume` | Process available Kafka events, quarantine invalid records, and publish rolling results. |
| `make answers` | Show the three database answer tables and query timings. |
| `make infra` | Start Kafka and PostgreSQL and wait until ready. |
| `make init` | Prepare database tables, the Kafka topic, and checkpoint storage. |
| `make db-init` | Create database tables if missing. |
| `make kafka-init` | Create the Kafka topic if missing. |
| `make checkpoints` | Prepare checkpoint storage permissions. |
| `make test-validation` | Test Silver validation and show rejected Parquet rows. |
| `make test-invalid` | Test Kafka quarantine and replay; show rejected database records. |
| `make test-consumer-sinks` | Test bulk database writes, rollback, retries, and cache cleanup. |
| `make test-quality` | Run batch validation, consumer sink, and Kafka quarantine tests. |
| `make test-rolling` | Test rolling averages and Gold publication. Requires loaded dimensions. |
| `make status` | Show service containers. |
| `make logs` | Follow service logs. |
| `make down` | Stop services while preserving data. |
| `make clean` | **Delete Kafka data, database data, Silver/rejected files, and checkpoints** for a fresh run. Preserves raw input, logs, and evidence. |

## Common options

```sh
make produce MAX_EVENTS=1000                   # Replay only 1,000 events; 0 means all
make consume MAX_OFFSETS_PER_TRIGGER=200000    # Maximum input events per micro-batch
make logs SERVICE=consumer                     # Follow one service's logs
```
