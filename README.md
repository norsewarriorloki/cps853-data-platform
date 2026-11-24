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



