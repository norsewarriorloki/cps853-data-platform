SHELL := /bin/bash
.DEFAULT_GOAL := help

# Pipeline stages must finish in order, including when invoked with make -j.
.NOTPARALLEL:

COMPOSE := docker compose
RUN_STAGE := python3 scripts/run_stage.py
MAX_EVENTS ?= 0
ENABLE_ROLLING ?= true
PUBLISH_GOLD ?= true
MAX_OFFSETS_PER_TRIGGER ?= 200000
SERVICE ?=

.PHONY: help build infra init db-init kafka-init checkpoints silver dimensions gold \
        produce consume batch stream pipeline answers test-invalid test-rolling \
        status logs down clean postgres-ready kafka-ready test-validation test-quality test-consumer-sinks

help: ## Show commands and configurable options (default)
	@awk 'BEGIN {FS = ":.*## "; print "Usage: make <target> [VARIABLE=value]\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(firstword $(MAKEFILE_LIST))
	@printf '\nOptions: MAX_EVENTS=0 (all), ENABLE_ROLLING=true, PUBLISH_GOLD=true,\n         MAX_OFFSETS_PER_TRIGGER=200000, SERVICE=<service> (for logs)\n'

build: ## Build all pipeline images
	@$(RUN_STAGE) $@ $(COMPOSE) --profile pipeline build

infra: ## Start Kafka and PostgreSQL; wait until healthy
	@$(RUN_STAGE) $@ $(COMPOSE) up -d --wait kafka postgres

postgres-ready:
	@$(RUN_STAGE) $@ $(COMPOSE) up -d --wait postgres

kafka-ready:
	@$(RUN_STAGE) $@ $(COMPOSE) up -d --wait kafka

db-init: postgres-ready ## Apply database schema scripts
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T db-init

kafka-init: kafka-ready ## Create the pipeline Kafka topic if missing
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T kafka-init

checkpoints: ## Prepare Spark checkpoint volume permissions
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T spark-checkpoint-init

init: db-init kafka-init checkpoints ## Initialize infrastructure, schemas, topic, and checkpoints

silver: ## Convert raw CSV into Silver Parquet and quarantine rejected rows
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T spark-batch

dimensions: db-init ## Load dimensions from existing Silver Parquet
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T dimension-loader

gold: db-init ## Publish batch Gold aggregates (requires Silver and dimensions)
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T spark-gold

produce: kafka-init ## Replay existing Silver into Kafka; MAX_EVENTS=0 sends all rows
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T -e MAX_EVENTS="$(MAX_EVENTS)" producer

consume: init ## Process available Kafka events and exit (requires dimensions to publish Gold)
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T \
		-e ENABLE_ROLLING="$(ENABLE_ROLLING)" \
		-e PUBLISH_GOLD="$(PUBLISH_GOLD)" \
		-e MAX_OFFSETS_PER_TRIGGER="$(MAX_OFFSETS_PER_TRIGGER)" consumer

batch: silver dimensions gold ## Run Silver, dimensions, then batch Gold

stream: init produce consume ## Replay existing Silver, then process available Kafka events

pipeline: batch stream ## Run the complete batch and streaming pipeline in order
	@printf '\n[pipeline] Complete: batch processing, Kafka replay, and consumption finished\n[pipeline] View analytical results with: make answers\n'

answers: postgres-ready ## Display analytical query results (requires populated Gold tables)
	@bash scripts/show_answers.sh

test-validation: ## Run synthetic batch validation tests (no dataset or services needed)
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T spark-batch \
		/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
		--conf spark.ui.enabled=false --conf spark.ui.showConsoleProgress=false \
		/app/tests/test_batch_validation.py

test-consumer-sinks: postgres-ready ## Test COPY transactions, replay, rollback, and cache cleanup
	@$(RUN_STAGE) $@ $(COMPOSE) run --rm --no-deps -T \
		--volume "$(CURDIR)/tests:/app/tests:ro" \
		--entrypoint /opt/spark/bin/spark-submit consumer \
		--master 'local[2]' /app/tests/test_consumer_sinks.py

test-quality: test-validation test-consumer-sinks test-invalid ## Run batch, sink transaction, and streaming validation suites

test-invalid: ## Test streaming quarantine cases and replay idempotence
	@bash tests/test_invalid_stream.sh

test-rolling: ## Run the existing rolling-window integration test (requires dimensions)
	@bash tests/test_rolling_stream.sh

status: ## Show all service containers, including completed jobs
	$(COMPOSE) --profile pipeline ps -a

logs: ## Follow service logs; optionally set SERVICE=consumer (or another service)
	$(COMPOSE) --profile pipeline logs --follow $(SERVICE)

down: ## Stop and remove containers; preserve stored data
	@$(RUN_STAGE) $@ $(COMPOSE) --profile pipeline down --remove-orphans

clean: ## DELETE broker, database, checkpoints, and generated data; preserve raw input
	@$(RUN_STAGE) $@ $(COMPOSE) --profile pipeline down --volumes --remove-orphans
	@$(RUN_STAGE) clean-files docker run --rm --mount "type=bind,src=$(CURDIR)/data,dst=/data" \
		alpine:3.20 sh -ec 'rm -rf /data/silver /data/rejected /data/checkpoints'
