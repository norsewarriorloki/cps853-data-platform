# PM2.5 Data Platform — Architecture & Decision Log

**Status:** Working design notes; update as implementation progresses.  
**Data scope:** U.S. EPA hourly PM2.5 (parameter 88101), calendar year 2024.

> **Guiding rule:** Keep *Silver* at the individual monitor-hour observation level. Calculate county-level averages in *Gold*. This preserves the ability to revise aggregation rules and audit data quality.

## Confirmed decisions

| Area | Decision | Rationale |
|---|---|---|
| Dataset | Use the 2024 EPA hourly PM2.5 CSV (parameter code `88101`). | Supports historical and streaming analyses of fine-particle pollution. |
| Development setup | Work with a locally supplied CSV for now; automate downloading later. | Build and verify one pipeline stage at a time. |
| Source storage (Bronze) | Keep the original CSV under `data/raw/`, unmodified. | Preserve source records for reproducibility and debugging. |
| Clean storage (Silver) | Use compressed Parquet in `data/silver/`. | Efficient column-oriented reads for Spark and the Kafka replay producer. |
| Silver granularity | Preserve **one accepted monitoring observation per monitor and hour**, rather than grouping by county. | Avoid losing monitor-level details and support reprocessing. |
| Gold granularity | Compute **county-level pollution averages** for county/hour, county/day, and rolling-window analyses as needed. | Answer questions at a meaningful regional level. |
| Processing | Use a Spark batch job in a Docker container running in local mode. | Fits a single laptop; no separate Spark cluster required. |
| Streaming | Replay chronological events from Silver Parquet using a Python producer → Kafka → consumer. | Simulate hourly arrivals and maintain running results. |
| Kafka deployment | One KRaft Kafka server running combined `broker,controller` roles. | Simple single-node development setup. |
| Networking | Use internal Docker Compose service names (e.g., `kafka:9092`, `postgres:5432`); publish ports only if host access is needed. | Containers can communicate on the Compose network. |
| Serving | Write batch and streaming Gold results to one PostgreSQL database. | Serve three analytical queries and measure query response times. |
| Data overlap | Do not sum batch and stream aggregates from overlapping input data. | Prevent double-counting. |
| Git | Ignore raw CSV, Silver output, checkpoints, virtual environments, and secrets; commit source, Compose configuration, and docs. | Keep repository manageable. |
| Future setup | Aim for ≤3 startup commands from a fresh clone, including automated data acquisition eventually. | Assignment requirement. |

## Research questions

1. **Batch:** Which U.S. states had the highest average PM2.5 concentrations in 2024?
2. **Batch:** Which **25 county–day combinations** had the highest daily average PM2.5 concentrations in 2024?
3. **Streaming:** Which **counties** had the highest rolling **24-hour** average PM2.5 concentrations as hourly measurements were replayed?

**Terminology:** For questions 2 and 3, *region* currently means **county**, identified by `(state_code, county_code)`, not county name alone. Confirm this geographic definition before final implementation.

## Bronze → Silver design

### Source-row interpretation

Each raw CSV row represents a reported hourly PM2.5 observation at a monitor. The first inspected records were all from Alabama, Baldwin County, site `0010`, POC `3`, at successive local hours.

### Proposed Silver fields (pending full-file profiling)

- Identifiers: `state_code`, `county_code`, `site_num`, `poc`, `parameter_code`
- Geography: `state_name`, `county_name`, `latitude`, `longitude`
- Time: `event_time_utc` (combine `Date GMT` + `Time GMT`), `local_date` (and optionally `local_time`)
- Measurement: `pm25` (`double`), `units`
- Quality/provenance: `qualifier`, `method_code`, possibly `mdl` and `date_of_last_change`

Keep location IDs as **strings** to preserve leading zeros. Do not treat null optional metadata (`Uncertainty`, `Qualifier`) as automatic grounds for rejection. A concentration below `MDL` is not automatically invalid. Define quality policy explicitly.

### Data-quality expectations

- Automate **at least four checks**, each reporting failed-row counts (e.g., missing identity/time/measurement fields, parse failures, implausible values, repeated measurement keys, unit/schema inconsistencies).
- Count rows **in and out at each pipeline stage**.
- Keep an auditable rejected-record collection/table rather than silently discarding failures if feasible.
- Determine a valid measurement key before deduplication; a candidate is `(state_code, county_code, site_num, parameter_code, poc, event_time_utc)`.

## County-level aggregation design

**Proposed approach, not final:**

1. Start with validated monitor-hour observations from Silver.
2. Resolve multiple POCs at one site/hour so a site with several monitors does not unintentionally dominate its county average (aggregation/selection rule **TBD**).
3. Compute a **county-hour average** across eligible sites reporting in that hour.
4. Derive county-day averages and 24-hour rolling county averages from consistent hourly values, with clear rules for missing hours and minimum coverage.
5. For question 1, define how county or site summaries contribute to a **state** average. Avoid accidentally weighting states by the number or reporting frequency of monitors.

These are statistical design decisions, not just Spark implementation details. Record the chosen weighting and completeness rules before claiming county/state rankings.

## Processing and storage plan

```text
data/raw/*.csv  (Bronze)
        |
        v
Spark: inspect → type → validate → preserve rejected rows
        |
        v
data/silver/pm25_hourly/  (Parquet, monitor-hour)
        |                              |
        v                              v
Spark batch Gold aggregates      Python Kafka producer
        |                              |
        |                              v
        |                         Kafka topic
        |                              |
        |                              v
        |                      Stream consumer
        |                              |
        +-------------+----------------+
                      v
                  PostgreSQL
                (Gold serving)
```

### Planned components

- `postgres`: persistent Gold result tables.
- `kafka`: persistent single-node KRaft broker/controller.
- `spark-batch`: one-off local PySpark job for Silver and batch Gold.
- `producer`: one-off chronological Parquet replay.
- `consumer`: long-running or replay-bounded streaming aggregate writer.
- Optional Kafka topic initialization job.

Local development can run the Spark job independently using `docker compose run --rm --no-deps spark-batch` when its input bind mount is populated. Bind mounts should use `./data:/app/data` and `./src:/app/src:ro` (note the leading `./`).

## Open decisions / questions

- [ ] Confirm that **county** is the final definition of “region” for research questions 2 and 3.
- [ ] Define duplicate-event handling when multiple monitors / POCs report for the same site-hour.
- [ ] Choose how to weight sites, counties, and hours for state, county-day, and rolling averages.
- [ ] Establish minimum valid hourly coverage for a county's 24-hour window and daily averages.
- [ ] Decide whether to retain additional provenance columns such as MDL, method, and quality qualifiers in Silver.
- [ ] Choose whether the Kafka consumer uses plain Python or Spark Structured Streaming.
- [ ] Define idempotent writes / restart behavior; avoid duplicate increments after reprocessing Kafka messages.
- [ ] Define Gold table names, keys, indexes, and before/after query timing method.
- [ ] Add automated input download later, while satisfying the ≤3-command README requirement.
- [ ] Pin tested library/image versions once the Docker builds work.

## Decision changes

| Date | Change | Reason |
|---|---|---|
| 2026-11-22 | Decided to use **county-level averages for regional analyses**, while retaining monitor-hour records in Silver. | Preserve detail for validation; aggregate at the Gold serving layer. |

