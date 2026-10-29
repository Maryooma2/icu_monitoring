# ADR-001: Medallion Architecture (Bronze → Silver → Gold) on Delta Lake

* **Status:** Accepted
* **Date:** October 2026
* **Deciders:** Data Engineering Team (MantisAI Capstone)
* **Domain:** ICU patient monitoring (vital signs telemetry)

## 1. Context

Bedside monitors produce a continuous stream of readings per patient: heart rate, SpO₂ and temperature (the three vitals available in the capstone dataset; blood pressure and respiration rate are out of MVP scope). In practice this data is imperfect:

* readings are missing or physiologically impossible (sensor drop, probe off);
* temperature arrives in mixed units (°C / °F) or without a unit;
* the same reading can be delivered more than once (producer retries, replays);

We need an architecture that keeps the original data for audit and replay, produces clean and trustworthy tables for analytics, and serves fast KPIs to dashboards and alerts without querying raw data.

## 2. Decision Drivers

1. **Auditability:** raw medical telemetry must never be lost or silently altered.
2. **Re-processability:** when cleaning or scoring rules change, we must be able to rebuild downstream tables from the original data.
3. **Data quality:** bad records must be visible (flagged or quarantined), not hidden.
4. **Dashboard performance:** BI tools should read small, pre-aggregated tables.
5. **Fit for the capstone:** implementable with Kafka, PySpark and Delta Lake locally, and portable to cloud services later.

## 3. Considered Options

| Option | Pros | Cons |
|---|---|---|
| A. Single table (stream → table → dashboard) | Simplest, lowest latency | No raw copy, no replay, cleaning mixed with consumption |
| B. Two layers (raw + curated) | Less overhead than three layers | Cleaning and business aggregation are mixed; dashboards still read large tables |
| C. Load straight into a data warehouse | Mature SQL tooling | Costlier, less suited to raw JSON and replay, not available locally |
| **D. Medallion on Delta Lake (chosen)** | Clear separation of concerns, replay, ACID tables, schema enforcement, local and cloud portable | More storage, more jobs to orchestrate |

## 4. Decision

We adopt the **Medallion architecture** with Delta Lake tables:

* **Bronze (raw):** a Spark Structured Streaming job reads the `patient_vitals` Kafka topic and appends to an append-only Delta table partitioned by `ingest_date`. It stores the untouched `raw_payload`, Kafka coordinates (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`), `bronze_ingested_at`, the parsed fields (timestamps kept as strings) and an `is_parse_ok` flag. Nothing is cleaned, filtered or classified here.
* **Silver (clean and modeled):** a batch job separates unusable rows into `silver_quarantine_vitals`, deduplicates on `(patient_id, device_id, event_ts)`, sets impossible values to NULL with `*_out_of_range` flags, normalizes temperature to °C (inferring the unit from the value range when missing, with a `temp_unit_missing` flag) and writes `silver_fact_patient_vitals` with a Delta `MERGE`, so re-runs never create duplicates. Dimensions: `silver_dim_patient` and `silver_dim_device`.
* **Gold (business-ready):** a batch job produces `gold_patient_vitals_scored`, `gold_patient_current_status`, `gold_daily_patient_kpi` and `gold_critical_status_summary` (see ADR-002 for the scoring), and exports CSV copies for BI tools.

## 5. Consequences

### Positive
* The append-only bronze layer allows audit and full replay when rules evolve.
* Ingestion is decoupled from transformation and consumption.
* Data-quality problems are measurable (quarantine counts, out-of-range flags).
* Dashboards read small gold tables.
* The same pattern maps directly to cloud services (S3/ADLS/GCS + Glue/ADF/Dataflow).

### Negative / trade-offs
* Three copies of the data increase storage use.
* End-to-end latency is higher than a direct stream-to-dashboard design.
* More jobs means more orchestration complexity (see ADR-003).
* Silver and gold currently recompute from the full upstream table on every run (simple and correct for the MVP); processing only new data using `bronze_ingested_at` is a planned improvement.

## 6. Related Decisions
* ADR-002: Patient status classification in the Gold layer
* ADR-003: Orchestration with Airflow