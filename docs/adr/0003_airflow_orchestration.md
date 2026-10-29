# ADR-003: Orchestrate the Pipeline with Airflow Running the Spark Notebooks

* **Status:** Accepted
* **Date:** October 2026
* **Deciders:** Data Engineering Team (MantisAI Capstone)
* **Domain:** Pipeline orchestration, scheduling and alerting

## 1. Context

The pipeline has five steps (event producer, bronze, silver, gold, business alert) that must run in order, on a schedule, with retries and failure notifications. The Spark layers were developed as Jupyter notebooks in a separate Jupyter container, while Airflow runs in its own container (Docker Compose, local machine).

## 2. Decision Drivers

1. Airflow must be the single place that schedules, retries and monitors every step.
2. Failures must be visible and must notify people.
3. Runs must be reproducible and leave an audit trail.
4. Must work on a laptop with Docker, without cloud accounts.
5. Should map cleanly to managed services later.

## 3. Considered Options

| Option | Pros | Cons |
|---|---|---|
| A. Airflow image with Java and the same Spark build, running the notebooks with papermill (chosen) | Single orchestrator, simple operators, executed notebooks saved per run | Heavier Airflow image |
| B. Airflow calls `docker exec` on the Jupyter container | Reuses the existing Spark container | Requires mounting the Docker socket into Airflow, fragile and a security concern |
| C. SparkSubmitOperator against a separate Spark cluster | Closest to production | Extra services, too heavy for the MVP timeline |
| D. Run notebooks manually or with cron | Nothing to build | No retries, no alerting, no lineage |
| E. Managed cloud services (ADF/Glue/Dataflow with MWAA/Composer) | Production grade | Cost and setup out of scope; not used in this project |

## 4. Decision

We build a custom Airflow image (defined inline in `docker-compose.yml`) that contains Java 17, the Spark 3.5.0 distribution copied from the Jupyter image (guaranteeing identical versions), `papermill` and the Kafka client. The DAG `icu_monitoring_pipeline`:

* runs every 15 minutes with `max_active_runs=1` and `catchup=False`;
* publishes new data with `produce_events`, which by default runs a live simulator that emits one synthetic reading per patient per 5-minute slot (a CSV replay mode is also available), then executes `bronze_ingestion → silver_transformation → gold_aggregation → check_critical_patients`;
* runs the Spark notebooks with papermill; a failing cell fails the task, and the executed notebook of each run is stored in `notebook_runs/`;
* runs bronze with the `availableNow` trigger so that each run drains the Kafka backlog and finishes;
* retries each task twice with exponential backoff and sends a failure alert through `on_failure_callback`;
* sends a business-facing alert (Slack or Power Automate webhook) when patients are Critical or devices stop sending data.

Duplicate deliveries are handled by an idempotent Kafka producer, plus deduplication and `MERGE` in Silver, so repeated runs are safe.

## 5. Consequences

### Positive
* One tool provides scheduling, retries, alerting and run history.
* Reproducible environment from a single `docker-compose.yml`.
* The executed notebooks are an audit trail and help debugging.

### Negative / trade-offs
* **Latency:** with micro-batch runs every 15 minutes, end-to-end freshness equals the schedule interval. True real-time would need a continuously running streaming job, which is a future improvement.
* `SequentialExecutor` (Airflow standalone) runs one task at a time and is not for production.
* Notebooks are harder to test and review than plain Python modules; converting them to `.py` scripts is a planned improvement.
* The Airflow container is heavy because it carries Java and Spark.
* The project runs locally. Cloud-native triggers (ADF/Glue/Dataflow) are described as equivalents in the README but are not implemented.

## 6. Related Decisions
* ADR-001: Medallion architecture
* ADR-002: Patient status classification in the Gold layer