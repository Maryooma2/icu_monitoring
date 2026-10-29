# Scope, User Stories and Success Metrics

Project: ICU Monitoring Data Platform (MantisAI Data Engineering Capstone)

## 1. Team

| Name | 
| [Mariam Mustafa] 

## 2. Problem and goal

ICU monitors produce a continuous stream of vital signs (heart rate, SpO₂, temperature). The data is imperfect (missing values, impossible values, mixed units, duplicates), and clinical staff need to know quickly which patients need attention.

**Goal:** a reliable, orchestrated pipeline that turns the raw stream into clean, modeled data, assigns each patient a simplified early-warning status, and serves dashboards and automatic alerts.

## 3. Scope

### In scope (MVP)
* **Source:** a live simulator that publishes synthetic readings (one per patient every 5 minutes, with deterioration episodes, silent devices and dirty data), plus a CSV replay mode.
* **Ingestion:** Kafka topic `patient_vitals`; Spark Structured Streaming into the bronze Delta table.
* **Transformation:** silver (cleaning, deduplication, unit normalization, quarantine, star schema) and gold (early-warning score, patient status, KPIs).
* **Orchestration:** one Airflow DAG with schedule (every 15 minutes), retries and alerts.
* **Alerting:** pipeline failures and Critical patients / silent devices through a webhook (Slack or Power Automate).
* **Consumption:** a Power BI dashboard with three pages (ward overview, patient trends, daily KPIs).
* **Documentation:** README, three ADRs, architecture diagram.

### Out of scope
* Real patient data and real device integration (all data is synthetic).
* Clinically validated scoring: the score is a simplified early-warning score adapted from NEWS2 (heart rate, SpO₂, temperature only), for decision support, not diagnosis.
* Cloud deployment (ADF, Glue, Dataflow and managed services); the platform runs locally in Docker.
* Authentication, patient privacy controls and role-based access.
* Machine-learning prediction.
* Automatic (scheduled) refresh of the Power BI Desktop report.

### Assumptions and constraints
* 61 patients and their devices come from the source dataset; vitals are generated.
* Freshness equals the schedule interval (15 minutes) plus the run time.
* Single machine, Docker Compose, Airflow standalone with the sequential executor.

## 4. Users

| User | Needs |
|---|---|
| ICU nurse | See every patient's current status and be told immediately when someone becomes Critical |
| ICU doctor | Prioritize patients, see how a patient's vitals evolved |
| Ward manager | Daily view of how many readings and patients were Critical, Warning or Stable, and how good the data is |
| Biomedical engineer | Know when a monitor stops sending data |
| Data engineer (maintainer) | A pipeline that recovers by itself, can be re-run safely and keeps bad data visible |

## 5. User stories

| ID | Story | Acceptance criteria | Delivered by |
|---|---|---|---|
| US-1 | As a **nurse**, I want to see the latest vitals and status of every patient on one screen, so that I do not have to check each bed. | The page lists every monitored patient with the latest readings and a color-coded status; counts of Critical, Warning and Stable are shown; "Data as of" shows when it was last updated. | `gold_patient_current_status`, Ward overview page |
| US-2 | As a **doctor**, I want patients ranked by early-warning score, so that I see who needs intervention first. | The table is sorted by score, highest first; each status follows the documented rule (ADR-002). | Gold scoring, Ward overview table |
| US-3 | As a **nurse**, I want an alert when a patient becomes Critical, so that I can react quickly. | After the run that contains the reading, an alert listing the Critical patients is sent (webhook) and logged. | `check_critical_patients` task |
| US-4 | As a **biomedical engineer**, I want an alert when a device stops sending data, so that no patient is monitored by a dead device. | A patient with no reading for more than `STALE_MINUTES` is flagged as stale, counted in "Silent devices" and included in the alert. | `is_stale` in gold, DAG alert task |
| US-5 | As a **doctor**, I want to see one patient's vitals over time, so that I can judge whether the patient is improving or deteriorating. | Selecting a patient shows heart rate, SpO₂, temperature and score as time series, and the latest readings table. | Patient Trends page |
| US-6 | As a **ward manager**, I want daily KPIs and data-quality indicators, so that I can evaluate the ward and trust the data. | Readings per day by status, patients per status per day, out-of-range and missing-vital counts are available. | `gold_daily_patient_kpi`, `gold_critical_status_summary`, Daily KPIs page |
| US-7 | As a **data engineer**, I want failed tasks to retry automatically and notify me, so that transient failures do not need manual work. | Each task retries twice with backoff; a failure triggers an alert with the run and task. | Airflow retries, `on_failure_callback` |
| US-8 | As a **data engineer**, I want re-runs to be safe, so that retries and replays never duplicate data. | No duplicate `(patient_id, device_id, event_ts)` in the silver fact table after repeated runs. | Idempotent producer, deduplication, Delta MERGE |
| US-9 | As a **data engineer**, I want unusable records kept and visible, so that no data is silently lost. | Unparseable or invalid rows appear in the quarantine table with a reason; out-of-range values are flagged, not deleted. | Bronze `is_parse_ok`, `silver_quarantine_vitals`, quality flags |
| US-10 | As an **auditor**, I want the raw data preserved, so that results can be traced and re-processed. | The bronze table is append-only and stores the raw payload and Kafka offsets. | Bronze layer (ADR-001) |

## 6. Success metrics

| Area | Metric | Target | How it is measured | Result |
|---|---|---|---|---|
| Reliability | DAG run success rate | At least 95 % of runs during the demo period | Airflow run history | [fill in] |
| Performance | Duration of a full run | Shorter than the 15-minute schedule | Airflow Gantt / task duration | [fill in] |
| Freshness | Age of the dashboard data | At most one schedule interval plus run time | "Data as of" card vs the current time | [fill in] |
| Integrity | Duplicate rows in the silver fact table | 0 | Count rows grouped by `(patient_id, device_id, event_ts)` having more than one row | [fill in] |
| Completeness | Messages published vs rows in bronze | Equal | "Published N events" in the log vs the growth of the bronze row count | [fill in] |
| Data quality | Unusable records visible | 100 % of them in quarantine; quarantine, out-of-range and missing-vital counts reported | Quarantine table and Daily KPIs page | [fill in] |
| Correctness | Status for reference readings | All reference cases give the expected status (for example SpO₂ 88 and heart rate 135 gives Critical; normal vitals give Stable) | Test inputs through the scoring logic | [fill in] |
| Alerting | Alerts delivered | A Critical patient and a failed task each produce an alert in the same run | One deliberate test of each, with a screenshot | [fill in] |
| Usability | Dashboard answers the user stories | US-1, US-2, US-5 and US-6 are visible in the three pages; refresh works without errors after a run | Walk-through of the stories on the dashboard | [fill in] |
| Reproducibility | Setup from the README | A new person can start the platform and run the DAG by following the README | Peer test | [fill in] |

## 7. Definition of done (MVP)
* A full DAG run is green from event generation to the alert check.
* The dashboard refreshes from the gold exports and shows the three pages.
* Every user story above has its acceptance criteria demonstrated or explicitly marked as not delivered.
* README, ADRs and the architecture diagram match what was built.
