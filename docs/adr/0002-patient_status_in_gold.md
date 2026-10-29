# ADR-002: Compute Patient Status in the Gold Layer with a Simplified Early Warning Score

* **Status:** Accepted
* **Date:** October 2026
* **Deciders:** Data Engineering Team (MantisAI Capstone)
* **Domain:** ICU patient monitoring, clinical decision support

## 1. Context

The first version of the producer classified each reading as Stable / Warning / Critical using fixed heart-rate and SpO₂ thresholds before sending it to Kafka. This made the producer responsible for business logic, ignored temperature, treated a missing reading as zero, and meant any threshold change required re-sending data.

Patient status drives dashboards and alerts, so the rule must be transparent, versioned with the code and re-computable over historical data.

## 2. Decision Drivers

1. Raw data must stay raw (ADR-001).
2. Rules will be reviewed and changed; changes must apply to history without touching the source.
3. A single reading in isolation is a weak signal; combining vitals is more meaningful.
4. The rule must be explainable to stakeholders.
5. Only heart rate, SpO₂ and temperature exist in the dataset.

## 3. Considered Options

| Option | Pros | Cons |
|---|---|---|
| A. Classify in the producer (original) | Simple | Business logic at the source, no re-processing, raw data altered |
| B. Classify in the BI tool (DAX/Looker) | No pipeline change | Logic hidden in dashboards, not reusable by alerts, slower |
| C. Full NEWS2 | Clinically established | Needs respiration rate, blood pressure, consciousness and oxygen use, which are not in the data |
| D. Machine-learning model | Can learn patterns | No labeled outcomes available, hard to explain, out of scope |
| **E. Simplified score adapted from NEWS2, computed in Gold (chosen)** | Explainable, reproducible, uses available vitals | Not clinically validated |

## 4. Decision

Status is computed in the **Gold layer (Spark)** from cleaned Silver values. The producer sends raw readings only.

Each vital receives 0–3 points (thresholds adapted from the published NEWS2 chart):

| Vital | 3 points | 2 points | 1 point | 0 points |
|---|---|---|---|---|
| Heart rate (bpm) | ≤ 40 or > 130 | 111–130 | 41–50 or 91–110 | 51–90 |
| SpO₂ (%) | < 92 | 92–93 | 94–95 | ≥ 96 |
| Temperature (°C) | ≤ 35.0 | ≥ 39.1 | 35.1–36.0 or 38.1–39.0 | 36.1–38.0 |

The total score gives the status:

* **Critical:** total ≥ 5, or any single vital scores 3
* **Warning:** total 3–4
* **Stable:** otherwise
* **Unknown:** all three vitals missing

A missing vital contributes 0 points but is counted in `vitals_missing`, so incomplete readings stay visible. Thresholds are constants at the top of the Gold job.

## 5. Consequences

### Positive
* Rules live in version-controlled code and can be re-applied to all history.
* Alerts, dashboards and KPIs share one definition of status.
* The score components (`hr_score`, `spo2_score`, `temp_score`) make every classification explainable.

### Negative / trade-offs
* **The score is not full NEWS2 and has not been clinically validated.** It is decision support for a training project, not a diagnostic tool, and the thresholds should be reviewed by a clinical stakeholder before any real use.
* Scoring a missing vital as 0 can understate risk; this is mitigated by `vitals_missing` and the `Unknown` status.
* Status is only as fresh as the last Gold run (see ADR-003).

## 6. Related Decisions
* ADR-001: Medallion architecture
* ADR-003: Orchestration with Airflow