"""
ICU monitoring pipeline: Airflow as the central orchestrator.

Flow (runs every 15 minutes, one run at a time):

    produce_events -> bronze_ingestion -> silver_transformation -> gold_aggregation -> check_critical_patients

- produce_events         : stands in for the real device feed and publishes to Kafka (topic patient_vitals).
                           SOURCE_MODE=simulator (default): icu_simulator.py generates NEW synthetic readings every run
                           (one reading per patient per 5-minute slot, with deterioration episodes, silent devices and dirty data).
                           SOURCE_MODE=replay: healthcare_producer.py replays the historical CSV.
- bronze_ingestion       : (Python script) Spark reads Kafka and appends raw events to the Delta bronze table (availableNow trigger).
- silver_transformation  : (Python script) Spark cleans, deduplicates and models the data (MERGE, so re-runs are idempotent).
- gold_aggregation       : (Python script) Spark computes the simplified early warning score, patient status and KPIs, and exports CSVs for BI.
- check_critical_patients: reads the exported current-status CSV and sends a business-facing alert
                           (Slack / Power Automate webhook) when patients are Critical or devices went silent.

Reliability:
- Every task retries twice with exponential backoff.
- Any task failure triggers on_failure_callback -> alert webhook (+ Airflow logs).

Alerts always go to the task log and to workspace/alerts/alerts.jsonl; if ALERT_WEBHOOK_URL is set
(Slack / Discord / Power Automate), they are also posted there.
"""
import csv
import glob
import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ==========================================
# Configuration (paths are inside the Airflow container)
# ==========================================
WORKSPACE = os.getenv("WORKSPACE", "/opt/airflow/workspace")
PRODUCER_SCRIPT = os.getenv("PRODUCER_SCRIPT", f"{WORKSPACE}/healthcare_producer.py")

BRONZE_SCRIPT = os.getenv("BRONZE_SCRIPT", f"{WORKSPACE}/bronze_ingestion.py")
SILVER_SCRIPT = os.getenv("SILVER_SCRIPT", f"{WORKSPACE}/silver_transformation.py")
GOLD_SCRIPT = os.getenv("GOLD_SCRIPT", f"{WORKSPACE}/gold_aggregation.py")

# Source data lives in the project-root ./data folder (mounted read-only at /opt/airflow/data by docker-compose)
DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
CSV_PATH = os.getenv("CSV_PATH", f"{DATA_DIR}/icu_vitals.csv")
EXPORT_DIR = os.getenv("EXPORT_DIR", f"{WORKSPACE}/exports")

# Data source: "simulator" (new synthetic readings every run) or "replay" (replays the historical CSV)
SOURCE_MODE = os.getenv("SOURCE_MODE", "simulator").lower()
SIMULATOR_SCRIPT = os.getenv("SIMULATOR_SCRIPT", f"{WORKSPACE}/icu_simulator.py")


SIMULATOR_ENV = {
    "KAFKA_BROKER": "kafka:9092",
    "KAFKA_TOPIC": "patient_vitals",
    "CSV_PATH": CSV_PATH,  # only used to take patient ids / devices
    "LOG_DIR": f"{WORKSPACE}/logs",
    "STATE_PATH": f"{WORKSPACE}/simulator_state.json",
    "DEFAULT_STEPS": "1",           # the first run starts at the current 5-minute slot (no history)
    "MAX_CATCHUP_STEPS": "288",    # after a pause (PC asleep), fill the gap, up to 1 day
    "DELIVERY_TIMEOUT_SEC": "60",
}

PRODUCER_ENV = {
    "KAFKA_BROKER": "kafka:9092",  # internal Docker listener (not localhost:29092)
    "KAFKA_TOPIC": "patient_vitals",
    "CSV_PATH": CSV_PATH,
    "LOG_DIR": f"{WORKSPACE}/logs",  # keep producer logs next to the other layers' logs
    "SEND_INTERVAL_SEC": os.getenv("SEND_INTERVAL_SEC", "0.05"),
}
SPARK_ENV = {
    "BASE_DIR": WORKSPACE,
    "KAFKA_BOOTSTRAP": "kafka:9092",
    "TRIGGER_MODE": "availableNow",
    "EXPORT_DIR": EXPORT_DIR,
    "IVY_DIR": f"{WORKSPACE}/.ivy2",
    # A patient with no reading for this long (vs the newest reading) is flagged as a silent device.
    # Live simulator: 20 min (readings arrive every 5 min). Historical replay: very large, the CSV spans months.
    "STALE_MINUTES": os.getenv("STALE_MINUTES", "20" if SOURCE_MODE == "simulator" else "400000"),
}


# ==========================================
# Notifications
# ==========================================
ALERT_LOG_PATH = os.getenv("ALERT_LOG_PATH", f"{WORKSPACE}/alerts/alerts.jsonl")


def notify(title, lines, severity="info"):
    """Always logs and appends the alert to a local JSON-lines file; also posts to ALERT_WEBHOOK_URL if set."""
    text = f"[{severity.upper()}] {title}\n" + "\n".join(lines)
    log.warning(text)
    delivered = []

    # 1. local record (works with no account)
    try:
        os.makedirs(os.path.dirname(ALERT_LOG_PATH), exist_ok=True)
        record = {"time": datetime.utcnow().isoformat() + "Z", "severity": severity, "title": title, "details": lines}
        with open(ALERT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        delivered.append("file")
    except Exception:
        log.exception("Could not write the local alert record")

    # 2. optional webhook (Slack / Discord / Power Automate)
    url = os.getenv("ALERT_WEBHOOK_URL")
    if url:
        payload = json.dumps({"text": text, "title": title, "severity": severity}).encode("utf-8")
        request = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(request, timeout=10)
            delivered.append("webhook")
        except Exception:
            log.exception("Alert webhook call failed")

    log.info("Alert delivered through: %s", ", ".join(delivered) or "none")
    return delivered


def on_failure(context):
    """Airflow on_failure_callback: sends an alert when a task fails."""
    ti = context["task_instance"]
    notify(
        f"Pipeline task failed in ICU Pipeline: {ti.dag_id}.{ti.task_id}",
        [
            f"Run ID: {context['run_id']}",
            f"Failed Task: {ti.task_id}",
            f"Attempt Number: {ti.try_number}",
            f"Log URL: {ti.log_url}",
            f"Exception: {context.get('exception', 'Unknown error')}"
        ],
        severity="error",
    )


# ==========================================
# Business alert task
# ==========================================
def check_critical_patients():
    """Read the gold current-status export and alert on Critical patients and silent devices."""
    files = glob.glob(os.path.join(EXPORT_DIR, "gold_patient_current_status", "part-*.csv"))
    if not files:
        raise FileNotFoundError(f"No current-status export found in {EXPORT_DIR}. Did gold run with EXPORT_CSV=true?")

    latest = max(files, key=os.path.getmtime)
    with open(latest, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    critical = [r for r in rows if r["patient_status"] == "Critical"]
    warning = [r for r in rows if r["patient_status"] == "Warning"]
    stale = [r for r in rows if r["is_stale"].strip().lower() == "true"]

    summary = {
        "patients": len(rows),
        "critical": len(critical),
        "warning": len(warning),
        "stale_devices": len(stale),
    }
    log.info("Current status summary: %s", summary)

    if critical:
        ids = ", ".join(r["patient_id"] for r in critical[:10])
        notify(
            f"{len(critical)} patient(s) in CRITICAL status",
            [f"Patients (up to 10): {ids}", f"Total monitored: {len(rows)}"],
            severity="critical",
        )
    if stale:
        ids = ", ".join(f"{r['patient_id']}/{r['device_id']}" for r in stale[:10])
        notify(
            f"{len(stale)} device(s) stopped sending data",
            [f"Patient/device (up to 10): {ids}"],
            severity="warning",
        )
    return summary


# ==========================================
# Python Script execution helper
# ==========================================
def spark_script_task(task_id, script_path):
    """Returns a BashOperator that runs a Python script (Spark job) with the proper environment."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"python {script_path}",
        env=SPARK_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=20),
    )


# ==========================================
# DAG
# ==========================================
default_args = {
    "owner": "data-eng-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure,  # تفعيل التنبيه التلقائي عند فشل أي مهمة في الـ Pipeline
}

with DAG(
    dag_id="icu_monitoring_pipeline",
    description="Kafka -> bronze -> silver -> gold ICU vitals pipeline with alerting using pure Python scripts",
    start_date=datetime(2026, 10, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["icu", "medallion", "production"],
    doc_md=__doc__,
) as dag:

    produce_events = BashOperator(
        task_id="produce_events",
        bash_command=f"python {SIMULATOR_SCRIPT if SOURCE_MODE == 'simulator' else PRODUCER_SCRIPT}",
        env=SIMULATOR_ENV if SOURCE_MODE == "simulator" else PRODUCER_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=10 if SOURCE_MODE == "simulator" else 15),
    )

    # [تعديل]: استخدام دالة تشغيل ملفات البايثون العادية لطبقات الميداليون
    bronze_ingestion = spark_script_task("bronze_ingestion", BRONZE_SCRIPT)
    silver_transformation = spark_script_task("silver_transformation", SILVER_SCRIPT)
    gold_aggregation = spark_script_task("gold_aggregation", GOLD_SCRIPT)

    alert_check = PythonOperator(
        task_id="check_critical_patients",
        python_callable=check_critical_patients,
    )

    # [تعديل]: تسلسل المهام بالترتيب المطلوب مع تفعيل نظام التنبيهات الشامل على مستوى الـ Pipeline
    (
        produce_events
        >> bronze_ingestion
        >> silver_transformation
        >> gold_aggregation
        >> alert_check
    )