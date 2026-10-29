"""
Gold layer: silver fact -> scored readings, current patient status, KPIs.

Outputs (Delta, fully recomputed each run because they are cheap and deterministic):
- gold_patient_vitals_scored  : every silver reading + score components + total score + status
- gold_patient_current_status : latest reading per patient (status, score, staleness) -> live dashboard / alerts
- gold_daily_patient_kpi      : per patient per day KPIs
- gold_critical_status_summary: per day per status: readings and distinct patients

Status rule (documented in README / ADR):
- Unknown : all three vitals are missing
- Critical: total score >= 5, or any single vital scores 3
- Warning : total score 3-4
- Stable  : otherwise
"""
import glob
import logging
import os
import re
import sys
import traceback

import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ==========================================
# 1. Config
# ==========================================
BASE_DIR = os.getenv("BASE_DIR", "/home/jovyan/work")
LOG_DIR = os.path.join(BASE_DIR, "logs")
SILVER_FACT_PATH = os.getenv("SILVER_FACT_PATH", os.path.join(BASE_DIR, "delta", "silver_fact_patient_vitals"))
GOLD_DIR = os.getenv("GOLD_DIR", os.path.join(BASE_DIR, "delta"))
EXPORT_DIR = os.getenv("EXPORT_DIR", os.path.join(BASE_DIR, "exports"))
EXPORT_CSV = os.getenv("EXPORT_CSV", "true").lower() == "true"  # CSV copies for Power BI / Looker Studio

# A patient/device with no reading for this long (relative to the newest reading in the data) is "stale"
STALE_MINUTES = int(os.getenv("STALE_MINUTES", "5"))

CRITICAL_TOTAL = 5
WARNING_TOTAL = 3

GOLD_TABLES = {
    "scored": "gold_patient_vitals_scored",
    "current": "gold_patient_current_status",
    "daily": "gold_daily_patient_kpi",
    "summary": "gold_critical_status_summary",
}


def gold_path(key):
    return os.path.join(GOLD_DIR, GOLD_TABLES[key])


SPARK_VERSION = pyspark.__version__
SPARK_MAJOR = int(SPARK_VERSION.split(".")[0])


def detect_scala_suffix():
    spark_home = os.environ.get("SPARK_HOME", "/usr/local/spark")
    jars = glob.glob(os.path.join(spark_home, "jars", "scala-library-*.jar"))
    if jars:
        m = re.search(r"scala-library-(\d+\.\d+)", os.path.basename(jars[0]))
        if m:
            return m.group(1)
    return "2.13" if SPARK_MAJOR >= 4 else "2.12"


SCALA_SUFFIX = os.getenv("SCALA_SUFFIX", detect_scala_suffix())
DELTA_VERSION = os.getenv("DELTA_VERSION", "4.0.0" if SPARK_MAJOR >= 4 else "3.1.0")
PACKAGES = f"io.delta:delta-spark_{SCALA_SUFFIX}:{DELTA_VERSION}"

# ==========================================
# 2. Logging
# ==========================================
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [Gold] - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "gold_aggregation.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)


# ==========================================
# 3. Spark
# ==========================================
def create_spark():
    logging.info("Initializing Spark session (Delta)...")
    spark = (
        SparkSession.builder
        .appName("ICU_Gold_Aggregation")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.jars.ivy", os.getenv("IVY_DIR", "/tmp/.ivy2"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==========================================
# 4. Scoring
# ==========================================
def add_scores(df):
    hr, spo2, temp = F.col("heart_rate"), F.col("spo2"), F.col("temperature_c")

    # Thresholds adapted from NEWS2 (missing reading -> 0 points, but counted in vitals_missing)
    hr_score = (
        F.when(hr.isNull(), 0)
        .when(hr <= 40, 3).when(hr <= 50, 1).when(hr <= 90, 0)
        .when(hr <= 110, 1).when(hr <= 130, 2).otherwise(3)
    )
    spo2_score = (
        F.when(spo2.isNull(), 0)
        .when(spo2 < 92, 3).when(spo2 < 94, 2).when(spo2 < 96, 1).otherwise(0)
    )
    temp_score = (
        F.when(temp.isNull(), 0)
        .when(temp <= 35.0, 3).when(temp <= 36.0, 1).when(temp <= 38.0, 0)
        .when(temp <= 39.0, 1).otherwise(2)
    )

    scored = (
        df.withColumn("hr_score", hr_score)
        .withColumn("spo2_score", spo2_score)
        .withColumn("temp_score", temp_score)
        .withColumn("total_score", F.col("hr_score") + F.col("spo2_score") + F.col("temp_score"))
        .withColumn("max_component_score", F.greatest("hr_score", "spo2_score", "temp_score"))
        .withColumn(
            "vitals_missing",
            hr.isNull().cast("int") + spo2.isNull().cast("int") + temp.isNull().cast("int"),
        )
    )
    return scored.withColumn(
        "patient_status",
        F.when(F.col("vitals_missing") == 3, "Unknown")
        .when((F.col("total_score") >= CRITICAL_TOTAL) | (F.col("max_component_score") == 3), "Critical")
        .when(F.col("total_score") >= WARNING_TOTAL, "Warning")
        .otherwise("Stable"),
    ).withColumn("gold_processed_at", F.current_timestamp())


# ==========================================
# 5. Gold tables
# ==========================================
def build_current_status(scored):
    w = Window.partitionBy("patient_id").orderBy(F.col("event_ts").desc())
    latest = scored.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # "Now" = newest reading in the data, so replayed/simulated data is judged fairly
    data_clock = scored.agg(F.max("event_ts").alias("data_clock")).collect()[0]["data_clock"]
    return (
        latest.withColumn("data_clock", F.lit(data_clock))
        .withColumn(
            "minutes_since_last_reading",
            F.round((F.unix_timestamp("data_clock") - F.unix_timestamp("event_ts")) / 60, 1),
        )
        .withColumn("is_stale", F.col("minutes_since_last_reading") > STALE_MINUTES)
        .select(
            "patient_id", "device_id", "event_ts", "heart_rate", "spo2", "temperature_c",
            "total_score", "patient_status", "vitals_missing",
            "minutes_since_last_reading", "is_stale", "gold_processed_at",
        )
    )


def build_daily_kpi(scored):
    return scored.groupBy("patient_id", "event_date").agg(
        F.count("*").alias("readings_count"),
        F.round(F.avg("heart_rate"), 1).alias("avg_heart_rate"),
        F.max("heart_rate").alias("max_heart_rate"),
        F.round(F.avg("spo2"), 1).alias("avg_spo2"),
        F.min("spo2").alias("min_spo2"),
        F.round(F.avg("temperature_c"), 2).alias("avg_temperature_c"),
        F.max("temperature_c").alias("max_temperature_c"),
        F.round(F.avg("total_score"), 2).alias("avg_score"),
        F.max("total_score").alias("max_score"),
        F.sum((F.col("patient_status") == "Critical").cast("int")).alias("critical_readings"),
        F.sum((F.col("patient_status") == "Warning").cast("int")).alias("warning_readings"),
        F.sum((F.col("patient_status") == "Stable").cast("int")).alias("stable_readings"),
        F.sum(
            (F.col("hr_out_of_range") | F.col("spo2_out_of_range") | F.col("temp_out_of_range")).cast("int")
        ).alias("out_of_range_readings"),
        F.sum((F.col("vitals_missing") > 0).cast("int")).alias("readings_with_missing_vitals"),
    ).withColumn(
        "pct_readings_critical", F.round(F.col("critical_readings") / F.col("readings_count") * 100, 1)
    ).withColumn("gold_processed_at", F.current_timestamp())


def build_status_summary(scored):
    return scored.groupBy("event_date", "patient_status").agg(
        F.count("*").alias("readings_count"),
        F.countDistinct("patient_id").alias("patients_count"),
    ).withColumn("gold_processed_at", F.current_timestamp())


# ==========================================
# 6. Write
# ==========================================
def write_delta(df, path):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def export_csv(spark, key):
    """CSV copy for BI tools that cannot read Delta directly."""
    out = os.path.join(EXPORT_DIR, GOLD_TABLES[key])
    (spark.read.format("delta").load(gold_path(key))
     .coalesce(1).write.mode("overwrite").option("header", "true").csv(out))
    logging.info(f"Exported CSV: {out}")


def main():
    spark = None
    try:
        spark = create_spark()

        if not os.path.exists(os.path.join(SILVER_FACT_PATH, "_delta_log")):
            raise FileNotFoundError(f"Silver fact table not found: {SILVER_FACT_PATH}. Run silver first.")

        silver = spark.read.format("delta").load(SILVER_FACT_PATH)
        logging.info(f"Silver rows read: {silver.count()}")

        scored = add_scores(silver)
        write_delta(scored, gold_path("scored"))

        # Read back from Delta so the next tables are built from the persisted result
        scored = spark.read.format("delta").load(gold_path("scored"))
        write_delta(build_current_status(scored), gold_path("current"))
        write_delta(build_daily_kpi(scored), gold_path("daily"))
        write_delta(build_status_summary(scored), gold_path("summary"))

        if EXPORT_CSV:
            for key in GOLD_TABLES:
                export_csv(spark, key)

        logging.info("Current status per patient:")
        spark.read.format("delta").load(gold_path("current")).orderBy(
            F.col("total_score").desc()
        ).show(10, truncate=False)
        logging.info("Status summary:")
        spark.read.format("delta").load(gold_path("summary")).orderBy("event_date", "patient_status").show(20)
        logging.info("Gold job finished.")

    except Exception as e:
        logging.error(f"Gold job failed: {e}")
        logging.error(traceback.format_exc())
        raise  # fail the notebook/task visibly
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()