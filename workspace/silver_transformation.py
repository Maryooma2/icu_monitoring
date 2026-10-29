"""
Silver layer: bronze Delta -> cleansed, deduplicated, modeled Delta tables.

Outputs (Delta):
- silver_fact_patient_vitals : one clean row per (patient_id, device_id, event_ts), MERGE-upserted (idempotent)
- silver_dim_patient         : one row per patient (first/last seen, reading counts)
- silver_dim_device          : one row per device
- silver_quarantine_vitals   : bronze rows that could not be used (bad JSON, missing ids, bad timestamp)

Cleaning rules:
- Impossible values become NULL (never silently fixed) and a *_out_of_range flag is set.
  The original raw value is still available in bronze.
- Temperature is normalized to Celsius. If temp_unit is missing, the unit is inferred from the
  value range and temp_unit_missing is flagged.
- Duplicates (same patient/device/timestamp, e.g. from producer retries) are collapsed.

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
BRONZE_PATH = os.getenv("BRONZE_PATH", os.path.join(BASE_DIR, "delta", "bronze_patient_vitals"))
FACT_PATH = os.getenv("SILVER_FACT_PATH", os.path.join(BASE_DIR, "delta", "silver_fact_patient_vitals"))
DIM_PATIENT_PATH = os.getenv("SILVER_DIM_PATIENT_PATH", os.path.join(BASE_DIR, "delta", "silver_dim_patient"))
DIM_DEVICE_PATH = os.getenv("SILVER_DIM_DEVICE_PATH", os.path.join(BASE_DIR, "delta", "silver_dim_device"))
QUARANTINE_PATH = os.getenv("SILVER_QUARANTINE_PATH", os.path.join(BASE_DIR, "delta", "silver_quarantine_vitals"))

# Plausible physiological ranges (values outside become NULL + flag).
HR_MIN, HR_MAX = 20, 250
SPO2_MIN, SPO2_MAX = 50, 100
TEMP_C_MIN, TEMP_C_MAX = 30, 45
# Ranges used to infer the unit when temp_unit is missing
INFER_C_MIN, INFER_C_MAX = 30, 45
INFER_F_MIN, INFER_F_MAX = 86, 113

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
    format="%(asctime)s - %(levelname)s - [Silver] - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "silver_transformation.log"), encoding="utf-8"),
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
        .appName("ICU_Silver_Transformation")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.jars.ivy", os.getenv("IVY_DIR", "/tmp/.ivy2"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==========================================
# 4. Transformations
# ==========================================
def split_good_and_bad(bronze):
    """Parse the timestamp, then separate usable rows from rows that must be quarantined."""
    df = bronze.withColumn("event_ts", F.to_timestamp(F.col("event_timestamp")))
    usable = F.col("is_parse_ok") & F.col("event_ts").isNotNull()

    good = df.filter(usable)
    bad = (
        df.filter(~usable)
        .withColumn(
            "quarantine_reason",
            F.when(~F.col("is_parse_ok"), F.lit("unparseable_json_or_missing_ids"))
            .otherwise(F.lit("invalid_timestamp")),
        )
        .select(
            "kafka_topic", "kafka_partition", "kafka_offset", "raw_payload",
            "bronze_ingested_at", "quarantine_reason",
        )
    )
    return good, bad


def deduplicate(df):
    """Keep the most recently ingested copy of each (patient, device, event time)."""
    w = Window.partitionBy("patient_id", "device_id", "event_ts").orderBy(
        F.col("bronze_ingested_at").desc(), F.col("kafka_offset").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def clean_vitals(df):
    """Range-check vitals, normalize temperature to Celsius, add data-quality flags."""
    hr = F.col("heart_rate")
    spo2 = F.col("spo2")
    temp = F.col("temperature")

    unit_raw = F.upper(F.trim(F.col("temp_unit")))
    unit_valid = F.when(unit_raw.isin("C", "F"), unit_raw)  # anything else -> NULL
    unit_final = (
        F.when(unit_valid.isNotNull(), unit_valid)
        .when(temp.between(INFER_C_MIN, INFER_C_MAX), F.lit("C"))
        .when(temp.between(INFER_F_MIN, INFER_F_MAX), F.lit("F"))
    )
    temp_c_raw = (
        F.when(unit_final == "F", (temp - 32) * 5 / 9)
        .when(unit_final == "C", temp)
    )

    heart_rate_clean = F.when(hr.between(HR_MIN, HR_MAX), hr)
    spo2_clean = F.when(spo2.between(SPO2_MIN, SPO2_MAX), spo2)
    temp_c_clean = F.when(temp_c_raw.between(TEMP_C_MIN, TEMP_C_MAX), F.round(temp_c_raw, 2))

    return (
        df.withColumn("temp_unit_original", F.col("temp_unit"))
        .withColumn("temp_unit_missing", unit_valid.isNull() & temp.isNotNull())
        .withColumn("heart_rate_clean", heart_rate_clean)
        .withColumn("spo2_clean", spo2_clean)
        .withColumn("temperature_c", temp_c_clean)
        .withColumn("hr_out_of_range", hr.isNotNull() & heart_rate_clean.isNull())
        .withColumn("spo2_out_of_range", spo2.isNotNull() & spo2_clean.isNull())
        .withColumn("temp_out_of_range", temp.isNotNull() & temp_c_clean.isNull())
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("silver_processed_at", F.current_timestamp())
        .select(
            "patient_id", "device_id", "event_ts", "event_date",
            F.col("heart_rate_clean").alias("heart_rate"),
            F.col("spo2_clean").alias("spo2"),
            "temperature_c", "temp_unit_original",
            "hr_out_of_range", "spo2_out_of_range", "temp_out_of_range", "temp_unit_missing",
            "bronze_ingested_at", "silver_processed_at",
        )
    )


# ==========================================
# 5. Writes
# ==========================================
def delta_exists(path):
    return os.path.exists(os.path.join(path, "_delta_log"))


def upsert_fact(spark, fact_df):
    """Idempotent write: re-running silver never creates duplicates."""
    if not delta_exists(FACT_PATH):
        logging.info("Creating silver fact table...")
        fact_df.write.format("delta").partitionBy("event_date").save(FACT_PATH)
        return

    fact_df.createOrReplaceTempView("silver_updates")
    spark.sql(f"""
        MERGE INTO delta.`{FACT_PATH}` AS t
        USING silver_updates AS s
        ON  t.patient_id = s.patient_id
        AND t.device_id  = s.device_id
        AND t.event_ts   = s.event_ts
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def rebuild_dimensions(spark):
    fact = spark.read.format("delta").load(FACT_PATH)

    dim_patient = fact.groupBy("patient_id").agg(
        F.min("event_ts").alias("first_seen_ts"),
        F.max("event_ts").alias("last_seen_ts"),
        F.count("*").alias("readings_count"),
        F.countDistinct("device_id").alias("devices_count"),
    )
    dim_device = fact.groupBy("device_id").agg(
        F.min("event_ts").alias("first_seen_ts"),
        F.max("event_ts").alias("last_seen_ts"),
        F.count("*").alias("readings_count"),
        F.countDistinct("patient_id").alias("patients_count"),
    )
    for df, path in ((dim_patient, DIM_PATIENT_PATH), (dim_device, DIM_DEVICE_PATH)):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def summarize(spark):
    fact = spark.read.format("delta").load(FACT_PATH)
    logging.info(f"Silver fact rows: {fact.count()}")
    fact.select(
        F.sum(F.col("hr_out_of_range").cast("int")).alias("hr_out_of_range"),
        F.sum(F.col("spo2_out_of_range").cast("int")).alias("spo2_out_of_range"),
        F.sum(F.col("temp_out_of_range").cast("int")).alias("temp_out_of_range"),
        F.sum(F.col("temp_unit_missing").cast("int")).alias("temp_unit_missing"),
    ).show()
    fact.orderBy(F.col("event_ts").desc()).show(10, truncate=False)


# ==========================================
# 6. Main
# ==========================================
def main():
    spark = None
    try:
        spark = create_spark()

        bronze = spark.read.format("delta").load(BRONZE_PATH)
        bronze_count = bronze.count()
        logging.info(f"Bronze rows read: {bronze_count}")

        good, bad = split_good_and_bad(bronze)

        bad.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(QUARANTINE_PATH)
        logging.info(f"Quarantined rows: {spark.read.format('delta').load(QUARANTINE_PATH).count()}")

        fact_df = clean_vitals(deduplicate(good))
        upsert_fact(spark, fact_df)
        rebuild_dimensions(spark)

        summarize(spark)
        logging.info("Silver job finished.")

    except Exception as e:
        logging.error(f"Silver job failed: {e}")
        logging.error(traceback.format_exc())
        raise  # fail the notebook/task visibly
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()