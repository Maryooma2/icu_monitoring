"""
Bronze layer: Kafka (patient_vitals) -> Delta Lake.
"""
import glob
import logging
import os
import re
import sys
import traceback

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_date
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# ==========================================
# 1. Config (override with environment variables)
# ==========================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "patient_vitals")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")  # only used on the first run
TRIGGER_MODE = os.getenv("TRIGGER_MODE", "availableNow")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")

BASE_DIR = os.getenv("BASE_DIR", "/home/jovyan/work")
LOG_DIR = os.path.join(BASE_DIR, "logs")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", os.path.join(BASE_DIR, "checkpoints", "bronze_patient_vitals"))
BRONZE_PATH = os.getenv("BRONZE_PATH", os.path.join(BASE_DIR, "delta", "bronze_patient_vitals"))

# Package versions must match the Spark build installed in the container:
#   Spark 3.5.x -> Scala 2.12 + Delta 3.1.x
#   Spark 4.x   -> Scala 2.13 + Delta 4.x
SPARK_VERSION = pyspark.__version__
SPARK_MAJOR = int(SPARK_VERSION.split(".")[0])


def detect_scala_suffix():
    """Read the Scala version from the Spark jars (e.g. scala-library-2.12.18.jar -> 2.12)."""
    spark_home = os.environ.get("SPARK_HOME", "/usr/local/spark")
    jars = glob.glob(os.path.join(spark_home, "jars", "scala-library-*.jar"))
    if jars:
        m = re.search(r"scala-library-(\d+\.\d+)", os.path.basename(jars[0]))
        if m:
            return m.group(1)
    return "2.13" if SPARK_MAJOR >= 4 else "2.12"


SCALA_SUFFIX = os.getenv("SCALA_SUFFIX", detect_scala_suffix())
DELTA_VERSION = os.getenv("DELTA_VERSION", "4.0.0" if SPARK_MAJOR >= 4 else "3.1.0")
PACKAGES = ",".join([
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_SUFFIX}:{SPARK_VERSION}",
    f"io.delta:delta-spark_{SCALA_SUFFIX}:{DELTA_VERSION}",
])

# ==========================================
# 2. Logging
# ==========================================
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [Bronze] - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "bronze_ingestion.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,  # Jupyter pre-configures logging; without this basicConfig is ignored
)

# ==========================================
# 3. Schema of the raw event payload (JSON)
# ==========================================
SCHEMA = StructType([
    StructField("patient_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("heart_rate", DoubleType(), True),
    StructField("spo2", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("temp_unit", StringType(), True),
    StructField("event_timestamp", StringType(), True),      # parsed to timestamp in silver
    StructField("ingestion_timestamp", StringType(), True),  # set by the producer
])


# ==========================================
# 4. Spark session
# ==========================================
def create_spark():
    logging.info("Initializing Spark session (Kafka + Delta)...")
    spark = (
        SparkSession.builder
        .appName("ICU_Bronze_Ingestion")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.jars.ivy", os.getenv("IVY_DIR", "/tmp/.ivy2"))  # writable cache for downloaded jars
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==========================================
# 5. Read from Kafka and shape the bronze DataFrame
# ==========================================
def read_bronze_stream(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("data", from_json(col("raw_payload"), SCHEMA))
        .select(
            "kafka_key", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
            "raw_payload", "data.*",
        )
        .withColumn("bronze_ingested_at", current_timestamp())
        .withColumn("ingest_date", to_date(col("bronze_ingested_at")))
        # Flag unparseable messages instead of dropping them; silver decides what to do.
        .withColumn("is_parse_ok", col("patient_id").isNotNull() & col("device_id").isNotNull())
    )


# ==========================================
# 6. Write to Delta
# ==========================================
def start_bronze_stream(df):
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .queryName("bronze_patient_vitals")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingest_date")
    )
    if TRIGGER_MODE == "availableNow":
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=TRIGGER_INTERVAL)

    logging.info(f"Starting bronze stream (mode={TRIGGER_MODE}) -> {BRONZE_PATH}")
    return writer.start(BRONZE_PATH)


def inspect_bronze(spark):
    """Quick sanity check (only used after a finite run)."""
    df = spark.read.format("delta").load(BRONZE_PATH)
    total = df.count()
    bad = df.filter(~col("is_parse_ok")).count()
    logging.info(f"Bronze rows: {total} | unparseable: {bad}")
    df.orderBy(col("kafka_timestamp").desc()).select(
        "patient_id", "device_id", "heart_rate", "spo2", "temperature", "event_timestamp", "is_parse_ok"
    ).show(10, truncate=False)


# ==========================================
# 7. Main
# ==========================================
def main():
    spark = None
    query = None
    try:
        spark = create_spark()
        query = start_bronze_stream(read_bronze_stream(spark))
        query.awaitTermination()  # blocks; raises if the stream fails

        if TRIGGER_MODE == "availableNow":
            inspect_bronze(spark)
        logging.info("Bronze job finished.")

    except KeyboardInterrupt:
        logging.info("Stopped by user.")
    except Exception as e:
        logging.error(f"Bronze job failed: {e}")
        logging.error(traceback.format_exc())
        raise  # fails the notebook/process so Airflow marks the task as failed
    finally:
        if query is not None and query.isActive:
            query.stop()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()