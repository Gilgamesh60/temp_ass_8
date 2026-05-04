"""
spark_clean.py - PySpark implementation of the NYC Taxi preprocessing pipeline.

Pipeline:
  1. Ingest parquet trip files + zone lookup CSV
  2. Clean: drop nulls, drop duplicates, cast timestamps
  3. Heavy join: trips x zone lookup on PULocationID
  4. UDF: compute avg_speed_mph per trip (Python UDF -> JVM overhead)
  5. Aggregate: mean speed by (pickup_hour, Borough)
  6. Export to parquet

Metrics captured in metrics/spark_metrics.json.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import platform
import shutil
import socket
import time
from pathlib import Path
from functools import reduce

# --- Ensure Java is findable (macOS Homebrew installs don't add to PATH) ---
if not shutil.which("java"):
    for java_path in [
        "/opt/homebrew/opt/openjdk@17",
        "/usr/local/opt/openjdk@17",
        "/opt/homebrew/opt/openjdk",
        "/usr/local/opt/openjdk",
    ]:
        if os.path.isfile(f"{java_path}/bin/java"):
            os.environ["JAVA_HOME"] = java_path
            os.environ["PATH"] = f"{java_path}/bin:" + os.environ.get("PATH", "")
            break

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import DoubleType

TRIPS_GLOB = "./data/trips/*.parquet"
ZONES_CSV = "./data/zones/taxi_zone_lookup.csv"
OUTPUT_DIR = "./output/spark_result"
AGG_OUTPUT_DIR = "./output/spark_agg"
METRICS_FILE_DEFAULT = "./metrics/spark_metrics.json"

# The only columns we actually use. We select + cast these from every file,
# which sidesteps all schema drift issues (INT vs BIGINT, Airport_fee vs airport_fee).
KEEP_COLS = {
    "VendorID": "long",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "double",
    "trip_distance": "double",
    "PULocationID": "long",
    "DOLocationID": "long",
    "payment_type": "long",
    "fare_amount": "double",
    "total_amount": "double",
}


def build_spark(master: str | None, app_name: str = "SparkClean") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def load_and_normalize(spark: SparkSession, path: str) -> DataFrame:
    """Read one parquet file, select only the columns we need, cast to uniform types."""
    df = spark.read.parquet(path)
    # Lowercase all column names to handle Airport_fee vs airport_fee
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    # Select and cast only what we need
    selects = []
    for col_name, col_type in KEEP_COLS.items():
        lc = col_name.lower()
        if lc in [c.lower() for c in df.columns]:
            selects.append(F.col(lc).cast(col_type).alias(col_name))
    return df.select(selects)


# --- Python UDF: intentionally a plain Python function, NOT a pandas_udf,
# so we measure the "classic" JVM<->Python serialization overhead the brief asks about.
def avg_speed_mph(distance_miles, duration_seconds):
    if distance_miles is None or duration_seconds is None:
        return None
    if duration_seconds <= 0:
        return None
    hours = duration_seconds / 3600.0
    if hours <= 0:
        return None
    return float(distance_miles) / hours


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", default=os.environ.get("MASTER_URL"),
                        help="Spark master URL, e.g. spark://host:7077. If unset, uses local[*].")
    parser.add_argument("--trips", default=TRIPS_GLOB)
    parser.add_argument("--zones", default=ZONES_CSV)
    parser.add_argument("--output", default=OUTPUT_DIR)
    parser.add_argument("--agg-output", default=AGG_OUTPUT_DIR)
    parser.add_argument("--metrics", default=METRICS_FILE_DEFAULT)
    args = parser.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.metrics).parent.mkdir(parents=True, exist_ok=True)

    phase = {}
    t_total_start = time.perf_counter()

    spark = build_spark(args.master)
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark version: {spark.version}")
    print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

    # --- 1. INGEST -----------------------------------------------------------
    t0 = time.perf_counter()
    # Read each parquet file individually, normalize columns, then union.
    # This is the bulletproof way to handle NYC TLC schema drift across years.
    files = sorted(glob.glob(args.trips))
    print(f"[ingest]  found {len(files)} parquet files")
    dfs = [load_and_normalize(spark, f) for f in files]
    trips = reduce(DataFrame.unionByName, dfs)

    zones = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(args.zones)
    )
    input_rows = trips.count()
    zone_rows = zones.count()
    phase["ingestion_s"] = time.perf_counter() - t0
    print(f"[ingest]  trips={input_rows:,}  zones={zone_rows:,}  ({phase['ingestion_s']:.1f}s)")

    # --- 2. CLEANSE ----------------------------------------------------------
    t0 = time.perf_counter()
    required_cols = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "trip_distance", "PULocationID", "DOLocationID",
    ]
    cleaned = (
        trips
        .dropna(subset=required_cols)
        .dropDuplicates()
        .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("trip_duration_s",
                    F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long"))
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_duration_s") > 0)
        .filter(F.col("trip_duration_s") < 6 * 3600)
    )
    cleaned = cleaned.cache()
    cleaned_rows = cleaned.count()
    phase["cleansing_s"] = time.perf_counter() - t0
    print(f"[clean]   rows={cleaned_rows:,}  ({phase['cleansing_s']:.1f}s)")

    # --- 3. HEAVY JOIN -------------------------------------------------------
    t0 = time.perf_counter()
    joined = cleaned.join(
        zones,
        cleaned["PULocationID"] == zones["LocationID"],
        how="inner",
    )
    joined = joined.cache()
    joined_rows = joined.count()
    phase["join_s"] = time.perf_counter() - t0
    print(f"[join]    rows={joined_rows:,}  ({phase['join_s']:.1f}s)")

    # --- 4. PYTHON UDF (the main measurement for the rubric's UDF Deep-Dive) -
    speed_udf = F.udf(avg_speed_mph, DoubleType())
    t0 = time.perf_counter()
    with_speed = joined.withColumn(
        "avg_speed_mph",
        speed_udf(F.col("trip_distance"), F.col("trip_duration_s")),
    ).withColumn(
        "pickup_hour", F.hour("pickup_ts")
    )
    with_speed = with_speed.cache()
    udf_rows = with_speed.count()
    phase["udf_s"] = time.perf_counter() - t0
    print(f"[udf]     rows={udf_rows:,}  ({phase['udf_s']:.1f}s)  <- JVM<->Python overhead")

    # --- 5. AGGREGATE --------------------------------------------------------
    t0 = time.perf_counter()
    agg = (
        with_speed.groupBy("pickup_hour", "Borough")
        .agg(
            F.avg("avg_speed_mph").alias("mean_speed_mph"),
            F.count("*").alias("trip_count"),
        )
        .orderBy("pickup_hour", "Borough")
    )
    agg.cache()
    agg_rows = agg.count()
    phase["aggregate_s"] = time.perf_counter() - t0
    print(f"[agg]     groups={agg_rows:,}  ({phase['aggregate_s']:.1f}s)")

    # --- 6. EXPORT -----------------------------------------------------------
    t0 = time.perf_counter()
    (with_speed
        .select("pickup_ts", "dropoff_ts", "trip_distance",
                "trip_duration_s", "PULocationID", "Borough", "Zone",
                "avg_speed_mph", "pickup_hour")
        .write.mode("overwrite").parquet(args.output))
    agg.write.mode("overwrite").parquet(args.agg_output)
    phase["export_s"] = time.perf_counter() - t0
    print(f"[export]  {args.output}  ({phase['export_s']:.1f}s)")

    total = time.perf_counter() - t_total_start
    phase["total_s"] = total

    metrics = {
        "framework": "spark",
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "spark_version": spark.version,
        "master": args.master or "local[*]",
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
        "input_rows": input_rows,
        "cleaned_rows": cleaned_rows,
        "joined_rows": joined_rows,
        "agg_rows": agg_rows,
        "phases": phase,
    }
    with open(args.metrics, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[metrics] written to {args.metrics}")
    print(f"[TOTAL]   {total:.1f}s")

    hold = int(os.environ.get("SPARK_HOLD_SECONDS", "0"))
    if hold > 0:
        print(f"Holding SparkContext for {hold}s so you can screenshot :4040 ...")
        time.sleep(hold)

    spark.stop()


if __name__ == "__main__":
    main()
