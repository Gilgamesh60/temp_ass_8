"""
ray_clean.py - Ray Data implementation of the NYC Taxi preprocessing pipeline.

Mirror of spark_clean.py -- same inputs, same filters, same UDF, same outputs.
The UDF here is a plain Python function applied via Dataset.map_batches, which
stays Python-native the whole time (no JVM serialization).

Metrics captured in metrics/<label>_ray_metrics.json.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import time
from pathlib import Path

import numpy as np
import pandas as pd
import ray

TRIPS_GLOB = "./data/trips"
ZONES_CSV = "./data/zones/taxi_zone_lookup.csv"
OUTPUT_DIR = "./output/ray_result"
AGG_OUTPUT_DIR = "./output/ray_agg"
METRICS_FILE_DEFAULT = "./metrics/ray_metrics.json"

REQUIRED_COLS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "PULocationID", "DOLocationID",
]

DEDUP_KEYS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "trip_distance",
]


def clean_batch(batch: pd.DataFrame) -> pd.DataFrame:
    """Stage 2: drop nulls, cast timestamps, filter bad rows, per-block dedup."""
    batch = batch.dropna(subset=REQUIRED_COLS)
    batch["pickup_ts"] = pd.to_datetime(batch["tpep_pickup_datetime"], utc=True)
    batch["dropoff_ts"] = pd.to_datetime(batch["tpep_dropoff_datetime"], utc=True)
    batch["trip_duration_s"] = (
        (batch["dropoff_ts"] - batch["pickup_ts"]).dt.total_seconds().astype("int64")
    )
    batch = batch[batch["trip_distance"] > 0]
    batch = batch[batch["trip_duration_s"] > 0]
    batch = batch[batch["trip_duration_s"] < 6 * 3600]
    # Per-block dedup. Spark's dropDuplicates() is a full shuffle; we approximate
    # it here because NYC TLC parquet files do not duplicate rows across files
    # (each month is a separate source). Documented as a parity note in the report.
    batch = batch.drop_duplicates(subset=DEDUP_KEYS)
    return batch


def compute_speed_batch(batch: pd.DataFrame) -> pd.DataFrame:
    """Stage 4: the Python-native UDF. Same formula as Spark's avg_speed_mph()."""
    d = batch["trip_distance"].astype(float)
    s = batch["trip_duration_s"].astype(float)
    hours = s / 3600.0
    speed = np.where(hours > 0, d / hours, np.nan)
    batch = batch.copy()
    batch["avg_speed_mph"] = speed
    batch["pickup_hour"] = pd.to_datetime(batch["pickup_ts"], utc=True).dt.hour
    return batch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default=os.environ.get("RAY_ADDRESS"),
                        help="Ray head address, e.g. 10.0.0.1:6379. If unset, starts local cluster.")
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

    if args.address:
        ray.init(address=args.address)
    else:
        ray.init()

    print(f"Ray cluster resources: {ray.cluster_resources()}")

    # --- 1. INGEST -----------------------------------------------------------
    t0 = time.perf_counter()
    trips = ray.data.read_parquet(args.trips)
    zones_pd = pd.read_csv(args.zones)
    input_rows = trips.count()
    phase["ingestion_s"] = time.perf_counter() - t0
    print(f"[ingest]  trips={input_rows:,}  zones={len(zones_pd):,}  ({phase['ingestion_s']:.1f}s)")

    # --- 2. CLEANSE ----------------------------------------------------------
    t0 = time.perf_counter()
    cleaned = trips.map_batches(clean_batch, batch_format="pandas").materialize()
    cleaned_rows = cleaned.count()
    phase["cleansing_s"] = time.perf_counter() - t0
    print(f"[clean]   rows={cleaned_rows:,}  ({phase['cleansing_s']:.1f}s)")

    # --- 3. HEAVY JOIN -------------------------------------------------------
    # Zones is tiny (~265 rows) -> broadcast-style join via a map_batches closure.
    # This is the natural Ray idiom; Spark equivalent would be F.broadcast(zones)
    # but we chose NOT to use that hint on the Spark side to keep the shuffle-join
    # behavior observable in the Spark UI.
    t0 = time.perf_counter()
    zones_lookup = zones_pd.set_index("LocationID")[["Borough", "Zone"]].to_dict(orient="index")

    def broadcast_join(batch: pd.DataFrame) -> pd.DataFrame:
        boroughs, zones_ = [], []
        for loc in batch["PULocationID"].astype(int).tolist():
            row = zones_lookup.get(loc)
            boroughs.append(row["Borough"] if row else None)
            zones_.append(row["Zone"] if row else None)
        batch = batch.copy()
        batch["Borough"] = boroughs
        batch["Zone"] = zones_
        return batch.dropna(subset=["Borough"])  # inner-join semantics

    joined = cleaned.map_batches(broadcast_join, batch_format="pandas").materialize()
    joined_rows = joined.count()
    phase["join_s"] = time.perf_counter() - t0
    print(f"[join]    rows={joined_rows:,}  ({phase['join_s']:.1f}s)")

    # --- 4. PYTHON UDF -------------------------------------------------------
    t0 = time.perf_counter()
    with_speed = joined.map_batches(compute_speed_batch, batch_format="pandas").materialize()
    udf_rows = with_speed.count()
    phase["udf_s"] = time.perf_counter() - t0
    print(f"[udf]     rows={udf_rows:,}  ({phase['udf_s']:.1f}s)  <- Python-native")

    # --- 5. AGGREGATE --------------------------------------------------------
    t0 = time.perf_counter()
    from ray.data.aggregate import Mean, Count
    agg_ds = (
        with_speed
        .groupby(["pickup_hour", "Borough"])
        .aggregate(Mean("avg_speed_mph"), Count())
    )
    agg_df = agg_ds.to_pandas()
    # Normalize column names to match Spark output
    agg_df = agg_df.rename(columns={
        "mean(avg_speed_mph)": "mean_speed_mph",
        "count()": "trip_count",
    })
    agg_df = agg_df.sort_values(["pickup_hour", "Borough"]).reset_index(drop=True)
    phase["aggregate_s"] = time.perf_counter() - t0
    agg_rows = len(agg_df)
    print(f"[agg]     groups={agg_rows:,}  ({phase['aggregate_s']:.1f}s)")

    # --- 6. EXPORT -----------------------------------------------------------
    t0 = time.perf_counter()
    export_cols = [
        "pickup_ts", "dropoff_ts", "trip_distance", "trip_duration_s",
        "PULocationID", "Borough", "Zone", "avg_speed_mph", "pickup_hour",
    ]
    (with_speed
        .select_columns(export_cols)
        .write_parquet(args.output))
    Path(args.agg_output).mkdir(parents=True, exist_ok=True)
    agg_df.to_parquet(Path(args.agg_output) / "part-0.parquet", index=False)
    phase["export_s"] = time.perf_counter() - t0
    print(f"[export]  {args.output}  ({phase['export_s']:.1f}s)")

    total = time.perf_counter() - t_total_start
    phase["total_s"] = total

    metrics = {
        "framework": "ray",
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "ray_version": ray.__version__,
        "address": args.address or "local",
        "cluster_resources": ray.cluster_resources(),
        "input_rows": input_rows,
        "cleaned_rows": cleaned_rows,
        "joined_rows": joined_rows,
        "agg_rows": agg_rows,
        "phases": phase,
    }
    with open(args.metrics, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"\n[metrics] written to {args.metrics}")
    print(f"[TOTAL]   {total:.1f}s")

    hold = int(os.environ.get("RAY_HOLD_SECONDS", "0"))
    if hold > 0:
        print(f"Holding Ray cluster for {hold}s so you can screenshot :8265 ...")
        time.sleep(hold)

    ray.shutdown()


if __name__ == "__main__":
    main()
