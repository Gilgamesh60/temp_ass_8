"""
verify_parity.py - Confirms the Spark and Ray pipelines produced equivalent results.

Checks:
  1. Row counts in metrics JSON match across frameworks
  2. Aggregate parquets have identical (pickup_hour, Borough) group sets
  3. mean_speed_mph values match within a tolerance
  4. trip_count values match exactly

Usage:
    python3 verify_parity.py
    python3 verify_parity.py --spark metrics/spark_metrics.json --ray metrics/ray_metrics.json

Exit code 0 on parity, 1 on mismatch.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


def load_agg(path: Path) -> pd.DataFrame:
    # Works for both Spark's multi-part parquet dir and Ray's single-file parquet
    if path.is_dir():
        files = sorted(path.glob("**/*.parquet"))
        if not files:
            raise FileNotFoundError(f"No parquet files under {path}")
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return pd.read_parquet(path)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--spark", default="metrics/spark_metrics.json")
    p.add_argument("--ray", default="metrics/ray_metrics.json")
    p.add_argument("--spark-agg", default="output/spark_agg")
    p.add_argument("--ray-agg", default="output/ray_agg")
    p.add_argument("--speed-tol", type=float, default=0.01,
                   help="Absolute tolerance for mean_speed_mph differences (mph)")
    args = p.parse_args()

    failures: list[str] = []

    # ---- 1. Row counts -----
    sm = json.loads(Path(args.spark).read_text())
    rm = json.loads(Path(args.ray).read_text())

    print("== Row-count parity ==")
    for k in ("input_rows", "cleaned_rows", "joined_rows", "agg_rows"):
        s, r = sm[k], rm[k]
        ok = s == r
        flag = "OK" if ok else "DIFF"
        print(f"  {k:14s} spark={s:>12,}  ray={r:>12,}  [{flag}]")
        if not ok:
            failures.append(f"row count mismatch: {k} spark={s} ray={r}")

    # ---- 2 + 3 + 4. Aggregate DataFrame equivalence -----
    print("\n== Aggregate parity ==")
    sdf = load_agg(Path(args.spark_agg)).sort_values(["pickup_hour", "Borough"]).reset_index(drop=True)
    rdf = load_agg(Path(args.ray_agg)).sort_values(["pickup_hour", "Borough"]).reset_index(drop=True)

    common = ["pickup_hour", "Borough", "mean_speed_mph", "trip_count"]
    for col in common:
        if col not in sdf.columns:
            failures.append(f"Spark agg missing column: {col}")
        if col not in rdf.columns:
            failures.append(f"Ray agg missing column: {col}")
    if failures:
        for f in failures:
            print(f"  [FAIL] {f}")
        sys.exit(1)

    sdf = sdf[common]
    rdf = rdf[common]

    if len(sdf) != len(rdf):
        failures.append(f"agg row count mismatch: spark={len(sdf)} ray={len(rdf)}")
        print(f"  [FAIL] agg row count: spark={len(sdf)} ray={len(rdf)}")
    else:
        key_diff = (sdf[["pickup_hour", "Borough"]].reset_index(drop=True)
                    != rdf[["pickup_hour", "Borough"]].reset_index(drop=True)).any().any()
        if key_diff:
            failures.append("group keys differ between Spark and Ray agg outputs")
            print("  [FAIL] group keys differ")
        else:
            tc_diff = (sdf["trip_count"] != rdf["trip_count"])
            if tc_diff.any():
                bad = int(tc_diff.sum())
                failures.append(f"{bad} groups have different trip_count")
                print(f"  [FAIL] trip_count differs in {bad} groups")
            else:
                print(f"  [OK ] trip_count matches across all {len(sdf):,} groups")

            diffs = (sdf["mean_speed_mph"] - rdf["mean_speed_mph"]).abs()
            max_diff = float(diffs.max())
            bad = int((diffs > args.speed_tol).sum())
            if bad > 0:
                failures.append(f"{bad} groups exceed speed tolerance (max |diff|={max_diff:.4f})")
                print(f"  [FAIL] mean_speed_mph: {bad} groups exceed tol={args.speed_tol} (max diff {max_diff:.4f})")
            else:
                print(f"  [OK ] mean_speed_mph within tol={args.speed_tol} (max |diff|={max_diff:.4f})")

    print("\n== Summary ==")
    if failures:
        print("PARITY: FAIL")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("PARITY: PASS")


if __name__ == "__main__":
    main()
