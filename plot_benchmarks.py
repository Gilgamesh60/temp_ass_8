"""
Reads metrics/spark_metrics.json + metrics/ray_metrics.json and produces:
  - figs/phase_comparison.png   : per-phase bar chart
  - figs/total_time.png         : total runtime comparison
  - figs/udf_comparison.png     : UDF-only comparison (UDF Deep-Dive)
  - figs/benchmark_table.csv    : flat table for the report
"""
import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def load(path):
    with open(path) as f:
        return json.load(f)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--spark", default="metrics/spark_metrics.json")
    p.add_argument("--ray", default="metrics/ray_metrics.json")
    p.add_argument("--outdir", default="figs")
    args = p.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    spark = load(args.spark)
    ray = load(args.ray)

    phases = ["ingestion_s", "cleansing_s", "join_s", "udf_s", "aggregate_s", "export_s"]
    nice = ["Ingest", "Clean", "Join", "UDF", "Aggregate", "Export"]

    df = pd.DataFrame(
        {
            "Phase": nice,
            "Spark (s)": [spark["phases"][ph] for ph in phases],
            "Ray (s)":   [ray["phases"][ph] for ph in phases],
        }
    )
    df.loc[len(df)] = ["Total", spark["phases"]["total_s"], ray["phases"]["total_s"]]
    df.to_csv(outdir / "benchmark_table.csv", index=False)
    print(df.to_string(index=False))

    # --- phase-by-phase bar chart
    fig, ax = plt.subplots(figsize=(9, 5))
    x = range(len(nice))
    w = 0.38
    ax.bar([i - w / 2 for i in x], df["Spark (s)"][:-1], width=w, label="Spark", color="#E25A1C")
    ax.bar([i + w / 2 for i in x], df["Ray (s)"][:-1],   width=w, label="Ray",   color="#2C8EBB")
    ax.set_xticks(list(x))
    ax.set_xticklabels(nice)
    ax.set_ylabel("Seconds")
    ax.set_title("Per-phase wall-clock time: Spark vs Ray")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(outdir / "phase_comparison.png", dpi=140)

    # --- totals
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.bar(["Spark", "Ray"],
           [spark["phases"]["total_s"], ray["phases"]["total_s"]],
           color=["#E25A1C", "#2C8EBB"])
    ax.set_ylabel("Total wall-clock seconds")
    ax.set_title("End-to-end pipeline runtime")
    for i, v in enumerate([spark["phases"]["total_s"], ray["phases"]["total_s"]]):
        ax.text(i, v, f"{v:.1f}s", ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(outdir / "total_time.png", dpi=140)

    # --- UDF zoom
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.bar(["Spark (Python UDF / JVM)", "Ray (Python-native)"],
           [spark["phases"]["udf_s"], ray["phases"]["udf_s"]],
           color=["#E25A1C", "#2C8EBB"])
    ax.set_ylabel("Seconds")
    ax.set_title("Python UDF overhead")
    for i, v in enumerate([spark["phases"]["udf_s"], ray["phases"]["udf_s"]]):
        ax.text(i, v, f"{v:.1f}s", ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(outdir / "udf_comparison.png", dpi=140)

    print(f"\nFigures written to {outdir}/")


if __name__ == "__main__":
    main()
