# DA5402 A8: Spark vs. Ray — The Data Engineering Duel

Benchmarks an identical NYC Taxi data preprocessing pipeline on Apache Spark and Ray across a 2-node cluster.

## Layout

```
.
├── spark_clean.py           # PySpark pipeline
├── ray_clean.py             # Ray Data pipeline (identical logic)
├── download_data.py         # Pulls NYC Taxi parquet + zone lookup
├── verify_parity.py         # Proves Spark and Ray produced the same results
├── plot_benchmarks.py       # Per-phase / total / UDF bar charts
├── run_benchmark.sh         # Runs both pipelines, samples resources, plots
├── monitor_resources.sh     # Periodic `top` sampler during a run
├── cluster/
│   ├── spark_master.sh
│   ├── spark_worker.sh
│   ├── ray_head.sh
│   └── ray_worker.sh
├── report/
│   └── report_template.md   # Fill in and export to PDF
├── screenshots/             # UI evidence goes here
├── metrics/                 # Generated JSON
├── output/                  # Generated parquet
├── figs/                    # Generated charts
├── requirements.txt
└── .gitignore
```

## Quick start (single machine, for rehearsal)

```bash
pip install -r requirements.txt
python3 download_data.py
bash run_benchmark.sh
```

Runs both pipelines in local mode and produces:
- `metrics/spark_metrics.json`, `metrics/ray_metrics.json`
- `figs/phase_comparison.png`, `total_time.png`, `udf_comparison.png`
- parity verification output

## Full run on the 2-node cluster

On **Node 1** (master / head):
```bash
bash cluster/spark_master.sh
```
Open `http://<node1>:8080` — screenshot showing 2 workers.

On **Node 2** (worker):
```bash
MASTER_IP=<node1> bash cluster/spark_worker.sh
```

Run Spark:
```bash
SPARK_HOLD_SECONDS=180 MASTER_URL=spark://<node1>:7077 python3 spark_clean.py
```
Screenshot `http://<node1>:4040` during the 180s hold.

Stop Spark, bring up Ray:
```bash
# Node 1
$SPARK_HOME/sbin/stop-master.sh
bash cluster/ray_head.sh
# Node 2
$SPARK_HOME/sbin/stop-worker.sh
HEAD_IP=<node1> bash cluster/ray_worker.sh
```
Screenshot `http://<node1>:8265`.

Run Ray:
```bash
RAY_HOLD_SECONDS=180 RAY_ADDRESS=<node1>:6379 python3 ray_clean.py
```

Verify and plot:
```bash
python3 verify_parity.py
python3 plot_benchmarks.py
```

## Rubric crosswalk

| Rubric item | Where it lives |
|---|---|
| Cluster Orchestration (25) | `cluster/*.sh`, `screenshots/` |
| Pipeline Parity (25) | Same 6-stage contract in both scripts; `verify_parity.py` proves it |
| Performance Analysis (20) | `metrics/*.json`, `plot_benchmarks.py`, `metrics/top_*.log` |
| UDF Deep-Dive (15) | `udf_s` timed in both; `udf_comparison.png`; Section 5 of report |
| Documentation (15) | `report/report_template.md` → export to PDF |

## Why a classic Python UDF in Spark (not `pandas_udf`)

The brief asks you to measure "how long the custom Python transformation takes in Spark (JVM overhead) vs. Ray (Python-native)." A `pandas_udf` uses Arrow to bypass most of that overhead and closes the gap — which would hide the exact effect the assignment wants to surface. So `spark_clean.py` uses a row-level `F.udf(...)` deliberately.
