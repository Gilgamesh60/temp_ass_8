#!/usr/bin/env bash
# Runs the Spark pipeline, then the Ray pipeline, with top sampling.
# Assumes both clusters are already up (or uses local mode if env vars unset).
#
# env:
#   MASTER_URL     spark master URL (e.g. spark://10.0.0.1:7077)
#   RAY_ADDRESS    ray head address (e.g. 10.0.0.1:6379)
#   SPARK_HOLD_SECONDS, RAY_HOLD_SECONDS   seconds to keep UI alive after run
set -euo pipefail

mkdir -p metrics output figs

run() {
  local tag="$1"; shift
  echo; echo "=== $tag ==="
  bash monitor_resources.sh "$tag" &
  local mon_pid=$!
  sleep 1
  "$@"
  kill "$mon_pid" 2>/dev/null || true
  wait "$mon_pid" 2>/dev/null || true
}

run spark python3 spark_clean.py ${MASTER_URL:+--master "$MASTER_URL"}
run ray   python3 ray_clean.py   ${RAY_ADDRESS:+--address "$RAY_ADDRESS"}

echo
echo "=== Parity check ==="
python3 verify_parity.py || echo "Parity check reported issues (see above)."

echo
echo "=== Plots ==="
python3 plot_benchmarks.py

echo
echo "=== Done. Artifacts ==="
echo "  metrics/spark_metrics.json"
echo "  metrics/ray_metrics.json"
echo "  output/spark_result/  output/ray_result/"
echo "  figs/phase_comparison.png  figs/total_time.png  figs/udf_comparison.png"
