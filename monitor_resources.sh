#!/usr/bin/env bash
# Samples CPU + memory every 2 seconds and writes to metrics/top_<label>.log
# Usage: bash monitor_resources.sh <label>  (runs until you Ctrl-C)
set -euo pipefail

LABEL="${1:-run}"
OUT="metrics/top_${LABEL}.log"
mkdir -p metrics

echo "Sampling to $OUT (Ctrl-C to stop)"
while true; do
  ts="$(date -Iseconds)"
  # -b batch, -n 1 single iteration
  if command -v top >/dev/null 2>&1; then
    # Darwin vs Linux top differ; this works on Linux. On macOS use `top -l 1 -n 0`.
    if [[ "$(uname)" == "Darwin" ]]; then
      cpu=$(top -l 1 -n 0 | awk '/CPU usage/ {print $0}')
      mem=$(top -l 1 -n 0 | awk '/PhysMem/ {print $0}')
    else
      cpu=$(top -bn1 | awk '/Cpu\(s\)/ {print $0}')
      mem=$(free -m | awk '/Mem:/ {print "mem_used_mb="$3" mem_total_mb="$2}')
    fi
    echo "[$ts] $cpu | $mem" >> "$OUT"
  fi
  sleep 2
done
