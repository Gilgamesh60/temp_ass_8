#!/usr/bin/env bash
# Starts a Ray head node. Dashboard at :8265.
set -euo pipefail

ray stop || true
ray start --head \
  --port=6379 \
  --dashboard-host=0.0.0.0 \
  --dashboard-port=8265

echo
echo "Ray head up. Dashboard: http://$(hostname -I | awk '{print $1}'):8265"
echo "Give your teammate: ray start --address='$(hostname -I | awk '{print $1}'):6379'"
