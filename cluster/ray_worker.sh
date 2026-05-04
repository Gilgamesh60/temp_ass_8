#!/usr/bin/env bash
# Joins a Ray cluster as a worker.
set -euo pipefail

: "${HEAD_IP:?Set HEAD_IP to the head node IP}"

ray stop || true
ray start --address="${HEAD_IP}:6379"
echo "Worker joined. Verify in the dashboard at http://${HEAD_IP}:8265"
