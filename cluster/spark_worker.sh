#!/usr/bin/env bash
# Starts a Spark standalone worker pointing at MASTER_IP.
set -euo pipefail

: "${MASTER_IP:?Set MASTER_IP to the master node IP}"
MASTER_URL="spark://${MASTER_IP}:7077"

if [[ -z "${SPARK_HOME:-}" ]]; then
  SPARK_HOME="$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
fi

echo "Joining $MASTER_URL"
"$SPARK_HOME/sbin/start-worker.sh" "$MASTER_URL"
echo "Worker joined. Check http://${MASTER_IP}:8080 to verify."
