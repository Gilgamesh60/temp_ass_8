#!/usr/bin/env bash
# Starts a Spark standalone worker pointing at MASTER_IP.
# Works with pip-installed PySpark.
set -euo pipefail

: "${MASTER_IP:?Set MASTER_IP to the master node IP}"
MASTER_URL="spark://${MASTER_IP}:7077"

# --- Find Java ---
if ! java -version &>/dev/null; then
  for p in /opt/homebrew/opt/openjdk@17 /usr/local/opt/openjdk@17 \
           /opt/homebrew/opt/openjdk    /usr/local/opt/openjdk; do
    if [[ -x "$p/bin/java" ]]; then
      export JAVA_HOME="$p"
      export PATH="$p/bin:$PATH"
      break
    fi
  done
fi
echo "Java: $(java -version 2>&1 | head -1)"

SPARK_HOME="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export SPARK_HOME
echo "SPARK_HOME=$SPARK_HOME"

# Get this machine's IP
if [[ "$(uname)" == "Darwin" ]]; then
  MY_IP="$(ipconfig getifaddr en0 2>/dev/null || echo 127.0.0.1)"
else
  MY_IP="$(hostname -I | awk '{print $1}')"
fi

echo "Joining $MASTER_URL as worker ($MY_IP) ..."
"$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
  --host "$MY_IP" \
  "$MASTER_URL" &

WORKER_PID=$!
echo "Worker PID: $WORKER_PID"
sleep 3
echo
echo "Worker joined. Check http://${MASTER_IP}:8080 to verify."
echo "To stop: kill $WORKER_PID"
