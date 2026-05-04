#!/usr/bin/env bash
# Starts a Spark standalone master on this node.
# Works with pip-installed PySpark (no need for a full Spark download).
# UIs: master :8080, app :4040
set -euo pipefail

# --- Find Java (macOS has a /usr/bin/java shim that doesn't actually work) ---
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

echo "Starting Spark master on $MY_IP ..."
"$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
  --host "$MY_IP" \
  --port 7077 \
  --webui-port 8080 &

MASTER_PID=$!
echo "Master PID: $MASTER_PID"
sleep 3
echo
echo "Spark master started."
echo "  Master UI:  http://${MY_IP}:8080"
echo "  Worker URL: spark://${MY_IP}:7077"
echo
echo "To stop: kill $MASTER_PID"
