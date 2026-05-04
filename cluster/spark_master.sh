#!/usr/bin/env bash
# Starts a Spark standalone master on this node.
# Spark UIs: master :8080, app :4040
set -euo pipefail

if [[ -z "${SPARK_HOME:-}" ]]; then
  SPARK_HOME="$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
fi
echo "Using SPARK_HOME=$SPARK_HOME"

"$SPARK_HOME/sbin/start-master.sh"
echo
echo "Spark master started. Open http://$(hostname -I | awk '{print $1}'):8080"
echo "Worker URL to share with teammate: spark://$(hostname -I | awk '{print $1}'):7077"
