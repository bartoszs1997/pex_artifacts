#!/usr/bin/env bash
# Submit spark_ui_debug.py to the Spark Standalone cluster running in Docker.
# Usage: ./spark_submit.sh

set -euo pipefail

docker exec pex-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.ui.port=4040 \
    /opt/spark_job/spark_ui_debug.py
