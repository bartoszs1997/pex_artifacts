#!/usr/bin/env bash
# Submit optimized job to Spark cluster

set -euo pipefail

docker exec perf-opt-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark_job/optimized_solution.py
