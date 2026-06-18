#!/usr/bin/env bash
# Submit baseline (unoptimized) job to Spark cluster

set -euo pipefail

docker exec perf-opt-spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark_job/baseline_solution.py
