"""
Spark UI Debug Job — designed to populate every tab of the Spark UI.

Jobs/Stages tab  → multiple actions (count, show, collect)
SQL tab          → spark.sql() with aggregation
Storage tab      → .cache() + action to materialize
Executors tab    → tasks distributed to worker
Environment tab  → custom configs visible

After all jobs finish, sleeps 60s so you can browse http://localhost:4040

Run:
    # 0. Go to the task directory (from repo root):
    cd src/batch_processing/11_spark_cluster

    # 1. Start the cluster (requires Colima running + docker in PATH):
    docker compose up -d

    # 2. Submit the job to the cluster:
    ./spark_submit.sh

    # 3. Open Spark UI (available for 60s after job finishes):
    open http://localhost:4040

    # 4. Stop the cluster when done:
    docker compose down
"""

import logging
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LOG_DIR = "/opt/spark_job/logs"
LOG_FILE = f"{LOG_DIR}/spark_cluster.log"

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("Spark Cluster")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

# File handler — logs persisted to host via Docker volume mount
import os
os.makedirs(LOG_DIR, exist_ok=True)
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def main():
    spark = (
        SparkSession.builder
        .appName("SparkUIDebug")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:///tmp/spark-events")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )

    print("=" * 60)
    print(f"APP ID   : {spark.sparkContext.applicationId}")
    print(f"MASTER   : {spark.sparkContext.master}")
    print(f"SPARK UI : http://localhost:4040")
    print("=" * 60)
    log.info(f"APP ID: {spark.sparkContext.applicationId} | MASTER: {spark.sparkContext.master}")

    # ------------------------------------------------------------------
    # Job 1: Narrow transformation + count (Jobs tab: 1 job, 1 stage)
    # ------------------------------------------------------------------
    print("\n>>> JOB 1: Create dataset + count")
    log.info("JOB 1: Create dataset + count")
    data = [
        (f"EMP_{i:04d}", dept, 40000 + (i * 137) % 60000)
        for i in range(500)
        for dept in ["Engineering", "Marketing", "Sales", "HR", "Finance"]
    ]
    df = spark.createDataFrame(data, ["employee_id", "department", "salary"])
    row_count = df.count()
    print(f"    Rows: {row_count}")
    log.info(f"    Rows: {row_count}")

    # ------------------------------------------------------------------
    # Job 2: SQL aggregation (SQL tab: shows logical/physical plan)
    # ------------------------------------------------------------------
    print("\n>>> JOB 2: SQL aggregation")
    log.info("JOB 2: SQL aggregation")
    df.createOrReplaceTempView("employees")
    result = spark.sql("""
        SELECT department,
               COUNT(*)              AS cnt,
               ROUND(AVG(salary), 2) AS avg_salary,
               MAX(salary)           AS max_salary,
               MIN(salary)           AS min_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """)
    result.show()

    # ------------------------------------------------------------------
    # Job 3: GroupBy + cache (Storage tab: cached RDD visible)
    # ------------------------------------------------------------------
    print("\n>>> JOB 3: GroupBy + cache")
    log.info("JOB 3: GroupBy + cache")
    dept_stats = (
        df.groupBy("department")
        .agg(
            F.count("*").alias("count"),
            F.avg("salary").alias("avg_salary"),
            F.stddev("salary").alias("std_salary"),
        )
    )
    dept_stats.cache()
    dept_stats.show()
    cached_parts = dept_stats.rdd.getNumPartitions()
    print(f"    Cached partitions: {cached_parts}")
    log.info(f"    Cached partitions: {cached_parts}")

    # ------------------------------------------------------------------
    # Job 4: Deliberate data skew (straggler task visible in Stages tab)
    # ------------------------------------------------------------------
    print("\n>>> JOB 4: Skewed repartition + aggregation")
    log.info("JOB 4: Skewed repartition + aggregation")
    skewed = df.withColumn(
        "skew_key",
        F.when(F.col("department") == "Engineering", F.lit("HOT"))
         .otherwise(F.col("department"))
    )
    skew_result = skewed.groupBy("skew_key").agg(F.sum("salary").alias("total"))
    skew_result.show()

    # ------------------------------------------------------------------
    # Job 5: Write output (creates shuffle + write stage)
    # ------------------------------------------------------------------
    print("\n>>> JOB 5: Write output")
    log.info("JOB 5: Write output")
    output_path = "/opt/spark_job/output"

    dept_stats.coalesce(1).write.mode("overwrite").json(f"{output_path}/dept_stats")
    print("    Written dept_stats (JSON)")
    log.info("    Written dept_stats (JSON)")

    result.coalesce(1).write.mode("overwrite").csv(
        f"{output_path}/sql_aggregation", header=True
    )
    print("    Written sql_aggregation (CSV)")
    log.info("    Written sql_aggregation (CSV)")

    skew_result.coalesce(1).write.mode("overwrite").parquet(f"{output_path}/skew_analysis")
    print("    Written skew_analysis (Parquet)")
    log.info("    Written skew_analysis (Parquet)")

    # ------------------------------------------------------------------
    # Hold the driver alive so Spark UI stays accessible
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("ALL JOBS COMPLETE — Spark UI available at http://localhost:4040")
    print("Sleeping 60s... (Ctrl+C to exit)")
    print("=" * 60)
    log.info("ALL JOBS COMPLETE — Sleeping 60s for Spark UI access")

    time.sleep(60)
    spark.stop()
    print("Done.")
    log.info("Done.")


if __name__ == "__main__":
    main()
