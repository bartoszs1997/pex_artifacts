"""OPTIMIZED Spark job on Food Orders.

Applies ALL NEBo optimization techniques:

  * Kryo serialization (faster than Java)
  * DataFrame API + built-in functions (Catalyst + Tungsten optimize)
  * Source DataFrame cached + reused
  * Small lookup table broadcast (avoids shuffle on big side)
  * agg(F.avg(...)) instead of RDD groupByKey (map-side combine)
  * Parquet format (columnar, compressed, splittable)
  * Tuned shuffle partitions (4 instead of 200 for 2-core cluster)
  * AQE enabled (handles skew + coalesces small partitions)

Usage:
    # Run on the Spark cluster via Docker (from this task's directory):
    #   src/batch_processing/12_performance_optimization
    docker compose up -d
    ./run_optimized.sh
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SCRIPT_DIR = Path(__file__).resolve().parent
PARQUET_INPUT = str(SCRIPT_DIR / "data" / "input" / "food_orders_large.parquet")
OUTPUT_DIR = str(SCRIPT_DIR / "data" / "output" / "optimized")
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("Performance Optimized")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "optimized.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)



CUISINE_REGION = [
    ("American", "North America"),
    ("Italian", "Europe"),
    ("Japanese", "Asia"),
    ("Chinese", "Asia"),
    ("Indian", "Asia"),
    ("Mexican", "North America"),
    ("Mediterranean", "Europe"),
    ("French", "Europe"),
]


def main() -> int:
    # Create output dir (LOG_DIR already created at module level)
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("FoodOrders-OPTIMIZED")
        # Kryo - smaller serialized objects, faster shuffle
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # AQE handles skew / small partitions at runtime
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Fit shuffle partitions to cluster size (2 cores -> 4 partitions)
        .config("spark.sql.shuffle.partitions", "4")
        # Force broadcast for tables under 50 MB (default 10 MB)
        .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    log.info("=" * 70)
    log.info("OPTIMIZED JOB - All NEBo techniques applied")
    log.info("=" * 70)
    t0 = time.monotonic()

    # --- Optimization: read pre-built Parquet + cache ---
    # The dataset is stored as Parquet at prep time (see download_data.py),
    # so the job only READS it here. Choosing the storage format is a one-time
    # data-prep decision - the optimized job never pays a conversion cost.
    log.info("[1/5] Reading pre-built Parquet + caching in memory...")
    df = spark.read.parquet(PARQUET_INPUT).cache()
    df.count()  # materialize the cache once
    log.info(f"      Cached {df.count():,} rows")

    # --- DataFrame filter using SQL columns (Catalyst pushdown) ---
    log.info("[2/5] Filtering rated orders (DataFrame API, optimized)...")
    rated = df.filter(F.col("rating") != F.lit("Not given"))
    rated_count = rated.count()
    log.info(f"      Rated orders: {rated_count:,}")

    # --- DataFrame aggregation: groupBy().agg(avg) ---
    log.info("[3/5] Computing avg cost per customer (DataFrame agg, map-side combine)...")
    avg_cost_by_customer = (
        df.groupBy("customer_id")
          .agg(F.avg("cost_of_the_order").alias("avg_cost"))
    )
    n_customers = avg_cost_by_customer.count()
    log.info(f"      Customers: {n_customers:,}")

    # --- Optimization: broadcast the small lookup table ---
    log.info("[4/5] Joining with cuisine lookup (BROADCAST = no shuffle on big side)...")
    lookup = spark.createDataFrame(CUISINE_REGION, ["cuisine_type", "region"])
    joined = df.join(F.broadcast(lookup), on="cuisine_type", how="left")
    by_region = (joined.groupBy("region")
                       .agg(F.sum("cost_of_the_order").alias("total_cost")))
    log.info("      Total cost by region:")
    by_region.show()

    # --- Optimization: write Parquet output (compressed, fast) ---
    log.info("[5/5] Writing Parquet output (compressed, columnar)...")
    by_region.coalesce(1).write.mode("overwrite").parquet(f"{OUTPUT_DIR}/by_region")
    (df.groupBy("restaurant_name").count()
        .write.mode("overwrite").parquet(f"{OUTPUT_DIR}/by_restaurant"))

    elapsed = time.monotonic() - t0
    log.info("=" * 70)
    log.info(f"[optimized] rated_count={rated_count:,} n_customers={n_customers:,}")
    log.info(f"[optimized] elapsed={elapsed:.2f}s")
    log.info("=" * 70)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
