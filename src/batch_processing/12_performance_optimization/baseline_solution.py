"""BASELINE (UNOPTIMIZED) Spark job on Food Orders.

Deliberately uses anti-patterns to demonstrate performance issues:

  * Java serialization (Spark's slow default)
  * Reads CSV multiple times (no caching, no Parquet conversion)
  * Uses RDD.groupByKey() + Python lambdas (slow, ships all data)
  * Joins small lookup without broadcasting (full shuffle)
  * Default 200 shuffle partitions (huge per-task scheduling overhead)
  * Writes CSV (uncompressed, slow)
  * Re-evaluates source DataFrame multiple times

Usage:
    # Run on the Spark cluster via Docker (from this task's directory):
    #   src/batch_processing/12_performance_optimization
    docker compose up -d
    ./run_baseline.sh
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession


SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_PATH = str(SCRIPT_DIR / "data" / "input" / "food_orders_large.csv")
OUTPUT_DIR = str(SCRIPT_DIR / "data" / "output" / "baseline")
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("Performance Baseline")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "baseline.log"))
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
        .appName("FoodOrders-BASELINE")
        # Anti-pattern: Java serializer (default, slow)
        # .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
        # Anti-pattern: leave shuffle partitions at default 200
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    log.info("=" * 70)
    log.info("BASELINE JOB - Deliberately unoptimized")
    log.info("=" * 70)
    t0 = time.monotonic()

    # --- Anti-pattern: read CSV repeatedly, no caching ---
    log.info("[1/6] Reading CSV (3 times, no cache)...")
    df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
    df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)
    df3 = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)

    # --- Anti-pattern: filter via Python lambda on RDD ---
    log.info("[2/6] Filtering rated orders (RDD filter with Python lambda)...")
    rated = df1.rdd.filter(lambda r: r["rating"] != "Not given")
    rated_count = rated.count()
    log.info(f"      Rated orders: {rated_count:,}")

    # --- Anti-pattern: groupByKey + mean in Python ---
    log.info("[3/6] Computing avg cost per customer (RDD groupByKey + Python mean)...")
    avg_cost_by_customer = (
        df2.rdd
        .map(lambda r: (r["customer_id"], float(r["cost_of_the_order"])))
        .groupByKey()  # Anti-pattern: ships ALL values per key to one executor
        .mapValues(lambda vs: sum(vs) / len(list(vs)) if vs else 0.0)
    )
    n_customers = avg_cost_by_customer.count()
    log.info(f"      Customers: {n_customers:,}")

    # --- Anti-pattern: join small lookup without broadcasting ---
    log.info("[4/6] Joining with cuisine lookup (NO broadcast = full shuffle)...")
    lookup = spark.createDataFrame(CUISINE_REGION, ["cuisine_type", "region"])
    joined = df3.join(lookup, on="cuisine_type", how="left")
    by_region = (joined.groupBy("region")
                       .agg({"cost_of_the_order": "sum"}))
    log.info("      Total cost by region:")
    by_region.show()

    # --- Anti-pattern: write CSV (uncompressed) ---
    log.info("[5/6] Writing CSV output (uncompressed, slow)...")
    by_region.write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_DIR}/by_region")
    (df1.groupBy("restaurant_name").count()
        .write.mode("overwrite").option("header", "true")
        .csv(f"{OUTPUT_DIR}/by_restaurant"))

    elapsed = time.monotonic() - t0
    log.info("=" * 70)
    log.info(f"[baseline] rated_count={rated_count:,} n_customers={n_customers:,}")
    log.info(f"[baseline] elapsed={elapsed:.2f}s")
    log.info("=" * 70)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
