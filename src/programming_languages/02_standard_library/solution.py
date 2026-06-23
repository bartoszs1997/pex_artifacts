"""Instacart Orders — Standard Library Demonstration.

Demonstrates the use of Python standard library modules (re, datetime, math,
os, subprocess, zipfile) alongside PySpark RDDs and DataFrames to clean and
analyze the Instacart orders dataset.

Tasks performed:
  1. (re)         — Identify morning orders (hours 05-10), add morning_order column.
  2. (datetime)   — Calculate date_ordered from days_since_prior_order.
  3. (math)       — Count orders where order_dow == 3 using math module.
  4. (os)         — Create a 'reports' directory for generated reports.
  5. (subprocess) — List files in the current working directory via system command.
  6. (zipfile)    — Compress orders.csv into orders.zip archive.

Dataset columns:
  order_id, user_id, eval_set, order_number, order_dow,
  order_hour_of_day, days_since_prior_order

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/02_standard_library/solution.py
"""

# ---------------------------------------------------------------------------
# Imports — Standard Library
# ---------------------------------------------------------------------------
import datetime
import logging
import math
import os
import re
import subprocess
import sys
import zipfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Imports — PySpark
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "data" / "input"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"
LOG_DIR = SCRIPT_DIR / "logs"
REPORTS_DIR = SCRIPT_DIR / "reports"

ORDERS_FILE = INPUT_DIR / "orders.csv"
LOG_FILE = LOG_DIR / "standard_library.log"

# Reference date for computing date_ordered (arbitrary anchor)
REFERENCE_DATE = datetime.date(2024, 1, 1)

# ---------------------------------------------------------------------------
# Logging (dual: console + file)
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("standard_library")
log.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), nullable=False),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("eval_set", StringType(), nullable=False),
        StructField("order_number", IntegerType(), nullable=False),
        StructField("order_dow", IntegerType(), nullable=False),
        StructField("order_hour_of_day", StringType(), nullable=False),
        StructField("days_since_prior_order", IntegerType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Regex pattern for morning hours (05, 06, 07, 08, 09, 10)
# ---------------------------------------------------------------------------
MORNING_PATTERN = re.compile(r"^(0[5-9]|10)$")


# ---------------------------------------------------------------------------
# Task Functions
# ---------------------------------------------------------------------------


def task1_morning_orders(spark, df):
    """Task 1: Use re module to mark morning orders (hours 05-10).

    Creates a 'morning_order' column with values 'yes' or 'no'.
    """
    log.info("=" * 70)
    log.info("TASK 1: Identify morning orders using the re module")
    log.info("=" * 70)

    def is_morning(hour_str):
        """Check if hour is between 05 and 10 using regex."""
        if hour_str is None:
            return "no"
        return "yes" if MORNING_PATTERN.match(hour_str) else "no"

    morning_udf = udf(is_morning, StringType())
    df_with_morning = df.withColumn("morning_order", morning_udf("order_hour_of_day"))

    log.info("Sample rows with morning_order column:")
    df_with_morning.select(
        "order_id", "order_hour_of_day", "morning_order"
    ).show(20, truncate=False)

    morning_count = df_with_morning.filter(df_with_morning.morning_order == "yes").count()
    total_count = df_with_morning.count()
    log.info(
        "Morning orders (05-10): %d / %d (%.1f%%)",
        morning_count,
        total_count,
        100.0 * morning_count / total_count if total_count > 0 else 0,
    )

    # Save result
    output_path = str(OUTPUT_DIR / "morning_orders")
    df_with_morning.write.mode("overwrite").csv(output_path, header=True)
    log.info("Result written to: %s", output_path)

    return df_with_morning


def task2_date_ordered(spark, df):
    """Task 2: Use datetime module to compute date_ordered from days_since_prior_order.

    For each order, subtracts days_since_prior_order from a reference date
    to produce a date_ordered column.
    """
    log.info("=" * 70)
    log.info("TASK 2: Compute date_ordered using the datetime module")
    log.info("=" * 70)

    def compute_date(days_since):
        """Subtract days from reference date to get order date."""
        if days_since is None:
            return None
        try:
            order_date = REFERENCE_DATE - datetime.timedelta(days=int(days_since))
            return order_date.isoformat()
        except (ValueError, TypeError):
            return None

    date_udf = udf(compute_date, StringType())
    df_with_date = df.withColumn("date_ordered", date_udf("days_since_prior_order"))

    log.info("Sample rows with date_ordered column:")
    df_with_date.select(
        "order_id", "days_since_prior_order", "date_ordered"
    ).show(20, truncate=False)

    non_null_dates = df_with_date.filter(df_with_date.date_ordered.isNotNull()).count()
    log.info("Orders with computed date_ordered: %d", non_null_dates)

    # Save result
    output_path = str(OUTPUT_DIR / "date_ordered")
    df_with_date.write.mode("overwrite").csv(output_path, header=True)
    log.info("Result written to: %s", output_path)

    return df_with_date


def task3_total_dow_3(spark, df):
    """Task 3: Calculate total orders where order_dow == 3 using math module.

    Collects the count per partition using RDD and uses math.fsum for the total.
    """
    log.info("=" * 70)
    log.info("TASK 3: Count orders with order_dow == 3 using the math module")
    log.info("=" * 70)

    # Filter orders where order_dow == 3, get counts per partition via RDD
    dow_3_rdd = df.filter(df.order_dow == 3).rdd
    partition_counts = dow_3_rdd.mapPartitions(
        lambda partition: [math.fsum(1 for _ in partition)]
    ).collect()

    # Use math.fsum to sum all partition counts
    total = int(math.fsum(partition_counts))

    log.info("Total orders with order_dow == 3: %d", total)
    log.info("  (computed using math.fsum across %d partitions)", len(partition_counts))

    # Also display using math.ceil/math.floor to demonstrate module usage
    log.info("  math.ceil(total / 1000) = %d (thousands, rounded up)", math.ceil(total / 1000))
    log.info("  math.floor(total / 1000) = %d (thousands, rounded down)", math.floor(total / 1000))
    log.info("  math.log10(total) = %.2f", math.log10(total) if total > 0 else 0)

    return total


def task4_create_reports_dir():
    """Task 4: Create a 'reports' directory using the os module."""
    log.info("=" * 70)
    log.info("TASK 4: Create reports directory using the os module")
    log.info("=" * 70)

    reports_path = str(REPORTS_DIR)

    if not os.path.exists(reports_path):
        os.makedirs(reports_path)
        log.info("Created directory: %s", reports_path)
    else:
        log.info("Directory already exists: %s", reports_path)

    # List directory contents using os module
    log.info("Directory listing of parent (os.listdir):")
    parent_contents = os.listdir(str(SCRIPT_DIR))
    for item in sorted(parent_contents):
        item_path = os.path.join(str(SCRIPT_DIR), item)
        item_type = "DIR" if os.path.isdir(item_path) else "FILE"
        log.info("  [%s] %s", item_type, item)

    return reports_path


def task5_list_files_subprocess():
    """Task 5: Use subprocess module to list files in current directory."""
    log.info("=" * 70)
    log.info("TASK 5: List files using the subprocess module")
    log.info("=" * 70)

    # Execute 'ls -la' (macOS/Linux) to list files in the script directory
    try:
        result = subprocess.run(
            ["ls", "-la"],
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            check=True,
        )
        log.info("Command: ls -la (in %s)", SCRIPT_DIR)
        log.info("Return code: %d", result.returncode)
        log.info("Output:\n%s", result.stdout)
    except subprocess.CalledProcessError as e:
        log.error("Command failed with return code %d: %s", e.returncode, e.stderr)
    except FileNotFoundError:
        log.error("Command 'ls' not found. Trying 'dir' for Windows...")
        result = subprocess.run(
            ["cmd", "/c", "dir"],
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
        log.info("Output:\n%s", result.stdout)

    return result


def task6_compress_zip():
    """Task 6: Compress orders.csv into a ZIP archive using zipfile module."""
    log.info("=" * 70)
    log.info("TASK 6: Compress orders.csv using the zipfile module")
    log.info("=" * 70)

    zip_path = REPORTS_DIR / "orders.zip"
    source_path = ORDERS_FILE

    if not source_path.exists():
        log.error("Source file not found: %s", source_path)
        return None

    # Create ZIP archive
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(source_path, arcname="orders.csv")

    original_size = source_path.stat().st_size
    zip_size = zip_path.stat().st_size
    compression_ratio = (1 - zip_size / original_size) * 100

    log.info("Source file: %s (%.1f MB)", source_path.name, original_size / (1024 * 1024))
    log.info("ZIP archive: %s (%.1f MB)", zip_path, zip_size / (1024 * 1024))
    log.info("Compression ratio: %.1f%%", compression_ratio)

    # Verify archive contents
    with zipfile.ZipFile(zip_path, "r") as zf:
        log.info("Archive contents:")
        for info in zf.infolist():
            log.info(
                "  %s — %d bytes (compressed: %d bytes)",
                info.filename,
                info.file_size,
                info.compress_size,
            )

    return str(zip_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Run all 6 tasks demonstrating Python standard library modules."""
    log.info("=" * 70)
    log.info("STANDARD LIBRARY DEMONSTRATION — Instacart Orders Dataset")
    log.info("=" * 70)

    # Check input file
    if not ORDERS_FILE.exists():
        log.error(
            "Input file not found: %s — run download_data.py first.", ORDERS_FILE
        )
        return 1

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = None
    try:
        # Initialize SparkSession
        spark = (
            SparkSession.builder.appName("StandardLibraryDemo")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        log.info("SparkSession created successfully.")

        # Load dataset
        df = spark.read.csv(
            str(ORDERS_FILE), header=True, schema=ORDERS_SCHEMA, quote='"'
        )
        row_count = df.count()
        log.info("Loaded %d orders from %s", row_count, ORDERS_FILE.name)
        df.printSchema()

        # ----- Task 1: re module -----
        df_morning = task1_morning_orders(spark, df)

        # ----- Task 2: datetime module -----
        df_dated = task2_date_ordered(spark, df)

        # ----- Task 3: math module -----
        total_dow_3 = task3_total_dow_3(spark, df)

        # ----- Task 4: os module -----
        task4_create_reports_dir()

        # ----- Task 5: subprocess module -----
        task5_list_files_subprocess()

        # ----- Task 6: zipfile module -----
        task6_compress_zip()

        # Final summary
        log.info("=" * 70)
        log.info("ALL 6 TASKS COMPLETED SUCCESSFULLY")
        log.info("=" * 70)
        log.info("Modules demonstrated: re, datetime, math, os, subprocess, zipfile")
        log.info("Output directory: %s", OUTPUT_DIR)
        log.info("Reports directory: %s", REPORTS_DIR)

    except Exception:
        log.exception("Pipeline failed with an unexpected error.")
        return 1
    finally:
        if spark:
            spark.stop()
            log.info("SparkSession stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
