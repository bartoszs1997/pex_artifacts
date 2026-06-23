"""Package Managers — Retrieve and process Spotify user data with requests + Spark.

Demonstrates using the `requests` package (installed via uv package manager)
to retrieve data from an internet source and processing it with Apache Spark.

Tasks:
  1. Retrieve artists with popularity > 1 and release_date after 2020-01-01.
  2. Retrieve songs with positive duration_ms and negative danceability.

Dataset: sauravkumaragarwal/user-datacsv (final_user_scores.csv)
Columns used: name, artists, release_date, popularity, duration_ms, danceability

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/04_package_managers/solution.py
"""

import logging
import sys
from pathlib import Path

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
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

DATA_FILE = INPUT_DIR / "final_user_scores.csv"
LOG_FILE = LOG_DIR / "package_managers.log"

# Kaggle dataset URL — used by requests to verify data source accessibility
DATASET_URL = "https://www.kaggle.com/datasets/sauravkumaragarwal/user-datacsv"

# ---------------------------------------------------------------------------
# Logging (dual: console + file)
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("package_managers")
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
# Schema (relevant columns only — Spark will ignore extra columns via select)
# ---------------------------------------------------------------------------
DATA_SCHEMA = StructType(
    [
        StructField("_c0", IntegerType(), nullable=True),
        StructField("id", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("artists", StringType(), nullable=True),
        StructField("release_date", StringType(), nullable=True),
        StructField("popularity", DoubleType(), nullable=True),
        StructField("duration_ms", DoubleType(), nullable=True),
        StructField("danceability", DoubleType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def verify_data_source() -> bool:
    """Use requests module to verify the dataset URL is accessible."""
    log.info("Verifying data source accessibility using requests module...")
    try:
        response = requests.head(DATASET_URL, timeout=10, allow_redirects=True)
        log.info(
            "Data source check: %s -> HTTP %d", DATASET_URL, response.status_code
        )
        if response.status_code == 200:
            log.info("Data source is accessible.")
            return True
        else:
            log.warning("Data source returned HTTP %d.", response.status_code)
            return False
    except requests.RequestException as e:
        log.error("Failed to reach data source: %s", e)
        return False


def task1_popular_recent_artists(df):
    """Retrieve artists with popularity > 1 and release_date after 2020-01-01."""
    log.info("=" * 60)
    log.info("TASK 1: Artists with popularity > 1 and release_date > 2020-01-01")
    log.info("=" * 60)

    df_result = df.filter(
        (F.col("popularity") > 1.0) & (F.col("release_date") > "2020-01-01")
    ).select("name", "artists", "release_date", "popularity")

    count = df_result.count()
    log.info("Found %d records matching criteria.", count)
    df_result.show(20, truncate=False)

    # Save output
    output_path = str(OUTPUT_DIR / "popular_recent_artists")
    df_result.write.mode("overwrite").csv(output_path, header=True)
    log.info("Result written to: %s", output_path)

    return df_result


def task2_positive_duration_negative_dance(df):
    """Retrieve songs with positive duration_ms and negative danceability."""
    log.info("=" * 60)
    log.info("TASK 2: Songs with duration_ms > 0 and danceability < 0")
    log.info("=" * 60)

    df_result = df.filter(
        (F.col("duration_ms") > 0.0) & (F.col("danceability") < 0.0)
    ).select("name", "artists", "duration_ms", "danceability")

    count = df_result.count()
    log.info("Found %d records matching criteria.", count)
    df_result.show(20, truncate=False)

    # Save output
    output_path = str(OUTPUT_DIR / "positive_duration_negative_dance")
    df_result.write.mode("overwrite").csv(output_path, header=True)
    log.info("Result written to: %s", output_path)

    return df_result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the package managers demonstration pipeline."""
    log.info("=" * 60)
    log.info("PACKAGE MANAGERS DEMO — Spotify User Data Analysis")
    log.info("=" * 60)

    # Step 1: Verify data source using requests
    verify_data_source()

    # Step 2: Check local file
    if not DATA_FILE.exists():
        log.error("Input file not found: %s. Run download_data.py first.", DATA_FILE)
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("PackageManagersDemo")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        log.info("SparkSession created.")

        # Load data with explicit schema for the first 8 columns
        df = spark.read.csv(str(DATA_FILE), header=True, schema=DATA_SCHEMA)
        df = df.select("name", "artists", "release_date", "popularity",
                       "duration_ms", "danceability")

        row_count = df.count()
        log.info("Loaded %d rows from %s", row_count, DATA_FILE.name)

        # Task 1
        task1_popular_recent_artists(df)

        # Task 2
        task2_positive_duration_negative_dance(df)

        log.info("=" * 60)
        log.info("ALL TASKS COMPLETED SUCCESSFULLY")
        log.info("=" * 60)

    except Exception:
        log.exception("Pipeline failed.")
        return 1
    finally:
        if spark:
            spark.stop()
            log.info("SparkSession stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
