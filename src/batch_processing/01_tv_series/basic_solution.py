"""Spark application for batch module.

Run:
    # Java 17 must be on PATH (PySpark 4.x requirement):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/batch_processing/01_tv_series/basic_solution.py
    uv run python src/batch_processing/01_tv_series/basic_solution.py --pause
"""

import argparse
import logging
import sys
from argparse import Namespace
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode

formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S")
log = logging.getLogger("Tv Series Analysis")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler("spark_series.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

DEFAULT_JSON = Path(__file__).resolve().parent / "data" / "input" / "tvs.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "output"


def get_spark_session() -> SparkSession:
    """Get spark session."""
    log.info("Creating spark session...")

    return (
        SparkSession.builder.appName("ReadingData")
        # Force loopback networking so Spark's internal block transfers stay on
        # 127.0.0.1 (avoids the flaky LAN-IP shuffle failures seen on macOS).
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def read_data(session: SparkSession, path: str) -> DataFrame:
    """Read data."""
    log.info(f"Reading data from {path}...")
    try:
        return session.read.json(str(path), multiLine=True)
    except Exception as e:
        log.error(f"Failed to read data from {path} with error: {e}")
        raise


def log_stats(data) -> None:
    """Log basic stats about the data."""
    log.info(f"""
    Number of rows: {data.count()}
    Number of columns: {len(data.columns)}
    """)


def get_canceled_creators(data) -> DataFrame:
    """Retrieve all names of created_by with the status Canceled."""
    log.info("Retrieving cancelled creators...")

    return data.filter(col("status") == "Canceled").select(explode("created_by.name").alias("creator_name")).distinct()


def get_popular_countries(data) -> DataFrame:
    """Retrieve all origin_country with popularity higher than 5.0."""
    log.info("Retrieving popular countries...")

    return data.filter(col("popularity") > 5.0).select(explode("origin_country").alias("country")).distinct()


def get_short_series(data) -> DataFrame:
    """Retrieve all names of series with the number_of_episodes less than 100."""
    log.info("Retrieving short series...")

    return data.filter(col("number_of_episodes") < 100).select("name")


def write_to_csv(data, path: str) -> None:
    """Write data frame to csv file."""
    log.info(f"Writing data to {path}...")
    data.write.mode("overwrite").csv(path, header=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark application for batch module.")
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_JSON,
        help="Path to the TV Series JSON file (default: ./data/tvs.json).",
    )    
    parser.add_argument(
        "--pause",
        action="store_true",
        help="Keep the Spark session alive after the queries so the Spark UI "
        "at http://localhost:4040 can be explored. Press Enter to stop.",
    )
    args: Namespace = parser.parse_args()

    spark: SparkSession = get_spark_session()
    tv_series: DataFrame = read_data(spark, args.path)

    log_stats(tv_series)

    cancelled_creators: DataFrame = get_canceled_creators(tv_series)
    cancelled_creators.show(truncate=False)
    write_to_csv(cancelled_creators, str(OUTPUT_DIR / "canceled_creators"))

    popular_countries: DataFrame = get_popular_countries(tv_series)
    popular_countries.show(truncate=False)
    write_to_csv(popular_countries, str(OUTPUT_DIR / "popular_countries"))

    short_series: DataFrame = get_short_series(tv_series)
    short_series.show(truncate=False)
    write_to_csv(short_series, str(OUTPUT_DIR / "short_series"))

    if args.pause:
        log.info("Spark UI is live at http://localhost:4040")
        input("Press Enter to stop the Spark session and exit...")

    spark.stop()