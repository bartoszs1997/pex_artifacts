"""Spark batch application: read structured and semi-structured TV Series data.

Task (PEX, Batch Processing):
    Build a Spark application that reads the TV Series JSON dataset and:
      Q1: Retrieve all names of `created_by` where status == "Canceled".
      Q2: Retrieve all `origin_country` where popularity > 5.0.
      Q3: Retrieve all names of series where number_of_episodes < 100.

Phase 2 (planned, not in this file yet):
    Stand up a local MySQL database and read / join via the JDBC driver
    (and demonstrate an ODBC connection from Python). To keep that step
    cheap, build_spark() already accepts an optional `jdbc_jar`, and the
    query functions are pure DataFrame transformations so the future join
    can reuse them without rewrites.

Run:
    # Java 17 must be on PATH (PySpark 4.x requirement):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/batch_processing/tv_series/solution.py
    uv run python src/batch_processing/tv_series/solution.py --pause
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("tv_series")

DEFAULT_JSON = Path(__file__).resolve().parent / "data" / "input" / "tvs.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "output"


def build_spark(jdbc_jar: str | None = None) -> SparkSession:
    """Create a local SparkSession.

    jdbc_jar: optional path to a JDBC driver jar (e.g. the MySQL connector).
    Reserved for phase 2 (read from / join with a local MySQL database).
    When None, the job stays JSON-only and simple.
    """
    builder = SparkSession.builder.appName("tv-series-batch").master("local[*]")
    # Force loopback networking. On macOS Spark may resolve the host to a LAN
    # IP (e.g. 192.168.x.x) and then fail to fetch its own shuffle/result
    # blocks ("Failed to connect", "Broken pipe", TaskResultLost), which can
    # abort the job non-deterministically. Pinning the driver to 127.0.0.1
    # keeps all block transfers on the loopback device and makes runs stable.
    builder = builder.config("spark.driver.host", "127.0.0.1")
    builder = builder.config("spark.driver.bindAddress", "127.0.0.1")

    if jdbc_jar:
        builder = builder.config("spark.jars", jdbc_jar)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_series(spark: SparkSession, json_path: Path) -> DataFrame:
    """Read the TV Series JSON file into a DataFrame.

    multiLine=True is required: the dataset is a single JSON array
    ([{...}, {...}]), not one JSON object per line (JSONL).
    """
    return spark.read.option("multiLine", True).json(str(json_path))


def q1_canceled_creators(df: DataFrame) -> DataFrame:
    """Q1: names of created_by for series whose status is 'Canceled'.

    created_by is an array<struct<id, name, ...>>, so we explode the array
    into one row per creator, then pull the nested name field.
    """
    return (
        df.filter(F.col("status") == "Canceled")
        .select(F.explode("created_by.name").alias("creator_name"))
        .distinct()
        .orderBy("creator_name")
    )


def q2_popular_origin_countries(df: DataFrame) -> DataFrame:
    """Q2: origin_country values for series with popularity > 5.0.

    origin_country is an array<string>, so we explode it into one row
    per country and de-duplicate. The task asks for the list of countries,
    so we keep just the distinct country codes.
    """
    return (
        df.filter(F.col("popularity") > 5.0)
        .select(F.explode("origin_country").alias("country"))
        .distinct()
        .orderBy("country")
    )


def q3_short_series(df: DataFrame) -> DataFrame:
    """Q3: names of series with fewer than 100 episodes."""
    return (
        df.filter(F.col("number_of_episodes") < 100)
        .select("name", "number_of_episodes")
        .orderBy("number_of_episodes", "name")
    )


def write_to_parquet(data, path: str) -> None:
    """Write data frame to parquet file."""
    log.info(f"Writing data to {path}...")
    data.write.mode("overwrite").parquet(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark application for batch module.")
    parser.add_argument(
        "--json",
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
    args = parser.parse_args()

    spark = build_spark()
    try:
        series = read_series(spark, args.json)
        log.info(f"Loaded {series.count()} series rows from {args.json}")
        series.printSchema()

        canceled_creators = q1_canceled_creators(series)
        log.info("== Q1: creators of Canceled series ==")
        canceled_creators.show(truncate=False)
        write_to_parquet(canceled_creators, str(OUTPUT_DIR / "canceled_creators"))

        popular_countries = q2_popular_origin_countries(series)
        log.info("== Q2: origin countries with popularity > 5.0 ==")
        popular_countries.show(truncate=False)
        write_to_parquet(popular_countries, str(OUTPUT_DIR / "popular_countries"))

        short_series = q3_short_series(series)
        log.info("== Q3: series with fewer than 100 episodes ==")
        short_series.show(truncate=False)
        write_to_parquet(short_series, str(OUTPUT_DIR / "short_series"))

        if args.pause:
            log.info("Spark UI is live at http://localhost:4040")
            input("Press Enter to stop the Spark session and exit...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
