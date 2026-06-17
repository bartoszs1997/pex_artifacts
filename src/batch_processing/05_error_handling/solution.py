"""Spark batch application: TV Series analysis with robust error handling.

This is the error-handling deliverable (task 05). It is based on the TV Series
Phase-1 analysis (``01_tv_series/basic_solution.py`` — same three queries) and
adds a full error-handling / observability layer:

  * Dual logging. ALL logs are printed to the terminal down to DEBUG level,
    while only WARNING and above are persisted to the log file.
  * A specific WARNING message raised when a data-quality defect is detected.
  * Exception handling that, at the ERROR level, logs the failure, alerts, and
    applies a fix (a fallback) so processing can recover.
  * Run-time information: execution time, driver resource utilization, and
    data-processing statistics (row / column / partition counts).
  * A ``--simulate-error`` switch that intentionally triggers a failure so the
    catch -> log -> alert -> fix (recover) path can be demonstrated on demand.

The dataset is the TV Series JSON already downloaded for module 01; this module
reuses it (via a relative path) instead of duplicating a 151 MB file. Override
with ``--path`` if it lives elsewhere.

Run:
    # Java 17 must be on PATH (PySpark 4.x requirement):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"

    # normal run (queries succeed):
    uv run python src/batch_processing/05_error_handling/solution.py

    # demonstrate the error-handling path (simulated failure + recovery):
    uv run python src/batch_processing/05_error_handling/solution.py --simulate-error
"""

import argparse
import logging
import resource
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode

BASE_DIR = Path(__file__).resolve().parent
# Reuse the TV Series dataset already downloaded for module 01 (no 151 MB duplicate).
DEFAULT_JSON = BASE_DIR.parent / "01_tv_series" / "data" / "input" / "tvs.json"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# --- Logging configuration -------------------------------------------------
# The logger's own level is the first gate every record passes through, so we
# open it fully (DEBUG). Each *handler* then applies its own threshold, which is
# what lets the two destinations keep different levels:
#   * console handler @ DEBUG   -> the terminal shows EVERYTHING (requirement 1)
#   * file handler    @ WARNING -> the file keeps only WARNING+ (requirement 2)
formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("TV Series Error Handling")
log.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "error_handling.log"))
file_handler.setLevel(logging.WARNING)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def alert(message: str) -> None:
    """Raise a high-visibility alert about an error.

    In production this is where you would page on-call (Slack / PagerDuty /
    email / SNS). Here we surface it as a CRITICAL log so it stands out in the
    terminal and — because CRITICAL >= WARNING — is also persisted to the file.
    """
    log.critical(f"[ALERT] {message}")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="TV Series analysis with error handling.")
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_JSON,
        help="Path to the TV Series JSON file (defaults to module 01's dataset).",
    )
    parser.add_argument(
        "--simulate-error",
        action="store_true",
        help="Intentionally point the primary read at a missing file to demonstrate "
        "the error-handling / recovery path (the run still completes via fallback).",
    )
    return parser.parse_args()


def get_spark_session(app_name: str) -> SparkSession:
    """Create a local SparkSession."""
    log.info("Creating Spark session...")

    return (
        SparkSession.builder.appName(app_name)
        # Run Spark locally using all available cores.
        .master("local[*]")
        # Force loopback networking so Spark's internal block transfers stay on
        # 127.0.0.1 (avoids the flaky LAN-IP shuffle failures seen on macOS).
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def log_runtime_context(spark: SparkSession) -> None:
    """Log Spark runtime / resource context (part of 'runtime information')."""
    sc = spark.sparkContext
    conf = sc.getConf()
    log.info(f"Spark {sc.version} | app {sc.applicationId} | master {sc.master}")
    # defaultParallelism reflects the cores Spark can use -> a resource signal.
    log.debug(f"Default parallelism (usable cores): {sc.defaultParallelism}")
    log.debug(f"Driver memory:   {conf.get('spark.driver.memory', '<spark default>')}")
    log.debug(f"Executor memory: {conf.get('spark.executor.memory', '<spark default>')}")


def read_data(spark: SparkSession, path: Path, fallback_path: Path) -> DataFrame:
    """Read the TV Series JSON, recovering from a read failure at the ERROR level.

    This is the heart of the task's "apply appropriate fixes at the error level":
    if the primary read fails (e.g. the simulated missing path), we log the
    failure at ERROR, raise an alert, then apply the fix -- fall back to the
    known-good dataset so the pipeline can continue.
    """
    log.info(f"Reading data from {path}...")
    try:
        return spark.read.json(str(path), multiLine=True)
    except Exception as error:
        # ERROR level: capture the defect...
        log.error(f"Failed to read data from {path}: {error}")
        alert(f"Primary data read failed ({path.name}); attempting recovery.")
        if path == fallback_path:
            # No safety net left -> nothing to recover to.
            log.error("No fallback available; re-raising.")
            raise
        # ...and apply the fix: fall back to the known-good dataset.
        log.warning(f"Recovery: falling back to known-good dataset at {fallback_path}")
        return spark.read.json(str(fallback_path), multiLine=True)


def check_data_quality(data: DataFrame) -> None:
    """Inspect key query columns and emit a specific WARNING for any defect found.

    This satisfies "create a specific message to be stored for a warning level":
    real TMDB data has rows with missing popularity / episode counts / origin
    country, which would silently shrink the query results. We surface that as a
    WARNING (kept in the log file) rather than letting it pass unnoticed.
    """
    total = data.count()
    null_popularity = data.filter(col("popularity").isNull()).count()
    null_episodes = data.filter(col("number_of_episodes").isNull()).count()

    if null_popularity:
        log.warning(
            f"Data quality: {null_popularity}/{total} rows have a NULL 'popularity' "
            f"and are excluded from the popular-countries query."
        )
    if null_episodes:
        log.warning(
            f"Data quality: {null_episodes}/{total} rows have a NULL "
            f"'number_of_episodes' and are excluded from the short-series query."
        )
    if not (null_popularity or null_episodes):
        log.debug("Data quality: no NULLs in popularity / number_of_episodes.")


def get_canceled_creators(data: DataFrame) -> DataFrame:
    """Retrieve distinct creator names for series with status 'Canceled'."""
    log.info("Query: canceled creators")

    return (
        data.filter(col("status") == "Canceled")
        .select(explode("created_by.name").alias("creator_name"))
        .distinct()
    )


def get_popular_countries(data: DataFrame) -> DataFrame:
    """Retrieve distinct origin countries for series with popularity > 5.0."""
    log.info("Query: popular countries")

    return data.filter(col("popularity") > 5.0).select(explode("origin_country").alias("country")).distinct()


def get_short_series(data: DataFrame) -> DataFrame:
    """Retrieve names of series with fewer than 100 episodes."""
    log.info("Query: short series")

    return data.filter(col("number_of_episodes") < 100).select("name")


def write_to_csv(data: DataFrame, path: str) -> None:
    """Write a DataFrame to CSV (overwrite, with header)."""
    log.info(f"Writing results to {path}")

    data.write.mode("overwrite").csv(path, header=True)


def run_query(data: DataFrame, name: str, query, output_subdir: str) -> None:
    """Run one query: execute, log its row count (data statistics), show, write."""
    result = query(data)
    rows = result.count()  # action -> also a data-processing statistic
    log.info(f"[{name}] produced {rows} rows across {result.rdd.getNumPartitions()} partition(s)")
    result.show(truncate=False)
    write_to_csv(result, str(OUTPUT_DIR / output_subdir))


def log_resource_usage() -> None:
    """Log driver-process resource utilization (stdlib `resource`, no extra deps)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # ru_maxrss is in bytes on macOS but kilobytes on Linux.
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    peak_mb = usage.ru_maxrss / divisor
    log.info(
        f"Resource utilization (driver): peak memory {peak_mb:.0f} MB, "
        f"CPU user {usage.ru_utime:.1f}s / sys {usage.ru_stime:.1f}s"
    )


def main() -> None:
    """Run the TV Series analysis end to end with error handling and timing."""
    args = parse_args()
    start = time.perf_counter()
    spark = get_spark_session("TV Series Error Handling")
    try:
        log_runtime_context(spark)

        primary_path: Path = args.path
        if args.simulate_error:
            # Criterion 4: intentionally introduce a simulated error. Point the
            # primary read at a file that does not exist so read_data's ERROR /
            # alert / fallback path runs and the program recovers.
            primary_path = BASE_DIR / "data" / "input" / "does_not_exist.json"
            log.debug(f"--simulate-error active: primary path set to bogus {primary_path}")

        data: DataFrame = read_data(spark, primary_path, fallback_path=args.path)

        # Data-processing statistics for the loaded dataset.
        rows, cols = data.count(), len(data.columns)
        log.info(f"Loaded TV Series data: {rows} rows x {cols} columns")

        check_data_quality(data)

        run_query(data, "canceled creators", get_canceled_creators, "canceled_creators")
        run_query(data, "popular countries", get_popular_countries, "popular_countries")
        run_query(data, "short series", get_short_series, "short_series")

    except Exception:
        # Last line of defence: anything not recovered is logged with traceback,
        # alerted, and re-raised (no silent masking, non-zero exit).
        log.exception("TV Series processing failed")
        alert("Unrecoverable error in TV Series processing -- see traceback above.")
        raise
    finally:
        elapsed = time.perf_counter() - start
        log.info(f"Total execution time: {elapsed:.2f}s")
        log_resource_usage()
        spark.stop()
        log.info("Error-handling script finished")


if __name__ == "__main__":
    main()
