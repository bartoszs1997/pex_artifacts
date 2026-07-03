"""Concurrent PySpark analysis using multithreading.

Demonstrates concurrency patterns in a Spark application:
  1. ThreadPoolExecutor — run multiple Spark analyses in parallel
  2. threading.Lock — thread-safe shared state (results collector)
  3. queue.Queue — message passing between producer/consumer threads

Dataset: IBM AML Transactions (HI-Large_Trans.csv, ~22.3M rows, 11 columns)

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/12_concurrency/solution.py
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from queue import Queue

from pyspark.sql import DataFrame, SparkSession
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
INPUT_PATH = SCRIPT_DIR / "data" / "input" / "HI-Large_Trans.csv"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"
LOG_DIR = SCRIPT_DIR / "logs"

MAX_WORKERS = 4

SCHEMA = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("From Bank", StringType(), True),
    StructField("From Account", StringType(), True),
    StructField("To Bank", StringType(), True),
    StructField("To Account", StringType(), True),
    StructField("Amount Received", DoubleType(), True),
    StructField("Receiving Currency", StringType(), True),
    StructField("Amount Paid", DoubleType(), True),
    StructField("Payment Currency", StringType(), True),
    StructField("Payment Format", StringType(), True),
    StructField("Is Laundering", IntegerType(), True),
])

# ---------------------------------------------------------------------------
# Logging (dual: console + file)
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

_fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
_ch.setFormatter(_fmt)
log.addHandler(_ch)

_fh = logging.FileHandler(LOG_DIR / "concurrency.log", mode="a")
_fh.setLevel(logging.INFO)
_fh.setFormatter(_fmt)
log.addHandler(_fh)


# ===========================================================================
# Thread-safe results collector (shared memory + Lock)
# ===========================================================================
class ResultsCollector:
    """Thread-safe container for collecting analysis results.

    Uses threading.Lock to prevent race conditions when multiple
    threads write results concurrently.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._results: dict[str, DataFrame] = {}

    def add(self, name: str, df: DataFrame) -> None:
        with self._lock:
            self._results[name] = df
            log.info("[collector] Received result: %s", name)

    def get_all(self) -> dict[str, DataFrame]:
        with self._lock:
            return dict(self._results)


# ===========================================================================
# Analysis tasks (each runs in a separate thread)
# ===========================================================================

def analyze_by_currency(df: DataFrame) -> tuple[str, DataFrame]:
    """Aggregate transaction volume by receiving currency."""
    result = (
        df.groupBy("Receiving Currency")
        .agg(
            F.round(F.sum("Amount Received"), 2).alias("total_received"),
            F.count("*").alias("tx_count"),
        )
        .orderBy(F.desc("total_received"))
    )
    return "by_currency", result


def analyze_by_payment_format(df: DataFrame) -> tuple[str, DataFrame]:
    """Aggregate transaction count and avg amount by payment format."""
    result = (
        df.groupBy("Payment Format")
        .agg(
            F.round(F.avg("Amount Paid"), 2).alias("avg_paid"),
            F.count("*").alias("tx_count"),
        )
        .orderBy(F.desc("tx_count"))
    )
    return "by_payment_format", result


def analyze_laundering_ratio(df: DataFrame) -> tuple[str, DataFrame]:
    """Compute laundering ratio per bank (filter + aggregate)."""
    result = (
        df.groupBy("`From Bank`")
        .agg(
            F.count("*").alias("total_tx"),
            F.sum("Is Laundering").alias("laundering_tx"),
        )
        .withColumn(
            "laundering_pct",
            F.round(F.col("laundering_tx") / F.col("total_tx") * 100, 4),
        )
        .orderBy(F.desc("laundering_pct"))
    )
    return "laundering_ratio", result


def analyze_top_senders(df: DataFrame) -> tuple[str, DataFrame]:
    """Top 20 sender accounts by total amount paid (sort)."""
    result = (
        df.groupBy("`From Bank`", "`From Account`")
        .agg(F.round(F.sum("Amount Paid"), 2).alias("total_paid"))
        .orderBy(F.desc("total_paid"))
        .limit(20)
    )
    return "top_senders", result


def analyze_high_value_txns(df: DataFrame) -> tuple[str, DataFrame]:
    """Filter transactions above 1M (filtering)."""
    result = (
        df.filter(F.col("Amount Paid") > 1_000_000)
        .select(
            "Timestamp", "`From Bank`", "`To Bank`",
            "Amount Paid", "Payment Currency", "Is Laundering",
        )
        .orderBy(F.desc("Amount Paid"))
    )
    return "high_value_txns", result


def analyze_hourly_volume(df: DataFrame) -> tuple[str, DataFrame]:
    """Aggregate transaction volume by hour of day."""
    result = (
        df.withColumn("hour", F.hour(F.to_timestamp("Timestamp", "yyyy/MM/dd HH:mm")))
        .groupBy("hour")
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.sum("Amount Paid"), 2).alias("total_paid"),
        )
        .orderBy("hour")
    )
    return "hourly_volume", result


# List of all analysis functions
ANALYSES = [
    analyze_by_currency,
    analyze_by_payment_format,
    analyze_laundering_ratio,
    analyze_top_senders,
    analyze_high_value_txns,
    analyze_hourly_volume,
]


# ===========================================================================
# Message-passing saver (Queue-based consumer)
# ===========================================================================

def save_worker(q: Queue, stop_event: threading.Event) -> None:
    """Consumer thread: reads (name, df) from queue and saves to CSV.

    Runs until stop_event is set AND queue is empty.
    Demonstrates message passing via Queue.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    while not stop_event.is_set() or not q.empty():
        try:
            name, df = q.get(timeout=0.5)
        except Exception:
            continue
        path = OUTPUT_DIR / name
        df.coalesce(1).write.mode("overwrite").csv(str(path), header=True)
        log.info("[saver] Saved -> %s/", name)
        q.task_done()


# ===========================================================================
# Main
# ===========================================================================

def main() -> int:
    """Run concurrent analysis pipeline."""
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("ConcurrentAnalysis")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        if not INPUT_PATH.exists():
            log.error("Input file not found: %s", INPUT_PATH)
            return 1

        df = spark.read.csv(str(INPUT_PATH), header=True, schema=SCHEMA).cache()
        row_count = df.count()
        log.info("Loaded and cached %d rows from %s", row_count, INPUT_PATH.name)

        # -- Setup concurrency primitives --
        collector = ResultsCollector()
        save_queue: Queue = Queue()
        stop_event = threading.Event()

        # Start saver consumer thread (message passing via Queue)
        saver_thread = threading.Thread(
            target=save_worker, args=(save_queue, stop_event), daemon=True
        )
        saver_thread.start()
        log.info("Started saver consumer thread")

        # -- Run analyses concurrently with ThreadPoolExecutor --
        start_time = time.time()
        log.info("Submitting %d analyses to ThreadPoolExecutor (workers=%d)",
                 len(ANALYSES), MAX_WORKERS)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fn, df): fn.__name__ for fn in ANALYSES
            }
            for future in as_completed(futures):
                fn_name = futures[future]
                try:
                    name, result = future.result()
                    collector.add(name, result)
                    result.show(10, truncate=False)
                    save_queue.put((name, result))
                    log.info("[main] Analysis '%s' done", fn_name)
                except Exception as exc:
                    log.error("[main] Analysis '%s' failed: %s", fn_name, exc)

        elapsed = time.time() - start_time
        log.info("All %d analyses completed in %.2fs", len(ANALYSES), elapsed)

        # Signal saver to stop and wait
        save_queue.join()
        stop_event.set()
        saver_thread.join(timeout=5)
        log.info("Saver thread stopped. All results saved.")

        # Summary
        all_results = collector.get_all()
        log.info("Collected %d results: %s", len(all_results), list(all_results.keys()))

        return 0

    except Exception:
        log.exception("Pipeline failed")
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
