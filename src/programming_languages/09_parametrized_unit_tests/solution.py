"""Salary data processing — PySpark application for parametrized testing.

Provides pure transformation functions that are easy to test with different
input parameters: filtering, categorization, aggregation, and validation.

Dataset: Employee Salaries (ds_salaries.csv, 607 rows, 12 columns)

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/09_parametrized_unit_tests/solution.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_PATH = SCRIPT_DIR / "data" / "input" / "ds_salaries.csv"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"
LOG_DIR = SCRIPT_DIR / "logs"

# ---------------------------------------------------------------------------
# Logging (dual: console + file)
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

_formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(_formatter)
log.addHandler(_console_handler)

_file_handler = logging.FileHandler(LOG_DIR / "parametrized_unit_tests.log", mode="a")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(_formatter)
log.addHandler(_file_handler)


# ===========================================================================
# TRANSFORMATION FUNCTIONS (testable units)
# ===========================================================================


def filter_by_salary_range(df: DataFrame, min_sal: int, max_sal: int) -> DataFrame:
    """Filter rows where salary_in_usd is within [min_sal, max_sal]."""
    return df.filter(
        (F.col("salary_in_usd") >= min_sal) & (F.col("salary_in_usd") <= max_sal)
    )


def categorize_experience(df: DataFrame) -> DataFrame:
    """Add 'experience_category' column based on experience_level code.

    Mapping: EN -> Junior, MI -> Mid, SE -> Senior, EX -> Executive.
    """
    mapping = {
        "EN": "Junior",
        "MI": "Mid",
        "SE": "Senior",
        "EX": "Executive",
    }
    mapping_expr = F.create_map([F.lit(x) for pair in mapping.items() for x in pair])
    return df.withColumn("experience_category", mapping_expr[F.col("experience_level")])


def classify_salary_tier(df: DataFrame) -> DataFrame:
    """Add 'salary_tier' column: Low (<70k), Mid (70k-120k), High (>120k)."""
    return df.withColumn(
        "salary_tier",
        F.when(F.col("salary_in_usd") < 70000, "Low")
        .when(F.col("salary_in_usd") <= 120000, "Mid")
        .otherwise("High"),
    )


def aggregate_by_column(df: DataFrame, group_col: str) -> DataFrame:
    """Compute avg, min, max salary grouped by given column."""
    return (
        df.groupBy(group_col)
        .agg(
            F.round(F.avg("salary_in_usd"), 2).alias("avg_salary"),
            F.min("salary_in_usd").alias("min_salary"),
            F.max("salary_in_usd").alias("max_salary"),
            F.count("*").alias("count"),
        )
        .orderBy(F.desc("avg_salary"))
    )


def filter_by_remote_ratio(df: DataFrame, ratio: int) -> DataFrame:
    """Filter rows matching a specific remote_ratio value (0, 50, or 100)."""
    return df.filter(F.col("remote_ratio") == ratio)


def top_n_job_titles(df: DataFrame, n: int) -> DataFrame:
    """Return top N job titles by average salary."""
    return (
        df.groupBy("job_title")
        .agg(F.round(F.avg("salary_in_usd"), 2).alias("avg_salary"))
        .orderBy(F.desc("avg_salary"))
        .limit(n)
    )


# ===========================================================================
# MAIN
# ===========================================================================


def main() -> int:
    """Run the salary data processing pipeline."""
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("ParametrizedUnitTests")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        if not INPUT_PATH.exists():
            log.error("Input file not found: %s", INPUT_PATH)
            return 1

        df = spark.read.csv(str(INPUT_PATH), header=True, inferSchema=True)
        log.info("Loaded %d rows from %s", df.count(), INPUT_PATH.name)

        # Apply transformations
        df = categorize_experience(df)
        df = classify_salary_tier(df)
        log.info("Added experience_category and salary_tier columns")

        # Aggregation: by experience
        by_exp = aggregate_by_column(df, "experience_category")
        log.info("Aggregated by experience_category: %d groups", by_exp.count())
        by_exp.show(truncate=False)

        # Aggregation: by salary tier
        by_tier = aggregate_by_column(df, "salary_tier")
        log.info("Aggregated by salary_tier: %d groups", by_tier.count())
        by_tier.show(truncate=False)

        # Top 5 job titles
        top5 = top_n_job_titles(df, 5)
        log.info("Top 5 job titles by avg salary")
        top5.show(truncate=False)

        # Save results
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        by_exp.coalesce(1).write.mode("overwrite").csv(
            str(OUTPUT_DIR / "by_experience"), header=True
        )
        by_tier.coalesce(1).write.mode("overwrite").csv(
            str(OUTPUT_DIR / "by_salary_tier"), header=True
        )
        top5.coalesce(1).write.mode("overwrite").csv(
            str(OUTPUT_DIR / "top_job_titles"), header=True
        )
        log.info("Results saved to %s", OUTPUT_DIR)
        return 0

    except Exception:
        log.exception("Pipeline failed")
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
