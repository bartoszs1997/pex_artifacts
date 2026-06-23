"""Cover Code with Tests — Employee Salary Data Processing Application.

PySpark batch application processing employee salary data. Designed to be
covered by unit tests with pytest-cov (>80% coverage target).

Dataset: inductiveanks/employee-salaries-for-different-job-roles (ds_salaries.csv)

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/07_cover_code_with_tests/solution.py
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "data" / "input"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"
LOG_DIR = SCRIPT_DIR / "logs"
DATA_FILE = INPUT_DIR / "ds_salaries.csv"
LOG_FILE = LOG_DIR / "cover_code_with_tests.log"

VALID_EXPERIENCE_LEVELS = ["EN", "MI", "SE", "EX"]
VALID_EMPLOYMENT_TYPES = ["FT", "PT", "CT", "FL"]
VALID_COMPANY_SIZES = ["S", "M", "L"]

# Logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
log = logging.getLogger("cover_code_with_tests")
log.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(_fmt)
log.addHandler(_ch)
_fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
_fh.setFormatter(_fmt)
log.addHandler(_fh)

# Schema
SALARY_SCHEMA = StructType([
    StructField("_c0", IntegerType(), True),
    StructField("work_year", IntegerType(), True),
    StructField("experience_level", StringType(), True),
    StructField("employment_type", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("salary_currency", StringType(), True),
    StructField("salary_in_usd", DoubleType(), True),
    StructField("employee_residence", StringType(), True),
    StructField("remote_ratio", IntegerType(), True),
    StructField("company_location", StringType(), True),
    StructField("company_size", StringType(), True),
])


# --- Data Loading ---

def load_data(spark: SparkSession, path: str) -> DataFrame:
    """Load salary CSV with explicit schema."""
    return spark.read.csv(path, header=True, schema=SALARY_SCHEMA)


# --- Validation ---

def validate_column_count(df: DataFrame, expected: int) -> bool:
    """Check DataFrame has expected number of columns."""
    return len(df.columns) == expected


def validate_no_null_salaries(df: DataFrame) -> int:
    """Count rows where salary_in_usd is null."""
    return df.filter(F.col("salary_in_usd").isNull()).count()


def validate_experience_levels(df: DataFrame) -> list[str]:
    """Return experience levels not in the valid set."""
    levels = df.select("experience_level").distinct().rdd.flatMap(lambda x: x).collect()
    return [lv for lv in levels if lv not in VALID_EXPERIENCE_LEVELS and lv is not None]


def validate_salary_range(df: DataFrame, min_val: float, max_val: float) -> int:
    """Count records with salary_in_usd outside [min_val, max_val]."""
    return df.filter(
        (F.col("salary_in_usd") < min_val) | (F.col("salary_in_usd") > max_val)
    ).count()


# --- Transformations ---

def add_salary_band(df: DataFrame) -> DataFrame:
    """Add salary_band column: low/medium/high/very_high."""
    return df.withColumn(
        "salary_band",
        F.when(F.col("salary_in_usd") < 50000, "low")
        .when(F.col("salary_in_usd") < 100000, "medium")
        .when(F.col("salary_in_usd") < 200000, "high")
        .otherwise("very_high"),
    )


def filter_by_experience(df: DataFrame, level: str) -> DataFrame:
    """Filter by experience level. Raises ValueError if invalid."""
    if level not in VALID_EXPERIENCE_LEVELS:
        raise ValueError(f"Invalid experience level: {level}")
    return df.filter(F.col("experience_level") == level)


def filter_full_time(df: DataFrame) -> DataFrame:
    """Keep only full-time employees."""
    return df.filter(F.col("employment_type") == "FT")


# --- Aggregation ---

def avg_salary_by_experience(df: DataFrame) -> DataFrame:
    """Average salary grouped by experience level."""
    return df.groupBy("experience_level").agg(
        F.avg("salary_in_usd").alias("avg_salary"),
        F.count("*").alias("count"),
    ).orderBy("experience_level")


def avg_salary_by_company_size(df: DataFrame) -> DataFrame:
    """Average salary grouped by company size."""
    return df.groupBy("company_size").agg(
        F.avg("salary_in_usd").alias("avg_salary"),
        F.count("*").alias("count"),
    ).orderBy("company_size")


def top_paying_jobs(df: DataFrame, n: int = 10) -> DataFrame:
    """Top N job titles by average salary."""
    return df.groupBy("job_title").agg(
        F.avg("salary_in_usd").alias("avg_salary"),
        F.count("*").alias("count"),
    ).orderBy(F.desc("avg_salary")).limit(n)


def salary_stats(df: DataFrame) -> Optional[dict]:
    """Overall salary statistics: min, max, avg, stddev, count."""
    row = df.agg(
        F.min("salary_in_usd").alias("min"),
        F.max("salary_in_usd").alias("max"),
        F.avg("salary_in_usd").alias("avg"),
        F.stddev("salary_in_usd").alias("stddev"),
        F.count("salary_in_usd").alias("count"),
    ).first()
    if row is None:
        return None
    return {"min": row["min"], "max": row["max"], "avg": row["avg"],
            "stddev": row["stddev"], "count": row["count"]}


# --- Main ---

def main() -> int:
    """Run the salary processing pipeline."""
    log.info("=" * 60)
    log.info("COVER CODE WITH TESTS — Salary Processing Application")
    log.info("=" * 60)

    if not DATA_FILE.exists():
        log.error("Input file not found: %s. Run download_data.py first.", DATA_FILE)
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("CoverCodeWithTests")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        log.info("SparkSession created.")

        df = load_data(spark, str(DATA_FILE))
        log.info("Loaded %d rows.", df.count())
        log.info("Null salaries: %d", validate_no_null_salaries(df))
        log.info("Out-of-range: %d", validate_salary_range(df, 0, 1_000_000))

        df = add_salary_band(df)

        exp_avg = avg_salary_by_experience(df)
        exp_avg.show(truncate=False)
        exp_avg.write.mode("overwrite").csv(str(OUTPUT_DIR / "avg_by_experience"), header=True)

        top_jobs = top_paying_jobs(df)
        top_jobs.show(truncate=False)
        top_jobs.write.mode("overwrite").csv(str(OUTPUT_DIR / "top_paying_jobs"), header=True)

        log.info("Stats: %s", salary_stats(df))
        log.info("ALL TASKS COMPLETED SUCCESSFULLY")

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
