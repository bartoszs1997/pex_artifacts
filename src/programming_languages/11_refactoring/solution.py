"""Salary data analysis — refactored PySpark application.

Refactored version of solution_before.py with improvements in:
  - Code structure (Extract Class, SRP)
  - Performance (cache, Spark-native aggregations, no collect() loops)
  - DRY (generic aggregate method replaces copy-paste)
  - Maintainability (logging, error handling, type hints, constants)
  - Spark best practices (explicit schema, loopback config)

Dataset: Employee Salaries (ds_salaries.csv, 607 rows, 12 columns)

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/11_refactoring/solution.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
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
INPUT_PATH = SCRIPT_DIR / "data" / "input" / "ds_salaries.csv"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"
LOG_DIR = SCRIPT_DIR / "logs"

SALARY_TIERS = {"Low": (0, 50_000), "Mid": (50_000, 100_000), "High": (100_000, None)}
TOP_N_JOBS = 10

SCHEMA = StructType([
    StructField("_c0", IntegerType(), True),
    StructField("work_year", IntegerType(), True),
    StructField("experience_level", StringType(), True),
    StructField("employment_type", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("salary_currency", StringType(), True),
    StructField("salary_in_usd", IntegerType(), True),
    StructField("employee_residence", StringType(), True),
    StructField("remote_ratio", IntegerType(), True),
    StructField("company_location", StringType(), True),
    StructField("company_size", StringType(), True),
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

_fh = logging.FileHandler(LOG_DIR / "refactoring.log", mode="a")
_fh.setLevel(logging.INFO)
_fh.setFormatter(_fmt)
log.addHandler(_fh)


# ===========================================================================
# Analyzer class (Extract Class refactoring — SRP)
# ===========================================================================
class SalaryAnalyzer:
    """Encapsulates all salary analysis operations."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._df: DataFrame | None = None

    def load(self) -> DataFrame:
        """Load CSV with explicit schema and cache for reuse."""
        self._df = (
            self.spark.read.csv(str(INPUT_PATH), header=True, schema=SCHEMA)
            .drop("_c0")
            .cache()
        )
        log.info("Loaded and cached %d rows from %s", self._df.count(), INPUT_PATH.name)
        return self._df

    @property
    def df(self) -> DataFrame:
        if self._df is None:
            raise RuntimeError("Call load() first.")
        return self._df

    # -- Generic aggregation (DRY — replaces 3x copy-paste) ----------------

    def aggregate_by(self, group_col: str) -> DataFrame:
        """Compute avg salary + count grouped by any column."""
        result = (
            self.df.groupBy(group_col)
            .agg(
                F.round(F.avg("salary_in_usd"), 2).alias("avg_salary"),
                F.count("*").alias("count"),
            )
            .orderBy(F.desc("avg_salary"))
        )
        log.info("Aggregated by %s -> %d groups", group_col, result.count())
        return result

    # -- Top N jobs (Spark-native, no collect() loop) -----------------------

    def top_n_jobs(self, n: int = TOP_N_JOBS) -> DataFrame:
        """Return top N job titles by avg salary using Spark aggregations."""
        result = (
            self.df.groupBy("job_title")
            .agg(
                F.round(F.avg("salary_in_usd"), 2).alias("avg_salary"),
                F.count("*").alias("count"),
            )
            .orderBy(F.desc("avg_salary"))
            .limit(n)
        )
        log.info("Top %d job titles computed", n)
        return result

    # -- Salary tiers (single pass with CASE WHEN, not 3 separate filters) -

    def salary_tiers(self) -> DataFrame:
        """Classify rows into salary tiers in a single pass."""
        result = (
            self.df.withColumn(
                "tier",
                F.when(F.col("salary_in_usd") < 50_000, "Low")
                .when(F.col("salary_in_usd") < 100_000, "Mid")
                .otherwise("High"),
            )
            .groupBy("tier")
            .agg(F.count("*").alias("count"))
            .orderBy("tier")
        )
        log.info("Salary tiers computed")
        return result

    # -- Year-over-year (Spark groupBy, no Python loop with collect()) ------

    def year_over_year(self) -> DataFrame:
        """Compute avg salary per year using Spark aggregation."""
        result = (
            self.df.groupBy("work_year")
            .agg(
                F.round(F.avg("salary_in_usd"), 2).alias("avg_salary"),
                F.count("*").alias("count"),
            )
            .orderBy("work_year")
        )
        log.info("Year-over-year computed")
        return result

    # -- Save helper (Extract Method) --------------------------------------

    @staticmethod
    def save(df: DataFrame, name: str) -> None:
        """Write DataFrame to CSV output directory."""
        path = OUTPUT_DIR / name
        df.coalesce(1).write.mode("overwrite").csv(str(path), header=True)
        log.info("Saved -> %s/", name)


# ===========================================================================
# Main
# ===========================================================================
def main() -> int:
    """Run the full analysis pipeline."""
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("RefactoredSalaryAnalysis")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        if not INPUT_PATH.exists():
            log.error("Input file not found: %s", INPUT_PATH)
            return 1

        analyzer = SalaryAnalyzer(spark)
        analyzer.load()

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # Aggregations (generic method used 3x — DRY)
        for col_name in ("experience_level", "company_size", "remote_ratio"):
            result = analyzer.aggregate_by(col_name)
            result.show(truncate=False)
            analyzer.save(result, f"by_{col_name}")

        # Top jobs (Spark-native)
        top = analyzer.top_n_jobs()
        top.show(truncate=False)
        analyzer.save(top, "top_jobs")

        # Salary tiers (single pass)
        tiers = analyzer.salary_tiers()
        tiers.show(truncate=False)
        analyzer.save(tiers, "salary_tiers")

        # Year-over-year (Spark groupBy)
        yoy = analyzer.year_over_year()
        yoy.show(truncate=False)
        analyzer.save(yoy, "year_over_year")

        log.info("All analyses complete.")
        return 0

    except Exception:
        log.exception("Pipeline failed")
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
