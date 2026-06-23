"""Stubs and Mocks in Unit Tests — Employee Salaries average calculation.

Demonstrates using stubs and mocks in unit testing by implementing a data
processing function that relies on an external service. The external service
retrieves salary data; in tests, it is replaced by a mock object.

Architecture:
    SalaryDataService (external service) → retrieves salary values from CSV
    calculate_average(numbers) → pure function, calculates mean of a list

Dataset: inductiveanks/employee-salaries-for-different-job-roles
File:    ds_salaries.csv (607 rows, 12 columns)
Column:  salary_in_usd (used as "Avg. Salary" equivalent)

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/06_mocks_unit_tests/solution.py
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
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

DATA_FILE = INPUT_DIR / "ds_salaries.csv"
LOG_FILE = LOG_DIR / "mocks_unit_tests.log"

# ---------------------------------------------------------------------------
# Logging (dual: console + file)
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("mocks_unit_tests")
log.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SALARY_SCHEMA = StructType(
    [
        StructField("_c0", IntegerType(), nullable=True),
        StructField("work_year", IntegerType(), nullable=True),
        StructField("experience_level", StringType(), nullable=True),
        StructField("employment_type", StringType(), nullable=True),
        StructField("job_title", StringType(), nullable=True),
        StructField("salary", DoubleType(), nullable=True),
        StructField("salary_currency", StringType(), nullable=True),
        StructField("salary_in_usd", DoubleType(), nullable=True),
        StructField("employee_residence", StringType(), nullable=True),
        StructField("remote_ratio", IntegerType(), nullable=True),
        StructField("company_location", StringType(), nullable=True),
        StructField("company_size", StringType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Core Function (under test)
# ---------------------------------------------------------------------------


def calculate_average(numbers: list[float]) -> float:
    """Calculate the average of a list of numbers.

    Args:
        numbers: List of float values (e.g. salary values).

    Returns:
        The arithmetic mean of the provided numbers.

    Raises:
        ValueError: If the input list is empty.
    """
    if not numbers:
        raise ValueError("Cannot calculate average of an empty list.")
    return sum(numbers) / len(numbers)


# ---------------------------------------------------------------------------
# External Service (to be mocked in tests)
# ---------------------------------------------------------------------------


class SalaryDataService:
    """External service that retrieves salary data from the dataset.

    In production, this reads from a CSV file via Spark.
    In tests, this class is mocked to return a fixed list of salaries.
    """

    def __init__(self, spark: SparkSession, data_path: str) -> None:
        """Initialize with a SparkSession and path to the data file."""
        self.spark = spark
        self.data_path = data_path

    def get_salaries(self) -> list[float]:
        """Retrieve all salary_in_usd values from the dataset.

        Returns:
            List of salary values as floats.
        """
        df = self.spark.read.csv(
            self.data_path, header=True, schema=SALARY_SCHEMA
        )
        rows = df.select("salary_in_usd").filter("salary_in_usd IS NOT NULL").collect()
        return [float(row["salary_in_usd"]) for row in rows]

    def get_salaries_by_experience(self, level: str) -> list[float]:
        """Retrieve salary values filtered by experience level.

        Args:
            level: Experience level code (EN, MI, SE, EX).

        Returns:
            List of salary values for the given experience level.
        """
        df = self.spark.read.csv(
            self.data_path, header=True, schema=SALARY_SCHEMA
        )
        rows = (
            df.filter(df["experience_level"] == level)
            .select("salary_in_usd")
            .filter("salary_in_usd IS NOT NULL")
            .collect()
        )
        return [float(row["salary_in_usd"]) for row in rows]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the salary data processing pipeline."""
    log.info("=" * 60)
    log.info("MOCKS & STUBS DEMO — Employee Salary Average Calculation")
    log.info("=" * 60)

    if not DATA_FILE.exists():
        log.error("Input file not found: %s. Run download_data.py first.", DATA_FILE)
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("MocksUnitTestsDemo")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        log.info("SparkSession created.")

        # Use the external service to retrieve salaries
        service = SalaryDataService(spark, str(DATA_FILE))

        # All salaries
        all_salaries = service.get_salaries()
        log.info("Retrieved %d salary values.", len(all_salaries))

        avg_all = calculate_average(all_salaries)
        log.info("Average salary (all): $%.2f", avg_all)

        # Per experience level
        log.info("=" * 60)
        log.info("Average Salary by Experience Level")
        log.info("=" * 60)

        for level in ["EN", "MI", "SE", "EX"]:
            salaries = service.get_salaries_by_experience(level)
            if salaries:
                avg = calculate_average(salaries)
                log.info("  %s: %d records, avg = $%.2f", level, len(salaries), avg)
            else:
                log.info("  %s: no records", level)

        # Save summary using Spark
        df = spark.read.csv(str(DATA_FILE), header=True, schema=SALARY_SCHEMA)
        summary = df.groupBy("experience_level").agg(
            {"salary_in_usd": "avg", "*": "count"}
        )
        summary.show(truncate=False)

        output_path = str(OUTPUT_DIR / "salary_averages")
        summary.write.mode("overwrite").csv(output_path, header=True)
        log.info("Summary written to: %s", output_path)

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
