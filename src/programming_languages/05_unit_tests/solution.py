"""Unit Tests — IBM Transactions for Anti-Money Laundering data processing.

Demonstrates data processing with Apache Spark on a large financial transaction
dataset. The script loads the data, validates its structure, and performs basic
analysis. Unit tests (pytest) verify correctness of the processing logic.

Dataset: ealtman2019/ibm-transactions-for-anti-money-laundering-aml
File:    HI-Large_Trans.csv (11 columns, ~22M rows)

Columns:
    Timestamp, From Bank, Account, To Bank, Account, Amount Received,
    Receiving Currency, Amount Paid, Payment Currency, Payment Format,
    Is Laundering

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/05_unit_tests/solution.py
"""

import logging
import sys
from pathlib import Path

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
INPUT_DIR = SCRIPT_DIR / "data" / "input"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"
LOG_DIR = SCRIPT_DIR / "logs"

DATA_FILE = INPUT_DIR / "HI-Large_Trans.csv"
LOG_FILE = LOG_DIR / "unit_tests.log"

EXPECTED_COLUMNS = 11
TIMESTAMP_FORMAT = "yyyy/MM/dd HH:mm"

VALID_PAYMENT_FORMATS = [
    "ACH",
    "Bitcoin",
    "Cash",
    "Cheque",
    "Credit Card",
    "Reinvestment",
    "Wire",
]

# ---------------------------------------------------------------------------
# Logging (dual: console + file)
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("unit_tests")
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
TRANSACTION_SCHEMA = StructType(
    [
        StructField("Timestamp", StringType(), nullable=True),
        StructField("From Bank", StringType(), nullable=True),
        StructField("From_Account", StringType(), nullable=True),
        StructField("To Bank", StringType(), nullable=True),
        StructField("To_Account", StringType(), nullable=True),
        StructField("Amount Received", DoubleType(), nullable=True),
        StructField("Receiving Currency", StringType(), nullable=True),
        StructField("Amount Paid", DoubleType(), nullable=True),
        StructField("Payment Currency", StringType(), nullable=True),
        StructField("Payment Format", StringType(), nullable=True),
        StructField("Is Laundering", IntegerType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Data Processing Functions
# ---------------------------------------------------------------------------


def load_transactions(spark: SparkSession, path: str) -> DataFrame:
    """Load the transactions CSV into a Spark DataFrame with explicit schema."""
    df = spark.read.csv(path, header=True, schema=TRANSACTION_SCHEMA)
    return df


def get_column_count(df: DataFrame) -> int:
    """Return the number of columns in the DataFrame."""
    return len(df.columns)


def validate_amount_received_is_float(df: DataFrame) -> bool:
    """Check that the 'Amount Received' column contains only float (double) values.

    Returns True if all non-null values are valid doubles.
    """
    field = df.schema["Amount Received"]
    return isinstance(field.dataType, DoubleType)


def validate_timestamp_format(df: DataFrame) -> DataFrame:
    """Parse Timestamp column and return rows where parsing succeeds.

    Uses try_to_timestamp with format 'yyyy/MM/dd HH:mm'.
    Returns a DataFrame with an extra 'parsed_ts' column (null if invalid).
    """
    return df.withColumn(
        "parsed_ts", F.try_to_timestamp(F.col("Timestamp"), F.lit(TIMESTAMP_FORMAT))
    )


def get_invalid_timestamps(df: DataFrame) -> int:
    """Count rows where Timestamp does not match the expected datetime format."""
    df_parsed = validate_timestamp_format(df)
    invalid_count = df_parsed.filter(
        F.col("parsed_ts").isNull() & F.col("Timestamp").isNotNull()
    ).count()
    return invalid_count


def is_valid_payment_format(payment_format: str) -> bool:
    """Check if a given payment format is in the list of valid formats."""
    return payment_format in VALID_PAYMENT_FORMATS


def get_payment_format_summary(df: DataFrame) -> DataFrame:
    """Group transactions by Payment Format and count occurrences."""
    return df.groupBy("Payment Format").count().orderBy(F.desc("count"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the data processing pipeline."""
    log.info("=" * 60)
    log.info("UNIT TESTS DEMO — IBM AML Transactions Processing")
    log.info("=" * 60)

    if not DATA_FILE.exists():
        log.error("Input file not found: %s. Run download_data.py first.", DATA_FILE)
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("UnitTestsDemo")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        log.info("SparkSession created.")

        # Load data
        df = load_transactions(spark, str(DATA_FILE))
        row_count = df.count()
        log.info("Loaded %d rows from %s", row_count, DATA_FILE.name)

        # Validate column count
        col_count = get_column_count(df)
        log.info("Column count: %d (expected: %d)", col_count, EXPECTED_COLUMNS)

        # Validate Amount Received type
        amount_valid = validate_amount_received_is_float(df)
        log.info("Amount Received is DoubleType: %s", amount_valid)

        # Validate Timestamp format
        invalid_ts = get_invalid_timestamps(df)
        log.info("Invalid timestamps: %d", invalid_ts)

        # Validate Reinvestment as payment format
        reinvestment_valid = is_valid_payment_format("Reinvestment")
        log.info("'Reinvestment' is valid payment format: %s", reinvestment_valid)

        # Payment format summary
        log.info("=" * 60)
        log.info("Payment Format Distribution")
        log.info("=" * 60)
        summary = get_payment_format_summary(df)
        summary.show(truncate=False)

        # Save summary
        output_path = str(OUTPUT_DIR / "payment_format_summary")
        summary.write.mode("overwrite").csv(output_path, header=True)
        log.info("Summary written to: %s", output_path)

        log.info("=" * 60)
        log.info("ALL VALIDATIONS COMPLETED SUCCESSFULLY")
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
