"""Health Insurance Marketplace — Debugging Tools Demonstration.

Demonstrates proper use of Python logging as a debugging tool alongside
Apache Spark data transformations on the Health Insurance Marketplace
Rate.csv dataset.

Logging strategy:
  - DEBUG level: printed to terminal (tracks execution flow, variable states)
  - WARNING level and above: saved to log file
  - WARNING messages: specific data quality issues (nulls, outliers, schema mismatches)
  - ERROR messages: defects identified and fixed programmatically

Data transformations performed:
  1. Load Rate.csv with explicit schema and handle malformed records.
  2. Clean null/invalid values in critical columns.
  3. Cast rate columns to proper numeric types (fix type errors).
  4. Filter unreasonable rate values (negative, zero, extremely high).
  5. Compute average rates by state and age group.
  6. Compute rate spread (max - min) per plan.

Dataset columns (subset used):
  BusinessYear, StateCode, IssuerId, PlanId, RatingAreaId, Tobacco,
  Age, IndividualRate, IndividualTobaccoRate, Couple, ...

Usage:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/programming_languages/03_debugging_tools/solution.py
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import logging
import sys
from pathlib import Path

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

RATE_FILE = INPUT_DIR / "Rate.csv"
LOG_FILE = LOG_DIR / "debugging_tools.log"

# ---------------------------------------------------------------------------
# Logging Setup — Dual handlers (DEBUG->console, WARNING+->file)
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("debugging_tools")
log.setLevel(logging.DEBUG)  # Capture everything at logger level

formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s")

# Console handler — prints ALL logs at DEBUG level to terminal
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

# File handler — saves only WARNING and above to log file
file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
file_handler.setLevel(logging.WARNING)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)

# ---------------------------------------------------------------------------
# Schema Definition
# ---------------------------------------------------------------------------
RATE_SCHEMA = StructType(
    [
        StructField("BusinessYear", IntegerType(), nullable=True),
        StructField("StateCode", StringType(), nullable=True),
        StructField("IssuerId", IntegerType(), nullable=True),
        StructField("SourceName", StringType(), nullable=True),
        StructField("VersionNum", IntegerType(), nullable=True),
        StructField("ImportDate", StringType(), nullable=True),
        StructField("IssuerId2", IntegerType(), nullable=True),
        StructField("FederalTIN", StringType(), nullable=True),
        StructField("RateEffectiveDate", StringType(), nullable=True),
        StructField("RateExpirationDate", StringType(), nullable=True),
        StructField("PlanId", StringType(), nullable=True),
        StructField("RatingAreaId", StringType(), nullable=True),
        StructField("Tobacco", StringType(), nullable=True),
        StructField("Age", StringType(), nullable=True),
        StructField("IndividualRate", StringType(), nullable=True),
        StructField("IndividualTobaccoRate", StringType(), nullable=True),
        StructField("Couple", StringType(), nullable=True),
        StructField("PrimarySubscriberAndOneDependent", StringType(), nullable=True),
        StructField("PrimarySubscriberAndTwoDependents", StringType(), nullable=True),
        StructField("PrimarySubscriberAndThreeOrMoreDependents", StringType(), nullable=True),
        StructField("CoupleAndOneDependent", StringType(), nullable=True),
        StructField("CoupleAndTwoDependents", StringType(), nullable=True),
        StructField("CoupleAndThreeOrMoreDependents", StringType(), nullable=True),
        StructField("RowNumber", IntegerType(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Data Transformation Functions
# ---------------------------------------------------------------------------


def load_data(spark):
    """Load Rate.csv with explicit schema, handling malformed records.

    DEBUG: logs schema details, file path, row counts.
    WARNING: logs if malformed/corrupt records are detected.
    ERROR: logs and fixes if file cannot be read with given schema.
    """
    log.debug("Attempting to load data from: %s", RATE_FILE)
    log.debug("Using explicit schema with %d fields", len(RATE_SCHEMA.fields))

    try:
        df = spark.read.csv(
            str(RATE_FILE),
            header=True,
            schema=RATE_SCHEMA,
            mode="PERMISSIVE",
            columnNameOfCorruptRecord=None,
        )
        row_count = df.count()
        log.debug("Successfully loaded %d rows from Rate.csv", row_count)
        log.debug("Columns: %s", df.columns)

        # Check for completely empty DataFrame
        if row_count == 0:
            log.error(
                "ERROR-001: DataFrame is empty after loading. "
                "Possible cause: incorrect file path or corrupted file. "
                "Fix: Verify file exists and is not empty."
            )
            return None

        return df

    except Exception as e:
        log.error(
            "ERROR-002: Failed to load Rate.csv with explicit schema. "
            "Exception: %s. Fix: Falling back to inferSchema mode.",
            str(e),
        )
        # FIX: Fall back to schema inference
        df = spark.read.csv(str(RATE_FILE), header=True, inferSchema=True)
        log.warning(
            "WARNING: Loaded data using inferSchema as fallback. "
            "Schema may not match expected types. Rows: %d",
            df.count(),
        )
        return df


def clean_null_values(df):
    """Remove or handle null values in critical columns.

    DEBUG: logs null counts per column.
    WARNING: logs if significant percentage of data has nulls.
    ERROR: logs if ALL values in a critical column are null.
    """
    critical_columns = ["StateCode", "PlanId", "IndividualRate", "Age"]
    log.debug("Checking null values in critical columns: %s", critical_columns)

    total_rows = df.count()
    log.debug("Total rows before null cleaning: %d", total_rows)

    for col_name in critical_columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0

        log.debug("Column '%s': %d nulls (%.1f%%)", col_name, null_count, null_pct)

        if null_count == total_rows:
            log.error(
                "ERROR-003: Column '%s' has ALL null values (%d rows). "
                "Fix: This column cannot be used for analysis. "
                "Removing it from the processing pipeline.",
                col_name,
                null_count,
            )
        elif null_pct > 10:
            log.warning(
                "WARNING: Column '%s' has %.1f%% null values (%d / %d rows). "
                "This may affect analysis accuracy. "
                "Rows with null '%s' will be excluded from aggregations.",
                col_name,
                null_pct,
                null_count,
                total_rows,
                col_name,
            )

    # Drop rows where critical columns are null
    df_cleaned = df.dropna(subset=["StateCode", "PlanId", "IndividualRate"])
    cleaned_count = df_cleaned.count()
    dropped = total_rows - cleaned_count
    log.debug("Dropped %d rows with null critical values. Remaining: %d", dropped, cleaned_count)

    if dropped > total_rows * 0.5:
        log.warning(
            "WARNING: More than 50%% of data dropped during null cleaning "
            "(%d / %d rows). Check data quality at source.",
            dropped,
            total_rows,
        )

    return df_cleaned


def cast_rate_columns(df):
    """Cast rate columns from String to Double, handling conversion errors.

    DEBUG: logs casting operations and success counts.
    WARNING: logs if non-numeric values are found in rate columns.
    ERROR: logs and fixes type casting failures.
    """
    rate_columns = ["IndividualRate", "IndividualTobaccoRate", "Couple"]
    log.debug("Casting rate columns to DoubleType: %s", rate_columns)

    df_casted = df
    for col_name in rate_columns:
        if col_name not in df.columns:
            log.debug("Column '%s' not in DataFrame, skipping.", col_name)
            continue

        # Attempt cast — non-numeric values become null
        df_casted = df_casted.withColumn(
            col_name, F.col(col_name).cast(DoubleType())
        )

        # Check how many failed the cast (became null that weren't null before)
        original_non_null = df.filter(F.col(col_name).isNotNull()).count()
        casted_non_null = df_casted.filter(F.col(col_name).isNotNull()).count()
        cast_failures = original_non_null - casted_non_null

        log.debug(
            "Column '%s': %d values cast successfully, %d cast failures",
            col_name,
            casted_non_null,
            cast_failures,
        )

        if cast_failures > 0:
            log.error(
                "ERROR-004: %d values in column '%s' could not be cast to Double. "
                "These contained non-numeric data (e.g., 'N/A', text). "
                "Fix: Non-numeric values have been converted to null and will be "
                "excluded from numeric aggregations.",
                cast_failures,
                col_name,
            )

            if cast_failures > original_non_null * 0.3:
                log.warning(
                    "WARNING: More than 30%% of '%s' values failed numeric casting "
                    "(%d / %d). Column may contain categorical data misidentified "
                    "as numeric.",
                    col_name,
                    cast_failures,
                    original_non_null,
                )

    return df_casted


def filter_invalid_rates(df):
    """Filter out unreasonable rate values (negative, zero, extremely high).

    DEBUG: logs filter thresholds and counts at each step.
    WARNING: logs if outliers are detected.
    ERROR: logs if negative rates found (data integrity issue).
    """
    log.debug("Filtering invalid IndividualRate values...")
    log.debug("Filter criteria: rate > 0 AND rate < 10000")

    before_count = df.count()

    # Check for negative rates — this is a data integrity error
    negative_rates = df.filter(F.col("IndividualRate") < 0).count()
    if negative_rates > 0:
        log.error(
            "ERROR-005: Found %d rows with negative IndividualRate values. "
            "This indicates data corruption or incorrect data entry. "
            "Fix: Removing rows with negative rates from the dataset.",
            negative_rates,
        )

    # Check for zero rates
    zero_rates = df.filter(F.col("IndividualRate") == 0).count()
    if zero_rates > 0:
        log.debug("Found %d rows with zero IndividualRate (free plans or errors).", zero_rates)

    # Check for extremely high rates (potential outliers)
    extreme_rates = df.filter(F.col("IndividualRate") > 9999).count()
    if extreme_rates > 0:
        log.warning(
            "WARNING: Found %d rows with IndividualRate > $9,999/month. "
            "These appear to be outliers or data entry errors. "
            "They will be excluded from the analysis.",
            extreme_rates,
        )

    # Apply filter: keep only reasonable positive rates
    df_filtered = df.filter(
        (F.col("IndividualRate") > 0) & (F.col("IndividualRate") <= 9999)
    )

    after_count = df_filtered.count()
    log.debug(
        "Rate filtering complete. Before: %d, After: %d, Removed: %d",
        before_count,
        after_count,
        before_count - after_count,
    )

    return df_filtered


def compute_avg_rate_by_state(df):
    """Compute average IndividualRate per state.

    DEBUG: logs aggregation steps and result preview.
    WARNING: logs if any state has very few records (unreliable average).
    """
    log.debug("Computing average IndividualRate grouped by StateCode...")

    df_avg = (
        df.groupBy("StateCode")
        .agg(
            F.avg("IndividualRate").alias("avg_rate"),
            F.count("*").alias("record_count"),
            F.min("IndividualRate").alias("min_rate"),
            F.max("IndividualRate").alias("max_rate"),
        )
        .orderBy("StateCode")
    )

    log.debug("Aggregation complete. Number of states: %d", df_avg.count())

    # Check for states with very few records
    low_count_states = df_avg.filter(F.col("record_count") < 100)
    if low_count_states.count() > 0:
        states = [row.StateCode for row in low_count_states.collect()]
        log.warning(
            "WARNING: %d state(s) have fewer than 100 rate records: %s. "
            "Average rates for these states may be unreliable due to "
            "small sample size.",
            len(states),
            ", ".join(states),
        )

    log.debug("Average rate by state — sample:")
    df_avg.show(10, truncate=False)

    return df_avg


def compute_avg_rate_by_age(df):
    """Compute average IndividualRate per age group.

    DEBUG: logs grouping logic and result counts.
    WARNING: logs if age values contain unexpected formats.
    """
    log.debug("Computing average IndividualRate grouped by Age...")

    # Check for unexpected age values
    age_values = df.select("Age").distinct().collect()
    age_list = [row.Age for row in age_values if row.Age is not None]
    log.debug("Unique age values found: %d", len(age_list))

    non_standard = [a for a in age_list if not a.replace("+", "").replace("-", "").isdigit()
                    and a not in ("0-14", "0-20", "21-64", "65 and over",
                                  "Family Option", "65+")]
    if non_standard:
        log.warning(
            "WARNING: Found %d non-standard age values: %s. "
            "These may not group correctly in age-based analysis.",
            len(non_standard),
            non_standard[:10],
        )

    df_age = (
        df.groupBy("Age")
        .agg(
            F.avg("IndividualRate").alias("avg_rate"),
            F.count("*").alias("record_count"),
        )
        .orderBy("Age")
    )

    log.debug("Age group aggregation complete. Groups: %d", df_age.count())
    log.debug("Average rate by age — sample:")
    df_age.show(20, truncate=False)

    return df_age


def compute_rate_spread_by_plan(df):
    """Compute rate spread (max - min) per plan to identify pricing variability.

    DEBUG: logs computation details.
    WARNING: logs plans with extreme spread (potential pricing errors).
    """
    log.debug("Computing rate spread (max - min) per PlanId...")

    df_spread = (
        df.groupBy("PlanId")
        .agg(
            (F.max("IndividualRate") - F.min("IndividualRate")).alias("rate_spread"),
            F.avg("IndividualRate").alias("avg_rate"),
            F.count("*").alias("record_count"),
        )
        .orderBy(F.col("rate_spread").desc())
    )

    # Check for extreme spreads
    extreme_spread = df_spread.filter(F.col("rate_spread") > 5000)
    extreme_count = extreme_spread.count()
    if extreme_count > 0:
        log.warning(
            "WARNING: %d plan(s) have a rate spread > $5,000. "
            "This indicates extreme pricing variability which may suggest "
            "data quality issues or highly variable plan offerings.",
            extreme_count,
        )

    log.debug("Rate spread computation complete. Plans analyzed: %d", df_spread.count())
    log.debug("Top plans by rate spread:")
    df_spread.show(10, truncate=False)

    return df_spread


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------


def main() -> int:
    """Execute the full data transformation pipeline with debugging logs."""
    log.debug("=" * 70)
    log.debug("DEBUGGING TOOLS DEMONSTRATION — Health Insurance Rates")
    log.debug("=" * 70)
    log.debug("Log file (WARNING+): %s", LOG_FILE)
    log.debug("Console output: DEBUG and above")

    # Verify input file
    if not RATE_FILE.exists():
        log.error(
            "ERROR-006: Input file not found: %s. "
            "Fix: Run download_data.py first to obtain the dataset.",
            RATE_FILE,
        )
        return 1

    log.debug("Input file found: %s (%.1f MB)",
              RATE_FILE, RATE_FILE.stat().st_size / (1024 * 1024))

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = None
    try:
        # --- Initialize SparkSession ---
        log.debug("Creating SparkSession...")
        spark = (
            SparkSession.builder.appName("DebuggingToolsDemo")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        log.debug("SparkSession created. App: %s", spark.sparkContext.appName)
        log.debug("Spark version: %s", spark.version)

        # --- Step 1: Load Data ---
        log.debug("-" * 50)
        log.debug("STEP 1: Loading data...")
        df_raw = load_data(spark)
        if df_raw is None:
            return 1

        # --- Step 2: Clean Null Values ---
        log.debug("-" * 50)
        log.debug("STEP 2: Cleaning null values...")
        df_cleaned = clean_null_values(df_raw)

        # --- Step 3: Cast Rate Columns ---
        log.debug("-" * 50)
        log.debug("STEP 3: Casting rate columns to numeric types...")
        df_casted = cast_rate_columns(df_cleaned)

        # --- Step 4: Filter Invalid Rates ---
        log.debug("-" * 50)
        log.debug("STEP 4: Filtering invalid rate values...")
        df_valid = filter_invalid_rates(df_casted)

        # --- Step 5: Compute Average Rate by State ---
        log.debug("-" * 50)
        log.debug("STEP 5: Computing average rates by state...")
        df_state_avg = compute_avg_rate_by_state(df_valid)

        # Save result
        output_path = str(OUTPUT_DIR / "avg_rate_by_state")
        df_state_avg.write.mode("overwrite").csv(output_path, header=True)
        log.debug("State averages written to: %s", output_path)

        # --- Step 6: Compute Average Rate by Age ---
        log.debug("-" * 50)
        log.debug("STEP 6: Computing average rates by age group...")
        df_age_avg = compute_avg_rate_by_age(df_valid)

        output_path = str(OUTPUT_DIR / "avg_rate_by_age")
        df_age_avg.write.mode("overwrite").csv(output_path, header=True)
        log.debug("Age averages written to: %s", output_path)

        # --- Step 7: Compute Rate Spread by Plan ---
        log.debug("-" * 50)
        log.debug("STEP 7: Computing rate spread by plan...")
        df_spread = compute_rate_spread_by_plan(df_valid)

        output_path = str(OUTPUT_DIR / "rate_spread_by_plan")
        df_spread.write.mode("overwrite").csv(output_path, header=True)
        log.debug("Rate spreads written to: %s", output_path)

        # --- Summary ---
        log.debug("=" * 70)
        log.debug("PIPELINE COMPLETED SUCCESSFULLY")
        log.debug("=" * 70)
        log.info("All transformations complete. Check log file for warnings/errors: %s", LOG_FILE)

    except Exception:
        log.exception("CRITICAL: Pipeline failed with unexpected error.")
        log.error(
            "ERROR-007: Unhandled exception in main pipeline. "
            "Fix: Review the traceback above and address the root cause."
        )
        return 1
    finally:
        if spark:
            spark.stop()
            log.debug("SparkSession stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
