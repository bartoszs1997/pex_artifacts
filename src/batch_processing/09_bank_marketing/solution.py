"""Spark application: SCD Type 2 incremental data loading (Bank Marketing).

Demonstrates Slowly Changing Dimension (SCD) Type 2 logic: changes to customer
records are tracked using effective start/end dates and an ``is_current`` flag.

The Bank Marketing dataset has no customer ID — the task specifies ``age`` as
the business key for detecting new/modified records, and ``education`` + ``loan``
as the tracked attributes.

Pipeline:
    1. Read the CSV into a Spark DataFrame (source).
    2. Simulate an existing target by loading the first half of the data,
       de-duplicated by age (= initial customer dimension table).
    3. Load the second half as the incremental source batch.
    4. Identify NEW records (ages not in target).
    5. Identify MODIFIED records (same age, different education or loan).
    6. Apply SCD Type 2:
       - New → insert with start_date = today, end_date = 9999-12-31, is_current = True.
       - Modified → expire old (end_date = today, is_current = False) + insert new version.
       - Unchanged → keep as-is.
    7. Write the updated target dimension table to Parquet.

Run (local; Java 17 must be on PATH for PySpark 4.x):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/batch_processing/09_bank_marketing/solution.py
"""

import logging
import sys
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "input" / "bank-additional-full.csv"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

TODAY = date.today().isoformat()
END_OF_TIME = "9999-12-31"

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("SCD Type 2")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "bank_marketing.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a SparkSession."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read the Bank Marketing CSV (semicolon-separated, quoted)."""
    return spark.read.csv(path, header=True, inferSchema=True, sep=";")


def build_initial_target(full_df: DataFrame) -> DataFrame:
    """Simulate the existing target: first-half rows, de-duplicated by age.

    Returns a dimension table with columns:
        age, education, loan, effective_start_date, effective_end_date, is_current
    """
    half = full_df.count() // 2
    initial = full_df.limit(half)

    target = initial.dropDuplicates(["age"]).select("age", "education", "loan")

    return (
        target
        .withColumn("effective_start_date", lit("2020-01-01"))
        .withColumn("effective_end_date", lit(END_OF_TIME))
        .withColumn("is_current", lit(True))
    )


def build_source_batch(full_df: DataFrame) -> DataFrame:
    """Simulate the incremental source: second-half rows, de-duplicated by age.

    Returns only age, education, loan (the "incoming" batch).
    """
    total = full_df.count()
    half = total // 2

    # Subtract the first half from the full dataset to get the second half.
    first_half = full_df.limit(half)
    second_half = full_df.subtract(first_half)

    return second_half.dropDuplicates(["age"]).select("age", "education", "loan")


def identify_new_records(source: DataFrame, target: DataFrame) -> DataFrame:
    """Find ages in source that do NOT exist in the current target."""
    target_ages = target.filter(col("is_current")).select("age")

    return source.join(target_ages, on="age", how="left_anti")


def identify_modified_records(
    source: DataFrame, target: DataFrame
) -> DataFrame:
    """Find ages present in both, where education or loan has changed."""
    current_target = target.filter(col("is_current")).select(
        col("age"),
        col("education").alias("education_old"),
        col("loan").alias("loan_old"),
    )

    joined = source.join(current_target, on="age", how="inner")

    changed = joined.filter(
        (col("education") != col("education_old"))
        | (col("loan") != col("loan_old"))
    )

    return changed.select("age", "education", "loan")


def apply_scd_type2(
    target: DataFrame,
    new_records: DataFrame,
    modified_records: DataFrame,
) -> DataFrame:
    """Apply SCD Type 2 to the target dimension table.

    - Unchanged rows: keep as-is.
    - Modified rows: expire old version (end_date=today, is_current=False)
                     and insert new version.
    - New rows: insert with start_date=today, end_date=9999-12-31, is_current=True.
    """
    modified_ages = modified_records.select("age")

    # 1) Expire old versions of modified records.
    expired = (
        target
        .join(modified_ages, on="age", how="inner")
        .filter(col("is_current"))
        .withColumn("effective_end_date", lit(TODAY))
        .withColumn("is_current", lit(False))
    )

    # 2) Keep unchanged current records + already-expired historical rows.
    unchanged = target.join(modified_ages, on="age", how="left_anti")

    # 3) New-version rows for modified records.
    modified_new = (
        modified_records
        .withColumn("effective_start_date", lit(TODAY))
        .withColumn("effective_end_date", lit(END_OF_TIME))
        .withColumn("is_current", lit(True))
    )

    # 4) Rows for brand-new records.
    new_inserts = (
        new_records
        .withColumn("effective_start_date", lit(TODAY))
        .withColumn("effective_end_date", lit(END_OF_TIME))
        .withColumn("is_current", lit(True))
    )

    return (
        unchanged
        .unionByName(expired)
        .unionByName(modified_new)
        .unionByName(new_inserts)
    )


def main() -> None:
    """Run the SCD Type 2 incremental load end to end."""
    spark: SparkSession = create_spark_session("BankMarketingSCD2")
    try:
        # --- Read full CSV ---
        full_df: DataFrame = read_csv(spark, str(INPUT_FILE))
        log.info(f"Full dataset rows: {full_df.count()}")

        # --- Simulate target + source ---
        target: DataFrame = build_initial_target(full_df)
        target.cache()
        log.info(f"Initial target (unique ages): {target.count()}")

        source: DataFrame = build_source_batch(full_df)
        source.cache()
        log.info(f"Source batch (unique ages): {source.count()}")

        # --- Identify changes ---
        new_records: DataFrame = identify_new_records(source, target)
        new_count = new_records.count()
        log.info(f"NEW records (age not in target): {new_count}")

        modified_records: DataFrame = identify_modified_records(source, target)
        mod_count = modified_records.count()
        log.info(f"MODIFIED records (education or loan changed): {mod_count}")

        unchanged_count = source.count() - new_count - mod_count
        log.info(f"UNCHANGED records: {unchanged_count}")

        # --- Show samples ---
        if new_count > 0:
            print("\n== Sample NEW records ==")
            new_records.orderBy("age").show(10, truncate=False)

        if mod_count > 0:
            print("== Sample MODIFIED records (new values) ==")
            modified_records.orderBy("age").show(10, truncate=False)

        # --- Apply SCD Type 2 ---
        updated_target: DataFrame = apply_scd_type2(
            target, new_records, modified_records
        )
        log.info(f"Updated target rows (with history): {updated_target.count()}")

        print("== Updated target — expired rows (is_current=false) ==")
        updated_target.filter(~col("is_current")).orderBy("age").show(
            10, truncate=False
        )

        print("== Updated target — sample current rows ==")
        updated_target.filter(col("is_current")).orderBy("age").show(
            10, truncate=False
        )

        # --- Write output ---
        updated_target.write.mode("overwrite").parquet(
            str(OUTPUT_DIR / "customer_dimension")
        )
        log.info("Updated target written to data/output/customer_dimension/")

    except Exception:
        log.exception("SCD Type 2 processing failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
