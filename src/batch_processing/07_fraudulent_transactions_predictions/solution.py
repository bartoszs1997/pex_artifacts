"""Spark RDD application: identify potentially fraudulent transactions.

The task asks to process the customer-transaction data using the **Spark RDD**
API (not DataFrames) and to run the job in cluster mode. The pipeline is:

    Step 1: filter transactions whose amount exceeds a threshold ($10,000),
    Step 2: compute the average transaction amount per account (nameOrig),
    Step 3: flag accounts whose average is at least twice the overall average.

The flagged accounts and their average amounts are written to CSV.

Dataset (PaySim, Kaggle "fraudulent-transactions-prediction"), columns:
    step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
    nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud
We only need column 2 (amount) and column 3 (nameOrig = originating account).

Caveat on this dataset: nameOrig is almost unique (~6.35M distinct values over
~6.36M rows, max 3 repeats), so "average per account" equals a single
transaction amount for almost every account. The pipeline is still correct and
general; the data simply happens to have few repeated accounts.

This app is meant to run on a Spark Standalone cluster via spark-submit: the
master is taken from --master and is NOT hard-coded in the session, so the same
file runs locally (default local[*]) or against a cluster. See the module
README for how to start the cluster and submit the job.

Run (Standalone cluster; Java 17 must be on PATH for PySpark 4.x):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    spark-submit --master spark://localhost:7077 \\
        src/batch_processing/07_fraudulent_transactions_predictions/solution.py
"""

import logging
import sys
from pathlib import Path

from pyspark import RDD
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "input" / "Fraud.csv"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Business rule: only transactions strictly above this amount are analysed.
AMOUNT_THRESHOLD = 10_000.0
# Column positions in the raw CSV (0-indexed).
AMOUNT_COL = 2
ACCOUNT_COL = 3

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("Fraud Detection")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "fraud.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def create_spark_session(app_name: str) -> SparkSession:
    """Create and return a SparkSession (master comes from --master)."""
    log.debug("Creating SparkSession...")

    return SparkSession.builder.appName(app_name).getOrCreate()


def log_runtime_context(spark: SparkSession) -> None:
    """Log how the job maps onto Spark components (driver, executors, master)."""
    sc = spark.sparkContext
    log.info(f"Spark version    : {sc.version}")
    log.info(f"Application id   : {sc.applicationId}")
    log.info(f"Master           : {sc.master}")
    log.info(f"Default partitions: {sc.defaultParallelism}")


def parse_line(line: str) -> tuple[str, float] | None:
    """Parse one CSV row into (account, amount); return None if malformed."""
    try:
        fields = line.split(",")
        account = fields[ACCOUNT_COL]
        amount = float(fields[AMOUNT_COL])
    except (IndexError, ValueError):
        # Header row and any corrupt line land here and are dropped downstream.
        return None
    return (account, amount)


def load_transactions(spark: SparkSession, path: str) -> RDD:
    """Read the CSV as an RDD of (account, amount), header and bad rows dropped."""
    log.debug(f"Reading transactions from {path}...")
    lines = spark.sparkContext.textFile(path)

    return lines.map(parse_line).filter(lambda row: row is not None)


def filter_high_value(transactions: RDD, threshold: float) -> RDD:
    """Step 1: keep only transactions whose amount exceeds the threshold."""
    log.debug(f"Filtering transactions with amount > {threshold}...")

    return transactions.filter(lambda row: row[1] > threshold)


def totals_by_account(transactions: RDD) -> RDD:
    """Group by account into (account, (sum_amount, count)) via reduceByKey."""
    log.debug("Aggregating sum and count per account...")

    return transactions.map(lambda row: (row[0], (row[1], 1))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )


def overall_average(account_totals: RDD) -> float:
    """Compute the overall average transaction amount across all filtered rows."""
    total_sum, total_count = account_totals.map(lambda row: row[1]).reduce(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    return total_sum / total_count


def average_by_account(account_totals: RDD) -> RDD:
    """Step 2: turn (account, (sum, count)) into (account, average_amount)."""
    return account_totals.mapValues(lambda sum_count: sum_count[0] / sum_count[1])


def identify_suspicious(account_avg: RDD, overall_avg: float) -> RDD:
    """Step 3: keep accounts whose average is at least twice the overall average."""
    log.debug(f"Flagging accounts with average >= 2 x {overall_avg:.2f}...")

    return account_avg.filter(lambda row: row[1] >= 2 * overall_avg)


def write_to_csv(rdd: RDD, path: str) -> None:
    """Write (account, average_amount) to CSV (overwrite, with header)."""
    log.debug(f"Writing flagged accounts to {path}...")
    rdd.toDF(["account", "avg_amount"]).write.mode("overwrite").csv(path, header=True)


def main() -> None:
    """Run the fraud-detection pipeline end to end."""
    spark: SparkSession = create_spark_session("FraudulentTransactionsAnalysis")
    try:
        log_runtime_context(spark)

        transactions: RDD = load_transactions(spark, str(INPUT_FILE))

        # Step 1: high-value transactions only.
        high_value: RDD = filter_high_value(transactions, AMOUNT_THRESHOLD)

        # account_totals is reused for the overall average, the per-account
        # average and the final write, so cache it to read the file only once.
        account_totals: RDD = totals_by_account(high_value).cache()

        num_transactions: int = account_totals.map(lambda row: row[1][1]).sum()
        num_accounts: int = account_totals.count()
        log.info(f"High-value transactions (> {AMOUNT_THRESHOLD:.0f}): {num_transactions}")
        log.info(f"Distinct accounts after grouping: {num_accounts}")

        # Step 2: average per account.
        account_avg: RDD = average_by_account(account_totals)

        # Step 3: overall average, then accounts at least twice above it.
        overall_avg: float = overall_average(account_totals)
        log.info(f"Overall average transaction amount: {overall_avg:.2f}")

        suspicious: RDD = identify_suspicious(account_avg, overall_avg)
        num_suspicious: int = suspicious.count()
        log.info(f"Accounts with average >= 2x overall: {num_suspicious}")

        print("== sample of flagged accounts (account, avg_amount) ==")
        for account, avg in suspicious.takeOrdered(10, key=lambda row: -row[1]):
            print(f"{account}\t{avg:,.2f}")

        write_to_csv(suspicious, str(OUTPUT_DIR / "flagged_accounts"))
    except Exception:
        log.exception("Fraud-detection processing failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
