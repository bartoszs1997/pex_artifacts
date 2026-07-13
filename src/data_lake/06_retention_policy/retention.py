"""Task 06 - Set up retention policy rules for a data lake (Delta Lake).

The data lake is imitated with LocalStack S3. We load US fund data into Delta
tables, attach retention-policy metadata to them, and run a retention engine that
enforces the policy: expired records are archived to a cheaper tier and then
removed from the active table, after which Delta VACUUM reclaims the storage.

The full policy framework (legal + business + data types + archiving) is written
up in RETENTION_POLICY.md; this script implements exactly those rules.

Data lake layout (LocalStack S3):
    active  : s3a://pex-datalake/active/etf_prices        (Delta, time-series)
              s3a://pex-datalake/active/etf_metadata      (Delta, reference data)
    archive : s3a://pex-datalake/archive/etf_prices       (Delta, archive tier)

Retention rules (see RETENTION_POLICY.md):
    etf_prices   : 7 years from price_date -> archive then delete (SEC 17a-4/SOX).
    etf_metadata : retain while active (no time-based expiry).

Reference "now" is fixed at POLICY_NOW = 2021-12-31 for a reproducible demo, so
the 7-year cutoff is 2014-12-31.

Prerequisite:
    docker compose -f src/data_lake/06_retention_policy/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/06_retention_policy/retention.py
"""

import csv
from datetime import date
from pathlib import Path

import boto3
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
INPUT = HERE / "data" / "input"
PRICES_CSV = INPUT / "ETF prices.csv"
METADATA_CSV = INPUT / "ETFs.csv"
PRICES_SAMPLE = HERE / "data" / "landing" / "etf_prices_sample.csv"
SAMPLE_ROWS = 100000

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
ACTIVE_PRICES = f"s3a://{BUCKET}/active/etf_prices"
ACTIVE_METADATA = f"s3a://{BUCKET}/active/etf_metadata"
ARCHIVE_PRICES = f"s3a://{BUCKET}/archive/etf_prices"

# Reference "now" for a reproducible demo (the dataset's as-of date).
POLICY_NOW = date(2021, 12, 31)

# Retention policy metadata (the single source of truth, mirrors RETENTION_POLICY.md).
RETENTION_POLICIES = {
    "etf_prices": {
        "active_path": ACTIVE_PRICES,
        "archive_path": ARCHIVE_PRICES,
        "date_column": "price_date",
        "retention_years": 7,
        "action": "archive_then_delete",
        "sensitivity": "low-public-market-data",
        "legal_basis": "SEC 17a-4 / FINRA 4511 / SOX 802",
    },
    "etf_metadata": {
        "active_path": ACTIVE_METADATA,
        "archive_path": None,
        "date_column": None,
        "retention_years": None,
        "action": "retain_while_active",
        "sensitivity": "low-public-reference-data",
        "legal_basis": "business need",
    },
}

PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.13:4.3.0",
        "org.apache.hadoop:hadoop-aws:3.4.2",
    ]
)


def create_bucket() -> None:
    """Create the data lake bucket on LocalStack (safe to call every time)."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket {BUCKET}")


def clear_lake() -> None:
    """Delete active and archive objects so a fresh demo run is deterministic."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    for prefix in ("active/", "archive/"):
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        keys = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
            print(f"Cleared {len(keys)} objects under {prefix}")


def create_spark_session() -> SparkSession:
    """Create a Spark session with Delta Lake and S3A to LocalStack."""
    return (
        SparkSession.builder.appName("Task06Retention")
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Allow VACUUM with a zero-hour retention so the demo reclaims storage now.
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )


def _sample_csv(source: Path, target: Path, rows: int) -> None:
    """Copy the header plus the first `rows` data lines of source into target."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open(newline="") as fin, target.open("w", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        writer.writerow(next(reader))  # header
        for i, row in enumerate(reader):
            if i >= rows:
                break
            writer.writerow(row)


def load_data_lake(spark: SparkSession) -> None:
    """Load the fund data into active Delta tables (the data lake being governed)."""
    _sample_csv(PRICES_CSV, PRICES_SAMPLE, SAMPLE_ROWS)

    prices = (
        spark.read.csv(str(PRICES_SAMPLE), header=True, inferSchema=True)
        .withColumn("price_date", F.to_date("price_date"))
        .withColumn("price_year", F.year("price_date"))
    )
    prices.write.format("delta").mode("overwrite").partitionBy("price_year").save(
        ACTIVE_PRICES
    )
    print(f"Loaded {prices.count()} price rows -> {ACTIVE_PRICES}")

    metadata = spark.read.csv(str(METADATA_CSV), header=True, inferSchema=True)
    metadata.write.format("delta").mode("overwrite").save(ACTIVE_METADATA)
    print(f"Loaded {metadata.count()} metadata rows -> {ACTIVE_METADATA}")


def enforce_time_based_policy(spark: SparkSession, name: str) -> dict:
    """Enforce one archive_then_delete policy: archive expired rows, then delete.

    Returns a small report so the caller can verify the outcome.
    """
    policy = RETENTION_POLICIES[name]
    date_col = policy["date_column"]
    cutoff = date(POLICY_NOW.year - policy["retention_years"], POLICY_NOW.month, POLICY_NOW.day)
    print(f"Policy '{name}': retain {policy['retention_years']}y, cutoff = {cutoff}")

    active = spark.read.format("delta").load(policy["active_path"])
    expired = active.filter(F.col(date_col) <= F.lit(cutoff))
    expired_count = expired.count()

    # 1. Archive expired rows to the cheaper archive tier.
    expired.write.format("delta").mode("overwrite").save(policy["archive_path"])
    print(f"Archived {expired_count} expired rows -> {policy['archive_path']}")

    # 2. Delete the expired rows from the active table.
    active_delta = DeltaTable.forPath(spark, policy["active_path"])
    active_delta.delete(F.col(date_col) <= F.lit(cutoff))
    print(f"Deleted {expired_count} expired rows from {policy['active_path']}")

    # 3. Physically reclaim storage: VACUUM removes now-unreferenced data files.
    active_delta.vacuum(0)
    print(f"Vacuumed {policy['active_path']} (storage reclaimed)")

    return {"cutoff": cutoff, "expired_count": expired_count}


def verify(spark: SparkSession, report: dict) -> None:
    """Verify retention: active is within policy, archive holds the expired data."""
    policy = RETENTION_POLICIES["etf_prices"]
    date_col = policy["date_column"]
    cutoff = report["cutoff"]

    active = spark.read.format("delta").load(policy["active_path"])
    archive = spark.read.format("delta").load(policy["archive_path"])

    # 1. Active table retains ONLY data within the retention window.
    too_old = active.filter(F.col(date_col) <= F.lit(cutoff)).count()
    assert too_old == 0, f"active still has {too_old} expired rows"

    # 2. Archive holds exactly the expired rows.
    archived = archive.count()
    assert archived == report["expired_count"], (
        f"archive has {archived}, expected {report['expired_count']}"
    )

    # 3. Every archived row is genuinely older than the cutoff.
    bad_archive = archive.filter(F.col(date_col) > F.lit(cutoff)).count()
    assert bad_archive == 0, f"archive has {bad_archive} non-expired rows"

    active_min = active.agg(F.min(date_col)).first()[0]
    print(
        f"VERIFIED: active retains {active.count()} rows (oldest {active_min}, "
        f"all after cutoff {cutoff}); archive holds {archived} expired rows; "
        f"etf_metadata retained while active"
    )


def main() -> None:
    create_bucket()
    clear_lake()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Configure the data lake: load the data under retention governance.
    load_data_lake(spark)

    # Enforce the time-based retention policy on the price records.
    report = enforce_time_based_policy(spark, "etf_prices")

    # Reference data has no time-based expiry; report it explicitly.
    print("Policy 'etf_metadata': retain_while_active (no time-based expiry)")

    # Verify retention was enforced correctly.
    verify(spark, report)
    spark.stop()


if __name__ == "__main__":
    main()
