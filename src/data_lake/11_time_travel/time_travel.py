"""Task 11 - Time travel and rollbacks in a data lake (Delta Lake).

Delta Lake keeps every version of a table in its transaction log, which gives us
- for free - data versioning, full historical audit trails, point-in-time
queries, and rollbacks. This script builds a versioned, time-partitioned
customers table on the lake, makes a series of commits (including a deliberate
bad one), and then demonstrates every time-travel capability the task requires:

    - versioning        : each write creates a new Delta version
    - audit trail       : DESCRIBE HISTORY lists version, timestamp, operation
    - time partitioning : the table is partitioned by capture_date
    - query by version  : read `versionAsOf`
    - query by timestamp: read `timestampAsOf`
    - reproducible ML    : reading a fixed version always yields the same snapshot
    - rollback          : RESTORE TABLE ... VERSION AS OF undoes a bad commit

A small CLI (history / show / restore) is the user-facing interface; see the
`cli` function and the README.

Governance, access control, lineage, monitoring and maintenance are covered in
TIME_TRAVEL.md (the documentation deliverable).

Data lake layout (LocalStack S3):
    s3a://pex-datalake/customers   (Delta, partitioned by capture_date)

Prerequisite:
    docker compose -f src/data_lake/11_time_travel/docker-compose.yml up -d

Run the full demo (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/11_time_travel/time_travel.py

Use the CLI instead:
    uv run python src/data_lake/11_time_travel/time_travel.py history
    uv run python src/data_lake/11_time_travel/time_travel.py show --version 1
    uv run python src/data_lake/11_time_travel/time_travel.py restore --version 1
"""

import sys
from pathlib import Path

import boto3
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
INPUT_CSV = HERE / "data" / "input" / "Customers.csv"

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
CUSTOMERS = f"s3a://{BUCKET}/customers"

RENAME = {
    "CustomerID": "customer_id",
    "Gender": "gender",
    "Age": "age",
    "Annual Income ($)": "annual_income",
    "Spending Score (1-100)": "spending_score",
    "Profession": "profession",
    "Work Experience": "work_experience",
    "Family Size": "family_size",
}

PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.13:4.3.0",
        "org.apache.hadoop:hadoop-aws:3.4.2",
    ]
)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def reset_lake(s3) -> None:
    """Create the bucket and clear previous objects for a deterministic run."""
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket {BUCKET}")
    resp = s3.list_objects_v2(Bucket=BUCKET)
    keys = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
    if keys:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
        print(f"Cleared {len(keys)} existing objects")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Task11TimeTravel")
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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


def load_with_capture_time(spark: SparkSession) -> DataFrame:
    """Read the raw CSV and add capture-time columns (the task's timestamped data).

    A capture_time column records when each record was captured; capture_date is
    the time-based partition key.
    """
    raw = spark.read.csv(str(INPUT_CSV), header=True, inferSchema=True)
    for old, new in RENAME.items():
        raw = raw.withColumnRenamed(old, new)
    return (
        raw.withColumn("capture_time", F.current_timestamp())
        .withColumn("capture_date", F.current_date())
    )


def build_versions(spark: SparkSession) -> None:
    """Create a sequence of commits so the table has a rich version history."""
    base = load_with_capture_time(spark)

    # v0: initial load, partitioned by capture_date (time-based partitioning).
    base.write.format("delta").mode("overwrite").partitionBy("capture_date").save(
        CUSTOMERS
    )
    print(f"v0: initial load of {base.count()} rows (partitioned by capture_date)")

    table = DeltaTable.forPath(spark, CUSTOMERS)

    # v1: a legitimate business UPDATE (bump a customer's spending score).
    table.update(
        condition=F.col("customer_id") == 1,
        set={"spending_score": F.lit(99)},
    )
    print("v1: UPDATE customer 1 spending_score -> 99")

    # v2: a GDPR delete (right to be forgotten).
    table.delete(F.col("customer_id") == 2)
    print("v2: DELETE customer 2 (GDPR)")

    # v3: a BAD commit - an accidental mass update we will roll back.
    table.update(set={"annual_income": F.lit(0)})
    print("v3: BAD update - annual_income wiped to 0 for everyone")


def show_history(spark: SparkSession) -> DataFrame:
    """Audit trail: version, timestamp, and operation for every commit."""
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{CUSTOMERS}`").select(
        "version", "timestamp", "operation"
    )
    hist.orderBy("version").show(truncate=False)
    return hist


def query_by_version(spark: SparkSession, version: int) -> DataFrame:
    """Point-in-time query by version number."""
    return spark.read.format("delta").option("versionAsOf", version).load(CUSTOMERS)


def query_by_timestamp(spark: SparkSession, timestamp: str) -> DataFrame:
    """Point-in-time query by timestamp (yyyy-MM-dd HH:mm:ss)."""
    return spark.read.format("delta").option("timestampAsOf", timestamp).load(CUSTOMERS)


def rollback_to(spark: SparkSession, version: int) -> None:
    """Rollback: restore the table to an earlier version (undo bad commits)."""
    spark.sql(f"RESTORE TABLE delta.`{CUSTOMERS}` TO VERSION AS OF {version}")
    print(f"ROLLBACK: table restored to version {version}")


def demo(spark: SparkSession) -> None:
    """Run the full time-travel demonstration end to end."""
    build_versions(spark)

    print("\nHistorical audit trail (DESCRIBE HISTORY):")
    show_history(spark)

    # Reproducible ML: a fixed version is an immutable snapshot. Reading v0 twice
    # yields identical income totals no matter what later commits did.
    snap_a = query_by_version(spark, 0).agg(F.sum("annual_income")).first()[0]
    snap_b = query_by_version(spark, 0).agg(F.sum("annual_income")).first()[0]
    print(f"\nReproducible ML: version 0 income sum is stable: {snap_a} == {snap_b}")

    # The bad commit (v3) zeroed all incomes; roll back to v2 to undo it.
    bad_sum = query_by_version(spark, 3).agg(F.sum("annual_income")).first()[0]
    print(f"Version 3 (bad) income sum = {bad_sum} (wiped)")
    rollback_to(spark, 2)

    verify(spark, snap_a)


def verify(spark: SparkSession, v0_income_sum: int) -> None:
    """Assert versioning, time travel and rollback all behave correctly."""
    history = spark.sql(f"DESCRIBE HISTORY delta.`{CUSTOMERS}`")

    # 1. History records every commit (v0..v3 plus the RESTORE) = at least 5.
    versions = history.count()
    assert versions >= 5, f"expected >= 5 versions, found {versions}"

    # 2. Time travel to v0 returns the original data (customer 2 still present,
    #    incomes intact).
    v0 = query_by_version(spark, 0)
    assert v0.filter(F.col("customer_id") == 2).count() == 1, "v0 should still have customer 2"

    # 3. After RESTORE to v2, current state equals v2: customer 2 deleted,
    #    incomes NOT wiped (the bad v3 was undone).
    current = spark.read.format("delta").load(CUSTOMERS)
    assert current.filter(F.col("customer_id") == 2).count() == 0, "customer 2 should stay deleted"
    zeroed = current.filter(F.col("annual_income") == 0).count()
    v2_zeroed = query_by_version(spark, 2).filter(F.col("annual_income") == 0).count()
    assert zeroed == v2_zeroed, "current incomes should match v2 (bad update undone)"

    # 4. The restored table's income sum matches v2, and differs from the bad v3.
    current_sum = current.agg(F.sum("annual_income")).first()[0]
    v2_sum = query_by_version(spark, 2).agg(F.sum("annual_income")).first()[0]
    assert current_sum == v2_sum, "restored sum must equal v2 sum"

    print(
        f"VERIFIED: {versions} versions tracked; time travel to v0 returns "
        f"original data (income sum {v0_income_sum}); rollback to v2 undid the "
        f"bad commit (restored income sum {current_sum}, not 0)"
    )


# --- Command-line interface (the user-facing interface) --------------------


def cli(spark: SparkSession, args: list[str]) -> None:
    """A small CLI over the versioned table: history / show / restore."""
    cmd = args[0]
    if cmd == "history":
        show_history(spark)
    elif cmd == "show":
        version = int(args[args.index("--version") + 1])
        print(f"Customers at version {version}:")
        query_by_version(spark, version).orderBy("customer_id").show(5, truncate=False)
    elif cmd == "restore":
        version = int(args[args.index("--version") + 1])
        rollback_to(spark, version)
        show_history(spark)
    else:
        print(f"Unknown command: {cmd}. Use: history | show --version N | restore --version N")


def main() -> None:
    args = sys.argv[1:]
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    if args:
        # CLI mode operates on the existing table (no reset).
        cli(spark, args)
    else:
        # Full demo mode: rebuild from scratch and verify.
        s3 = s3_client()
        reset_lake(s3)
        demo(spark)

    spark.stop()


if __name__ == "__main__":
    main()
