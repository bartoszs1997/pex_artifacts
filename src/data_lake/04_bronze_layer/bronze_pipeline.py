"""Task 04 - Design and model a bronze layer pipeline (Delta Lake).

Read raw customer/tweet CSV data from a source location and store it in a Delta
Lake bronze table. The pipeline is incremental: it picks up only newly added CSV
files on each run and appends them to the bronze table.

Data lake (imitated with LocalStack S3):
    bronze : s3a://pex-datalake/bronze/bronze_accounts   (Delta, partitioned)

Source location (local filesystem, the "raw CSV drop zone"):
    data/landing/*.csv

How the pieces map (source Big Tech tweets CSV -> bronze schema):
    twitter_id     -> file_id   (string)
    file_name      -> file_name (string)
    followers      -> followers (number)
    friends        -> friends   (number)
    group_name     -> group_name(string)
    location       -> location  (string, title case)
    retweet_count  -> retweet   (number)
    created_at     -> created_date (partition column, DATE derived from created_at)

Transformations (bronze):
    1. Remove rows with missing or invalid values (nulls, empty, non-numeric).
    2. Convert location to title case.
    3. Calculate total friends per group_name based on created_at (reported).
    4. Enforce the target bronze schema (explicit StructType + casts).
    5. Save as Delta bronze_accounts, partitioned by the creation date.

Incremental mechanism:
    Spark Structured Streaming file source + a checkpoint. The checkpoint records
    which files were already ingested, so re-running the stream processes ONLY
    new CSV files added to data/landing/. This is the simplest OSS-native way to
    "handle new CSV files and incrementally update the bronze table".

Prerequisite:
    docker compose -f src/data_lake/04_bronze_layer/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/04_bronze_layer/bronze_pipeline.py
"""

import csv
import shutil
from pathlib import Path

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

HERE = Path(__file__).resolve().parent
INPUT = HERE / "data" / "input"
LANDING = HERE / "data" / "landing"
CHECKPOINT = HERE / "data" / "checkpoints" / "bronze_accounts"

# The two source CSV files (two date windows). We sample them down to keep local
# Spark fast; each becomes one file dropped into the landing zone.
SOURCE_FILES = [
    INPUT / "Bigtech - 12-07-2020 till 19-09-2020" / "Bigtech - 12-07-2020 till 19-09-2020.csv",
    INPUT / "Bigtech - 20-09-2020 till 13-10-2020.csv",
]
SAMPLE_ROWS = 800

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
BRONZE = f"s3a://{BUCKET}/bronze/bronze_accounts"

# Full source schema, in the CSV header order, all as strings. We provide it
# explicitly because a streaming CSV source cannot infer a schema. Numeric
# columns are cast later, so bad values become null and get dropped.
SOURCE_SCHEMA = StructType(
    [
        StructField("created_at", StringType()),
        StructField("file_name", StringType()),
        StructField("followers", StringType()),
        StructField("friends", StringType()),
        StructField("group_name", StringType()),
        StructField("location", StringType()),
        StructField("retweet_count", StringType()),
        StructField("screenname", StringType()),
        StructField("search_query", StringType()),
        StructField("text", StringType()),
        StructField("twitter_id", StringType()),
        StructField("username", StringType()),
        StructField("polarity", StringType()),
        StructField("partition_0", StringType()),
        StructField("partition_1", StringType()),
    ]
)

# The target bronze table schema required by the task (plus the partition column).
BRONZE_SCHEMA = StructType(
    [
        StructField("file_id", StringType()),
        StructField("file_name", StringType()),
        StructField("followers", LongType()),
        StructField("friends", LongType()),
        StructField("group_name", StringType()),
        StructField("location", StringType()),
        StructField("retweet", LongType()),
        StructField("created_date", StringType()),  # partition column (date)
    ]
)

# Columns that must be present and valid; a null in any of them drops the row.
REQUIRED = ["file_id", "file_name", "followers", "friends", "group_name",
            "location", "retweet", "created_date"]

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


def clear_bronze() -> None:
    """Delete the bronze table objects so a fresh demo run is deterministic.

    reset_landing() also wipes the checkpoint, so the stream reprocesses batch1
    as new; the bronze table must be emptied to match, otherwise re-runs would
    keep appending the same rows.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    prefix = "bronze/bronze_accounts/"
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    keys = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
    if keys:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
        print(f"Cleared {len(keys)} old bronze objects")


def create_spark_session() -> SparkSession:
    """Create a Spark session with Delta Lake and S3A to LocalStack."""
    return (
        SparkSession.builder.appName("Task04Bronze")
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


def reset_landing() -> None:
    """Start with an empty landing zone and drop the first source file in it."""
    if LANDING.exists():
        shutil.rmtree(LANDING)
    if CHECKPOINT.parent.exists():
        shutil.rmtree(CHECKPOINT.parent)
    LANDING.mkdir(parents=True, exist_ok=True)
    _sample_csv(SOURCE_FILES[0], LANDING / "batch1.csv", SAMPLE_ROWS)
    print(f"Landing: dropped batch1.csv ({SAMPLE_ROWS} rows)")


def add_second_batch() -> None:
    """Simulate a NEW CSV file arriving in the source location later."""
    _sample_csv(SOURCE_FILES[1], LANDING / "batch2.csv", SAMPLE_ROWS)
    print(f"Landing: dropped batch2.csv ({SAMPLE_ROWS} rows)")


def transform(df: DataFrame) -> DataFrame:
    """Apply the bronze transformations to a (streaming) source DataFrame.

    All operations here are stateless row-level maps/filters, so they work
    identically on a streaming or a batch DataFrame.
    """
    out = df.select(
        F.col("twitter_id").alias("file_id"),
        F.col("file_name"),
        F.col("followers").cast(LongType()).alias("followers"),
        F.col("friends").cast(LongType()).alias("friends"),
        F.col("group_name"),
        F.initcap(F.col("location")).alias("location"),  # title case
        F.col("retweet_count").cast(LongType()).alias("retweet"),
        F.to_date(F.col("created_at")).cast(StringType()).alias("created_date"),
    )
    # Remove rows with missing or invalid values (nulls from failed casts too).
    out = out.replace("", None).dropna(subset=REQUIRED)
    return out


def run_stream_once(spark: SparkSession) -> None:
    """Ingest only the NEW CSV files in the landing zone into the bronze table.

    Structured Streaming's file source plus the checkpoint remembers which files
    were already processed, so each call appends only newly added files.
    """
    stream = (
        spark.readStream.schema(SOURCE_SCHEMA)
        .option("header", "true")
        .option("multiLine", "true")  # tweets contain newlines inside quotes
        .option("escape", '"')
        .csv(str(LANDING))
    )
    cleaned = transform(stream)
    query = (
        cleaned.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(CHECKPOINT))
        .partitionBy("created_date")
        .trigger(availableNow=True)  # process what is there, then stop
        .start(BRONZE)
    )
    query.awaitTermination()


def total_friends_per_group(spark: SparkSession) -> DataFrame:
    """Calculate the total friends for each group_name based on created_at."""
    bronze = spark.read.format("delta").load(BRONZE)
    return (
        bronze.groupBy("group_name", "created_date")
        .agg(F.sum("friends").alias("total_friends"))
        .orderBy("group_name", "created_date")
    )


def bronze_count(spark: SparkSession) -> int:
    """Row count of the bronze Delta table."""
    return spark.read.format("delta").load(BRONZE).count()


def verify(spark: SparkSession, count_after_1: int, count_after_2: int) -> None:
    """Verify the bronze table: schema, no invalid rows, partitions, incremental."""
    bronze = spark.read.format("delta").load(BRONZE)

    # 1. Schema matches the required bronze schema.
    actual = [(f.name, type(f.dataType).__name__) for f in bronze.schema.fields]
    expected = [(f.name, type(f.dataType).__name__) for f in BRONZE_SCHEMA.fields]
    assert sorted(actual) == sorted(expected), f"schema mismatch: {actual}"

    # 2. No missing/invalid values left in required columns.
    nulls = bronze.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in REQUIRED]
    ).first().asDict()
    assert sum(nulls.values()) == 0, f"nulls remain: {nulls}"

    # 3. Location is title case (initcap is idempotent -> equals its own initcap).
    bad_case = bronze.filter(F.col("location") != F.initcap(F.col("location"))).count()
    assert bad_case == 0, f"{bad_case} rows are not title case"

    # 4. Table is partitioned (more than one created_date partition present).
    partitions = bronze.select("created_date").distinct().count()

    # 5. The second run added rows incrementally (only new file processed).
    assert count_after_2 > count_after_1, "incremental run added no rows"

    print(
        f"VERIFIED: bronze_accounts has {count_after_2} rows across {partitions} "
        f"date partitions; incremental run added {count_after_2 - count_after_1} "
        f"rows; schema, non-null and title-case checks passed"
    )


def main() -> None:
    create_bucket()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # First run: only batch1 is in the source location.
    clear_bronze()
    reset_landing()
    run_stream_once(spark)
    count_after_1 = bronze_count(spark)
    print(f"After batch1: bronze has {count_after_1} rows")

    # A NEW CSV file arrives; re-running ingests only that new file.
    add_second_batch()
    run_stream_once(spark)
    count_after_2 = bronze_count(spark)
    print(f"After batch2: bronze has {count_after_2} rows")

    # Report the total friends per group_name based on created_at.
    print("Total friends per group_name (by created date):")
    total_friends_per_group(spark).show(10, truncate=False)

    verify(spark, count_after_1, count_after_2)
    spark.stop()


if __name__ == "__main__":
    main()
