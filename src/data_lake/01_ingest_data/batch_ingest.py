"""Task 01 - BATCH ingestion into the bronze layer.

The data lake is imitated with LocalStack S3 (a local, free stand-in for AWS S3).
The bronze layer lives at s3a://pex-datalake/bronze/. This script reads the DL
Course Data in BOTH required formats and writes each one there as Parquet,
keeping the original columns (structure and integrity preserved).

    data/input/housing.csv   ->  s3a://pex-datalake/bronze/housing_csv   (Parquet)
    data/input/housing.json  ->  s3a://pex-datalake/bronze/housing_json  (Parquet)

At the end it reads both bronze tables back and checks the row counts, so we can
see the data was really stored in the lake.

Prerequisite:
    docker compose -f src/data_lake/01_ingest_data/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/01_ingest_data/batch_ingest.py
"""

from pathlib import Path

import boto3
from pyspark.sql import DataFrame, SparkSession

INPUT = Path(__file__).resolve().parent / "data" / "input"

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
BRONZE_CSV = f"s3a://{BUCKET}/bronze/housing_csv"
BRONZE_JSON = f"s3a://{BUCKET}/bronze/housing_json"

# hadoop-aws gives Spark the s3a:// filesystem; pulled from Maven on first run.
PACKAGES = "org.apache.hadoop:hadoop-aws:3.4.2"


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


def create_spark_session() -> SparkSession:
    """Create a Spark session that can write to LocalStack S3 via s3a://."""
    return (
        SparkSession.builder.appName("Task01BatchIngest")
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
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


def read_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Read a CSV file with a header and inferred column types."""
    return spark.read.csv(str(path), header=True, inferSchema=True)


def read_json(spark: SparkSession, path: Path) -> DataFrame:
    """Read a newline-delimited JSON file (one JSON object per line)."""
    return spark.read.json(str(path))


def write_bronze(df: DataFrame, target: str) -> None:
    """Write a DataFrame to the bronze layer as Parquet (overwrite)."""
    df.write.mode("overwrite").parquet(target)


def main() -> None:
    create_bucket()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # --- CSV -> bronze ---
    csv_df = read_csv(spark, INPUT / "housing.csv")
    csv_count = csv_df.count()
    write_bronze(csv_df, BRONZE_CSV)
    print(f"CSV : read {csv_count} rows -> {BRONZE_CSV}")

    # --- JSON -> bronze ---
    json_df = read_json(spark, INPUT / "housing.json")
    json_count = json_df.count()
    write_bronze(json_df, BRONZE_JSON)
    print(f"JSON: read {json_count} rows -> {BRONZE_JSON}")

    # --- Verify: read both back from the lake and check the counts match ---
    csv_back = spark.read.parquet(BRONZE_CSV).count()
    json_back = spark.read.parquet(BRONZE_JSON).count()
    assert csv_back == csv_count, "CSV row count mismatch"
    assert json_back == json_count, "JSON row count mismatch"
    print(f"VERIFIED: bronze has {csv_back} CSV rows and {json_back} JSON rows")

    spark.stop()


if __name__ == "__main__":
    main()
