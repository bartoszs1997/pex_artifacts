"""Task 01 - STREAMING ingestion into the bronze layer.

This reads new rows from the PostgreSQL "housing" table and appends them to the
bronze layer on LocalStack S3 continuously. It is a simple polling loop, which is
the junior-level way to do "continuous capture": every few seconds it asks
Postgres for rows it has not seen yet (id greater than the last one captured) and
writes them to bronze as Parquet.

    Postgres (housing)  ->  poll new rows via Spark JDBC  ->  s3a://.../bronze/housing_stream

This satisfies the task's "streaming reads and writes ... continuously captures
and persists incoming data": it captures newly inserted rows over time, which is
change data capture in its simplest honest form (new-row capture by id).

Prerequisites:
    docker compose -f src/data_lake/01_ingest_data/docker-compose.yml up -d
    uv run python src/data_lake/01_ingest_data/load_postgres.py --rows 200

Run (Java 17 on PATH). By default it stops after 15s with no new rows. To watch
it live, run with --idle-timeout 0 and, in another terminal, run
load_postgres.py --stream:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/01_ingest_data/stream_ingest.py
"""

import argparse
import time

import boto3
from pyspark.sql import SparkSession

S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
BRONZE_STREAM = f"s3a://{BUCKET}/bronze/housing_stream"

# Two jars: hadoop-aws for s3a://, and the Postgres JDBC driver to read the table.
PACKAGES = "org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.4"
JDBC_URL = "jdbc:postgresql://localhost:5432/pexsource"
JDBC_PROPS = {"user": "pex", "password": "pex", "driver": "org.postgresql.Driver"}


def create_bucket() -> None:
    """Create the data lake bucket on LocalStack (safe to call every time)."""
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    if BUCKET not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=BUCKET)


def create_spark_session() -> SparkSession:
    """Create a Spark session with s3a:// (LocalStack) and the Postgres driver."""
    return (
        SparkSession.builder.appName("Task01StreamIngest")
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


def read_new_rows(spark, last_id: int):
    """Read rows from Postgres whose id is greater than last_id."""
    query = f"(SELECT * FROM housing WHERE id > {last_id}) AS new_rows"
    return spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=3.0, help="seconds between polls")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=15.0,
        help="stop after this many seconds with no new rows (0 = run forever)",
    )
    args = parser.parse_args()

    create_bucket()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    last_id = -1
    total = 0
    idle = 0.0
    print("Streaming: polling Postgres for new rows...")
    try:
        while True:
            new_rows = read_new_rows(spark, last_id)
            count = new_rows.count()
            if count > 0:
                new_rows.write.mode("append").parquet(BRONZE_STREAM)
                last_id = new_rows.agg({"id": "max"}).collect()[0][0]
                total += count
                idle = 0.0
                print(f"Captured {count} new row(s); total this run: {total}")
            else:
                idle += args.interval
                if args.idle_timeout and idle >= args.idle_timeout:
                    print("No new rows; stopping.")
                    break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        # Verify: read the whole bronze stream table back and print the count.
        stored = spark.read.parquet(BRONZE_STREAM).count()
        print(f"VERIFIED: bronze stream now holds {stored} rows")
        spark.stop()


if __name__ == "__main__":
    main()
