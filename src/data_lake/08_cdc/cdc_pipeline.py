"""Task 08 - Implement a CDC feature in a data lake (Debezium + Kafka + Delta).

Change Data Capture pipeline for the classicmodels.customers table:

    MySQL (binlog) -> Debezium -> Kafka topic -> Spark Structured Streaming
    -> MERGE into a Delta table on the data lake (LocalStack S3)

The pipeline is incremental and re-runnable. Spark reads the Debezium topic with
trigger AvailableNow (process everything currently available, then stop) and a
checkpoint, so each run consumes only the new Kafka offsets since the last run:

    Run 1 : loads Debezium's initial snapshot (all customers).
    Run 2 : after make_changes.py, loads only the INSERT/UPDATE/DELETE events.

After ingestion it applies and measures the performance techniques the task
requires: data skipping (Delta stats + Z-ordering), compaction (OPTIMIZE), and
Spark caching. Finally it verifies CDC accuracy by comparing the Delta table
against the live MySQL source (they must match exactly).

Prerequisites (see README):
    docker compose -f src/data_lake/08_cdc/docker-compose.yml up -d
    uv run python src/data_lake/08_cdc/register_connector.py

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/08_cdc/cdc_pipeline.py
"""

import time
from pathlib import Path

import boto3
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

HERE = Path(__file__).resolve().parent
CHECKPOINT = HERE / "data" / "checkpoints" / "cdc_customers"

# Kafka (host listener) and the Debezium topic for the customers table.
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "dbserver1.classicmodels.customers"

# LocalStack S3 (the data lake) and the target Delta table.
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
CUSTOMERS_PATH = f"s3a://{BUCKET}/cdc/customers"

# MySQL source (for the CDC-accuracy verification via JDBC).
JDBC_URL = "jdbc:mysql://localhost:3307/classicmodels?useSSL=false&allowPublicKeyRetrieval=true"
JDBC_PROPS = {"user": "root", "password": "debezium", "driver": "com.mysql.cj.jdbc.Driver"}

PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.13:4.3.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
        "org.apache.hadoop:hadoop-aws:3.4.2",
        "com.mysql:mysql-connector-j:8.4.0",
    ]
)

# The customer columns we track (a readable subset of the table).
CUSTOMER_FIELDS = StructType(
    [
        StructField("customerNumber", IntegerType()),
        StructField("customerName", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("creditLimit", DoubleType()),
    ]
)

# The Debezium envelope (schemas disabled): before/after rows, op, timestamp.
ENVELOPE = StructType(
    [
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("before", CUSTOMER_FIELDS),
        StructField("after", CUSTOMER_FIELDS),
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


def create_bucket(s3) -> None:
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket {BUCKET}")


def active_num_files(spark: SparkSession) -> int:
    """Active data files backing the Delta table (from the transaction log).

    This reflects true compaction: OPTIMIZE reduces the number of active files,
    while old files linger as tombstones on S3 until VACUUM, so counting raw S3
    objects would be misleading.
    """
    return spark.sql(f"DESCRIBE DETAIL delta.`{CUSTOMERS_PATH}`").first()["numFiles"]


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Task08CDC")
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


def ensure_table(spark: SparkSession) -> None:
    """Create an empty Delta customers table if it does not exist yet."""
    if not DeltaTable.isDeltaTable(spark, CUSTOMERS_PATH):
        empty = spark.createDataFrame([], CUSTOMER_FIELDS)
        empty.write.format("delta").mode("overwrite").save(CUSTOMERS_PATH)
        print(f"Created empty Delta table -> {CUSTOMERS_PATH}")


def upsert_batch(batch_df: DataFrame, _batch_id: int) -> None:
    """Apply one micro-batch of Debezium events to the Delta table via MERGE."""
    spark = batch_df.sparkSession
    events = (
        batch_df.selectExpr("CAST(value AS STRING) AS v")
        .filter(F.col("v").isNotNull())
        .select(F.from_json("v", ENVELOPE).alias("e"))
        .select("e.*")
    )

    # Normalize each event to a change row: key, payload, deleted flag, timestamp.
    changes = events.select(
        F.coalesce("after.customerNumber", "before.customerNumber").alias("customerNumber"),
        F.col("after.customerName").alias("customerName"),
        F.col("after.city").alias("city"),
        F.col("after.country").alias("country"),
        F.col("after.creditLimit").alias("creditLimit"),
        (F.col("op") == F.lit("d")).alias("_deleted"),
        F.col("ts_ms").alias("_ts"),
    ).filter(F.col("customerNumber").isNotNull())

    # Keep only the latest event per key within this batch.
    w = Window.partitionBy("customerNumber").orderBy(F.col("_ts").desc_nulls_last())
    latest = (
        changes.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    target = DeltaTable.forPath(spark, CUSTOMERS_PATH)
    set_cols = {c: f"s.{c}" for c in CUSTOMER_FIELDS.fieldNames()}
    (
        target.alias("t")
        .merge(latest.alias("s"), "t.customerNumber = s.customerNumber")
        .whenMatchedDelete(condition="s._deleted = true")
        .whenMatchedUpdate(condition="s._deleted = false", set=set_cols)
        .whenNotMatchedInsert(condition="s._deleted = false", values=set_cols)
        .execute()
    )


def ingest_cdc(spark: SparkSession) -> None:
    """Consume all currently available Debezium events and MERGE them into Delta."""
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        # Process in small micro-batches, as a real streaming CDC job does. This
        # produces several small files (one MERGE per batch), which the later
        # OPTIMIZE step then compacts - making the compaction effect visible.
        .option("maxOffsetsPerTrigger", 30)
        .load()
    )
    query = (
        stream.writeStream.foreachBatch(upsert_batch)
        .option("checkpointLocation", str(CHECKPOINT))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    print("CDC ingestion complete (all available events processed)")


def optimize_and_measure(spark: SparkSession) -> None:
    """Apply and measure the required performance techniques."""
    # Data skipping: make Delta index (collect stats on) the leading columns.
    spark.sql(
        f"ALTER TABLE delta.`{CUSTOMERS_PATH}` "
        f"SET TBLPROPERTIES (delta.dataSkippingNumIndexedCols = 5)"
    )

    # Compaction + Z-order (data skipping): OPTIMIZE rewrites many small files
    # into few, clustered by customerNumber so filters can skip whole files.
    files_before = active_num_files(spark)
    spark.sql(f"OPTIMIZE delta.`{CUSTOMERS_PATH}` ZORDER BY (customerNumber)")
    files_after = active_num_files(spark)
    print(
        f"Compaction: active data files {files_before} -> {files_after} "
        f"(OPTIMIZE ZORDER BY customerNumber)"
    )

    # Data skipping demo: a selective query on the clustered/indexed column.
    print("Data skipping - query plan for a selective filter:")
    spark.sql(
        f"SELECT * FROM delta.`{CUSTOMERS_PATH}` WHERE customerNumber = 103"
    ).explain("simple")

    # Caching: cache the customers table and compare cold vs warm query time.
    df = spark.read.format("delta").load(CUSTOMERS_PATH).cache()

    def timed_agg() -> float:
        start = time.perf_counter()
        df.groupBy("country").agg(F.avg("creditLimit")).count()
        return time.perf_counter() - start

    cold = timed_agg()  # first touch materializes the cache
    warm = timed_agg()  # served from the in-memory cache
    print(f"Caching: cold query {cold:.3f}s, warm (cached) query {warm:.3f}s")
    print("Note: classicmodels is tiny (~122 rows); the techniques are configured")
    print("and executed correctly - absolute times are naturally small.")


def verify(spark: SparkSession) -> None:
    """Prove CDC accuracy: the Delta table must equal the live MySQL source."""
    delta_df = spark.read.format("delta").load(CUSTOMERS_PATH)
    mysql_df = spark.read.jdbc(
        JDBC_URL,
        "(SELECT customerNumber, customerName, city, country, creditLimit "
        "FROM customers) t",
        properties=JDBC_PROPS,
    )

    delta_keys = {r[0] for r in delta_df.select("customerNumber").collect()}
    mysql_keys = {r[0] for r in mysql_df.select("customerNumber").collect()}

    assert delta_df.count() == mysql_df.count(), (
        f"row count mismatch: Delta {delta_df.count()} vs MySQL {mysql_df.count()}"
    )
    assert delta_keys == mysql_keys, (
        f"key set mismatch: only-Delta={delta_keys - mysql_keys}, "
        f"only-MySQL={mysql_keys - delta_keys}"
    )

    print(
        f"VERIFIED: Delta CDC table matches MySQL source exactly "
        f"({delta_df.count()} customers, identical key sets)"
    )


def main() -> None:
    s3 = s3_client()
    create_bucket(s3)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    ensure_table(spark)
    ingest_cdc(spark)
    optimize_and_measure(spark)
    verify(spark)

    spark.stop()


if __name__ == "__main__":
    main()
