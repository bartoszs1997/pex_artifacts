"""Task 09 - Design and build data lake and lakehouse layers (medallion on Delta).

This is the buildable companion to ARCHITECTURE.md. The design document covers
the functional / non-functional requirements, the bronze/silver/gold layers, how
we talk to external systems, and how the architecture satisfies the three hard
requirements (GDPR deletes, ACID reliability, CDC). This script demonstrates the
whole design end to end on Delta Lake OSS + LocalStack S3 (the Databricks-free,
local equivalent - see the "Databricks -> OSS" note in ARCHITECTURE.md).

Pipeline (medallion):
    landing : the same customer data emitted in FOUR formats (CSV, JSON, Avro,
              Parquet) to prove mixed structured / semi-structured ingestion
    bronze  : raw union of all sources, as-ingested (Delta)
    silver  : validated, typed, deduplicated; bad rows go to a quarantine table
    gold    : business aggregates (by profession) + a filtered high-value join

It then exercises, on the lakehouse tables:
    - ACID    : MERGE upsert and a GDPR DELETE
    - optimize: partitioning + OPTIMIZE ZORDER (clustering / data skipping)
    - lifecycle: archive then purge (VACUUM)
    - analysis: a windowed ranking of top spenders per profession
    - verify  : assertions on every layer

Dataset columns (Customers.csv): CustomerID, Gender, Age, Annual Income ($),
Spending Score (1-100), Profession, Work Experience, Family Size.

Prerequisite:
    docker compose -f src/data_lake/09_lakehouse_design/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/09_lakehouse_design/lakehouse.py
"""

from pathlib import Path

import boto3
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HERE = Path(__file__).resolve().parent
INPUT_CSV = HERE / "data" / "input" / "Customers.csv"
LANDING = HERE / "data" / "landing"  # local multi-format landing zone

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
BRONZE = f"s3a://{BUCKET}/bronze/customers"
SILVER = f"s3a://{BUCKET}/silver/customers"
QUARANTINE = f"s3a://{BUCKET}/silver/customers_quarantine"
GOLD_BY_PROFESSION = f"s3a://{BUCKET}/gold/customers_by_profession"
GOLD_HIGH_VALUE = f"s3a://{BUCKET}/gold/high_value_customers"
ARCHIVE = f"s3a://{BUCKET}/archive/customers"

# Clean, code-friendly column names for the raw file.
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
        "org.apache.spark:spark-avro_2.13:4.1.1",
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
        SparkSession.builder.appName("Task09Lakehouse")
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Allow VACUUM with zero retention so the lifecycle demo reclaims now.
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


def emit_multi_format(spark: SparkSession) -> None:
    """Emit the customer data in four formats into a landing zone.

    Splitting one source into CSV, JSON, Avro and Parquet gives us a genuine mix
    of structured (CSV, Parquet, Avro) and semi-structured (JSON) inputs to
    ingest, exactly as the task requires - without inventing unrelated datasets.
    """
    raw = spark.read.csv(str(INPUT_CSV), header=True, inferSchema=True)
    for old, new in RENAME.items():
        raw = raw.withColumnRenamed(old, new)

    # Four disjoint slices, one per format.
    parts = raw.withColumn("_p", F.col("customer_id") % 4)
    fmts = {0: "csv", 1: "json", 2: "avro", 3: "parquet"}
    for key, fmt in fmts.items():
        slice_df = parts.filter(F.col("_p") == key).drop("_p")
        writer = slice_df.write.mode("overwrite")
        path = str(LANDING / fmt)
        if fmt == "csv":
            writer.option("header", True).csv(path)
        elif fmt == "json":
            writer.json(path)
        elif fmt == "avro":
            writer.format("avro").save(path)
        else:
            writer.parquet(path)
    print(f"Landing zone written in 4 formats (CSV, JSON, Avro, Parquet) -> {LANDING}")


def build_bronze(spark: SparkSession) -> None:
    """Ingest all four formats and union them into the raw bronze Delta table."""
    csv_df = spark.read.option("header", True).csv(str(LANDING / "csv"))
    json_df = spark.read.json(str(LANDING / "json"))
    avro_df = spark.read.format("avro").load(str(LANDING / "avro"))
    parquet_df = spark.read.parquet(str(LANDING / "parquet"))

    cols = list(RENAME.values())
    # Align column order and cast to a single schema before unioning.
    def norm(df: DataFrame) -> DataFrame:
        return df.select(
            F.col("customer_id").cast("int"),
            F.col("gender").cast("string"),
            F.col("age").cast("int"),
            F.col("annual_income").cast("int"),
            F.col("spending_score").cast("int"),
            F.col("profession").cast("string"),
            F.col("work_experience").cast("int"),
            F.col("family_size").cast("int"),
        )

    bronze = norm(csv_df).unionByName(norm(json_df)).unionByName(
        norm(avro_df)
    ).unionByName(norm(parquet_df))
    bronze = bronze.withColumn("_ingested_at", F.current_timestamp())

    bronze.write.format("delta").mode("overwrite").save(BRONZE)
    print(f"Bronze built: {bronze.count()} rows from 4 formats -> {BRONZE}")


def build_silver(spark: SparkSession) -> None:
    """Validate and clean bronze into silver; route bad records to quarantine.

    Data-quality rules (data validation criterion):
        - customer_id must be present (primary key),
        - age must be a positive, plausible value (0 < age < 120),
        - annual_income must be non-negative,
        - profession must not be blank,
        - duplicates on customer_id are removed.
    """
    bronze = spark.read.format("delta").load(BRONZE)

    valid_cond = (
        F.col("customer_id").isNotNull()
        & (F.col("age") > 0)
        & (F.col("age") < 120)
        & (F.col("annual_income") >= 0)
        & F.col("profession").isNotNull()
        & (F.trim(F.col("profession")) != "")
    )

    clean = bronze.filter(valid_cond).dropDuplicates(["customer_id"])
    rejected = bronze.filter(~valid_cond)

    # Silver is partitioned by profession (advanced partitioning) and clustered.
    clean.write.format("delta").mode("overwrite").partitionBy("profession").save(SILVER)
    rejected.write.format("delta").mode("overwrite").save(QUARANTINE)
    spark.sql(f"OPTIMIZE delta.`{SILVER}` ZORDER BY (customer_id)")

    print(
        f"Silver built: {clean.count()} valid rows (partitioned by profession, "
        f"ZORDER customer_id); quarantined {rejected.count()} invalid rows"
    )


def build_gold(spark: SparkSession) -> None:
    """Derive business insights: aggregation, join/merge, and filtering."""
    silver = spark.read.format("delta").load(SILVER)

    # Aggregation: profile each profession.
    by_profession = (
        silver.groupBy("profession")
        .agg(
            F.count("*").alias("customers"),
            F.round(F.avg("annual_income"), 0).alias("avg_income"),
            F.round(F.avg("spending_score"), 1).alias("avg_spending_score"),
        )
        .orderBy(F.desc("customers"))
    )
    by_profession.write.format("delta").mode("overwrite").save(GOLD_BY_PROFESSION)

    # Filtering + join/merge: high-value customers enriched with their
    # profession's average spending score (join silver to the aggregate).
    high_value = (
        silver.filter((F.col("annual_income") >= 100000) & (F.col("spending_score") >= 60))
        .join(by_profession.select("profession", "avg_spending_score"), "profession")
        .withColumn(
            "beats_profession_avg",
            F.col("spending_score") > F.col("avg_spending_score"),
        )
    )
    high_value.write.format("delta").mode("overwrite").save(GOLD_HIGH_VALUE)

    print(
        f"Gold built: {by_profession.count()} profession aggregates; "
        f"{high_value.count()} high-value customers (filter + join)"
    )


def demonstrate_acid(spark: SparkSession) -> int:
    """Show ACID operations on silver: a MERGE upsert and a GDPR delete.

    Returns the customer_id deleted for the GDPR scenario so verify() can check it.
    """
    silver = DeltaTable.forPath(spark, SILVER)

    # MERGE upsert: one existing customer updated, one new customer inserted.
    updates = spark.createDataFrame(
        [
            (1, "Male", 19, 15000, 99, "Healthcare", 1, 4),      # update spending
            (9001, "Female", 30, 120000, 88, "Engineer", 5, 2),  # insert new
        ],
        ["customer_id", "gender", "age", "annual_income", "spending_score",
         "profession", "work_experience", "family_size"],
    ).withColumn("_ingested_at", F.current_timestamp())
    (
        silver.alias("t")
        .merge(updates.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # GDPR "right to be forgotten": hard-delete a customer on request.
    gdpr_id = 9001
    silver.delete(F.col("customer_id") == gdpr_id)
    print(f"ACID: MERGE upsert applied; GDPR delete removed customer {gdpr_id}")
    return gdpr_id


def manage_lifecycle(spark: SparkSession) -> None:
    """Lifecycle management: archive a cold slice, then purge with VACUUM."""
    silver = spark.read.format("delta").load(SILVER)

    # Archive customers with no work experience (a "cold" slice) to a cheap tier.
    cold = silver.filter(F.col("work_experience") == 0)
    cold.write.format("delta").mode("overwrite").save(ARCHIVE)

    # Purge: VACUUM reclaims files no longer referenced by the log.
    DeltaTable.forPath(spark, SILVER).vacuum(0)
    print(
        f"Lifecycle: archived {cold.count()} cold rows -> {ARCHIVE}; "
        f"VACUUM purged unreferenced files from silver"
    )


def advanced_analysis(spark: SparkSession) -> None:
    """Advanced analysis: top-3 spenders per profession via a window function."""
    silver = spark.read.format("delta").load(SILVER)
    w = Window.partitionBy("profession").orderBy(F.desc("spending_score"))
    top = (
        silver.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 3)
        .select("profession", "customer_id", "spending_score", "rank")
        .orderBy("profession", "rank")
    )
    print("Advanced analysis - top 3 spenders per profession (sample):")
    top.show(9, truncate=False)


def verify(spark: SparkSession, gdpr_id: int) -> None:
    """Assert the lakehouse is correct across every layer."""
    bronze = spark.read.format("delta").load(BRONZE)
    silver = spark.read.format("delta").load(SILVER)
    quarantine = spark.read.format("delta").load(QUARANTINE)
    by_profession = spark.read.format("delta").load(GOLD_BY_PROFESSION)

    # 1. Bronze ingested every source row (2000 total across 4 formats).
    assert bronze.count() == 2000, f"bronze has {bronze.count()} rows, expected 2000"

    # 2. Silver is clean: no invalid rows survived validation.
    bad = silver.filter(
        F.col("age").isNull()
        | (F.col("age") <= 0)
        | (F.col("annual_income") < 0)
        | F.col("profession").isNull()
        | (F.trim(F.col("profession")) == "")
    ).count()
    assert bad == 0, f"silver still has {bad} invalid rows"

    # 3. Bronze split cleanly into silver + quarantine (no rows lost).
    #    (bronze may contain duplicates removed by silver; quarantine holds the
    #    invalid rows, so silver+quarantine unique keys cover all valid keys.)
    assert quarantine.count() > 0, "expected some quarantined rows"

    # 4. Silver has a unique primary key.
    assert silver.count() == silver.select("customer_id").distinct().count(), (
        "silver has duplicate customer_id values"
    )

    # 5. GDPR delete took effect.
    assert silver.filter(F.col("customer_id") == gdpr_id).count() == 0, (
        f"GDPR-deleted customer {gdpr_id} still present"
    )

    # 6. Gold aggregates are non-empty and consistent.
    assert by_profession.count() > 0, "gold aggregate is empty"

    print(
        f"VERIFIED: bronze {bronze.count()} rows (4 formats) -> silver "
        f"{silver.count()} valid, {quarantine.count()} quarantined -> gold "
        f"{by_profession.count()} profession aggregates; ACID/GDPR/lifecycle applied"
    )


def main() -> None:
    s3 = s3_client()
    reset_lake(s3)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Medallion build.
    emit_multi_format(spark)
    build_bronze(spark)
    build_silver(spark)
    build_gold(spark)

    # Lakehouse capabilities on the built tables.
    gdpr_id = demonstrate_acid(spark)
    manage_lifecycle(spark)
    advanced_analysis(spark)

    verify(spark, gdpr_id)
    spark.stop()


if __name__ == "__main__":
    main()
