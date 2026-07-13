"""Task 05 - Silver and gold layer pipelines (Delta Lake).

Two pipelines over the e-commerce Customers dataset:

    1. Silver pipeline (ETL): read the raw CSV, clean and conform it, and store
       it as a Delta table customer_silver with an explicit schema.
    2. Gold pipeline: load customer_silver, narrow it down for customer
       segmentation (filter + aggregate), and store it as a Delta table
       customer_gold with its own schema.

Then we demonstrate Delta Lake features on customer_gold: UPDATE, INSERT (via
MERGE), DELETE, and schema evolution.

Data lake (imitated with LocalStack S3):
    silver : s3a://pex-datalake/silver/customer_silver   (Delta)
    gold   : s3a://pex-datalake/gold/customer_gold       (Delta)

Source columns (Customers.csv):
    CustomerID, Gender, Age, Annual Income ($), Spending Score (1-100),
    Profession, Work Experience, Family Size

Silver transformations (clean + conform):
    - Rename columns to snake_case, cast to the target types.
    - Drop invalid rows: age <= 0 or annual_income <= 0 (impossible values).
    - Impute missing profession with "Unknown".
    - Standardize gender to title case.

Gold (customer segmentation):
    - Derive age_group (Young/Adult/Senior) and spending_level (Low/Medium/High).
    - Aggregate per (age_group, spending_level): customer count, average income,
      average spending score, average family size.

Prerequisite:
    docker compose -f src/data_lake/05_silver_gold_layers/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/05_silver_gold_layers/silver_gold.py
"""

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

INPUT = Path(__file__).resolve().parent / "data" / "input"
SOURCE_CSV = INPUT / "Customers.csv"

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
SILVER = f"s3a://{BUCKET}/silver/customer_silver"
GOLD = f"s3a://{BUCKET}/gold/customer_gold"

# Explicit silver schema (cleaned and conformed).
SILVER_SCHEMA = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("gender", StringType()),
        StructField("age", IntegerType()),
        StructField("annual_income", IntegerType()),
        StructField("spending_score", IntegerType()),
        StructField("profession", StringType()),
        StructField("work_experience", IntegerType()),
        StructField("family_size", IntegerType()),
    ]
)

# Explicit gold schema (customer segmentation).
GOLD_SCHEMA = StructType(
    [
        StructField("age_group", StringType()),
        StructField("spending_level", StringType()),
        StructField("customer_count", LongType()),
        StructField("avg_income", DoubleType()),
        StructField("avg_spending_score", DoubleType()),
        StructField("avg_family_size", DoubleType()),
    ]
)

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


def create_spark_session() -> SparkSession:
    """Create a Spark session with Delta Lake and S3A to LocalStack."""
    return (
        SparkSession.builder.appName("Task05SilverGold")
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


# --- Silver pipeline (ETL: clean and conform) --------------------------------
def build_silver(spark: SparkSession) -> DataFrame:
    """Read the raw CSV and produce the cleaned, conformed silver DataFrame."""
    raw = spark.read.csv(str(SOURCE_CSV), header=True, inferSchema=True)

    conformed = raw.select(
        F.col("CustomerID").cast(LongType()).alias("customer_id"),
        F.initcap(F.trim(F.col("Gender"))).alias("gender"),  # standardize
        F.col("Age").cast(IntegerType()).alias("age"),
        F.col("Annual Income ($)").cast(IntegerType()).alias("annual_income"),
        F.col("Spending Score (1-100)").cast(IntegerType()).alias("spending_score"),
        F.col("Profession").alias("profession"),
        F.col("Work Experience").cast(IntegerType()).alias("work_experience"),
        F.col("Family Size").cast(IntegerType()).alias("family_size"),
    )

    # Handle missing values: profession -> "Unknown".
    conformed = conformed.fillna({"profession": "Unknown"})
    # Drop impossible values (invalid age or income).
    conformed = conformed.filter((F.col("age") > 0) & (F.col("annual_income") > 0))
    return conformed


def write_silver(df: DataFrame) -> None:
    """Load the transformed data into the customer_silver Delta table."""
    df.write.format("delta").mode("overwrite").save(SILVER)
    print(f"Silver: wrote {df.count()} rows -> {SILVER}")


# --- Gold pipeline (narrow for customer segmentation) ------------------------
def build_gold(spark: SparkSession) -> DataFrame:
    """Load silver and derive the gold segmentation table (filter + aggregate)."""
    silver = spark.read.format("delta").load(SILVER)

    # Filter: keep only working-age customers relevant for segmentation.
    segmented = silver.filter((F.col("age") >= 18) & (F.col("age") <= 70))

    # Derive segmentation dimensions.
    segmented = segmented.withColumn(
        "age_group",
        F.when(F.col("age") < 30, "Young")
        .when(F.col("age") < 50, "Adult")
        .otherwise("Senior"),
    ).withColumn(
        "spending_level",
        F.when(F.col("spending_score") < 34, "Low")
        .when(F.col("spending_score") < 67, "Medium")
        .otherwise("High"),
    )

    # Aggregate per segment.
    gold = (
        segmented.groupBy("age_group", "spending_level")
        .agg(
            F.count("*").alias("customer_count"),
            F.round(F.avg("annual_income"), 2).alias("avg_income"),
            F.round(F.avg("spending_score"), 2).alias("avg_spending_score"),
            F.round(F.avg("family_size"), 2).alias("avg_family_size"),
        )
        .orderBy("age_group", "spending_level")
    )
    return gold


def write_gold(df: DataFrame) -> None:
    """Create the customer_gold Delta table optimized for segmentation."""
    df.write.format("delta").mode("overwrite").save(GOLD)
    print(f"Gold: wrote {df.count()} segments -> {GOLD}")


# --- Delta Lake feature demonstration on customer_gold -----------------------
def demonstrate_delta_features(spark: SparkSession) -> None:
    """Demonstrate Delta features on customer_gold: UPDATE, INSERT, DELETE, evolve."""
    gold = DeltaTable.forPath(spark, GOLD)

    # UPDATE: bump one segment's customer_count (an example correction).
    gold.update(
        condition=(F.col("age_group") == "Young") & (F.col("spending_level") == "High"),
        set={"customer_count": F.col("customer_count") + 1},
    )
    print("Delta UPDATE: incremented Young/High customer_count by 1")

    # INSERT via MERGE: add a brand-new segment if it does not exist yet.
    new_segment = spark.createDataFrame(
        [("Unknown", "Unknown", 0, 0.0, 0.0, 0.0)], schema=GOLD_SCHEMA
    )
    (
        gold.alias("t")
        .merge(
            new_segment.alias("s"),
            "t.age_group = s.age_group AND t.spending_level = s.spending_level",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Delta MERGE INSERT: added Unknown/Unknown segment")

    # DELETE: remove the placeholder segment we just inserted (irrelevant record).
    gold.delete(
        (F.col("age_group") == "Unknown") & (F.col("spending_level") == "Unknown")
    )
    print("Delta DELETE: removed Unknown/Unknown segment")

    # SCHEMA EVOLUTION: add a new column via a write with mergeSchema=true.
    evolved = spark.read.format("delta").load(GOLD).withColumn(
        "high_value", F.col("avg_income") > 120000
    )
    evolved.write.format("delta").option("mergeSchema", "true").mode(
        "overwrite"
    ).save(GOLD)
    print("Delta SCHEMA EVOLUTION: added high_value column")


def verify(spark: SparkSession) -> None:
    """Verify both tables exist, have the right schema, and gold evolved."""
    silver = spark.read.format("delta").load(SILVER)
    gold = spark.read.format("delta").load(GOLD)

    # Silver schema matches the declared schema (order-insensitive).
    silver_actual = sorted((f.name, type(f.dataType).__name__) for f in silver.schema)
    silver_expected = sorted(
        (f.name, type(f.dataType).__name__) for f in SILVER_SCHEMA
    )
    assert silver_actual == silver_expected, f"silver schema mismatch: {silver_actual}"

    # Silver is clean: no invalid ages/incomes, no null professions.
    bad = silver.filter(
        (F.col("age") <= 0) | (F.col("annual_income") <= 0) | F.col("profession").isNull()
    ).count()
    assert bad == 0, f"silver still has {bad} invalid rows"

    # Gold evolved: the high_value column exists after schema evolution.
    assert "high_value" in gold.columns, "gold did not evolve (high_value missing)"

    print("Gold table (customer_gold) after Delta operations:")
    gold.orderBy("age_group", "spending_level").show(truncate=False)
    print(
        f"VERIFIED: customer_silver has {silver.count()} clean rows; "
        f"customer_gold has {gold.count()} segments with evolved schema "
        f"{gold.columns}"
    )


def main() -> None:
    create_bucket()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Silver pipeline: ETL to cleaned, conforming Delta table.
    silver_df = build_silver(spark)
    write_silver(silver_df)

    # Gold pipeline: narrow silver for customer segmentation.
    gold_df = build_gold(spark)
    write_gold(gold_df)

    # Demonstrate Delta Lake features on the gold table.
    demonstrate_delta_features(spark)

    # Verify both tables.
    verify(spark)
    spark.stop()


if __name__ == "__main__":
    main()
