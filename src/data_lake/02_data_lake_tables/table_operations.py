"""Task 02 - Work with tables in a data lake (Parquet, CSV, Avro + CRUD + merge).

The data lake is imitated with LocalStack S3 (a local, free stand-in for AWS S3).
All tables live under s3a://pex-datalake/tables/. The source table is the
Powerlifting Database (openpowerlifting.csv). To keep local Spark fast, we only
use the meets with a small MeetID (MeetID <= 4, ~300 rows) as the source rows.

This script does, in order, exactly the eight steps the task requires:

    1. Create a Parquet table user_data (meet_id, user_name, user_age).
    2. Insert data into user_data from the source table.
    3. Query the average WeightClassKg from the CSV (source) table.
    4. Update user_data by adding the column division.
    5. Delete the rows where meet_id == 2.
    6. Create a CSV table characteristic_data (equipment, devision, best_squad_kg).
    7. Merge user_data and characteristic_data into a new table merged_data (Avro).
    8. Verify merged_data holds the combined data from both tables, no duplicates.

Column mapping (source openpowerlifting.csv -> our tables):
    MeetID       -> meet_id
    Name         -> user_name
    Age          -> user_age
    Division     -> division / devision
    Equipment    -> equipment
    BestSquatKg  -> best_squad_kg
    WeightClassKg-> used only for the average query

Prerequisite:
    docker compose -f src/data_lake/02_data_lake_tables/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/02_data_lake_tables/table_operations.py
"""

from pathlib import Path

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

INPUT = Path(__file__).resolve().parent / "data" / "input"
SOURCE_CSV = INPUT / "openpowerlifting.csv"

# Keep the source small and bounded so local Spark stays fast. These meets
# include meet_id == 2, which step 5 deletes.
SAMPLE_MAX_MEET_ID = 4

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
USER_DATA = f"s3a://{BUCKET}/tables/user_data"            # Parquet
CHARACTERISTIC_DATA = f"s3a://{BUCKET}/tables/characteristic_data"  # CSV
MERGED_DATA = f"s3a://{BUCKET}/tables/merged_data"        # Avro

# hadoop-aws gives Spark the s3a:// filesystem; spark-avro adds the Avro format.
# Both are pulled from Maven on first run.
PACKAGES = ",".join(
    [
        "org.apache.hadoop:hadoop-aws:3.4.2",
        "org.apache.spark:spark-avro_2.13:4.1.1",
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
    """Create a Spark session that can read/write LocalStack S3 and Avro."""
    return (
        SparkSession.builder.appName("Task02Tables")
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


def read_source_csv(spark: SparkSession) -> DataFrame:
    """Read the raw powerlifting CSV table (this is 'the CSV table')."""
    return spark.read.csv(str(SOURCE_CSV), header=True, inferSchema=True)


def build_source_sample(csv_df: DataFrame) -> DataFrame:
    """Take a small, bounded slice of the source table (a few meets)."""
    return csv_df.filter(F.col("MeetID") <= SAMPLE_MAX_MEET_ID)


# --- Step 1: create the Parquet table user_data (empty, correct schema) -------
def create_user_data_table(spark: SparkSession) -> None:
    """Create an empty Parquet table with columns meet_id, user_name, user_age."""
    empty = spark.createDataFrame(
        [],
        schema="meet_id INT, user_name STRING, user_age INT",
    )
    empty.write.mode("overwrite").parquet(USER_DATA)
    print(f"Step 1: created empty Parquet table user_data -> {USER_DATA}")


# --- Step 2: insert data into user_data from the source table -----------------
def insert_source_into_user_data(source: DataFrame) -> None:
    """Insert (append) rows from the source table into user_data."""
    rows = source.select(
        F.col("MeetID").cast("int").alias("meet_id"),
        F.col("Name").alias("user_name"),
        F.col("Age").cast("int").alias("user_age"),
    )
    rows.write.mode("append").parquet(USER_DATA)
    print(f"Step 2: inserted {rows.count()} rows into user_data from the source")


# --- Step 3: average WeightClassKg from the CSV table -------------------------
def average_weight_class(csv_df: DataFrame) -> float:
    """Query the average WeightClassKg over the whole CSV table.

    Some rows use open-ended classes like '140+', so the column is text. We
    try_cast to double (bad values become NULL) and average the numeric ones.
    """
    weight = F.expr("try_cast(WeightClassKg as double)")
    avg = csv_df.select(F.avg(weight).alias("avg_weight_class")).first()[0]
    print(f"Step 3: average WeightClassKg on the CSV table = {avg:.4f}")
    return avg


# --- Step 4: update user_data by adding the column division -------------------
def add_division_column(spark: SparkSession, source: DataFrame) -> None:
    """Add a division column to user_data, taken from the source table."""
    # One division per person (a person can appear in several rows); pick one so
    # the join stays one-to-one and does not multiply user_data rows.
    division_lookup = source.groupBy(
        F.col("MeetID").cast("int").alias("meet_id"),
        F.col("Name").alias("user_name"),
        F.col("Age").cast("int").alias("user_age"),
    ).agg(F.first("Division").alias("division"))

    user_data = spark.read.parquet(USER_DATA)
    updated = user_data.join(
        division_lookup, on=["meet_id", "user_name", "user_age"], how="left"
    ).dropDuplicates(["meet_id", "user_name", "user_age"])

    updated.write.mode("overwrite").parquet(USER_DATA)
    print("Step 4: updated user_data, added column division")


# --- Step 5: delete the rows where meet_id == 2 ------------------------------
def delete_meet_id(spark: SparkSession, meet_id: int) -> None:
    """Delete rows from user_data where meet_id equals the given value."""
    user_data = spark.read.parquet(USER_DATA)
    kept = user_data.filter(F.col("meet_id") != meet_id)
    # Read fully into memory first: we overwrite the same path we are reading.
    kept = spark.createDataFrame(kept.collect(), kept.schema)
    kept.write.mode("overwrite").parquet(USER_DATA)
    print(f"Step 5: deleted rows where meet_id == {meet_id}")


# --- Step 6: create the CSV table characteristic_data ------------------------
def create_characteristic_data(source: DataFrame) -> None:
    """Create a CSV table with columns equipment, devision, best_squad_kg.

    One row per division (devision), so the later merge stays clean and
    duplicate-free.
    """
    characteristic = source.groupBy(F.col("Division").alias("devision")).agg(
        F.first("Equipment").alias("equipment"),
        F.first("BestSquatKg").cast("double").alias("best_squad_kg"),
    ).select("equipment", "devision", "best_squad_kg")

    characteristic.write.mode("overwrite").option("header", True).csv(CHARACTERISTIC_DATA)
    print(f"Step 6: created CSV table characteristic_data -> {CHARACTERISTIC_DATA}")


# --- Step 7: merge user_data and characteristic_data into merged_data --------
def merge_tables(spark: SparkSession) -> None:
    """Merge user_data (Parquet) and characteristic_data (CSV) into Avro."""
    user_data = spark.read.parquet(USER_DATA)
    characteristic = spark.read.csv(
        CHARACTERISTIC_DATA, header=True, inferSchema=True
    )

    merged = user_data.join(
        characteristic,
        user_data["division"] == characteristic["devision"],
        how="left",
    ).dropDuplicates()

    merged.write.format("avro").mode("overwrite").save(MERGED_DATA)
    print(f"Step 7: merged into Avro table merged_data -> {MERGED_DATA}")


# --- Step 8: verify merged_data -----------------------------------------------
def verify_merged(spark: SparkSession) -> None:
    """Verify merged_data has both tables' columns and no duplicate rows."""
    merged = spark.read.format("avro").load(MERGED_DATA)

    total = merged.count()
    distinct = merged.dropDuplicates().count()

    user_cols = {"meet_id", "user_name", "user_age", "division"}
    char_cols = {"equipment", "devision", "best_squad_kg"}
    have = set(merged.columns)

    assert user_cols.issubset(have), f"missing user_data columns: {user_cols - have}"
    assert char_cols.issubset(have), f"missing characteristic_data columns: {char_cols - have}"
    assert total == distinct, f"duplicates found: {total} rows, {distinct} distinct"

    merged.show(5, truncate=False)
    print(
        f"VERIFIED: merged_data has {total} rows, no duplicates, "
        f"and columns from both tables"
    )


def main() -> None:
    create_bucket()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    csv_df = read_source_csv(spark)
    source = build_source_sample(csv_df)

    create_user_data_table(spark)          # 1
    insert_source_into_user_data(source)   # 2
    average_weight_class(csv_df)           # 3
    add_division_column(spark, source)     # 4
    delete_meet_id(spark, meet_id=2)       # 5
    create_characteristic_data(source)     # 6
    merge_tables(spark)                    # 7
    verify_merged(spark)                   # 8

    spark.stop()


if __name__ == "__main__":
    main()
