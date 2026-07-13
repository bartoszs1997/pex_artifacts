"""Task 03 - Define data cleansing rules (bronze -> silver).

The data lake is imitated with LocalStack S3 (a local, free stand-in for AWS S3).
The medallion layers live under:

    bronze : s3a://pex-datalake/bronze/covid   (raw data, as ingested)
    silver : s3a://pex-datalake/silver/covid   (cleansed data)

The dataset is the US COVID-19 Cases and Deaths by State over Time. It has three
data quality issues that this task addresses:

    1. Missing values - many empty numeric fields (conf_cases, prob_cases, ...)
       and empty text fields (consent_cases, consent_deaths).
    2. Outliers      - new_death has impossible/extreme values (e.g. -1453),
       far outside the normal range.
    3. Duplicates    - records may repeat for the same (submission_date, state).

Cleansing rules (applied in this order, bronze -> silver):

    1. Impute missing values : numeric columns -> 0, text columns -> "default".
    2. Remove outliers       : drop rows where new_death is outside the IQR fence
                               [Q1 - 1.5*IQR, Q3 + 1.5*IQR].
    3. Deduplicate           : keep one record per (submission_date, state).

The script first loads the raw CSV into the bronze layer so there is a real
bronze table to read from, then runs the cleansing pipeline into silver, then
verifies the silver data is free of all three issues.

Prerequisite:
    docker compose -f src/data_lake/03_data_cleansing/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/03_data_cleansing/cleanse.py
"""

from pathlib import Path

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

INPUT = Path(__file__).resolve().parent / "data" / "input"
SOURCE_CSV = INPUT / "United_States_COVID-19_Cases_and_Deaths_by_State_over_Time.csv"

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
BRONZE = f"s3a://{BUCKET}/bronze/covid"
SILVER = f"s3a://{BUCKET}/silver/covid"

# The dataset columns, split by type so we can apply the two imputation rules.
NUMERIC_COLS = [
    "tot_cases", "conf_cases", "prob_cases", "new_case", "pnew_case",
    "tot_death", "conf_death", "prob_death", "new_death", "pnew_death",
]
TEXT_COLS = ["submission_date", "state", "created_at", "consent_cases", "consent_deaths"]

# The natural business key: one record per state per day.
KEY_COLS = ["submission_date", "state"]

# The measure we use to detect outliers.
OUTLIER_COL = "new_death"

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
    """Create a Spark session that can read/write LocalStack S3 via s3a://."""
    return (
        SparkSession.builder.appName("Task03Cleansing")
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


def load_bronze(spark: SparkSession) -> None:
    """Load the raw CSV into the bronze layer (as ingested, no cleansing)."""
    raw = spark.read.csv(str(SOURCE_CSV), header=True, inferSchema=True)
    raw.write.mode("overwrite").parquet(BRONZE)
    print(f"Bronze: wrote {raw.count()} raw rows -> {BRONZE}")


# --- Cleansing rule 1: impute missing values ---------------------------------
def impute_missing(df: DataFrame) -> DataFrame:
    """Replace missing numeric values with 0 and missing text values with 'default'."""
    df = df.fillna(0, subset=NUMERIC_COLS)
    df = df.fillna("default", subset=TEXT_COLS)
    return df


# --- Cleansing rule 2: remove outliers ---------------------------------------
def outlier_fence(df: DataFrame) -> tuple[float, float]:
    """Compute the IQR fence [Q1 - 1.5*IQR, Q3 + 1.5*IQR] on new_death (Tukey)."""
    q1, q3 = df.approxQuantile(OUTLIER_COL, [0.25, 0.75], 0.0)
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr


def remove_outliers(df: DataFrame, low: float, high: float) -> DataFrame:
    """Drop rows where new_death is outside the given IQR fence."""
    return df.filter((F.col(OUTLIER_COL) >= low) & (F.col(OUTLIER_COL) <= high))


# --- Cleansing rule 3: deduplicate -------------------------------------------
def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one record per (submission_date, state)."""
    return df.dropDuplicates(KEY_COLS)


def cleanse(df: DataFrame, low: float, high: float) -> DataFrame:
    """Apply all cleansing rules in order (this is the reusable transformation).

    The outlier fence is computed once on the incoming (bronze) data and passed
    in, so the removal and the later verification use the exact same bounds.
    """
    df = impute_missing(df)
    df = remove_outliers(df, low, high)
    df = deduplicate(df)
    return df


def verify_silver(spark: SparkSession, low: float, high: float) -> None:
    """Verify the silver data is free of the three identified issues.

    The outlier check uses the same fence [low, high] that removal used, so we
    truthfully confirm no row survived outside the applied bounds (we do not
    recompute the fence on the shrunken distribution).
    """
    silver = spark.read.parquet(SILVER)

    # 1. No missing values anywhere.
    null_counts = silver.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in silver.columns]
    ).first().asDict()
    total_nulls = sum(null_counts.values())

    # 2. No outliers left, measured against the fence that removal applied.
    outliers = silver.filter(
        (F.col(OUTLIER_COL) < low) | (F.col(OUTLIER_COL) > high)
    ).count()

    # 3. No duplicates on the natural key.
    total = silver.count()
    distinct = silver.dropDuplicates(KEY_COLS).count()

    assert total_nulls == 0, f"missing values remain: {null_counts}"
    assert outliers == 0, f"outliers remain outside [{low}, {high}]: {outliers}"
    assert total == distinct, f"duplicates remain: {total} rows, {distinct} distinct"
    print(
        f"VERIFIED: silver has {total} rows, {total_nulls} missing values, "
        f"{outliers} residual outliers, {total - distinct} duplicates"
    )


def main() -> None:
    create_bucket()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Bronze: raw data as ingested.
    load_bronze(spark)

    # Bronze -> silver: read raw, apply cleansing rules, write cleansed.
    bronze = spark.read.parquet(BRONZE)
    print(f"Read {bronze.count()} rows from bronze")

    # Compute the outlier fence once on the bronze data, reuse for verify.
    low, high = outlier_fence(bronze)
    print(f"Outlier fence on {OUTLIER_COL}: [{low}, {high}]")

    silver = cleanse(bronze, low, high)
    silver.write.mode("overwrite").parquet(SILVER)
    print(f"Silver: wrote {silver.count()} cleansed rows -> {SILVER}")

    # Verify the silver data is clean.
    verify_silver(spark, low, high)

    spark.stop()


if __name__ == "__main__":
    main()
