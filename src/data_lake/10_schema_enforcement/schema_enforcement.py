"""Task 10 - Define and follow schema enforcement rules (Delta Lake).

We define a strict schema for customer records (field names, data types, and
NOT NULL / CHECK constraints), then enforce it on a Delta table so bad records
can never be committed. Incoming raw data is validated against that schema and
non-compliant rows are handled with all three strategies the task lists:

    1. REJECT    : attempting to append rows that violate the constraints makes
                   Delta abort the whole transaction (schema enforcement in
                   action) - we catch and show the raised error.
    2. TRANSFORM : fixable violations are repaired to fit the schema (e.g. a
                   blank profession becomes "Unknown") and then admitted.
    3. DISCARD   : rows that still violate the schema after transform are routed
                   to a quarantine table instead of the clean table (nothing is
                   silently dropped).

The schema definition, the chosen mechanism, and the strategies are documented
in SCHEMA.md; this script implements exactly that.

Data lake layout (LocalStack S3):
    clean      : s3a://pex-datalake/customers            (Delta, schema-enforced)
    quarantine : s3a://pex-datalake/customers_quarantine (Delta, rejected rows)

Dataset columns (Customers.csv): CustomerID, Gender, Age, Annual Income ($),
Spending Score (1-100), Profession, Work Experience, Family Size.

Prerequisite:
    docker compose -f src/data_lake/10_schema_enforcement/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/10_schema_enforcement/schema_enforcement.py
"""

from pathlib import Path

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

HERE = Path(__file__).resolve().parent
INPUT_CSV = HERE / "data" / "input" / "Customers.csv"

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
CLEAN = f"s3a://{BUCKET}/customers"
QUARANTINE = f"s3a://{BUCKET}/customers_quarantine"

# 1. THE SCHEMA: explicit field names and data types (structural contract).
CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("gender", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=False),
        StructField("annual_income", IntegerType(), nullable=False),
        StructField("spending_score", IntegerType(), nullable=False),
        StructField("profession", StringType(), nullable=False),
        StructField("work_experience", IntegerType(), nullable=False),
        StructField("family_size", IntegerType(), nullable=False),
    ]
)

# The raw CSV headers mapped onto the schema field names.
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

# 1b. THE CONSTRAINTS: business rules enforced by Delta as named CHECK constraints.
#     Any write that violates one of these is rejected by Delta.
CONSTRAINTS = {
    "valid_age": "age > 0 AND age < 120",
    "non_negative_income": "annual_income >= 0",
    "valid_spending_score": "spending_score BETWEEN 0 AND 100",
    "non_blank_profession": "length(trim(profession)) > 0",
    "non_negative_experience": "work_experience >= 0",
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
        SparkSession.builder.appName("Task10SchemaEnforcement")
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


def create_enforced_table(spark: SparkSession) -> None:
    """Create the empty Delta table with the schema and CHECK/NOT NULL constraints.

    This is the enforcement mechanism: Delta stores the schema and the named
    constraints in the transaction log and rejects any write that breaks them.
    """
    cols = ",\n    ".join(
        f"{f.name} {f.dataType.simpleString()} NOT NULL" for f in CUSTOMER_SCHEMA
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{CLEAN}` ({cols}) USING delta")

    # Attach the business-rule CHECK constraints (idempotent create).
    existing = {
        k.replace("delta.constraints.", "")
        for k, _ in spark.sql(f"SHOW TBLPROPERTIES delta.`{CLEAN}`").collect()
        if k.startswith("delta.constraints.")
    }
    for name, expr in CONSTRAINTS.items():
        if name not in existing:
            spark.sql(
                f"ALTER TABLE delta.`{CLEAN}` ADD CONSTRAINT {name} CHECK ({expr})"
            )
    print(
        f"Created schema-enforced table with {len(CUSTOMER_SCHEMA)} typed NOT NULL "
        f"columns and {len(CONSTRAINTS)} CHECK constraints -> {CLEAN}"
    )


def load_raw(spark: SparkSession) -> DataFrame:
    """Read the raw CSV and map headers to the schema field names (types loose)."""
    raw = spark.read.csv(str(INPUT_CSV), header=True, inferSchema=True)
    for old, new in RENAME.items():
        raw = raw.withColumnRenamed(old, new)
    return raw


def all_constraints_expr() -> F.Column:
    """A single boolean column that is true only when every constraint holds."""
    cond = F.lit(True)
    for expr in CONSTRAINTS.values():
        cond = cond & F.expr(expr)
    # NOT NULL part of the schema contract.
    for field in CUSTOMER_SCHEMA:
        cond = cond & F.col(field.name).isNotNull()
    return cond


def transform_to_schema(df: DataFrame) -> DataFrame:
    """Strategy TRANSFORM: repair fixable violations so rows fit the schema.

    - blank/null profession -> "Unknown"
    - null numeric fields    -> 0 where that is a valid default
    Rows that are still invalid after this (e.g. age <= 0) are left invalid so
    they get quarantined downstream.
    """
    return (
        df.withColumn(
            "profession",
            F.when(
                F.col("profession").isNull() | (F.trim(F.col("profession")) == ""),
                F.lit("Unknown"),
            ).otherwise(F.col("profession")),
        )
        .withColumn("work_experience", F.coalesce(F.col("work_experience"), F.lit(0)))
        .withColumn("family_size", F.coalesce(F.col("family_size"), F.lit(0)))
    )


def demonstrate_reject(spark: SparkSession) -> str:
    """Strategy REJECT: prove Delta blocks a non-compliant write.

    Append a single row that breaks a CHECK constraint (age = 0) and confirm the
    transaction is aborted with a constraint-violation error.
    """
    bad = spark.createDataFrame(
        [(99999, "Male", 0, 50000, 50, "Engineer", 2, 3)],
        schema=CUSTOMER_SCHEMA,
    )
    try:
        bad.write.format("delta").mode("append").save(CLEAN)
        raise RuntimeError("REJECT failed: a bad row was accepted")
    except Exception as exc:  # Delta raises on the CHECK violation
        msg = str(exc)
        # Surface the constraint-violation line if present (Py4J wraps it deep).
        detail = next(
            (ln for ln in msg.splitlines() if "constraint" in ln.lower()),
            msg.splitlines()[0],
        )
        print(f"REJECT: bad row (age=0) blocked by Delta -> {detail.strip()[:110]}")
        return msg


def ingest_with_strategies(spark: SparkSession) -> dict:
    """Validate raw data and apply TRANSFORM + DISCARD, then load the clean rows."""
    raw = load_raw(spark)
    total = raw.count()

    # TRANSFORM: repair what we can.
    transformed = transform_to_schema(raw)

    # Split by the full schema contract: compliant vs still non-compliant.
    ok = all_constraints_expr()
    compliant = transformed.filter(ok).select(*[f.name for f in CUSTOMER_SCHEMA])
    non_compliant = transformed.filter(~ok)

    # DISCARD: quarantine the rows that remain non-compliant.
    non_compliant.write.format("delta").mode("overwrite").save(QUARANTINE)

    # Load the compliant rows into the schema-enforced table (Delta re-checks
    # every constraint on write; this succeeds because we pre-filtered).
    compliant.write.format("delta").mode("append").save(CLEAN)

    report = {
        "total": total,
        "loaded": compliant.count(),
        "quarantined": non_compliant.count(),
    }
    print(
        f"Ingest: {report['total']} raw rows -> {report['loaded']} loaded "
        f"(transformed + compliant), {report['quarantined']} quarantined"
    )
    return report


def verify(spark: SparkSession, report: dict, reject_msg: str) -> None:
    """Assert the schema is truly enforced and the strategies worked."""
    clean = spark.read.format("delta").load(CLEAN)
    quarantine = spark.read.format("delta").load(QUARANTINE)

    # 1. The clean table has zero constraint violations.
    bad = clean.filter(~all_constraints_expr()).count()
    assert bad == 0, f"clean table has {bad} rows violating the schema"

    # 2. The constraints really exist in the table metadata (the mechanism).
    props = dict(spark.sql(f"SHOW TBLPROPERTIES delta.`{CLEAN}`").collect())
    stored = [k for k in props if k.startswith("delta.constraints.")]
    assert len(stored) == len(CONSTRAINTS), (
        f"expected {len(CONSTRAINTS)} constraints, found {len(stored)}"
    )

    # 3. REJECT actually raised on the bad row.
    assert "constraint" in reject_msg.lower() or "check" in reject_msg.lower(), (
        f"reject did not report a constraint violation: {reject_msg}"
    )

    # 4. DISCARD kept the rejected rows (nothing silently lost).
    assert quarantine.count() == report["quarantined"], "quarantine count mismatch"
    assert report["loaded"] + report["quarantined"] == report["total"], (
        "loaded + quarantined must equal total raw rows"
    )

    print(
        f"VERIFIED: {report['loaded']} rows in the schema-enforced table (0 "
        f"violations, {len(stored)} CHECK constraints active); REJECT blocked a "
        f"bad write; {report['quarantined']} non-compliant rows quarantined; "
        f"loaded + quarantined = {report['total']} total"
    )


def main() -> None:
    s3 = s3_client()
    reset_lake(s3)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Define + enforce the schema, then validate incoming data against it.
    create_enforced_table(spark)
    reject_msg = demonstrate_reject(spark)
    report = ingest_with_strategies(spark)

    verify(spark, report, reject_msg)
    spark.stop()


if __name__ == "__main__":
    main()
