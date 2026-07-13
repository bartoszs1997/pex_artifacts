"""Task 07 - Organize data in a data catalog (data lake on LocalStack S3).

We take a flat sales file (Video Game Sales) that sits in the data lake and
organize it into a discoverable catalog: files are split, renamed to a naming
convention, laid out in a year-based folder hierarchy, and each file gets a
metadata entry. A central catalog index (a queryable Delta table) is then used
to search the sales data by date range, business unit, or file format.

The catalog design (naming convention, folder hierarchy, metadata attributes,
navigation instructions) is documented in CATALOG.md; this script implements it.

Domain mapping (how the raw dataset maps onto "sales data"):
    source system  -> "vgsales"          (the origin of the records)
    business unit  -> Genre              (the product line the sales belong to)
    date           -> Year               (annual granularity; quarter = "FY")

Naming convention (see CATALOG.md):
    sales_data_{source}_{business_unit}_{year}.csv
    e.g. sales_data_vgsales_action_2016.csv

Folder hierarchy in the lake (s3a://pex-datalake/sales_catalog/):
    Sales_{year}/sales_data_vgsales_{business_unit}_{year}.csv   (the data file)
    Sales_{year}/sales_data_vgsales_{business_unit}_{year}.metadata.json (sidecar)

Catalog index (the searchable data catalog):
    s3a://pex-datalake/catalog_index   (Delta table, one row per data file)

Prerequisite:
    docker compose -f src/data_lake/07_data_catalog/docker-compose.yml up -d

Run (Java 17 must be on PATH for PySpark):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/data_lake/07_data_catalog/catalog.py
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve().parent
INPUT_CSV = HERE / "data" / "input" / "vgsales.csv"

# LocalStack S3 (the cloud data lake).
S3_ENDPOINT = "http://localhost:4566"
BUCKET = "pex-datalake"
CATALOG_PREFIX = "sales_catalog"          # organized data files live here
CATALOG_INDEX = f"s3a://{BUCKET}/catalog_index"  # the searchable catalog (Delta)

SOURCE = "vgsales"
OWNER = "sales-analytics-team"

PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.13:4.3.0",
        "org.apache.hadoop:hadoop-aws:3.4.2",
    ]
)


def s3_client():
    """A boto3 S3 client pointed at LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


def create_bucket(s3) -> None:
    """Create the data lake bucket on LocalStack (safe to call every time)."""
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Created bucket {BUCKET}")


def clear_catalog(s3) -> None:
    """Delete organized files and the index so a fresh run is deterministic."""
    for prefix in (f"{CATALOG_PREFIX}/", "catalog_index/"):
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        keys = [{"Key": o["Key"]} for o in resp.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
            print(f"Cleared {len(keys)} objects under {prefix}")


def slug(value: str) -> str:
    """Lowercase, filesystem-safe token for a business unit (e.g. Role-Playing)."""
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")


def load_sales() -> pd.DataFrame:
    """Read the raw sales file and keep only well-formed, dated records."""
    df = pd.read_csv(INPUT_CSV)
    df = df[df["Year"].notna() & df["Genre"].notna()].copy()
    df["Year"] = df["Year"].astype(int)
    df["Publisher"] = df["Publisher"].fillna("Unknown")
    return df


def organize(s3, df: pd.DataFrame) -> list[dict]:
    """Split the sales data by (year, business unit), apply the naming convention
    and folder hierarchy in the lake, and emit a metadata entry per file.

    Returns the list of metadata entries (one per organized data file).
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    entries: list[dict] = []

    for (year, genre), group in df.groupby(["Year", "Genre"]):
        unit = slug(genre)
        filename = f"sales_data_{SOURCE}_{unit}_{year}.csv"
        folder = f"{CATALOG_PREFIX}/Sales_{year}"
        data_key = f"{folder}/{filename}"

        # 1. Write the organized data file under the naming convention + hierarchy.
        s3.put_object(Bucket=BUCKET, Key=data_key, Body=group.to_csv(index=False))

        # 2. Build the metadata entry for this file (the catalog record).
        entry = {
            "file_path": f"s3a://{BUCKET}/{data_key}",
            "file_name": filename,
            "data_source": SOURCE,
            "business_unit": genre,
            "year": int(year),
            "quarter": "FY",  # dataset is annual; schema supports quarter granularity
            "file_format": "csv",
            "row_count": int(len(group)),
            "description": f"Video game sales records for {genre} titles in {year}.",
            "owner": OWNER,
            "tags": [SOURCE, "sales", unit, str(year)],
            "created_at": now,
        }
        entries.append(entry)

        # 3. Write a per-file metadata sidecar next to the data file.
        meta_key = f"{folder}/{filename.replace('.csv', '.metadata.json')}"
        s3.put_object(Bucket=BUCKET, Key=meta_key, Body=json.dumps(entry, indent=2))

    print(f"Organized {len(entries)} files under s3://{BUCKET}/{CATALOG_PREFIX}/")
    return entries


def create_spark_session() -> SparkSession:
    """Create a Spark session with Delta Lake and S3A to LocalStack."""
    return (
        SparkSession.builder.appName("Task07Catalog")
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


def build_index(spark: SparkSession, entries: list[dict]) -> None:
    """Populate the searchable catalog: write the metadata entries as a Delta
    table and register it as a SQL view named `catalog`."""
    catalog = spark.createDataFrame(entries)
    catalog.write.format("delta").mode("overwrite").save(CATALOG_INDEX)
    spark.read.format("delta").load(CATALOG_INDEX).createOrReplaceTempView("catalog")
    print(f"Catalog index populated with {catalog.count()} entries -> {CATALOG_INDEX}")


def run_searches(spark: SparkSession) -> None:
    """Prove discoverability: locate files by date range, business unit, format."""
    print("\nSearch 1 - by date range (year between 2014 and 2016):")
    spark.sql(
        "SELECT file_name, business_unit, year FROM catalog "
        "WHERE year BETWEEN 2014 AND 2016 ORDER BY year, business_unit LIMIT 10"
    ).show(10, truncate=False)

    print("Search 2 - by business unit (Role-Playing):")
    spark.sql(
        "SELECT file_name, year, row_count FROM catalog "
        "WHERE business_unit = 'Role-Playing' ORDER BY year DESC LIMIT 10"
    ).show(10, truncate=False)

    print("Search 3 - by file format (csv), counted per business unit:")
    spark.sql(
        "SELECT business_unit, COUNT(*) AS files, SUM(row_count) AS records "
        "FROM catalog WHERE file_format = 'csv' "
        "GROUP BY business_unit ORDER BY records DESC"
    ).show(20, truncate=False)


def verify(spark: SparkSession, s3, entries: list[dict]) -> None:
    """Verify the catalog is complete, consistent, and discoverable."""
    catalog = spark.read.format("delta").load(CATALOG_INDEX)

    # 1. Every organized data file has exactly one catalog entry.
    data_objs = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{CATALOG_PREFIX}/")
    data_files = [
        o["Key"] for o in data_objs.get("Contents", []) if o["Key"].endswith(".csv")
    ]
    assert len(data_files) == len(entries), (
        f"{len(data_files)} data files vs {len(entries)} entries"
    )
    assert catalog.count() == len(entries), "catalog row count mismatch"

    # 2. Required metadata attributes are present on every entry.
    for col in ("data_source", "description", "owner", "business_unit", "year"):
        missing = catalog.filter(f"{col} IS NULL").count()
        assert missing == 0, f"{missing} entries missing {col}"

    # 3. Every file name follows the naming convention.
    bad = catalog.filter(
        "file_name NOT RLIKE '^sales_data_vgsales_[a-z0-9_]+_[0-9]{4}\\\\.csv$'"
    ).count()
    assert bad == 0, f"{bad} files violate the naming convention"

    # 4. The search view is discoverable (a date-range query returns results).
    hits = spark.sql("SELECT * FROM catalog WHERE year BETWEEN 2014 AND 2016").count()
    assert hits > 0, "date-range search returned no files"

    years = catalog.selectExpr("min(year)", "max(year)").first()
    units = catalog.select("business_unit").distinct().count()
    print(
        f"VERIFIED: catalog holds {catalog.count()} files across years "
        f"{years[0]}-{years[1]} and {units} business units; every file follows the "
        f"naming convention and carries complete metadata; searches return results"
    )


def main() -> None:
    s3 = s3_client()
    create_bucket(s3)
    clear_catalog(s3)

    # Organize the raw sales data into the catalog's naming + folder hierarchy.
    df = load_sales()
    entries = organize(s3, df)

    # Populate and query the searchable catalog index.
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    build_index(spark, entries)
    run_searches(spark)

    # Verify organization, metadata completeness, and discoverability.
    verify(spark, s3, entries)
    spark.stop()


if __name__ == "__main__":
    main()
