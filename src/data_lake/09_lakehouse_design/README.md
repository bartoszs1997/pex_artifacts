# Task 09 - Design and build data lake and lakehouse layers

Design and build a medallion lakehouse (bronze / silver / gold) on Delta Lake.
The task's outcome is "Diagrams, documentation", so the primary deliverable is
[`ARCHITECTURE.md`](./ARCHITECTURE.md) (requirements analysis, layer design,
external-system communication, and how the design meets the three hard
requirements: GDPR deletes, ACID reliability, CDC). The companion `lakehouse.py`
builds and demonstrates that design end to end so every acceptance criterion is
shown working, not just described.

## Databricks -> open-source

The task targets Databricks; we implement the same concepts on Delta Lake OSS +
PySpark + LocalStack S3 (see the mapping table in `ARCHITECTURE.md`). Every
feature used - ACID, `MERGE`, `OPTIMIZE`/`ZORDER`, `VACUUM`, partitioning -
exists identically in Databricks.

## Dataset

[Customers](https://www.kaggle.com/datasets/datascientistanna/customers-dataset)
(`datascientistanna/customers-dataset`, `Customers.csv`, 2000 rows). Columns:
`CustomerID, Gender, Age, Annual Income ($), Spending Score (1-100), Profession,
Work Experience, Family Size`. It contains real quality issues (blank
professions, `Age = 0`) that the silver validation catches. Downloaded into
`data/input/` (gitignored).

```bash
uv run python src/data_lake/09_lakehouse_design/download_data.py
```

## Pipeline

```
landing (CSV + JSON + Avro + Parquet)  ->  bronze (raw union, Delta)
    ->  silver (validated, typed, deduped; bad rows -> quarantine;
                partitioned by profession, ZORDER customer_id)
    ->  gold (per-profession aggregates + filtered high-value join)
```

To prove mixed structured / semi-structured ingestion, the single source file is
re-emitted into four formats in a landing zone and all four are read back and
unioned into bronze. On the built tables the script then exercises ACID (MERGE
upsert + GDPR delete), lifecycle (archive + VACUUM purge), and advanced analysis
(top-N per profession via a window function).

## How to run

Java 17 must be on PATH for PySpark.

```bash
docker compose -f src/data_lake/09_lakehouse_design/docker-compose.yml up -d

export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/data_lake/09_lakehouse_design/lakehouse.py

docker compose -f src/data_lake/09_lakehouse_design/docker-compose.yml down -v
```

## How to verify

`verify()` asserts across every layer: bronze ingested all 2000 rows from 4
formats; silver has zero invalid rows and a unique primary key; quarantine is
non-empty; the GDPR-deleted customer is gone; gold aggregates exist. Expected
final line:

```
VERIFIED: bronze 2000 rows (4 formats) -> silver 1942 valid, 58 quarantined
-> gold 9 profession aggregates; ACID/GDPR/lifecycle applied
```

## Acceptance criteria mapping

| Criterion | Where it is met |
|---|---|
| Scalable data lake architecture designed + implemented | `ARCHITECTURE.md` + the medallion build in `lakehouse.py` |
| Ingest structured + semi-structured (CSV, JSON, Avro, Parquet) | `emit_multi_format` + `build_bronze` read and union all four |
| Storage optimized for cost/performance | Delta = Parquet + Snappy; `OPTIMIZE` compaction |
| Data lifecycle (archiving + purging) | `manage_lifecycle`: archive tier + `VACUUM` |
| ETL raw -> usable | bronze -> silver -> gold transformations |
| Aggregation, joining/merging, filtering | `build_gold` (groupBy aggregate, join, filter) |
| Data quality via validation | `build_silver` rules + quarantine table |
| Robust lakehouse leveraging ACID | `demonstrate_acid` (MERGE upsert, GDPR delete) |
| Advanced partitioning / clustering / indexing | silver `partitionBy(profession)` + `ZORDER(customer_id)` |
| Advanced data analysis | `advanced_analysis` (windowed top-N) |

## Dependencies

`pyspark==4.1.1`, `delta-spark==4.3.0`, `boto3` (root `pyproject.toml`).
LocalStack S3 via Docker. Spark pulls `delta-spark`, `spark-avro` and
`hadoop-aws` jars on first run (cached in `~/.ivy2`).
