# Task 01 - Ingest data into a data lake

Ingest the DL Course Data into the **bronze layer** of a data lake, using two
pipelines: a **batch** pipeline (files -> bronze) and a **streaming** pipeline
(PostgreSQL -> bronze). The data lake is imitated locally and for free with
**LocalStack S3** (a stand-in for AWS S3), so the code looks like production AWS
(`s3a://...`) without needing a cloud account.

## What it does

| Pipeline | Source | Sink (bronze) | Script |
|---|---|---|---|
| Batch | `housing.csv` and `housing.json` | `s3a://pex-datalake/bronze/housing_csv` and `.../housing_json` | `batch_ingest.py` |
| Streaming | PostgreSQL `housing` table | `s3a://pex-datalake/bronze/housing_stream` | `stream_ingest.py` |

Bronze is written as **Parquet**, preserving the original columns (structure and
integrity kept). Both scripts end with a verification that reads the data back
from the lake and prints the row count.

## How each acceptance criterion is met

- **Data lake environment (Hadoop or cloud):** LocalStack S3, accessed via the
  Hadoop `s3a://` connector - the cloud-based option.
- **Ingests from CSV files and a PostgreSQL database:** `batch_ingest.py` reads
  files; `stream_ingest.py` reads the Postgres table.
- **Handles CSV and JSON:** `batch_ingest.py` reads both `housing.csv` and
  `housing.json`.
- **Stored in bronze, structure and integrity preserved:** raw columns written as
  Parquet, no reshaping.
- **Usage documented:** this README.

## Streaming = change data capture (the simple, honest form)

The task asks to stream from Postgres "using a change data capture mechanism like
logical replication". We do the simplest thing that genuinely captures changes:
`stream_ingest.py` polls the table on an interval and appends rows it has not seen
yet (id greater than the last captured id). New inserts are captured continuously
and persisted to bronze. No Kafka and no Debezium - just Spark reading Postgres.

## Dataset

Kaggle [`ryanholbrook/dl-course-data`](https://www.kaggle.com/datasets/ryanholbrook/dl-course-data).
It ships only CSV files, so `download_data.py` also writes a JSON copy of
`housing.csv` (`housing.json`, newline-delimited) so the CSV-and-JSON criterion is
genuinely exercised. `housing` is the California housing table (20,640 rows).

## How to run

Java 17 must be on PATH for PySpark:

```bash
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
```

1. Download the data (once):

   ```bash
   uv run python src/data_lake/01_ingest_data/download_data.py
   ```

2. Start the stack (LocalStack S3 + Postgres):

   ```bash
   docker compose -f src/data_lake/01_ingest_data/docker-compose.yml up -d
   ```

3. Batch ingestion (CSV + JSON -> bronze):

   ```bash
   uv run python src/data_lake/01_ingest_data/batch_ingest.py
   # -> VERIFIED: bronze has 20640 CSV rows and 20640 JSON rows
   ```

4. Streaming ingestion. First put rows in Postgres, then capture them:

   ```bash
   uv run python src/data_lake/01_ingest_data/load_postgres.py --rows 200
   uv run python src/data_lake/01_ingest_data/stream_ingest.py
   # -> VERIFIED: bronze stream now holds 200 rows
   ```

   To watch continuous capture live, run the stream with no idle timeout and, in
   another terminal, keep inserting rows:

   ```bash
   uv run python src/data_lake/01_ingest_data/stream_ingest.py --idle-timeout 0
   uv run python src/data_lake/01_ingest_data/load_postgres.py --rows 200 --stream
   ```

5. Stop the stack when done:

   ```bash
   docker compose -f src/data_lake/01_ingest_data/docker-compose.yml down -v
   ```

## Files

| File | Role |
|---|---|
| `download_data.py` | Download the dataset and emit the JSON copy. |
| `docker-compose.yml` | LocalStack S3 + Postgres (two services, nothing else). |
| `batch_ingest.py` | Batch: CSV + JSON -> bronze (Parquet) + verify. |
| `load_postgres.py` | Create and fill the Postgres `housing` table (streaming source). |
| `stream_ingest.py` | Streaming: poll Postgres for new rows -> bronze + verify. |

## Dependencies

All in the root `pyproject.toml`: `pyspark`, `boto3`, `psycopg2-binary`. Spark
pulls two jars from Maven Central on first run: `hadoop-aws` (for `s3a://`) and
the PostgreSQL JDBC driver.
