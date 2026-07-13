# Task 04 - Design a bronze layer pipeline (Delta Lake)

Read raw customer/tweet CSV data from a source location, transform it, and store
it in a **Delta Lake** bronze table. The pipeline is **incremental**: each run
ingests only newly added CSV files. The data lake is imitated with **LocalStack
S3**.

```
source location (local) : data/landing/*.csv
bronze (Delta, on S3)   : s3a://pex-datalake/bronze/bronze_accounts   (partitioned by created_date)
```

## Dataset

Big Tech Companies tweet sentiment (`wjia26/big-tech-companies-tweet-sentiment`).
Two CSV files covering two date windows:

- `Bigtech - 12-07-2020 till 19-09-2020.csv`
- `Bigtech - 20-09-2020 till 13-10-2020.csv`

Both are large (866k / 266k lines), so the pipeline samples the first
`SAMPLE_ROWS` (800) records of each into the landing zone as `batch1.csv` and
`batch2.csv`. The two files let us demonstrate incremental ingestion (batch2 is
"a new file arriving later").

## Source schema (raw CSV)

The raw files have 15 columns; we declare them explicitly (a streaming CSV source
cannot infer a schema): `created_at, file_name, followers, friends, group_name,
location, retweet_count, screenname, search_query, text, twitter_id, username,
polarity, partition_0, partition_1`. Tweets contain newlines and quotes inside
the `text` field, so the reader uses `multiLine=true` and `escape='"'`.

## Transformations (raw -> bronze)

1. **Remove missing/invalid rows** - numeric fields are cast to `long` (bad
   values become null), empty strings become null, then all required columns are
   `dropna`-filtered.
2. **Title case location** - `initcap(location)`.
3. **Total friends per group_name** - reported via a `groupBy(group_name,
   created_date).sum(friends)` aggregation over the bronze table.
4. **Enforce the bronze schema** - explicit `StructType` + casts (see below).
5. **Partitioned write** - saved as Delta, partitioned by `created_date`.

Column mapping:

| Source | Bronze | Type |
|---|---|---|
| `twitter_id` | `file_id` | string |
| `file_name` | `file_name` | string |
| `followers` | `followers` | number (long) |
| `friends` | `friends` | number (long) |
| `group_name` | `group_name` | string |
| `location` (title case) | `location` | string |
| `retweet_count` | `retweet` | number (long) |
| `created_at` -> date | `created_date` | partition column |

The task requires the schema `file_id, file_name, followers, friends,
group_name, location, retweet`. We add `created_date` (a DATE derived from
`created_at`) purely as the partition column, since partitioning by the full
timestamp would create one partition per second.

## Incremental mechanism

Spark **Structured Streaming file source + a checkpoint**
(`trigger(availableNow=True)`). The checkpoint records which files were already
ingested, so re-running the pipeline processes **only new CSV files** dropped in
`data/landing/` and appends them to the bronze table. This is the simplest
OSS-native way to satisfy "handle new CSV files and incrementally update the
bronze table" (no Databricks Auto Loader needed).

The script demonstrates this end to end in one run:
1. drop `batch1.csv`, run the stream -> bronze gets batch1 rows,
2. drop `batch2.csv` (a new file), run the stream again -> only batch2 is
   ingested and appended.

## How to run

```bash
# 1. Download the dataset (Kaggle credentials read from ~/.kaggle/)
uv run python src/data_lake/04_bronze_layer/download_data.py

# 2. Bring up the data lake (LocalStack S3)
docker compose -f src/data_lake/04_bronze_layer/docker-compose.yml up -d

# 3. Run the bronze pipeline (Java 17 must be on PATH for PySpark)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/data_lake/04_bronze_layer/bronze_pipeline.py

# 4. Tear the data lake down
docker compose -f src/data_lake/04_bronze_layer/docker-compose.yml down -v
```

## How to verify

The final line prints, for example:

```
VERIFIED: bronze_accounts has 1074 rows across 16 date partitions; incremental
run added 578 rows; schema, non-null and title-case checks passed
```

`verify()` asserts:
- the bronze schema equals the required `BRONZE_SCHEMA`,
- no nulls remain in any required column,
- every `location` equals its own `initcap` (title case),
- the second run added rows (incremental ingestion worked),
- the table has multiple `created_date` partitions.

## Dependencies and configuration

- **PySpark 4.1.1** (host), **Java 17** on PATH.
- Spark packages pulled from Maven on first run (cached in `~/.ivy2`):
  - `io.delta:delta-spark_2.13:4.3.0` - Delta Lake format.
  - `org.apache.hadoop:hadoop-aws:3.4.2` - the `s3a://` filesystem.
- **boto3** to create the bucket and reset the bronze table between demo runs.
- **LocalStack S3 3.8** (pinned; `latest` is a Pro build that fails activation).

## Acceptance criteria mapping

- *Spark pipeline reads raw data from CSV files* -> streaming CSV source over
  `data/landing/*.csv`.
- *Handles new CSV files and incrementally updates the bronze table* -> streaming
  file source + checkpoint; batch2 is ingested without reprocessing batch1.
- *Source locations, transformations and bronze schema documented* -> this README
  plus the module and function docstrings.
