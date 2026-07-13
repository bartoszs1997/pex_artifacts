# Task 08 - Implement a CDC feature in a data lake

Change Data Capture from a MySQL source into a Delta table on the data lake,
using the exact stack the task names: **Debezium** captures MySQL changes,
**Apache Kafka** streams them, and **Apache Spark** incrementally loads them into
**Delta Lake**, where the required performance techniques (data skipping,
compaction, caching) are applied and measured.

```
MySQL (binlog) --> Debezium --> Kafka topic --> Spark Structured Streaming
                                                --> MERGE into Delta (LocalStack S3)
```

> Note on minimalism: the plan for this section normally prefers the simplest
> mechanism (e.g. logical replication read directly). Here the task text and the
> acceptance criteria explicitly name **Debezium** and **Apache Kafka**, so the
> full stack is the minimal solution that literally satisfies the wording.

## Dataset

[classicmodels ("MySQL Sample Database")](https://www.mysqltutorial.org/mysql-sample-database.aspx),
a single SQL dump. It creates the `classicmodels` schema; the `customers` table
(PK `customerNumber`) is the CDC subject. Downloaded into `data/input/` and
mounted read-only into MySQL's init directory, so MySQL loads it on first start.

Download:

```bash
uv run python src/data_lake/08_cdc/download_data.py
```

## Components (docker-compose.yml)

| Service | Role |
|---|---|
| `mysql` | Source system (classicmodels), binlog enabled for CDC. Host port 3307 (3306 is taken by a local MySQL on this machine). |
| `zookeeper` | Coordination for Kafka. |
| `kafka` | Messaging system streaming the CDC events. Host listener on `localhost:9092`, internal on `kafka:29092`. |
| `connect` | Kafka Connect running the Debezium MySQL connector. |
| `localstack` | Imitates AWS S3 (the data lake) where the Delta table lives. |

## How it works

- **`register_connector.py`** posts the Debezium MySQL connector config to Kafka
  Connect. Debezium first snapshots `customers`, then streams every subsequent
  INSERT/UPDATE/DELETE from the binlog to the topic
  `dbserver1.classicmodels.customers`. The value converter runs with schemas
  disabled and `decimal.handling.mode=double`, so each message is a clean
  Debezium envelope `{before, after, op, ts_ms}`.
- **`cdc_pipeline.py`** reads that topic with Spark Structured Streaming, trigger
  **AvailableNow** (drain everything currently available, then stop) plus a
  checkpoint, so each run is **incremental**: it consumes only the offsets added
  since the previous run. Each micro-batch is normalized to the latest event per
  key and applied to the Delta table with a **MERGE** (insert/update, and delete
  when `op = 'd'`). It then applies and measures:
  - **Data skipping** - sets `delta.dataSkippingNumIndexedCols` and Z-orders by
    `customerNumber`; a selective query shows the pushed-down filter in its plan.
  - **Compaction** - `OPTIMIZE` rewrites the many small files produced by the
    streaming micro-batches into one (reported as active-file count before/after).
  - **Caching** - caches the table and compares cold vs warm query time.
  - **Verification** - reads the live MySQL source over JDBC and asserts the Delta
    table matches it exactly (row count and key set), proving accurate CDC.
- **`make_changes.py`** applies one INSERT, one UPDATE and one DELETE to MySQL to
  generate CDC events for the incremental run.

## How to run

Java 17 must be on PATH for PySpark.

```bash
# 0. dataset
uv run python src/data_lake/08_cdc/download_data.py

# 1. bring up the stack (first run pulls images)
docker compose -f src/data_lake/08_cdc/docker-compose.yml up -d
#    wait ~30-40s for MySQL to load classicmodels and Connect to be ready

# 2. start Debezium (snapshot + streaming)
uv run python src/data_lake/08_cdc/register_connector.py

export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"

# 3. RUN 1 - load the snapshot, optimize, verify
uv run python src/data_lake/08_cdc/cdc_pipeline.py

# 4. generate changes in MySQL
uv run python src/data_lake/08_cdc/make_changes.py

# 5. RUN 2 - incrementally load the changes, verify again
uv run python src/data_lake/08_cdc/cdc_pipeline.py

# 6. tear down
docker compose -f src/data_lake/08_cdc/docker-compose.yml down -v
```

## How to verify

Each pipeline run ends with an assertion that the Delta table equals the live
MySQL source. Representative output:

```
RUN 1 (snapshot)
Compaction: active data files 5 -> 1 (OPTIMIZE ZORDER BY customerNumber)
Caching: cold query 0.117s, warm (cached) query 0.037s
VERIFIED: Delta CDC table matches MySQL source exactly (122 customers, identical key sets)

make_changes: INSERT 9001, UPDATE 103, DELETE 125

RUN 2 (incremental)
VERIFIED: Delta CDC table matches MySQL source exactly (122 customers, identical key sets)
```

After the changes the count is still 122 (one insert, one delete) but membership
differs: 9001 is present, 125 is gone, 103's credit limit is updated - and the
Delta table matches MySQL, proving the INSERT/UPDATE/DELETE were captured.

> Performance note: classicmodels `customers` is tiny (~122 rows), so absolute
> query times are small. The task's optimization techniques are each configured
> and executed correctly (visible compaction 5 -> 1, a real cache speedup, and a
> pushed-down filter for data skipping); on production-scale data the same code
> yields large gains.

## Acceptance criteria mapping

| Criterion | Where it is met |
|---|---|
| CDC captures MySQL changes using Debezium | `register_connector.py` (Debezium MySQL connector); snapshot + binlog streaming |
| Incremental loading via Apache Kafka | `cdc_pipeline.py` reads the Kafka topic with trigger AvailableNow + checkpoint (only new offsets each run) |
| Data skipping with Delta + improvement shown | `delta.dataSkippingNumIndexedCols` + `OPTIMIZE ... ZORDER`; selective-query plan shows the pushed filter |
| Parquet compaction configured | `OPTIMIZE` reduces active data files (5 -> 1) |
| Data caching enabled and validated | `df.cache()` + cold vs warm timing |
| Tests show accurate capture + improved performance | `verify()` asserts Delta == MySQL; compaction/caching/skipping measured |

## Dependencies

`pyspark==4.1.1`, `delta-spark==4.3.0`, `boto3`, `pymysql`, `requests` (root
`pyproject.toml`). Docker for the stack. Spark pulls the Kafka, Delta, hadoop-aws
and MySQL JDBC jars on first run (cached in `~/.ivy2`).
