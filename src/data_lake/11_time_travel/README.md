# Task 11 - Time travel and rollbacks in a data lake

Implement data versioning, point-in-time queries and rollbacks on a Delta Lake
table. Delta keeps every version of a table in its transaction log, which gives
versioning, historical audit trails, reproducible reads and rollbacks out of the
box. The task outcome is "Documentation, program code", so the deliverables are
[`TIME_TRAVEL.md`](./TIME_TRAVEL.md) (versioning, rollback, governance, lineage,
monitoring, maintenance) and `time_travel.py` (the implementation + a small CLI).

## Dataset

[Customers](https://www.kaggle.com/datasets/datascientistanna/customers-dataset)
(`datascientistanna/customers-dataset`, `Customers.csv`, 2000 rows). Each record
is stamped with a `capture_time`; the table is partitioned by `capture_date`
(time-based partitioning). Downloaded into `data/input/` (gitignored).

```bash
uv run python src/data_lake/11_time_travel/download_data.py
```

## What it does

The demo builds a versioned, time-partitioned table and makes four commits, the
last of which is a deliberate bad update, then rolls it back:

| Version | Operation | Meaning |
|---|---|---|
| 0 | WRITE | initial load |
| 1 | UPDATE | legitimate change |
| 2 | DELETE | GDPR delete |
| 3 | UPDATE | BAD: all incomes wiped to 0 |
| 4 | RESTORE | rollback to version 2 |

It demonstrates: `DESCRIBE HISTORY` (audit trail), `versionAsOf` / `timestampAsOf`
(point-in-time queries), reproducible reads (stable version snapshot), and
`RESTORE` (rollback).

## How to run

Java 17 must be on PATH for PySpark.

```bash
docker compose -f src/data_lake/11_time_travel/docker-compose.yml up -d

export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"

# full demo (rebuilds and verifies)
uv run python src/data_lake/11_time_travel/time_travel.py

# or use the CLI on the existing table
uv run python src/data_lake/11_time_travel/time_travel.py history
uv run python src/data_lake/11_time_travel/time_travel.py show --version 1
uv run python src/data_lake/11_time_travel/time_travel.py restore --version 1

docker compose -f src/data_lake/11_time_travel/docker-compose.yml down -v
```

## How to verify

`verify()` asserts: at least 5 versions are tracked; time travel to v0 returns
the original data (customer 2 still present); after `RESTORE` to v2 the current
state matches v2 (customer 2 stays deleted, incomes are not zero); and the
restored income sum equals v2's. Expected final line:

```
VERIFIED: 5 versions tracked; time travel to v0 returns original data
(income sum 221463643); rollback to v2 undid the bad commit
(restored income sum 221428643, not 0)
```

## Acceptance criteria mapping

| Criterion | Where it is met |
|---|---|
| Accurate version tracking + rollback | `build_versions` + `DESCRIBE HISTORY` + `RESTORE`; asserted in `verify` |
| Efficient handling of changes/queries | `capture_date` partitioning + Delta data skipping; version reads are metadata-only |
| Access controls + governance | `TIME_TRAVEL.md` (IAM/bucket policy, GDPR + VACUUM, retention window) |
| Clear docs + user-friendly interface | `TIME_TRAVEL.md` + the `history`/`show`/`restore` CLI |
| Monitoring + maintenance plan | `TIME_TRAVEL.md` (history monitoring, OPTIMIZE/VACUUM schedule, updates) |

## Dependencies

`pyspark==4.1.1`, `delta-spark==4.3.0`, `boto3` (root `pyproject.toml`).
LocalStack S3 via Docker. Spark pulls `delta-spark` and `hadoop-aws` jars on
first run (cached in `~/.ivy2`).
