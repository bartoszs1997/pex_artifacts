# Time Travel, Versioning and Rollbacks

This document is the documentation deliverable for task 11 (outcome:
"Documentation, program code"). It explains how the data lake versions data,
how to time-travel and roll back, and how governance, lineage, monitoring and
maintenance are handled. The companion `time_travel.py` implements everything
described here.

## Foundation: the Delta transaction log

Every write to a Delta table appends a commit to its transaction log
(`_delta_log/`). Each commit is an immutable, numbered **version** with a
timestamp and the operation that produced it. Because old data files are retained
until `VACUUM`, any past version can be read exactly as it was. This single
mechanism provides versioning, audit trails, point-in-time queries and rollbacks
- no external versioning system is needed.

> Databricks -> OSS: Delta time travel behaves identically on Databricks and on
> Delta Lake OSS; this code runs unchanged on either.

## Data with capture timestamps and time partitioning

The customer records carry a `capture_time` (when the record was captured) and a
`capture_date`. The table is **partitioned by `capture_date`**, so time-scoped
queries prune to the relevant partitions - the time-based organization the task
asks for.

## Versioning and the audit trail

The demo creates a realistic history:

| Version | Operation | What happened |
|---|---|---|
| 0 | WRITE | initial load (2000 rows, partitioned by capture_date) |
| 1 | UPDATE | customer 1 spending score bumped |
| 2 | DELETE | customer 2 removed (GDPR right to be forgotten) |
| 3 | UPDATE | BAD commit: all incomes wiped to 0 |
| 4 | RESTORE | rollback to version 2 (undo the bad commit) |

`DESCRIBE HISTORY` returns the full trail (version, timestamp, operation, and
more) - the historical audit the task requires.

## Querying by version or timestamp

- By version: `spark.read.format("delta").option("versionAsOf", 0).load(path)`
- By timestamp: `... .option("timestampAsOf", "2026-07-13 16:25:27").load(path)`

Because a given version is immutable, reading it repeatedly always yields the
same data - this is what makes **reproducible ML experiments** possible: pin the
training set to a version and every re-run sees identical data.

## Rollbacks

A bad commit is undone with:

```sql
RESTORE TABLE delta.`s3a://.../customers` TO VERSION AS OF 2
```

`RESTORE` is itself a new commit (version 4), so the rollback is also auditable -
you can see that a restore happened and to which version.

## User interface (CLI)

`time_travel.py` doubles as a small CLI over the versioned table:

```bash
uv run python src/data_lake/11_time_travel/time_travel.py history
uv run python src/data_lake/11_time_travel/time_travel.py show --version 1
uv run python src/data_lake/11_time_travel/time_travel.py restore --version 1
```

## Data governance and access control

- **Access control:** in a real cloud this is enforced with IAM / bucket policies
  on the lake (on AWS: IAM roles + S3 bucket policies; LocalStack simulates IAM
  for demonstration). Read vs write vs restore can be split so that, e.g., only a
  data steward may `RESTORE`.
- **Governance:** GDPR deletes are first-class (version 2). Note that time travel
  retains deleted data in old versions until `VACUUM`; to truly honor a
  right-to-be-forgotten request, run `VACUUM` after the delete so the data is
  physically purged from history (the retention pattern from task 06).
- **Retention window:** `delta.logRetentionDuration` and
  `delta.deletedFileRetentionDuration` govern how far back time travel reaches.

## Data lineage

The transaction log is the lineage record: for every version it stores the
operation, its parameters/predicates, and the read/write metrics. Combined with
`capture_time` on each row and the `capture_date` partitioning, you can trace
when data arrived and how each version was derived from the previous one.

## Monitoring, maintenance and updates

- **Monitoring:** poll `DESCRIBE HISTORY` for new versions and unexpected
  operations (e.g. an unplanned mass UPDATE), and track table size / file counts.
- **Maintenance:** periodic `OPTIMIZE` (compaction) keeps queries fast; scheduled
  `VACUUM` reclaims storage and enforces the retention window. These jobs are the
  same ones used in tasks 06 and 08.
- **Updates:** schema changes go through Delta schema evolution (task 05); new
  data lands as new versions, so upgrades are always reversible via rollback.

## Note on the description's extra items (event-driven / real-time CDC)

The task description also mentions an event-driven architecture and real-time
CDC. Those are not part of the task 11 acceptance criteria (which focus on
versioning, rollback, performance, governance, documentation and monitoring).
The CDC capability itself is fully built in task 08 (Debezium + Kafka + Delta
MERGE); this task reuses that concept rather than duplicating the stack, in line
with the section's minimalism principle.
