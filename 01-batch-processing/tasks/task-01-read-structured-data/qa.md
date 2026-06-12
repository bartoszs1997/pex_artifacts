# Q&A — Read Structured/Semi-Structured (Task 01)

### Q1: Why `multiLine=True` when reading JSON?
**A:** Spark's default JSON reader expects **JSON Lines** (one document per line).
Our dataset is a single JSON array spanning multiple lines, so without `multiLine`
Spark would treat the opening `[` as a malformed record (the `_corrupt_record`
column). `multiLine` switches to the alternative reader that parses the whole file
as one document. Trade-off: multiLine cannot be split across executors, so it's
slower for huge files — prefer JSONL for production.

### Q2: How does `explode` work on an array-of-struct column?
**A:** `explode(arr)` produces one output row per element of `arr`, copying the
other columns. After `explode("created_by")` each series becomes N rows
(one per creator). We then `select("creator.name")` to dot-walk into the struct.
For null/empty arrays, `explode` drops the row — use `explode_outer` to preserve them.

### Q3: Why `distinct().orderBy(...)` after `explode`?
**A:** Two reasons. (1) **distinct**: a creator who appears in multiple cancelled
series would otherwise show up multiple times. (2) **orderBy**: keeps the output
deterministic, which matters for screenshots, golden-file tests, and reviewer
verification.

### Q4: What's the schema mapping from JSON to Spark types?
**A:** Spark infers `string`, `long` (for integers), `double` (for floats),
`array<T>` for JSON arrays, `struct<...>` for JSON objects. Date strings are
inferred as `string` — you have to explicitly cast to `date`/`timestamp`.
For production, define an explicit `StructType` schema rather than relying on
inference: faster, safer, catches drift.

### Q5: Why JDBC over reading PostgreSQL via psycopg2 + pandas?
**A:** JDBC reads execute on the Spark executors in parallel (with `numPartitions`,
`partitionColumn`, `lowerBound`, `upperBound`). psycopg2/pandas pulls everything
to the driver. For tables larger than driver memory, JDBC is the only option.
For < a few hundred MB, pandas-then-`createDataFrame` is fine and simpler.

### Q6: How do you parallelise a JDBC read for large tables?
**A:** Pass `partitionColumn` (numeric or date), `lowerBound`, `upperBound`, and
`numPartitions`. Spark issues N concurrent queries with `WHERE col BETWEEN ...`.
Pick a partition column that is indexed and roughly evenly distributed — a
clustered primary key works well. Without these settings, JDBC reads run as a
single executor task.

### Q7: How does `spark.jars` differ from `--packages` and `--jars`?
**A:** `spark.jars` is a **config** — set programmatically when building the
SparkSession. `--jars` is the equivalent CLI flag for `spark-submit`.
`--packages` resolves Maven coordinates and downloads transitively. For a
single driver jar like the PostgreSQL driver, `spark.jars` (or `--jars`)
is enough.

### Q8: Why does `multiLine` JSON read run on a single task?
**A:** Spark splits files by newline boundaries. With `multiLine=True`, the parser
consumes the whole document, so it can't be split — exactly one task per file.
For parallelism with multi-line JSON, write multiple smaller files instead of
one giant file.

### Q9: How would you handle schema evolution in this pipeline?
**A:** Three options. (1) Define a strict `StructType` and reject documents
that don't conform — safest. (2) `mergeSchema=True` and accept new fields
silently — fast iteration but risky. (3) Use Delta Lake's `mergeSchema` write
option — explicit, audit-logged, and reversible. For a bronze layer we typically
choose (3) so we can replay history if a downstream change is needed.

### Q10: How does this compare to the original task's MySQL JDBC requirement?
**A:** The task specifies MySQL; we run PostgreSQL because it's our project's
standard SQL database. The Spark JDBC API is identical — only the URL prefix
(`jdbc:mysql://` vs `jdbc:postgresql://`) and the driver jar name change.
Everything else (partitioning, predicate pushdown, connection pooling) works
the same way.
