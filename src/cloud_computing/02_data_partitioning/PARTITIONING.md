# Data Partitioning Strategy — Customer Data Lake

Design of how customer data is split into partitions in a data lake so that
queries prune irrelevant data instead of scanning the whole dataset. The
strategy is implemented as a Hive-style **folder structure** (the partition
columns become directories) and validated with benchmarks in `partitioning.py`.
The architecture is shown in `diagram.svg`.

## 1. Workloads (the design is driven by these)

The customer data lake serves retrieval "based on different criteria". The
dominant access patterns are:

| # | Query | Access pattern |
|---|---|---|
| Q1 | Customers of one profession and spending tier | equality on both partition columns |
| Q2 | Customers across a set of professions | multi-value / range over the primary partition column |
| Q3 | Aggregations per profession (e.g. avg age, counts) | grouped scan over all partitions |

## 2. Partitioning scheme: `partitionBy(Profession, Spending_Score)`

The lake is partitioned by **`Profession`** (primary) and **`Spending_Score`**
(secondary), producing a two-level folder tree:

```
customers/Profession=Artist/Spending_Score=Low/part-*.parquet
customers/Profession=Doctor/Spending_Score=High/part-*.parquet
...
```

With ~10 professions x 3 spending scores this yields **30 leaf partitions**.

Chosen against the required factors:

- **Load balancing.** Both columns are **low-to-moderate cardinality**, so the
  data spreads into a manageable number of balanced partitions (30), not
  millions of tiny files (which would happen if we partitioned by a high
  cardinality column like `ID` or `Age`) and not one giant partition (which would
  happen with no partitioning).
- **Data skewness.** `Profession` is reasonably even; combining it with
  `Spending_Score` further splits any dominant profession into three, reducing
  skew. A single very large partition is avoided.
- **Data growth patterns.** New customers arrive continuously but the **set of
  professions and spending tiers is stable**, so the number of partitions stays
  ~constant while each partition grows gradually — the ideal growth shape (no
  partition explosion over time, unlike partitioning by day or by ID).
- **Query pruning.** Q1 reads 1 of 30 partitions; Q2 reads only the matching
  professions; both skip the rest. This is the core benefit.

Rejected alternatives:

- Partition by `ID` or `Age` — extreme cardinality -> partition explosion / tiny
  files, terrible ingestion and query planning.
- Partition by `Segmentation` only (4 values) — too coarse, poor pruning.
- No partitioning — every query is a full scan.

## 3. Validation — tests and benchmarks

`partitioning.py` builds two copies of the lake (unpartitioned and partitioned),
inflates the data to ~3.2M rows so measurements are meaningful, and benchmarks
three scenarios. The **primary, deterministic metric is partitions scanned**
(data pruning); wall-clock is reported as an indicative secondary metric (at this
data scale it is dominated by fixed Spark overhead).

| Scenario | Partitions scanned | Speedup (representative) |
|---|---|---|
| Selective (`Profession='Artist' AND Spending_Score='Low'`) | 1 / 30 (96.7% skipped) | ~1.2x |
| Range (`Profession IN (Artist, Doctor, Engineer)`) | 9 / 30 (70% skipped) | ~1.1x |
| Aggregation (`avg(Age) GROUP BY Profession`) | 30 / 30 (reads all) | ~0.6x |

The selective query's physical plan confirms pruning — it contains
`PartitionFilters: [(Profession = Artist), (Spending_Score = Low)]`, so Spark
lists only the matching directory. The exact wall-clock numbers vary per run and
are regenerated into `data/output/benchmark_results.md` on each execution.

## 4. Impact on data operations and system performance

- **Ingestion.** Partitioned writes are slower (~3.5s vs ~1.2s here) because Spark
  must fan out rows into one file set per partition. This is the accepted cost of
  faster reads.
- **Updates.** Updating a customer whose partition key (profession / spending
  tier) changes means the row moves between directories — more expensive than an
  in-place update. Updates that do not touch the key stay within one partition.
- **Overall.** Read-heavy, criteria-based workloads win substantially (pruning);
  write/update-heavy workloads pay overhead. The lake here is read-optimized,
  matching the stated goal ("improve query performance").

## 5. Limitations and trade-offs

- **Only helps queries that filter on the partition columns.** A query filtering
  only by `Age` cannot prune and scans everything (secondary indexes / Z-ordering
  would be needed for that dimension).
- **Small-files risk.** Over-partitioning (too many columns / high cardinality)
  produces many tiny files and slows planning; we deliberately kept it to two
  low-cardinality columns.
- **Aggregations over all partitions** pay a small file-count overhead vs a single
  flat file (visible as the ~0.6x above).
- **Key-changing updates** require rewriting across partitions.

## 6. Scalability, compatibility, future growth

- **Scalability.** The partition count is bounded by the (stable) domain of
  professions x spending tiers, so it does not grow with row count — the lake
  scales to billions of rows without partition explosion.
- **Compatibility.** Hive-style `col=value` Parquet partitioning is a de-facto
  standard understood by Spark, Hive, Presto/Trino, Athena, BigQuery external
  tables, Dremio, and pandas/Arrow. The exact folder layout is portable to S3
  (`s3://lake/customers/Profession=.../Spending_Score=.../`) with no code change.
- **Future growth.** New professions or spending tiers simply add new
  directories; existing partitions and queries are unaffected.

## Dataset

Customer Segmentation — Kaggle `vetrirah/customer` (`Train.csv`). Columns:
`ID, Gender, Ever_Married, Age, Graduated, Profession, Work_Experience,
Spending_Score, Family_Size, Var_1, Segmentation`.

## Acceptance criteria mapping

| Criterion | Where |
|---|---|
| Effective design (load balancing, skew, growth) | section 2 |
| Query performance validated (selective, range, aggregation) | section 3 + `partitioning.py` |
| Impact on ingestion, updates, system performance | section 4 |
| Limitations and trade-offs identified | section 5 |
| Scalability, compatibility, future growth | section 6 |
