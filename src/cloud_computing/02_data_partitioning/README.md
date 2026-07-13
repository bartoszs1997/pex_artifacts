# Task 02 — Design a Data Partitioning Strategy (Customer Data Lake)

Design a data partitioning strategy for customer data in a data lake so queries
prune irrelevant data instead of scanning everything. The lake is partitioned as
a Hive-style **folder structure** by `(Profession, Spending_Score)`, and the
strategy is **validated with a real benchmark** (this task requires validation,
unlike a design-only task).

## Files

| File | Task outcome it delivers |
|---|---|
| `PARTITIONING.md` | **Documentation** — scheme, load balancing/skew/growth, benchmark results, impact on ingestion/updates, trade-offs, scalability/compatibility. |
| `diagram.svg` | **Diagram** — real image of the partition folder tree and query pruning. Open in a browser. |
| `partitioning.py` | **Code + benchmark** — builds partitioned vs unpartitioned lakes and measures selective/range/aggregation queries. |
| `download_data.py` | Fetches the dataset into `data/input/`. |

## Dataset

Customer Segmentation — Kaggle `vetrirah/customer` (`Train.csv`, ~8k rows,
inflated to ~3.2M in the benchmark so timings are meaningful).

## Run

```bash
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/cloud_computing/02_data_partitioning/download_data.py
uv run python src/cloud_computing/02_data_partitioning/partitioning.py
```

The script prints the partition folder tree, the benchmark table, and the
physical plan proving partition pruning; it also writes
`data/output/benchmark_results.md`.

## How to verify

- The partitioned lake has 30 leaf partitions (`Profession=*/Spending_Score=*`).
- The selective query scans **1 / 30** partitions; the plan shows
  `PartitionFilters` — proof of pruning.
- `main()` asserts partitioning produced multiple partitions and the selective
  query pruned them; it exits 0 on success.

## Strategy in one line

Partition by `(Profession, Spending_Score)` — two low-cardinality columns that
give balanced, bounded partitions (30), let criteria-based queries prune 70-97%
of the data, and stay constant in count as the customer base grows.

## Acceptance criteria mapping

| Criterion | Where |
|---|---|
| Effective design (load balancing, skew, growth) | `PARTITIONING.md` section 2 |
| Query performance validated (selective, range, aggregation) | `partitioning.py`, `PARTITIONING.md` section 3 |
| Impact on ingestion, updates, system performance | `PARTITIONING.md` section 4 |
| Limitations and trade-offs identified | `PARTITIONING.md` section 5 |
| Scalability, compatibility, future growth | `PARTITIONING.md` section 6 |

## Dependencies

`pyspark` (already in the root `pyproject.toml`), Java 17 on PATH. No Docker.
