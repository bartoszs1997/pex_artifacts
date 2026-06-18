# Bank Marketing — Incremental Data Loading (SCD Type 2)

Spark application implementing **Slowly Changing Dimension Type 2** on the UCI
Bank Marketing dataset. Changes to customer records are tracked with effective
start/end dates and an `is_current` flag — the standard approach for maintaining
history in a data warehouse.

## Task

Implement incremental data loading into a target customer database:

1. **Identify new records** — ages in the source batch that do not exist in the
   target database.
2. **Identify modified records** — same age, but `education` or `loan` differs.
3. **Insert new records** with `effective_start_date = today`,
   `effective_end_date = 9999-12-31`, `is_current = true`.
4. **Update modified records** — expire the old version (`end_date = today`,
   `is_current = false`) and insert a new current version.

## What is SCD Type 2?

SCD (Slowly Changing Dimension) is a technique for tracking how dimension data
changes over time. **Type 2** keeps **full history**: when a value changes, the
old row is "expired" and a new row is inserted.

| SCD Type | Behaviour | History |
|---|---|---|
| Type 0 | Never update | No |
| **Type 1** | Overwrite old value | current only |
| **Type 2** | **Expire old row + insert new** | full history |
| Type 3 | Add "previous" column | partial |

Example — age 20, education changed from `high.school` → `basic.9y`:

| age | education | loan | start_date | end_date | is_current |
|---|---|---|---|---|---|
| 20 | high.school | no | 2020-01-01 | **2026-06-17** | **false** |
| 20 | basic.9y | no | **2026-06-17** | 9999-12-31 | **true** |

Both rows coexist — you can query "what was the education for age 20 on
2023-03-15?" (answer: `high.school`, because `2020-01-01 ≤ 2023-03-15 < 2026-06-17`).

## Results

| Step | Count |
| --- | --- |
| Full dataset rows | 41 188 |
| Initial target (unique ages, first half) | **43** |
| Source batch (unique ages, second half) | **78** |
| NEW records (age not in target) | **35** |
| MODIFIED records (education or loan changed) | **38** |
| UNCHANGED records | **5** |
| Updated target rows (with history) | **116** |

Row count breakdown: 43 original + 38 expired (kept as history) + 35 new = **116**.

> Cross-checked with an independent pandas pass — identical counts and ages.

## Dataset

Kaggle [`henriqueyamahata/bank-marketing`](https://www.kaggle.com/datasets/henriqueyamahata/bank-marketing)
— the UCI Bank Marketing dataset, `bank-additional-full.csv` (~5.8 MB, 41 188
rows, semicolon-separated).

Key columns for this task:
- **`age`** — business key (no customer ID in the dataset; task specifies age)
- **`education`** — tracked attribute (SCD Type 2)
- **`loan`** — tracked attribute (SCD Type 2)

> **Note:** `age` is not a realistic customer identifier (78 unique values over
> 41 188 rows). The task explicitly uses it as the business key for the SCD
> exercise. In a real scenario this would be a `customer_id`.

## Layout

```
09_bank_marketing/
├── download_data.py   # fetch CSV from Kaggle -> data/input/
├── solution.py        # SCD Type 2: detect new/modified, expire, insert
├── data/              # gitignored
│   ├── input/bank-additional-full.csv
│   └── output/customer_dimension/   # Parquet (target with history)
└── logs/              # gitignored
    └── bank_marketing.log
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH`:
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```

## Run

```bash
# 0) Download dataset
uv run python src/batch_processing/09_bank_marketing/download_data.py

# 1) Run the SCD Type 2 pipeline
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/batch_processing/09_bank_marketing/solution.py
```

## `solution.py` — step by step

| Step | What happens |
| --- | --- |
| `read_csv` | Read semicolon-separated CSV into DataFrame (`sep=";"`) |
| `build_initial_target` | First 50% of rows, `dropDuplicates(age)` → 43 unique ages, add SCD columns (`start=2020-01-01`, `end=9999-12-31`, `is_current=true`) |
| `build_source_batch` | Second 50% of rows, `dropDuplicates(age)` → 78 unique ages (incoming batch) |
| `identify_new_records` | `left_anti` join source vs target on age → 35 ages not in target |
| `identify_modified_records` | `inner` join on age, filter where `education` or `loan` differs → 38 |
| `apply_scd_type2` | (1) expire old modified rows, (2) keep unchanged, (3) insert new versions for modified, (4) insert brand-new → `unionByName` all four |
| write | Parquet output with full history (116 rows) |

### Key Spark operations used

| Operation | Type | Purpose |
|---|---|---|
| `left_anti` join | transformation | "give me source rows whose key does NOT exist in target" — efficient for finding new records |
| `inner` join | transformation | match source + target by age to compare attributes |
| `unionByName` | transformation | merge expired rows + unchanged + new inserts into one DataFrame |
| `dropDuplicates` | transformation | dedup by business key |
| `cache()` | performance | target is reused in both identify steps + the final merge |
| `lit()` | expression | add constant columns (dates, booleans) |
| `.count()`, `.show()`, `.write.parquet()` | actions | materialize results |

## Acceptance criteria — status

| # | Criterion | Status |
| --- | --- | --- |
| 1 | Read source CSV into Spark DataFrame | `spark.read.csv(sep=";")` |
| 2 | Compare source with existing target records | `left_anti` + `inner` join on age |
| 3 | Handle data conversions/validations | `inferSchema`, `dropDuplicates`, null-safe |
| 4 | Test with sample data — only new/modified loaded | 35 new + 38 modified, 5 unchanged (untouched) |
| — | SCD Type 2 with effective dates | `effective_start_date`, `effective_end_date`, `is_current` |
| — | New records inserted with correct dates | start=today, end=9999-12-31, is_current=true |
| — | Modified records: old expired, new inserted | old end=today+false, new start=today+true |

## Implementation notes

- **Simulation split.** The dataset has no "batch" marker, so the first 50% of
  rows (by original order) serve as the initial target, and the second 50% as
  the incremental source. `monotonically_increasing_id()` tags rows for the
  split without requiring a Window.
- **Parquet output** (not CSV) — SCD target tables are analytical; Parquet
  supports predicate pushdown and columnar reads for efficient time-travel
  queries.
- **Loopback binding** (`spark.driver.host=127.0.0.1`) avoids macOS networking
  issues.
- **Data** (`data/input`, `data/output`) and **logs** are gitignored.
