# 11 — Refactoring and Reengineering

## Task

Perform refactoring and reengineering activities on an Apache Spark application
to improve its quality, maintainability, and performance. Document all
observations and improvements.

## Refactoring Summary

| # | Problem (before) | Refactoring Applied | Benefit |
|---|-------------------|---------------------|---------|
| 1 | All logic in top-level script, no structure | Extract Class (`SalaryAnalyzer`) | SRP, testability, reusability |
| 2 | Same groupBy/agg pattern copy-pasted 3 times | Extract Method (`aggregate_by`) | DRY — one place to maintain |
| 3 | `df.collect()` + Python loop for top jobs | Spark-native `groupBy().limit()` | Distributed, memory-safe |
| 4 | 3 separate `df.filter()` calls for salary tiers | Single pass with `F.when().otherwise()` | 1 scan instead of 3 |
| 5 | `collect()` in loop for year-over-year | Spark `groupBy("work_year")` | No driver memory pressure |
| 6 | `print()` for output | `logging` module (dual: console + file) | Structured, persistent logs |
| 7 | `os.path` string concatenation for paths | `pathlib.Path` with constants | Readable, cross-platform |
| 8 | `inferSchema=True` | Explicit `StructType` schema | Deterministic types, faster load |
| 9 | No error handling | `try/except/finally` + `sys.exit()` | Graceful failure, exit codes |
| 10 | No `.cache()` on reused DataFrame | `.cache()` after load | Avoids re-reading CSV 6 times |
| 11 | No type hints | Full type annotations | IDE support, self-documentation |
| 12 | No Spark network config | Loopback `127.0.0.1` config | Stable on macOS |

## Dataset

**Employee Salaries** (`ds_salaries.csv`) — 607 rows, 12 columns.
Source: [Kaggle](https://www.kaggle.com/datasets/inductiveanks/employee-salaries-for-different-job-roles)

## Layout

```
11_refactoring/
├── download_data.py       # Kaggle download script
├── solution_before.py     # Original version (before refactoring)
├── solution.py            # Refactored version (after)
├── README.md              # Documentation of changes
├── data/
│   ├── input/             # ds_salaries.csv
│   └── output/            # Analysis results
└── logs/
    └── refactoring.log
```

## Prerequisites

- uv (package manager)
- Java 17: `export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"`
- Kaggle API token at `~/.kaggle/kaggle.json`

## Run

```bash
# Download data
uv run python src/programming_languages/11_refactoring/download_data.py

# Run BEFORE version
uv run python src/programming_languages/11_refactoring/solution_before.py

# Run AFTER version (refactored)
uv run python src/programming_languages/11_refactoring/solution.py
```

## `solution_before.py` walkthrough

| Lines | What it does | Problem |
|-------|-------------|---------|
| 15–17 | Imports (no `logging`, no `Path`, no types) | Missing standard tooling |
| 19 | Global `SparkSession` at module level | Not wrapped in function, no error handling |
| 22–24 | `os.path` string concat to build path, `inferSchema=True` | Fragile paths, schema guessing |
| 27–30 | Analysis 1: groupBy experience | Long one-liner, hardcoded output path |
| 32–35 | Analysis 2: groupBy company size | Copy-paste of analysis 1 |
| 37–40 | Analysis 3: groupBy remote ratio | Copy-paste of analysis 1 |
| 42–55 | Top jobs: `collect()` + Python dict + manual sort | Brings all 607 rows to driver |
| 57–65 | Salary tiers: 3 separate `filter()` + 3 `count()` | 3 full scans of the same data |
| 67–73 | Year-over-year: `collect()` + loop + `collect()` per year | N+1 Spark actions in a loop |
| 75 | `spark.stop()` | No `finally`, not called on error |

## `solution.py` walkthrough

| Lines | What it does | Improvement |
|-------|-------------|-------------|
| 1–15 | Detailed docstring with usage | Self-documenting |
| 17–29 | Organized imports with type hints | Clear dependencies |
| 34–40 | Named constants (`SCRIPT_DIR`, `OUTPUT_DIR`, etc.) | No magic strings |
| 42–56 | Explicit `StructType` schema | No `inferSchema` guessing |
| 61–77 | Dual logging setup | Structured output + log file |
| 83–100 | `SalaryAnalyzer.__init__` + `load()` with `.cache()` | One load, cached for reuse |
| 106–117 | `aggregate_by(group_col)` — generic method | Replaces 3x copy-paste |
| 121–133 | `top_n_jobs(n)` — Spark-native | No `collect()` loop |
| 137–150 | `salary_tiers()` — single pass `CASE WHEN` | 1 scan instead of 3 |
| 154–165 | `year_over_year()` — Spark `groupBy` | No Python loop |
| 169–173 | `save()` static method | Reusable write logic |
| 178–238 | `main()` with `try/except/finally` + `sys.exit()` | Error handling, exit codes |

## Acceptance Criteria

| Criterion | How Met |
|-----------|---------|
| Code refactoring improves readability and maintainability | 12 documented improvements (see table above) |
| Observations and improvements are documented | This README + inline comments in `solution.py` |
| Modified code fulfills its intended purpose | Both versions produce the same analysis results |
| Code compiles and passes quality checks | `solution.py` runs successfully, produces output + logs |
| Refactoring decisions are documented with trade-offs | Each change has a "Problem → Solution → Benefit" mapping |

## Implementation Notes

- Both files produce the **same analytical results** — the refactoring changes
  structure and performance without altering functionality.
- The `solution_before.py` is intentionally kept as-is (no fixes) to serve as
  a clear baseline for comparison.
- Key Spark performance wins: `.cache()` avoids 6x CSV re-read; Spark-native
  aggregations replace `collect()` loops; single-pass tier classification
  replaces 3 separate filter scans.
