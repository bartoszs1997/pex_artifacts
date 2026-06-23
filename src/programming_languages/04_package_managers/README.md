# 04 — Use Package Managers

## Objective

Demonstrate proficiency with Python package managers by installing an external
library (`requests`) via **uv** and using it alongside Apache Spark to retrieve
and process data.

## Dataset

| Source | Kaggle: `sauravkumaragarwal/user-datacsv` |
|--------|-------------------------------------------|
| File   | `final_user_scores.csv` (~125 MB, 109 606 rows) |
| Columns used | `name`, `artists`, `release_date`, `popularity`, `duration_ms`, `danceability` |

Values in numeric columns are normalised/standardised floats (not raw Spotify
values).

## Tasks

| # | Description | Result |
|---|-------------|--------|
| 1 | Artists with `popularity > 1` AND `release_date > "2020-01-01"` | 14 149 records |
| 2 | Songs with `duration_ms > 0` AND `danceability < 0` | 30 658 records |

## Package Manager Usage

```bash
# Install requests using uv (declared in pyproject.toml)
uv add requests
```

The `requests` library is used in `solution.py` → `verify_data_source()` to
perform an HTTP HEAD check against the Kaggle dataset URL before proceeding
with local Spark processing.

## How to Run

```bash
# 1. Download data (requires Kaggle API credentials)
uv run python src/programming_languages/04_package_managers/download_data.py

# 2. Run the solution
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/programming_languages/04_package_managers/solution.py
```

## Project Structure

```
04_package_managers/
├── download_data.py          # Downloads dataset from Kaggle
├── solution.py               # Main pipeline (requests + Spark)
├── README.md                 # This file
├── data/
│   ├── input/                # Raw CSV (gitignored)
│   └── output/               # Task results (gitignored)
└── logs/
    └── package_managers.log  # Execution log (gitignored)
```

## Key Concepts Demonstrated

- **uv** as a modern Python package manager (fast, deterministic)
- Installing third-party packages (`uv add requests`)
- Using `requests` for HTTP communication (HEAD request, status codes)
- Explicit `StructType` schema to enforce correct column types
- PySpark DataFrame filtering and CSV output
