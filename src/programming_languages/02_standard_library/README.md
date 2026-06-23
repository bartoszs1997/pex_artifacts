# 02 Standard Library — Instacart Orders Analysis

> Section: `programming_languages` | Task: *Use standard library of a programming language*

## Task

Use Python standard library modules (`re`, `datetime`, `math`, `os`,
`subprocess`, `zipfile`) alongside PySpark to clean and analyze the Instacart
customer orders dataset. Each subtask demonstrates a specific stdlib module.

## Results

| # | Module       | Operation                                          | Result (sample run)              |
|---|-------------|----------------------------------------------------|----------------------------------|
| 1 | `re`        | Identify morning orders (hours 05-10)              | 811 / 3220 orders (25.2%)        |
| 2 | `datetime`  | Compute `date_ordered` from `days_since_prior_order` | 3220 dates calculated          |
| 3 | `math`      | Count orders where `order_dow == 3`                | 425 orders                       |
| 4 | `os`        | Create `reports/` directory                        | Directory created                |
| 5 | `subprocess`| List files in current directory (`ls -la`)         | 5 files/dirs listed              |
| 6 | `zipfile`   | Compress `orders.csv` into `orders.zip`            | 63.8% compression ratio          |

## Dataset

- **Source**: [billyrohh/instacart_dataset](https://github.com/billyrohh/instacart_dataset/blob/master/orders.csv)
- **Rows**: ~3,220 (GitHub raw file)
- **Columns**:

| Column                 | Type    | Description                              |
|-----------------------|---------|------------------------------------------|
| `order_id`            | int     | Unique order identifier                  |
| `user_id`             | int     | Unique customer identifier               |
| `eval_set`            | string  | Dataset split (prior/train/test)         |
| `order_number`        | int     | Sequential order number per user         |
| `order_dow`           | int     | Day of week (0=Sunday .. 6=Saturday)     |
| `order_hour_of_day`   | string  | Hour of order (00-23, zero-padded)       |
| `days_since_prior_order` | int  | Days since previous order (nullable)     |

## Layout

```
02_standard_library/
├── download_data.py       # Downloads orders.csv from GitHub
├── solution.py            # Main script — 6 stdlib tasks
├── README.md              # This file
├── data/                  # gitignored
│   ├── input/
│   │   └── orders.csv
│   └── output/
│       ├── morning_orders/
│       └── date_ordered/
├── reports/               # gitignored — created by Task 4
│   └── orders.zip         # created by Task 6
└── logs/                  # gitignored
    └── standard_library.log
```

## Prerequisites

- Python 3.13 (via `.python-version`)
- `uv` package manager
- Java 17 on PATH:
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```

## Run

From the repository root:

```bash
# 1. Download data
uv run python src/programming_languages/02_standard_library/download_data.py

# 2. Run all 6 tasks
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/programming_languages/02_standard_library/solution.py
```

## `solution.py` Walkthrough

| Step | Module        | Function                     | Logic                                                                 |
|------|--------------|------------------------------|-----------------------------------------------------------------------|
| 1    | `re`         | `task1_morning_orders()`     | UDF applies `re.compile(r'^(0[5-9]\|10)$')` to `order_hour_of_day`; adds `morning_order` column ("yes"/"no") |
| 2    | `datetime`   | `task2_date_ordered()`       | UDF subtracts `timedelta(days=days_since_prior_order)` from reference date `2024-01-01`; adds `date_ordered` column |
| 3    | `math`       | `task3_total_dow_3()`        | Filters `order_dow == 3`, counts via RDD `mapPartitions` + `math.fsum`; logs `math.ceil`, `math.floor`, `math.log10` |
| 4    | `os`         | `task4_create_reports_dir()` | `os.makedirs()` creates `reports/`; `os.listdir()` + `os.path.isdir()` lists contents |
| 5    | `subprocess` | `task5_list_files_subprocess()` | `subprocess.run(["ls", "-la"], capture_output=True)` executes system command |
| 6    | `zipfile`    | `task6_compress_zip()`       | `zipfile.ZipFile(..., ZIP_DEFLATED)` compresses `orders.csv`; verifies with `zf.infolist()` |

## Acceptance Criteria

| Criterion                                                     | Met? | Evidence                                                       |
|--------------------------------------------------------------|------|----------------------------------------------------------------|
| Required standard library modules are imported                | Yes  | `import re, datetime, math, os, subprocess, zipfile`           |
| Dataset is loaded and contains all necessary columns          | Yes  | `spark.read.csv()` with explicit 7-column schema; `.printSchema()` confirms |
| `re` module extracts morning hours and creates morning_order  | Yes  | `MORNING_PATTERN = re.compile(...)` applied via UDF            |
| `datetime` module creates date_ordered column                 | Yes  | `datetime.timedelta` subtraction in UDF                        |
| `math` module calculates total order_dow == 3                 | Yes  | `math.fsum()` across RDD partitions                            |
| `os` module creates reports directory                         | Yes  | `os.makedirs()`, `os.listdir()`, `os.path.exists()`           |
| `subprocess` module lists files via system command            | Yes  | `subprocess.run(["ls", "-la"], capture_output=True, check=True)` |
| `zipfile` module compresses orders.csv into orders.zip        | Yes  | `zipfile.ZipFile(..., ZIP_DEFLATED)` with verification         |

## Implementation Notes

- **PySpark + stdlib**: Data is loaded via PySpark DataFrame with explicit schema.
  Tasks 1-3 use UDFs and RDD operations that internally call stdlib modules.
  Tasks 4-6 are pure stdlib operations on the filesystem.
- **RDD usage**: Task 3 converts DataFrame to RDD and uses `mapPartitions` to
  demonstrate RDD-level processing (as referenced in the task's prerequisites).
- **Reference date**: Task 2 uses `2024-01-01` as an arbitrary anchor date.
  The `days_since_prior_order` column represents a relative offset, so the
  absolute dates are illustrative rather than historical.
- **Regex pattern**: `^(0[5-9]|10)$` matches exactly the zero-padded strings
  "05", "06", "07", "08", "09", "10" — the morning hour range specified.
- **Dataset size**: The GitHub raw file contains ~3,220 rows (subset). The full
  Instacart dataset has ~3.4M rows but is not hosted at the referenced URL.
