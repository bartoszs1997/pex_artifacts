# 03 Debugging Tools — Health Insurance Rates Analysis

> Section: `programming_languages` | Task: *Use debugging tools*

## Task

Develop a script that performs data transformation using Apache Spark with
strategic logging statements to track execution flow. Demonstrate proper use
of Python's `logging` module as a debugging tool with multi-level output:

- **DEBUG** level: printed to terminal (full execution trace)
- **WARNING** level and above: saved to log file (persistent record of issues)
- **ERROR** level: defects identified and programmatically fixed

## Results

| Metric | Value |
|--------|-------|
| Total rows loaded | 12,694,445 |
| Rows after cleaning | 11,963,091 |
| Rows with zero rate (excluded) | 682,484 |
| Rows with extreme rate >$9,999 (excluded) | 48,870 |
| States analyzed | 39 |
| Age groups | 47 |
| Plans with extreme spread (>$5,000) | 8 |

## Dataset

- **Source**: [Kaggle — Health Insurance Marketplace](https://www.kaggle.com/datasets/hhs/health-insurance-marketplace)
- **File**: `Rate.csv` (~1.9 GB, 12.7M rows)
- **Columns** (24): BusinessYear, StateCode, IssuerId, SourceName, VersionNum,
  ImportDate, IssuerId2, FederalTIN, RateEffectiveDate, RateExpirationDate,
  PlanId, RatingAreaId, Tobacco, Age, IndividualRate, IndividualTobaccoRate,
  Couple, PrimarySubscriberAndOneDependent, PrimarySubscriberAndTwoDependents,
  PrimarySubscriberAndThreeOrMoreDependents, CoupleAndOneDependent,
  CoupleAndTwoDependents, CoupleAndThreeOrMoreDependents, RowNumber

## Layout

```
03_debugging_tools/
├── download_data.py       # Downloads Rate.csv from Kaggle
├── solution.py            # Main script — transformations + logging demo
├── README.md              # This file
├── data/                  # gitignored
│   ├── input/
│   │   └── Rate.csv
│   └── output/
│       ├── avg_rate_by_state/
│       ├── avg_rate_by_age/
│       └── rate_spread_by_plan/
└── logs/                  # gitignored
    └── debugging_tools.log
```

## Prerequisites

- Python 3.13 (via `.python-version`)
- `uv` package manager
- Java 17 on PATH:
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```
- Kaggle API token (`~/.kaggle/kaggle.json`)

## Run

From the repository root:

```bash
# 1. Download data
uv run python src/programming_languages/03_debugging_tools/download_data.py

# 2. Run the pipeline (DEBUG logs appear in terminal)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/programming_languages/03_debugging_tools/solution.py

# 3. Check log file (only WARNING+ entries)
cat src/programming_languages/03_debugging_tools/logs/debugging_tools.log
```

## `solution.py` Walkthrough

| Step | Function | Logging demonstrated |
|------|----------|---------------------|
| 1 | `load_data()` | DEBUG: file path, schema size, row count. ERROR: empty DataFrame or schema failure with fallback fix |
| 2 | `clean_null_values()` | DEBUG: null counts per column. WARNING: >10% nulls. ERROR: all-null column detected |
| 3 | `cast_rate_columns()` | DEBUG: cast success counts. ERROR: non-numeric values found + fix (cast to null). WARNING: >30% failures |
| 4 | `filter_invalid_rates()` | DEBUG: filter criteria, before/after counts. ERROR: negative rates (data corruption). WARNING: extreme outliers |
| 5 | `compute_avg_rate_by_state()` | DEBUG: aggregation details. WARNING: states with <100 records (unreliable averages) |
| 6 | `compute_avg_rate_by_age()` | DEBUG: unique age values. WARNING: non-standard age formats |
| 7 | `compute_rate_spread_by_plan()` | DEBUG: computation details. WARNING: extreme spread >$5,000 |

## Logging Architecture

```
Logger: "debugging_tools" (level=DEBUG)
   │
   ├── ConsoleHandler (sys.stdout)
   │       level = DEBUG
   │       → Prints ALL log messages to terminal
   │
   └── FileHandler (logs/debugging_tools.log)
           level = WARNING
           → Saves only WARNING, ERROR, CRITICAL to file
```

## Acceptance Criteria

| Criterion | Met? | Evidence |
|-----------|------|----------|
| Script performs data transformation with Apache Spark | Yes | 7 transformation steps: load, clean, cast, filter, 3x aggregate |
| Logging statements track execution flow | Yes | DEBUG logs at every step showing entry/exit, counts, states |
| All DEBUG logs printed to terminal | Yes | `ConsoleHandler.setLevel(logging.DEBUG)` — visible in stdout |
| WARNING+ saved to log file | Yes | `FileHandler.setLevel(logging.WARNING)` — only WARNING entries in log file |
| Specific WARNING messages | Yes | Each warning describes the issue, affected data, and impact |
| ERROR-level defects addressed with fixes | Yes | ERROR-001 to ERROR-007 each include "Fix:" description and code applies the fix |

## Error Codes

| Code | Description | Fix Applied |
|------|-------------|-------------|
| ERROR-001 | Empty DataFrame after loading | Abort pipeline |
| ERROR-002 | Schema mismatch on load | Fallback to `inferSchema` |
| ERROR-003 | Column entirely null | Remove from pipeline |
| ERROR-004 | Non-numeric values in rate columns | Cast to null, exclude from aggregations |
| ERROR-005 | Negative rate values | Remove affected rows |
| ERROR-006 | Input file not found | Prompt to run download_data.py |
| ERROR-007 | Unhandled exception | Log traceback for debugging |

## Implementation Notes

- **File size**: Rate.csv is ~1.9 GB (12.7M rows). Processing takes ~40s locally.
- **Dual logging pattern**: Logger level set to `DEBUG` (captures everything),
  then each handler filters independently. Console gets everything, file gets
  only WARNING+. This is the standard Python logging best practice.
- **Error handling philosophy**: Each ERROR log includes (1) error code,
  (2) description of what went wrong, (3) programmatic fix applied. The script
  continues execution after applying fixes rather than crashing.
- **Data quality findings**: ~731K rows excluded (682K zero-rate + 49K extreme).
  8 plans have rate spreads >$5,000 indicating potential data quality issues.
