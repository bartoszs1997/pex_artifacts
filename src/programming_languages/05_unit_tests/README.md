# 05 — Implement Unit Tests

## Task

Develop a data processing script using Apache Spark and implement unit tests
with **pytest** to verify correctness of the code under different scenarios.

## Results

| Test Class | Tests | Status |
|------------|-------|--------|
| TestColumnCount | 3 | PASSED |
| TestAmountReceived | 4 | PASSED |
| TestTimestampFormat | 4 | PASSED |
| TestPaymentFormat | 5 | PASSED |
| **Total** | **16** | **ALL PASSED** |

## Dataset

| Source | Kaggle: `ealtman2019/ibm-transactions-for-anti-money-laundering-aml` |
|--------|----------------------------------------------------------------------|
| File   | `HI-Large_Trans.csv` (~22.3M rows, 11 columns) |
| Link   | https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml |

### Schema

| Column | Type | Description |
|--------|------|-------------|
| Timestamp | String | Transaction datetime (yyyy/MM/dd HH:mm) |
| From Bank | String | Originating bank code |
| Account (from) | String | Sender account ID |
| To Bank | String | Destination bank code |
| Account (to) | String | Receiver account ID |
| Amount Received | Double | Amount received (float) |
| Receiving Currency | String | Currency of received amount |
| Amount Paid | Double | Amount paid |
| Payment Currency | String | Currency of payment |
| Payment Format | String | ACH, Bitcoin, Cash, Cheque, Credit Card, Reinvestment, Wire |
| Is Laundering | Integer | 0 or 1 flag |

## Project Structure

```
05_unit_tests/
├── download_data.py      # Downloads HI-Large_Trans.csv from Kaggle
├── solution.py           # Data processing functions + main pipeline
├── test_solution.py      # Pytest unit tests (16 tests, 4 classes)
├── README.md             # This file
├── data/
│   ├── input/            # Raw CSV (gitignored)
│   └── output/           # Processing results (gitignored)
└── logs/
    └── unit_tests.log    # Execution log (gitignored)
```

## Prerequisites

- Python 3.13, uv
- Java 17: `export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"`
- Kaggle API credentials (`~/.kaggle/kaggle.json`)
- pytest (installed via `uv sync --extra dev`)

## Run

```bash
# 1. Download data
uv run python src/programming_languages/05_unit_tests/download_data.py

# 2. Run solution (data processing pipeline)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/programming_languages/05_unit_tests/solution.py

# 3. Run unit tests
cd src/programming_languages/05_unit_tests
uv run pytest test_solution.py -v
```

## solution.py Walkthrough

| Function | Purpose |
|----------|---------|
| `load_transactions()` | Loads CSV with explicit StructType schema |
| `get_column_count()` | Returns number of columns in DataFrame |
| `validate_amount_received_is_float()` | Checks schema type is DoubleType |
| `validate_timestamp_format()` | Parses timestamps with `try_to_timestamp` |
| `get_invalid_timestamps()` | Counts rows where timestamp parsing fails |
| `is_valid_payment_format()` | Checks if format is in the valid list |
| `get_payment_format_summary()` | Groups by Payment Format with counts |
| `main()` | Orchestrates all validations and saves summary |

## Acceptance Criteria

| Criterion | How It Is Met |
|-----------|---------------|
| Unit tests implemented with pytest | `test_solution.py` — 16 tests in 4 classes |
| Correct column count retrieval | `TestColumnCount::test_column_count_equals_expected` asserts 11 columns |
| Amount Received contains float values | `TestAmountReceived::test_amount_received_is_double_type` checks DoubleType |
| Timestamp has datetime format | `TestTimestampFormat::test_no_invalid_timestamps` — 0 invalid in 22M rows |
| Reinvestment is valid payment format | `TestPaymentFormat::test_reinvestment_is_valid` + `test_reinvestment_exists_in_data` |

## Implementation Notes

- **pytest** chosen over unittest for cleaner syntax, fixtures, and better output.
- `try_to_timestamp` (PySpark 4.x) used instead of `to_timestamp` — the latter
  throws exceptions on invalid input in strict ANSI mode rather than returning null.
- Tests use a session-scoped SparkSession fixture to avoid re-creating Spark
  for each test (16 tests run in ~19s).
- The test file imports functions from `solution.py` to test them in isolation.
- Both real-data tests (on 22M rows) and synthetic-data tests (edge cases with
  crafted DataFrames) are included.
