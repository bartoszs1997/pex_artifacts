# 09 — Parametrized Unit Tests

## Task

Implement parameterized unit tests for an Apache Spark application. Identify
testable functionality and create at least three parametrized tests with
different input values covering various scenarios and edge cases.

## Results

| Metric | Value |
|--------|-------|
| Total parametrized tests | 39 |
| Test classes | 6 |
| Functions tested | 6 |
| All passed | Yes |

## Dataset

**Employee Salaries** (`ds_salaries.csv`) — 607 rows, 12 columns.
Source: [Kaggle](https://www.kaggle.com/datasets/inductiveanks/employee-salaries-for-different-job-roles)

## Layout

```
09_parametrized_unit_tests/
├── download_data.py      # Kaggle download script
├── solution.py           # PySpark application with testable functions
├── test_solution.py      # 39 parametrized unit tests
├── README.md
├── data/
│   ├── input/            # ds_salaries.csv
│   └── output/           # Analysis results
└── logs/
    └── parametrized_unit_tests.log
```

## Prerequisites

- uv (package manager)
- Java 17: `export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"`
- Kaggle API token at `~/.kaggle/kaggle.json`

## Run

```bash
# Download data
uv run python src/programming_languages/09_parametrized_unit_tests/download_data.py

# Run application
uv run python src/programming_languages/09_parametrized_unit_tests/solution.py

# Run tests
cd src/programming_languages/09_parametrized_unit_tests
uv run coverage run --source=solution -m pytest test_solution.py -vv
uv run coverage report --show-missing
```

## `solution.py` walkthrough

| Function | Logic |
|----------|-------|
| `filter_by_salary_range` | Filters rows where salary_in_usd is within [min, max] |
| `categorize_experience` | Maps experience_level codes (EN/MI/SE/EX) to labels |
| `classify_salary_tier` | Assigns Low/Mid/High tier based on salary thresholds |
| `aggregate_by_column` | Groups by any column, computes avg/min/max/count |
| `filter_by_remote_ratio` | Filters by remote_ratio value (0, 50, 100) |
| `top_n_job_titles` | Returns top N job titles by average salary |
| `main()` | Orchestrates load, transform, aggregate, save |

## `test_solution.py` walkthrough

| Test Class | Parametrized Over | Cases |
|------------|-------------------|-------|
| `TestFilterBySalaryRange` | (min_sal, max_sal, expected_count) | 7 tests: ranges, boundaries, no-match |
| `TestCategorizeExperience` | (level, expected_category) | 7 tests: all 4 valid + 3 unknown |
| `TestClassifySalaryTier` | (salary, expected_tier) | 7 tests: boundary values for Low/Mid/High |
| `TestAggregateByColumn` | (group_col, expected_groups) | 7 tests: different grouping columns |
| `TestFilterByRemoteRatio` | (ratio, expected_count) | 4 tests: 0, 50, 100, no-match |
| `TestTopNJobTitles` | (n, expected_count) | 7 tests: various N, ordering check |

## Acceptance Criteria

| Criterion | How Met |
|-----------|---------|
| At least 3 parametrized unit tests with different input values | 39 parametrized tests across 6 classes |
| Unit testing framework is used | pytest with `@pytest.mark.parametrize` |
| Setup and teardown methods implemented | Session-scoped `spark` fixture (setup: create SparkSession, teardown: stop) |
| All test cases pass | 39 passed in 6.41s |
| Different input scenarios and edge cases covered | Boundaries, empty results, unknown values, N exceeding data |

## Implementation Notes

- All transformation functions are **pure** (input DataFrame -> output DataFrame),
  making them ideal for parametrized testing.
- Tests use a small in-memory DataFrame (7 rows) — no disk I/O needed.
- `@pytest.mark.parametrize` `ids` parameter provides readable test names.
- Session-scoped SparkSession ensures Spark starts only once across all 39 tests.
