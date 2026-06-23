# 07 — Cover Code with Tests

## Task

Create a PySpark application and cover it with unit tests. Perform code coverage
analysis using pytest-cov/coverage.py, ensuring at least 80% coverage. Document
the process and include the coverage report as evidence.

## Results

| Metric | Value |
|--------|-------|
| Total statements | 91 |
| Statements missed | 5 |
| **Coverage** | **95%** |
| Tests | 30 passed |
| Time | ~7s |

### Missing lines

| Lines | Reason |
|-------|--------|
| 157 | `log.warning` branch — never triggered on clean data |
| 205-207 | `except Exception` block — pipeline does not fail |
| 216 | `sys.exit(main())` — `__name__` guard |

## Dataset

| Source | Kaggle: `inductiveanks/employee-salaries-for-different-job-roles` |
|--------|-------------------------------------------------------------------|
| File   | `ds_salaries.csv` (607 rows, 12 columns) |

## Project Structure

```
07_cover_code_with_tests/
├── download_data.py      # Downloads ds_salaries.csv from Kaggle
├── solution.py           # PySpark application (216 lines)
├── test_solution.py      # Pytest unit tests (30 tests)
├── README.md             # This file
├── htmlcov/              # HTML coverage report (gitignored)
├── data/
│   ├── input/            # Raw CSV (gitignored)
│   └── output/           # Processing results (gitignored)
└── logs/
    └── cover_code_with_tests.log
```

## Prerequisites

- Python 3.13, uv
- Java 17: `export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"`
- Kaggle API credentials (`~/.kaggle/kaggle.json`)
- Dev dependencies: `uv sync --extra dev` (pytest, pytest-cov, coverage)

## Run

```bash
# 1. Download data
uv run python src/programming_languages/07_cover_code_with_tests/download_data.py

# 2. Run solution
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/programming_languages/07_cover_code_with_tests/solution.py

# 3. Run tests with coverage
cd src/programming_languages/07_cover_code_with_tests
uv run coverage run --source=solution -m pytest test_solution.py -vv
uv run coverage report --show-missing
uv run coverage html
```

## Coverage Report

```
Name          Stmts   Miss  Cover   Missing
-------------------------------------------
solution.py      91      5    95%   157, 205-207, 216
-------------------------------------------
TOTAL            91      5    95%
```

HTML report generated in `htmlcov/index.html`.

## solution.py Walkthrough

| Function | Purpose |
|----------|---------|
| `load_data()` | Loads CSV with explicit StructType schema |
| `validate_column_count()` | Checks expected column count |
| `validate_no_null_salaries()` | Counts null salary values |
| `validate_experience_levels()` | Detects invalid experience levels |
| `validate_salary_range()` | Counts out-of-range salaries |
| `add_salary_band()` | Adds categorical salary band column |
| `filter_by_experience()` | Filters by experience level with validation |
| `filter_full_time()` | Keeps only FT employees |
| `avg_salary_by_experience()` | Aggregates avg salary per experience |
| `avg_salary_by_company_size()` | Aggregates avg salary per company size |
| `top_paying_jobs()` | Top N jobs by avg salary |
| `salary_stats()` | Overall min/max/avg/stddev/count |
| `main()` | Orchestrates pipeline |

## Acceptance Criteria

| Criterion | How It Is Met |
|-----------|---------------|
| Project structure and dependencies set up | `pyproject.toml` has pytest + pytest-cov in dev deps |
| Unit tests cover different components | 30 tests across 6 classes (loading, validation, transformation, aggregation, constants, main) |
| Tests run and coverage report generated | `uv run coverage run` + `coverage report` + `coverage html` |
| Coverage report analyzed (percentage determined) | 95% coverage documented above |
| Process documented with report as evidence | This README + `htmlcov/` directory |

## Implementation Notes

- Reference application: [spirom/spark-streaming-with-kafka](https://github.com/spirom/spark-streaming-with-kafka).
  We implemented an equivalent batch data processing pipeline in PySpark to
  enable coverage analysis with pytest-cov.
- `coverage.py` is used directly (`coverage run -m pytest`) rather than the
  `--cov` pytest plugin, for consistency with the project's testing conventions.
- Tests use synthetic DataFrames (8 rows) for deterministic, fast execution.
  Integration tests against the real 607-row dataset are also included.
- The 5 uncovered lines (5%) are: a warning branch on clean data, the exception
  handler (no failures), and the `__name__` guard — all acceptable misses.
