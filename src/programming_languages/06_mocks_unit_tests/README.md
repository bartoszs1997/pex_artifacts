# 06 — Use Stubs and Mocks in Unit Tests

## Task

Implement a `calculate_average(numbers: list[float]) -> float` function that
processes salary data from an external service. Write unit tests using pytest
with stubs and mocks to isolate the function from the external data source.

## Results

| Test Class | Tests | Technique | Status |
|------------|-------|-----------|--------|
| TestCalculateAverage | 5 | Pure function (no mocks) | PASSED |
| TestCalculateAverageWithMockedService | 4 | MagicMock(spec=...) | PASSED |
| TestWithPatchDecorator | 3 | @patch.object decorator | PASSED |
| TestWithStubFixture | 5 | Stub via pytest fixture | PASSED |
| **Total** | **17** | | **ALL PASSED in 0.13s** |

## Dataset

| Source | Kaggle: `inductiveanks/employee-salaries-for-different-job-roles` |
|--------|-------------------------------------------------------------------|
| File   | `ds_salaries.csv` (607 rows, 12 columns) |
| Column | `salary_in_usd` — used as "Avg. Salary" equivalent |

## Project Structure

```
06_mocks_unit_tests/
├── download_data.py      # Downloads ds_salaries.csv from Kaggle
├── solution.py           # calculate_average() + SalaryDataService
├── test_solution.py      # Pytest with mocks/stubs (17 tests)
├── README.md             # This file
├── data/
│   ├── input/            # Raw CSV (gitignored)
│   └── output/           # Salary averages summary (gitignored)
└── logs/
    └── mocks_unit_tests.log
```

## Prerequisites

- Python 3.13, uv
- Java 17: `export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"`
- Kaggle API credentials (`~/.kaggle/kaggle.json`)
- pytest (via `uv sync --extra dev`)

## Run

```bash
# 1. Download data
uv run python src/programming_languages/06_mocks_unit_tests/download_data.py

# 2. Run solution (processes real data with Spark)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/programming_languages/06_mocks_unit_tests/solution.py

# 3. Run unit tests (mocks — no Spark or data needed)
cd src/programming_languages/06_mocks_unit_tests
uv run pytest test_solution.py -v
```

## solution.py Walkthrough

| Component | Purpose |
|-----------|---------|
| `calculate_average(numbers)` | Pure function — returns arithmetic mean |
| `SalaryDataService` | "External service" — reads CSV via Spark |
| `SalaryDataService.get_salaries()` | Returns all salary_in_usd values |
| `SalaryDataService.get_salaries_by_experience(level)` | Returns salaries filtered by experience |
| `main()` | Orchestrates: service retrieves data, calculates averages, saves |

## Mocking Techniques Used

| Technique | Where | Description |
|-----------|-------|-------------|
| `MagicMock(spec=SalaryDataService)` | TestCalculateAverageWithMockedService | Creates a mock that respects the service's interface |
| `.return_value = [...]` | All mock tests | Defines stub behavior (fixed salary list) |
| `.assert_called_once()` | Multiple tests | Verifies the mock was actually used |
| `@patch.object(Class, "method")` | TestWithPatchDecorator | Replaces method at class level |
| `.side_effect = lambda` | TestWithStubFixture | Dynamic stub returning different data per argument |
| `pytest.fixture` | stub_service fixture | Reusable stub shared across multiple tests |

## Acceptance Criteria

| Criterion | How It Is Met |
|-----------|---------------|
| Stub/mock simulates external service | `MagicMock(spec=SalaryDataService)` replaces real service |
| Test uses stub instead of actual service | All mock tests never call Spark or read CSV |
| Stub provides fixed list of numbers | `.return_value = [79833.0, 260000.0, ...]` |
| Assertions validate correct average | `assert result == pytest.approx(expected)` |
| Stubs and mocks used correctly | 3 techniques: MagicMock, @patch.object, fixture stub |

## Implementation Notes

- Tests run in **0.13s** because mocks eliminate Spark/IO overhead entirely.
- `MagicMock(spec=SalaryDataService)` ensures mock respects the class interface —
  calling a non-existent method raises `AttributeError`.
- `@patch.object` approach replaces the method at class level — useful when you
  instantiate the service normally but want to control what it returns.
- The stub fixture with `side_effect` demonstrates dynamic stubs that return
  different values depending on input arguments.
