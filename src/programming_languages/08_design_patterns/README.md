# 08 — Design Patterns in PySpark

## Objective

Implement a PySpark application using **at least two design patterns** that solve real problems in data processing.

## Design Patterns Used

### 1. Strategy Pattern

**Problem**: The application needs multiple interchangeable analysis algorithms (by experience level, by company size, by remote ratio) that share a common interface.

**Solution**: Abstract base class `AnalysisStrategy` defines the `analyze(df) -> DataFrame` interface. Concrete strategies (`ByExperienceLevel`, `ByCompanySize`, `ByRemoteRatio`) implement specific aggregation logic. New strategies can be added without modifying existing code (Open/Closed Principle).

```
AnalysisStrategy (ABC)
├── ByExperienceLevel
├── ByCompanySize
└── ByRemoteRatio
```

### 2. Builder Pattern

**Problem**: Report configuration has many optional parameters (year filter, salary filter, output format, list of strategies). Constructors with many optional args are hard to read and maintain.

**Solution**: `ReportBuilder` provides a fluent API to construct `ReportConfig` step-by-step. Validates that at least one strategy is provided before building. Separates construction from representation.

```python
config = (
    ReportBuilder()
    .add_strategy(ByExperienceLevel())
    .add_strategy(ByCompanySize())
    .filter_by_year(2023)
    .filter_by_min_salary(50_000)
    .build()
)
```

## Dataset

**Employee Salaries** (`ds_salaries.csv`) — 607 rows, 12 columns.  
Source: [Kaggle](https://www.kaggle.com/datasets/inductiveanks/employee-salaries-for-different-job-roles)

## Project Structure

```
08_design_patterns/
├── download_data.py      # Kaggle download script
├── solution.py           # Main application (Strategy + Builder patterns)
├── test_solution.py      # 19 unit tests
├── README.md
└── data/
    ├── input/            # ds_salaries.csv
    └── output/           # Analysis results
```

## How to Run

```bash
# Download data
uv run python src/programming_languages/08_design_patterns/download_data.py

# Run application
uv run python src/programming_languages/08_design_patterns/solution.py

# Run tests
cd src/programming_languages/08_design_patterns
uv run pytest test_solution.py -vv
```

## Test Results

```
19 passed in 6.88s
```

Tests cover:
- **Strategy pattern**: ABC enforcement, polymorphism, each concrete strategy
- **Builder pattern**: fluent API, validation, defaults, dataclass structure
- **ReportExecutor**: filtering logic, integration with strategies, extensibility
