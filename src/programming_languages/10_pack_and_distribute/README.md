# 10 — Pack and Distribute Own Project

## Task

Pack and distribute an Apache Spark project as a Python package (wheel).
Create a well-structured package with metadata, dependencies, and build
instructions that can be installed and used by others.

## Package: `salary-analyzer`

A minimal PySpark library providing reusable salary data analysis functions.

## Project Structure

```
10_pack_and_distribute/
├── salary_analyzer/          # Python package (importable)
│   ├── __init__.py           # Package init, exports SalaryAnalyzer
│   └── analyzer.py           # Core module with PySpark logic
├── pyproject.toml            # Package metadata, dependencies, build config
├── README.md                 # Documentation
└── dist/                     # Built artifacts (wheel + sdist)
    ├── salary_analyzer-0.1.0-py3-none-any.whl
    └── salary_analyzer-0.1.0.tar.gz
```

## Prerequisites

- Python >= 3.10
- Java 17: `export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"`
- uv (or pip)

## How to Build

```bash
cd src/programming_languages/10_pack_and_distribute

# Build wheel and sdist
uv run python -m build
```

This produces two files in `dist/`:
- `salary_analyzer-0.1.0-py3-none-any.whl` — wheel (binary distribution)
- `salary_analyzer-0.1.0.tar.gz` — sdist (source distribution)

## How to Install

```bash
# From wheel file (on any machine with Python + Java)
pip install dist/salary_analyzer-0.1.0-py3-none-any.whl

# Or directly from source
pip install .

# Or as editable (for development)
pip install -e .
```

## How to Use

```python
from pyspark.sql import SparkSession
from salary_analyzer import SalaryAnalyzer

spark = SparkSession.builder.appName("Example").master("local[*]").getOrCreate()

analyzer = SalaryAnalyzer(spark)
df = analyzer.load_csv("/path/to/ds_salaries.csv")

# Average salary by experience level
result = analyzer.avg_salary_by(df, "experience_level")
result.show()

# Top 5 jobs by salary
top_jobs = analyzer.top_n_jobs(df, n=5)
top_jobs.show()

spark.stop()
```

## How to Distribute

Options for sharing the package:
1. **Direct file sharing** — copy the `.whl` file to another machine
2. **Private PyPI** — upload with `twine upload dist/*`
3. **Git dependency** — `pip install git+https://github.com/user/repo.git#subdirectory=src/programming_languages/10_pack_and_distribute`

## spark-submit Usage

```bash
spark-submit --py-files dist/salary_analyzer-0.1.0-py3-none-any.whl my_job.py
```

## Acceptance Criteria

| Criterion | How Met |
|-----------|---------|
| Dependencies identified | `pyspark>=3.5.0` in `[project.dependencies]` |
| Build tool used | `hatchling` (PEP 517 compliant), invoked via `python -m build` |
| All required files included | Package contains `__init__.py`, `analyzer.py`, metadata |
| Can be distributed and installed on other machines | `.whl` is self-contained, installable via `pip install` |
| Steps documented | This README covers build, install, use, and distribute |

## Implementation Notes

- Uses `hatchling` as build backend — modern, fast, recommended by PyPA.
- `pyproject.toml` follows PEP 621 (standard metadata format).
- Wheel format (`-py3-none-any`) means: Python 3, no C extensions, any platform.
- PySpark is declared as dependency so `pip install` pulls it automatically.
