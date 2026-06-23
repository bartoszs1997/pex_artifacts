"""Parametrized unit tests for the salary data processing application.

Demonstrates pytest.mark.parametrize with different input scenarios and edge
cases for PySpark transformation functions defined in solution.py.

Tests cover:
  - filter_by_salary_range (range boundaries, empty results)
  - categorize_experience (all 4 levels + unknown)
  - classify_salary_tier (boundary values)
  - aggregate_by_column (different grouping columns)
  - filter_by_remote_ratio (valid ratios + no-match)
  - top_n_job_titles (various N values)

Run from repo root:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    cd src/programming_languages/09_parametrized_unit_tests
    uv run coverage run --source=solution -m pytest test_solution.py -vv
    uv run coverage report --show-missing
"""

import pytest
from pyspark.sql import SparkSession

from solution import (
    aggregate_by_column,
    categorize_experience,
    classify_salary_tier,
    filter_by_remote_ratio,
    filter_by_salary_range,
    top_n_job_titles,
)


# ---------------------------------------------------------------------------
# Fixtures (setup / teardown)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession — created once, shared by all tests."""
    session = (
        SparkSession.builder.appName("TestParametrized")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_df(spark):
    """Reusable test DataFrame mimicking ds_salaries schema."""
    data = [
        (2022, "EN", "FT", "Data Analyst", 45000, "USD", 45000, "US", 0, "US", "S"),
        (2022, "MI", "FT", "Data Scientist", 85000, "USD", 85000, "US", 50, "US", "M"),
        (2022, "SE", "FT", "Data Engineer", 130000, "USD", 130000, "US", 100, "US", "L"),
        (2022, "EX", "FT", "Director of DS", 200000, "USD", 200000, "US", 100, "US", "L"),
        (2021, "SE", "FT", "ML Engineer", 120000, "USD", 120000, "DE", 50, "DE", "M"),
        (2021, "MI", "CT", "Data Analyst", 60000, "USD", 60000, "UK", 0, "UK", "S"),
        (2020, "EN", "FT", "Data Scientist", 70000, "USD", 70000, "IN", 0, "IN", "M"),
    ]
    columns = [
        "work_year", "experience_level", "employment_type", "job_title",
        "salary", "salary_currency", "salary_in_usd", "employee_residence",
        "remote_ratio", "company_location", "company_size",
    ]
    return spark.createDataFrame(data, columns)


# ---------------------------------------------------------------------------
# Parametrized Tests: filter_by_salary_range
# ---------------------------------------------------------------------------
class TestFilterBySalaryRange:
    """Parametrized tests for salary range filtering."""

    @pytest.mark.parametrize(
        "min_sal, max_sal, expected_count",
        [
            (50000, 100000, 3),       # MI:85k, MI:60k, EN:70k
            (100000, 200000, 3),      # SE:130k, EX:200k, SE:120k
            (0, 50000, 1),            # EN:45k
            (200000, 500000, 1),      # EX:200k (boundary inclusive)
            (300000, 500000, 0),      # no match
        ],
        ids=["mid-range", "high-range", "low-range", "upper-boundary", "no-match"],
    )
    def test_salary_range_filter(self, sample_df, min_sal, max_sal, expected_count):
        """Verify row count for different salary ranges."""
        result = filter_by_salary_range(sample_df, min_sal, max_sal)
        assert result.count() == expected_count

    @pytest.mark.parametrize(
        "min_sal, max_sal",
        [
            (45000, 45000),   # exact match — single value range
            (70000, 85000),   # tight range
        ],
        ids=["exact-value", "tight-range"],
    )
    def test_salary_range_all_within_bounds(self, sample_df, min_sal, max_sal):
        """All returned rows have salary within specified bounds."""
        result = filter_by_salary_range(sample_df, min_sal, max_sal)
        salaries = [r["salary_in_usd"] for r in result.collect()]
        assert all(min_sal <= s <= max_sal for s in salaries)


# ---------------------------------------------------------------------------
# Parametrized Tests: categorize_experience
# ---------------------------------------------------------------------------
class TestCategorizeExperience:
    """Parametrized tests for experience level categorization."""

    @pytest.mark.parametrize(
        "level, expected_category",
        [
            ("EN", "Junior"),
            ("MI", "Mid"),
            ("SE", "Senior"),
            ("EX", "Executive"),
        ],
        ids=["entry", "mid", "senior", "executive"],
    )
    def test_experience_mapping(self, spark, level, expected_category):
        """Each experience_level code maps to correct category."""
        df = spark.createDataFrame(
            [(level, 100000)], ["experience_level", "salary_in_usd"]
        )
        result = categorize_experience(df)
        row = result.collect()[0]
        assert row["experience_category"] == expected_category

    @pytest.mark.parametrize(
        "level",
        ["XX", "ZZ", ""],
        ids=["unknown-XX", "unknown-ZZ", "empty-string"],
    )
    def test_unknown_experience_returns_null(self, spark, level):
        """Unknown experience_level codes map to null."""
        df = spark.createDataFrame(
            [(level, 100000)], ["experience_level", "salary_in_usd"]
        )
        result = categorize_experience(df)
        row = result.collect()[0]
        assert row["experience_category"] is None


# ---------------------------------------------------------------------------
# Parametrized Tests: classify_salary_tier
# ---------------------------------------------------------------------------
class TestClassifySalaryTier:
    """Parametrized tests for salary tier classification."""

    @pytest.mark.parametrize(
        "salary, expected_tier",
        [
            (30000, "Low"),       # well below 70k
            (69999, "Low"),       # just below boundary
            (70000, "Mid"),       # exact lower boundary
            (95000, "Mid"),       # middle of Mid range
            (120000, "Mid"),      # exact upper boundary
            (120001, "High"),     # just above boundary
            (250000, "High"),     # well above
        ],
        ids=["low-clear", "low-boundary", "mid-lower", "mid-middle",
             "mid-upper", "high-boundary", "high-clear"],
    )
    def test_tier_assignment(self, spark, salary, expected_tier):
        """Salary value maps to correct tier, including boundaries."""
        df = spark.createDataFrame([(salary,)], ["salary_in_usd"])
        result = classify_salary_tier(df)
        row = result.collect()[0]
        assert row["salary_tier"] == expected_tier


# ---------------------------------------------------------------------------
# Parametrized Tests: aggregate_by_column
# ---------------------------------------------------------------------------
class TestAggregateByColumn:
    """Parametrized tests for aggregation with different group columns."""

    @pytest.mark.parametrize(
        "group_col, expected_groups",
        [
            ("experience_level", 4),    # EN, MI, SE, EX
            ("company_size", 3),        # S, M, L
            ("work_year", 3),           # 2020, 2021, 2022
            ("remote_ratio", 3),        # 0, 50, 100
        ],
        ids=["by-experience", "by-company-size", "by-year", "by-remote"],
    )
    def test_group_count(self, sample_df, group_col, expected_groups):
        """Aggregation produces expected number of groups."""
        result = aggregate_by_column(sample_df, group_col)
        assert result.count() == expected_groups

    @pytest.mark.parametrize(
        "group_col",
        ["experience_level", "company_size", "work_year"],
        ids=["experience", "company", "year"],
    )
    def test_aggregation_columns_present(self, sample_df, group_col):
        """Result always contains avg_salary, min_salary, max_salary, count."""
        result = aggregate_by_column(sample_df, group_col)
        expected_cols = {group_col, "avg_salary", "min_salary", "max_salary", "count"}
        assert set(result.columns) == expected_cols


# ---------------------------------------------------------------------------
# Parametrized Tests: filter_by_remote_ratio
# ---------------------------------------------------------------------------
class TestFilterByRemoteRatio:
    """Parametrized tests for remote ratio filtering."""

    @pytest.mark.parametrize(
        "ratio, expected_count",
        [
            (0, 3),     # 3 rows with remote_ratio=0
            (50, 2),    # 2 rows with remote_ratio=50
            (100, 2),   # 2 rows with remote_ratio=100
            (75, 0),    # no rows with remote_ratio=75
        ],
        ids=["fully-onsite", "hybrid", "fully-remote", "no-match"],
    )
    def test_remote_ratio_filter(self, sample_df, ratio, expected_count):
        """Correct row count for each remote_ratio value."""
        result = filter_by_remote_ratio(sample_df, ratio)
        assert result.count() == expected_count


# ---------------------------------------------------------------------------
# Parametrized Tests: top_n_job_titles
# ---------------------------------------------------------------------------
class TestTopNJobTitles:
    """Parametrized tests for top N job titles extraction."""

    @pytest.mark.parametrize(
        "n, expected_count",
        [
            (1, 1),
            (3, 3),
            (5, 5),     # sample has 5 distinct titles
            (10, 5),    # N > distinct titles returns all
        ],
        ids=["top-1", "top-3", "top-5", "n-exceeds-data"],
    )
    def test_top_n_count(self, sample_df, n, expected_count):
        """Returns min(n, distinct_titles) rows."""
        result = top_n_job_titles(sample_df, n)
        assert result.count() == expected_count

    @pytest.mark.parametrize(
        "n",
        [1, 3, 5],
        ids=["top-1", "top-3", "top-5"],
    )
    def test_results_ordered_descending(self, sample_df, n):
        """Results are sorted by avg_salary descending."""
        result = top_n_job_titles(sample_df, n)
        salaries = [r["avg_salary"] for r in result.collect()]
        assert salaries == sorted(salaries, reverse=True)
