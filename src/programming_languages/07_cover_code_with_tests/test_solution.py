"""Unit tests for the salary data processing application.

Run from repo root:
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    cd src/programming_languages/07_cover_code_with_tests
    uv run coverage run --source=solution -m pytest test_solution.py -vv
    uv run coverage report --show-missing
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

from solution import (
    SALARY_SCHEMA,
    VALID_COMPANY_SIZES,
    VALID_EMPLOYMENT_TYPES,
    VALID_EXPERIENCE_LEVELS,
    add_salary_band,
    avg_salary_by_company_size,
    avg_salary_by_experience,
    filter_by_experience,
    filter_full_time,
    load_data,
    main,
    salary_stats,
    top_paying_jobs,
    validate_column_count,
    validate_experience_levels,
    validate_no_null_salaries,
    validate_salary_range,
)


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession."""
    session = (
        SparkSession.builder.appName("TestCoverage")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_df(spark):
    """Small sample DataFrame for testing."""
    data = [
        (0, 2022, "SE", "FT", "Data Engineer", 120000.0, "USD", 120000.0, "US", 100, "US", "L"),
        (1, 2022, "MI", "FT", "Data Scientist", 80000.0, "USD", 80000.0, "US", 50, "US", "M"),
        (2, 2021, "EN", "PT", "ML Engineer", 45000.0, "EUR", 50000.0, "DE", 0, "DE", "S"),
        (3, 2022, "EX", "FT", "Data Engineer", 250000.0, "USD", 250000.0, "US", 100, "US", "L"),
        (4, 2022, "SE", "FT", "Data Scientist", 150000.0, "USD", 150000.0, "US", 100, "US", "M"),
        (5, 2021, "MI", "CT", "Analytics Eng", 70000.0, "USD", 70000.0, "UK", 50, "UK", "M"),
        (6, 2022, "EN", "FT", "Data Analyst", 40000.0, "USD", 40000.0, "IN", 0, "IN", "S"),
        (7, 2022, "SE", "FT", "ML Engineer", 180000.0, "USD", 180000.0, "US", 100, "US", "L"),
    ]
    cols = ["_c0", "work_year", "experience_level", "employment_type",
            "job_title", "salary", "salary_currency", "salary_in_usd",
            "employee_residence", "remote_ratio", "company_location", "company_size"]
    return spark.createDataFrame(data, cols)


@pytest.fixture(scope="session")
def real_df(spark):
    """Load real dataset if available."""
    from pathlib import Path
    path = Path(__file__).resolve().parent / "data" / "input" / "ds_salaries.csv"
    if not path.exists():
        pytest.skip("Real data not available.")
    return load_data(spark, str(path))


# --- Loading ---

class TestLoadData:
    def test_load_returns_dataframe(self, real_df):
        assert real_df.count() > 0

    def test_schema_types(self, real_df):
        assert isinstance(real_df.schema["salary_in_usd"].dataType, DoubleType)
        assert isinstance(real_df.schema["work_year"].dataType, IntegerType)
        assert isinstance(real_df.schema["job_title"].dataType, StringType)


# --- Validation ---

class TestValidation:
    def test_column_count_correct(self, sample_df):
        assert validate_column_count(sample_df, 12) is True

    def test_column_count_incorrect(self, sample_df):
        assert validate_column_count(sample_df, 10) is False

    def test_no_null_salaries(self, sample_df):
        assert validate_no_null_salaries(sample_df) == 0

    def test_null_salaries_detected(self, spark):
        df = spark.createDataFrame([(None,), (50000.0,), (None,)], ["salary_in_usd"])
        assert validate_no_null_salaries(df) == 2

    def test_experience_levels_valid(self, sample_df):
        assert validate_experience_levels(sample_df) == []

    def test_experience_levels_invalid(self, spark):
        df = spark.createDataFrame([("XX",), ("EN",), ("ZZ",)], ["experience_level"])
        invalid = validate_experience_levels(df)
        assert "XX" in invalid and "ZZ" in invalid

    def test_salary_range_all_within(self, sample_df):
        assert validate_salary_range(sample_df, 0, 500000) == 0

    def test_salary_range_some_outside(self, sample_df):
        assert validate_salary_range(sample_df, 0, 100000) > 0


# --- Transformations ---

class TestTransformations:
    def test_salary_band_low(self, spark):
        df = spark.createDataFrame([(40000.0,)], ["salary_in_usd"])
        assert add_salary_band(df).first()["salary_band"] == "low"

    def test_salary_band_medium(self, spark):
        df = spark.createDataFrame([(75000.0,)], ["salary_in_usd"])
        assert add_salary_band(df).first()["salary_band"] == "medium"

    def test_salary_band_high(self, spark):
        df = spark.createDataFrame([(150000.0,)], ["salary_in_usd"])
        assert add_salary_band(df).first()["salary_band"] == "high"

    def test_salary_band_very_high(self, spark):
        df = spark.createDataFrame([(300000.0,)], ["salary_in_usd"])
        assert add_salary_band(df).first()["salary_band"] == "very_high"

    def test_filter_by_experience(self, sample_df):
        result = filter_by_experience(sample_df, "SE")
        assert result.count() == 3

    def test_filter_by_experience_invalid(self, sample_df):
        with pytest.raises(ValueError):
            filter_by_experience(sample_df, "XX")

    def test_filter_full_time(self, sample_df):
        result = filter_full_time(sample_df)
        assert result.count() == 6


# --- Aggregation ---

class TestAggregations:
    def test_avg_by_experience_groups(self, sample_df):
        assert avg_salary_by_experience(sample_df).count() == 4

    def test_avg_by_experience_values(self, sample_df):
        row = avg_salary_by_experience(sample_df).filter(
            F.col("experience_level") == "SE"
        ).first()
        assert row["avg_salary"] == pytest.approx(150000.0)
        assert row["count"] == 3

    def test_avg_by_company_size(self, sample_df):
        assert avg_salary_by_company_size(sample_df).count() == 3

    def test_top_paying_jobs_limit(self, sample_df):
        assert top_paying_jobs(sample_df, n=2).count() == 2

    def test_top_paying_jobs_ordered(self, sample_df):
        salaries = [r["avg_salary"] for r in top_paying_jobs(sample_df, n=5).collect()]
        assert salaries == sorted(salaries, reverse=True)

    def test_salary_stats_keys(self, sample_df):
        stats = salary_stats(sample_df)
        assert all(k in stats for k in ["min", "max", "avg", "stddev", "count"])

    def test_salary_stats_values(self, sample_df):
        stats = salary_stats(sample_df)
        assert stats["min"] == pytest.approx(40000.0)
        assert stats["max"] == pytest.approx(250000.0)
        assert stats["count"] == 8

    def test_salary_stats_empty(self, spark):
        df = spark.createDataFrame([], SALARY_SCHEMA)
        stats = salary_stats(df)
        assert stats["count"] == 0


# --- Constants ---

class TestConstants:
    def test_experience_levels(self):
        assert len(VALID_EXPERIENCE_LEVELS) == 4

    def test_employment_types(self):
        assert len(VALID_EMPLOYMENT_TYPES) == 4

    def test_company_sizes(self):
        assert len(VALID_COMPANY_SIZES) == 3


# --- Main function ---

class TestMain:
    def test_main_success(self):
        """main() should return 0 when data file exists."""
        result = main()
        assert result == 0

    def test_main_file_not_found(self, monkeypatch):
        """main() should return 1 when data file is missing."""
        from pathlib import Path
        monkeypatch.setattr("solution.DATA_FILE", Path("/nonexistent/file.csv"))
        result = main()
        assert result == 1
