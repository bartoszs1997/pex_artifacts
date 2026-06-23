"""Unit tests for design patterns implementation.

Tests cover:
  - Strategy pattern (each concrete strategy + polymorphism)
  - Builder pattern (fluent API, validation, defaults)
  - ReportExecutor (filtering, execution, integration)
"""

import pytest
from unittest.mock import MagicMock, patch
from dataclasses import fields

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from solution import (
    AnalysisStrategy,
    ByExperienceLevel,
    ByCompanySize,
    ByRemoteRatio,
    ReportBuilder,
    ReportConfig,
    ReportExecutor,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("TestDesignPatterns")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    """Small in-memory DataFrame mimicking ds_salaries schema."""
    data = [
        (2023, "SE", "FT", "Data Engineer", 120000, "USD", 120000, "US", 100, "US", "M"),
        (2023, "MI", "FT", "Data Scientist", 80000, "USD", 80000, "US", 50, "US", "L"),
        (2023, "EN", "FT", "ML Engineer", 60000, "USD", 60000, "DE", 0, "DE", "S"),
        (2022, "SE", "FT", "Data Engineer", 110000, "USD", 110000, "US", 100, "US", "L"),
        (2022, "MI", "PT", "Analyst", 40000, "USD", 40000, "UK", 0, "UK", "M"),
    ]
    columns = [
        "work_year", "experience_level", "employment_type", "job_title",
        "salary", "salary_currency", "salary_in_usd", "employee_residence",
        "remote_ratio", "company_location", "company_size",
    ]
    return spark.createDataFrame(data, columns)


# ---------------------------------------------------------------------------
# Strategy Pattern Tests
# ---------------------------------------------------------------------------
class TestStrategyPattern:
    """Tests for Strategy pattern implementation."""

    def test_strategy_is_abstract(self):
        """AnalysisStrategy cannot be instantiated directly."""
        with pytest.raises(TypeError):
            AnalysisStrategy()  # type: ignore

    def test_by_experience_level_name(self):
        assert ByExperienceLevel().name == "avg_salary_by_experience"

    def test_by_company_size_name(self):
        assert ByCompanySize().name == "avg_salary_by_company_size"

    def test_by_remote_ratio_name(self):
        assert ByRemoteRatio().name == "avg_salary_by_remote_ratio"

    def test_by_experience_level_analyze(self, sample_df):
        result = ByExperienceLevel().analyze(sample_df)
        rows = result.collect()
        levels = {r["experience_level"] for r in rows}
        assert levels == {"SE", "MI", "EN"}

    def test_by_company_size_analyze(self, sample_df):
        result = ByCompanySize().analyze(sample_df)
        rows = result.collect()
        sizes = {r["company_size"] for r in rows}
        assert sizes == {"S", "M", "L"}
        # Check max_salary column exists
        assert "max_salary_usd" in result.columns

    def test_by_remote_ratio_analyze(self, sample_df):
        result = ByRemoteRatio().analyze(sample_df)
        rows = result.collect()
        ratios = sorted(r["remote_ratio"] for r in rows)
        assert ratios == [0, 50, 100]

    def test_strategies_share_interface(self, sample_df):
        """All strategies implement the same interface (polymorphism)."""
        strategies = [ByExperienceLevel(), ByCompanySize(), ByRemoteRatio()]
        for s in strategies:
            assert isinstance(s, AnalysisStrategy)
            result = s.analyze(sample_df)
            assert isinstance(result, DataFrame)
            assert result.count() > 0


# ---------------------------------------------------------------------------
# Builder Pattern Tests
# ---------------------------------------------------------------------------
class TestBuilderPattern:
    """Tests for Builder pattern implementation."""

    def test_build_minimal(self):
        config = ReportBuilder().add_strategy(ByExperienceLevel()).build()
        assert len(config.strategies) == 1
        assert config.filter_year is None
        assert config.min_salary is None
        assert config.output_format == "csv"

    def test_build_full(self):
        config = (
            ReportBuilder()
            .add_strategy(ByExperienceLevel())
            .add_strategy(ByCompanySize())
            .filter_by_year(2023)
            .filter_by_min_salary(50000)
            .set_output_format("parquet")
            .build()
        )
        assert len(config.strategies) == 2
        assert config.filter_year == 2023
        assert config.min_salary == 50000
        assert config.output_format == "parquet"

    def test_build_without_strategy_raises(self):
        with pytest.raises(ValueError, match="At least one strategy"):
            ReportBuilder().build()

    def test_fluent_interface_returns_self(self):
        builder = ReportBuilder()
        assert builder.add_strategy(ByExperienceLevel()) is builder
        assert builder.filter_by_year(2023) is builder
        assert builder.filter_by_min_salary(100) is builder
        assert builder.set_output_format("json") is builder

    def test_report_config_is_dataclass(self):
        """ReportConfig uses dataclass for clean structure."""
        field_names = {f.name for f in fields(ReportConfig)}
        assert "strategies" in field_names
        assert "filter_year" in field_names


# ---------------------------------------------------------------------------
# ReportExecutor Tests
# ---------------------------------------------------------------------------
class TestReportExecutor:
    """Tests for ReportExecutor (integrates Strategy + Builder)."""

    def test_apply_filters_year(self, spark, sample_df):
        config = ReportConfig(strategies=[ByExperienceLevel()], filter_year=2023)
        executor = ReportExecutor(spark, config)
        filtered = executor.apply_filters(sample_df)
        years = [r["work_year"] for r in filtered.collect()]
        assert all(y == 2023 for y in years)

    def test_apply_filters_min_salary(self, spark, sample_df):
        config = ReportConfig(strategies=[ByExperienceLevel()], min_salary=70000)
        executor = ReportExecutor(spark, config)
        filtered = executor.apply_filters(sample_df)
        salaries = [r["salary_in_usd"] for r in filtered.collect()]
        assert all(s >= 70000 for s in salaries)

    def test_apply_filters_combined(self, spark, sample_df):
        config = ReportConfig(
            strategies=[ByExperienceLevel()], filter_year=2023, min_salary=70000
        )
        executor = ReportExecutor(spark, config)
        filtered = executor.apply_filters(sample_df)
        rows = filtered.collect()
        assert all(r["work_year"] == 2023 and r["salary_in_usd"] >= 70000 for r in rows)

    def test_apply_filters_none(self, spark, sample_df):
        """No filters applied when config has None values."""
        config = ReportConfig(strategies=[ByExperienceLevel()])
        executor = ReportExecutor(spark, config)
        filtered = executor.apply_filters(sample_df)
        assert filtered.count() == sample_df.count()

    @patch.object(ReportExecutor, "load_data")
    def test_execute_calls_all_strategies(self, mock_load, spark, sample_df):
        mock_load.return_value = sample_df
        config = (
            ReportBuilder()
            .add_strategy(ByExperienceLevel())
            .add_strategy(ByCompanySize())
            .build()
        )
        executor = ReportExecutor(spark, config)
        results = executor.execute()
        assert len(results) == 2
        assert "avg_salary_by_experience" in results
        assert "avg_salary_by_company_size" in results

    @patch.object(ReportExecutor, "load_data")
    def test_execute_with_custom_strategy(self, mock_load, spark, sample_df):
        """Can plug in a custom strategy (open/closed principle)."""
        mock_load.return_value = sample_df

        class TopEarners(AnalysisStrategy):
            @property
            def name(self):
                return "top_earners"

            def analyze(self, df):
                return df.orderBy(F.desc("salary_in_usd")).limit(2)

        config = ReportBuilder().add_strategy(TopEarners()).build()
        executor = ReportExecutor(spark, config)
        results = executor.execute()
        assert results["top_earners"].count() == 2
