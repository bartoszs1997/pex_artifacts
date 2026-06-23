"""Design Patterns in a PySpark application.

Patterns implemented:
  1. Strategy Pattern — interchangeable analysis algorithms
  2. Builder Pattern  — step-by-step report configuration

Dataset: Employee Salaries (ds_salaries.csv, 607 rows, 12 columns)

Usage:
    uv run python src/programming_languages/08_design_patterns/solution.py
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_PATH = SCRIPT_DIR / "data" / "input" / "ds_salaries.csv"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"


# ===========================================================================
# STRATEGY PATTERN
# ===========================================================================
class AnalysisStrategy(ABC):
    """Abstract base — defines interface for salary analysis algorithms."""

    @abstractmethod
    def analyze(self, df: DataFrame) -> DataFrame:
        """Run analysis and return aggregated DataFrame."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable strategy name."""


class ByExperienceLevel(AnalysisStrategy):
    """Aggregate average salary by experience level."""

    @property
    def name(self) -> str:
        return "avg_salary_by_experience"

    def analyze(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("experience_level")
            .agg(
                F.round(F.avg("salary_in_usd"), 2).alias("avg_salary_usd"),
                F.count("*").alias("count"),
            )
            .orderBy(F.desc("avg_salary_usd"))
        )


class ByCompanySize(AnalysisStrategy):
    """Aggregate average salary by company size."""

    @property
    def name(self) -> str:
        return "avg_salary_by_company_size"

    def analyze(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("company_size")
            .agg(
                F.round(F.avg("salary_in_usd"), 2).alias("avg_salary_usd"),
                F.round(F.max("salary_in_usd"), 2).alias("max_salary_usd"),
                F.count("*").alias("count"),
            )
            .orderBy(F.desc("avg_salary_usd"))
        )


class ByRemoteRatio(AnalysisStrategy):
    """Aggregate average salary by remote work ratio."""

    @property
    def name(self) -> str:
        return "avg_salary_by_remote_ratio"

    def analyze(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("remote_ratio")
            .agg(
                F.round(F.avg("salary_in_usd"), 2).alias("avg_salary_usd"),
                F.count("*").alias("count"),
            )
            .orderBy("remote_ratio")
        )


# ===========================================================================
# BUILDER PATTERN
# ===========================================================================
@dataclass
class ReportConfig:
    """Immutable report configuration built by ReportBuilder."""

    strategies: list[AnalysisStrategy] = field(default_factory=list)
    filter_year: int | None = None
    min_salary: int | None = None
    output_format: str = "csv"


class ReportBuilder:
    """Builds a ReportConfig step-by-step (fluent interface)."""

    def __init__(self) -> None:
        self._strategies: list[AnalysisStrategy] = []
        self._filter_year: int | None = None
        self._min_salary: int | None = None
        self._output_format: str = "csv"

    def add_strategy(self, strategy: AnalysisStrategy) -> ReportBuilder:
        self._strategies.append(strategy)
        return self

    def filter_by_year(self, year: int) -> ReportBuilder:
        self._filter_year = year
        return self

    def filter_by_min_salary(self, amount: int) -> ReportBuilder:
        self._min_salary = amount
        return self

    def set_output_format(self, fmt: str) -> ReportBuilder:
        self._output_format = fmt
        return self

    def build(self) -> ReportConfig:
        if not self._strategies:
            raise ValueError("At least one strategy must be added.")
        return ReportConfig(
            strategies=list(self._strategies),
            filter_year=self._filter_year,
            min_salary=self._min_salary,
            output_format=self._output_format,
        )


# ===========================================================================
# REPORT EXECUTOR
# ===========================================================================
class ReportExecutor:
    """Applies filters from config and runs each strategy."""

    def __init__(self, spark: SparkSession, config: ReportConfig) -> None:
        self.spark = spark
        self.config = config

    def load_data(self) -> DataFrame:
        df = self.spark.read.csv(str(INPUT_PATH), header=True, inferSchema=True)
        log.info("Loaded %d rows from %s", df.count(), INPUT_PATH.name)
        return df

    def apply_filters(self, df: DataFrame) -> DataFrame:
        if self.config.filter_year:
            df = df.filter(F.col("work_year") == self.config.filter_year)
            log.info("Filtered to year=%d -> %d rows", self.config.filter_year, df.count())
        if self.config.min_salary:
            df = df.filter(F.col("salary_in_usd") >= self.config.min_salary)
            log.info("Filtered min_salary>=%d -> %d rows", self.config.min_salary, df.count())
        return df

    def execute(self) -> dict[str, DataFrame]:
        df = self.load_data()
        df = self.apply_filters(df)

        results: dict[str, DataFrame] = {}
        for strategy in self.config.strategies:
            result = strategy.analyze(df)
            results[strategy.name] = result
            log.info("Strategy '%s' produced %d rows", strategy.name, result.count())
        return results

    def save_results(self, results: dict[str, DataFrame]) -> None:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        for name, df in results.items():
            path = OUTPUT_DIR / name
            df.coalesce(1).write.mode("overwrite").csv(str(path), header=True)
            log.info("Saved -> %s/", path.name)


# ===========================================================================
# MAIN
# ===========================================================================
def main() -> None:
    spark = SparkSession.builder.appName("DesignPatterns").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Builder pattern — construct report config
    config = (
        ReportBuilder()
        .add_strategy(ByExperienceLevel())
        .add_strategy(ByCompanySize())
        .add_strategy(ByRemoteRatio())
        .filter_by_year(2023)
        .filter_by_min_salary(50_000)
        .build()
    )

    # Execute
    executor = ReportExecutor(spark, config)
    results = executor.execute()
    executor.save_results(results)

    # Print summary
    for name, df in results.items():
        print(f"\n{'='*60}\n{name}\n{'='*60}")
        df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
