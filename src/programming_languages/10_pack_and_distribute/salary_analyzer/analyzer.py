"""Core analysis module — PySpark transformations for salary data."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class SalaryAnalyzer:
    """Reusable salary data analyzer powered by PySpark.

    Usage:
        from salary_analyzer import SalaryAnalyzer

        analyzer = SalaryAnalyzer(spark)
        df = analyzer.load_csv("/path/to/salaries.csv")
        result = analyzer.avg_salary_by(df, "experience_level")
        result.show()
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def load_csv(self, path: str) -> DataFrame:
        """Load a CSV file with header into a DataFrame."""
        return self.spark.read.csv(path, header=True, inferSchema=True)

    def avg_salary_by(self, df: DataFrame, group_col: str) -> DataFrame:
        """Compute average salary grouped by a column."""
        return (
            df.groupBy(group_col)
            .agg(
                F.round(F.avg("salary_in_usd"), 2).alias("avg_salary"),
                F.count("*").alias("count"),
            )
            .orderBy(F.desc("avg_salary"))
        )

    def filter_salary_range(
        self, df: DataFrame, min_sal: int, max_sal: int
    ) -> DataFrame:
        """Filter rows where salary_in_usd is within [min_sal, max_sal]."""
        return df.filter(
            (F.col("salary_in_usd") >= min_sal) & (F.col("salary_in_usd") <= max_sal)
        )

    def top_n_jobs(self, df: DataFrame, n: int = 5) -> DataFrame:
        """Return top N job titles by average salary."""
        return (
            df.groupBy("job_title")
            .agg(F.round(F.avg("salary_in_usd"), 2).alias("avg_salary"))
            .orderBy(F.desc("avg_salary"))
            .limit(n)
        )
