"""bp/task-01 — Read structured and semi-structured data with PySpark.

Three required queries on the TV Series JSON dataset:
  Q1: Names of `created_by` whose series status is "Canceled".
  Q2: All `origin_country` values where popularity > 5.0.
  Q3: Names of series with `number_of_episodes < 100`.

Optional JDBC join: when --postgres is passed, reads a `series_ratings` table
from the local PostgreSQL container (Docker, port 5433) and joins it on series id.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bp.task01")

DEFAULT_JSON = Path(__file__).resolve().parent / "data" / "tvs.json"
PG_URL = "jdbc:postgresql://localhost:5433/peex"
PG_PROPS = {"user": "peex", "password": "peex123", "driver": "org.postgresql.Driver"}


def build_spark(jdbc_jar: str | None = None) -> SparkSession:
    builder = SparkSession.builder.appName("bp-task01").master("local[*]")
    if jdbc_jar:
        builder = builder.config("spark.jars", jdbc_jar)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_series(spark: SparkSession, json_path: Path) -> DataFrame:
    return spark.read.option("multiLine", True).json(str(json_path))


def q1_canceled_creators(df: DataFrame) -> DataFrame:
    """Names of created_by where status = 'Canceled'."""
    return (
        df.filter(F.col("status") == "Canceled")
        .select(F.explode("created_by").alias("creator"))
        .select(F.col("creator.name").alias("creator_name"))
        .distinct()
        .orderBy("creator_name")
    )


def q2_popular_origin_countries(df: DataFrame) -> DataFrame:
    """origin_country values where popularity > 5.0."""
    return (
        df.filter(F.col("popularity") > 5.0)
        .select(F.explode("origin_country").alias("country"))
        .distinct()
        .orderBy("country")
    )


def q3_short_series(df: DataFrame) -> DataFrame:
    """Series with fewer than 100 episodes."""
    return (
        df.filter(F.col("number_of_episodes") < 100)
        .select("name", "number_of_episodes")
        .orderBy("number_of_episodes")
    )


def jdbc_join_demo(spark: SparkSession, series: DataFrame) -> DataFrame:
    """Optional: join Spark DataFrame with PostgreSQL table via JDBC."""
    ratings = spark.read.jdbc(url=PG_URL, table="series_ratings", properties=PG_PROPS)
    return (
        series.join(ratings, series.id == ratings.series_id, "inner")
        .select(series.name, ratings.imdb_rating, ratings.viewers_millions)
        .orderBy(F.desc("imdb_rating"))
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", type=Path, default=DEFAULT_JSON)
    parser.add_argument(
        "--postgres",
        action="store_true",
        help="Run optional JDBC join (requires Docker PostgreSQL up)",
    )
    parser.add_argument(
        "--jdbc-jar",
        default=None,
        help="Path to postgresql JDBC jar (only with --postgres)",
    )
    args = parser.parse_args()

    spark = build_spark(args.jdbc_jar if args.postgres else None)
    try:
        series = read_series(spark, args.json)
        log.info("Loaded %d series rows", series.count())
        series.printSchema()

        log.info("== Q1: creators of Canceled series ==")
        q1_canceled_creators(series).show(truncate=False)

        log.info("== Q2: origin countries with popularity > 5.0 ==")
        q2_popular_origin_countries(series).show(truncate=False)

        log.info("== Q3: series with < 100 episodes ==")
        q3_short_series(series).show(truncate=False)

        if args.postgres:
            log.info("== Optional: JDBC join with series_ratings ==")
            jdbc_join_demo(spark, series).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
