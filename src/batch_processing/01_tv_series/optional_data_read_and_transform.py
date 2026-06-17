"""
Read a MySQL table and the JSON series file with Spark, join them on the series
id, and run a basic aggregation on the joined data.

Pipeline:
    MySQL.series_ratings (series_id, imdb_rating, viewers_millions)
        |  join on series_ratings.series_id == series.id
    JSON series (id, name, status, number_of_episodes, popularity)
        |
        v  groupBy(status) -> count + avg(imdb_rating) + sum(viewers) + sum(episodes)

The MySQL JDBC driver is pulled from Maven automatically via
spark.jars.packages, so there is no jar file to download or commit.

Credentials are read from a gitignored `.env` file at the repo root
(MYSQL_USER / MYSQL_PASSWORD); copy `.env.example` to `.env` and fill it in.

Run (from the repo root):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"   # Java 17 for PySpark 4.x
    uv run python src/batch_processing/01_tv_series/optional_data_read_and_transform.py
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_JSON = Path(__file__).resolve().parent / "data" / "input" / "tvs.json"
OUTPUT_DIR = Path(__file__).resolve().parent / "data" / "output"

DB_NAME = "pex"
JDBC_URL = f"jdbc:mysql://localhost:3306/{DB_NAME}"
RATINGS_TABLE = "series_ratings"
MYSQL_CONNECTOR = "com.mysql:mysql-connector-j:9.0.0"


def create_spark_session() -> SparkSession:
    """Create a SparkSession that can talk to MySQL over JDBC."""
    return (
        SparkSession.builder.appName("mysql-spark")
        # Pull the MySQL JDBC driver from Maven (no local jar to manage).
        .config("spark.jars.packages", MYSQL_CONNECTOR)
        # Force loopback networking so Spark's internal block transfers stay on
        # 127.0.0.1 (avoids the flaky LAN-IP shuffle failures seen on macOS).
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def get_mysql_connection_properties() -> dict[str, str]:
    """Retrieve MySQL connection properties from environment variables."""

    return {
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "driver": "com.mysql.cj.jdbc.Driver",
    }


def read_mysql_data(spark: SparkSession, jdbc_url: str, table: str, properties: dict) -> DataFrame:
    """Read data from MySQL database using Spark."""

    return spark.read.jdbc(url=jdbc_url, table=table, properties=properties)


def read_json_data(spark: SparkSession, file_path: str) -> DataFrame:
    """Read JSON data from a file using Spark."""

    return spark.read.json(file_path, multiLine=True)


def join_ratings_with_series(ratings: DataFrame, series: DataFrame) -> DataFrame:
    """Join the MySQL ratings with the JSON series on series_id == id."""
    series_fields = series.select(
        "id",
        "name",
        "status",
        "number_of_episodes",
        "popularity",
    )
    return ratings.join(series_fields, ratings.series_id == series_fields.id, "inner")


def aggregate_by_status(joined: DataFrame) -> DataFrame:
    """Group the joined data by status and compute count / sum / avg."""
    return (
        joined.groupBy("status")
        .agg(
            F.count("*").alias("series_count"),
            F.round(F.avg("imdb_rating"), 2).alias("avg_imdb_rating"),
            F.round(F.sum("viewers_millions"), 2).alias("total_viewers_millions"),
            F.sum("number_of_episodes").alias("total_episodes"),
        )
        .orderBy(F.desc("series_count"))
    )


def main() -> None:
    """Read, join and aggregate, then display and save the results."""
    load_dotenv()  # read MYSQL_USER / MYSQL_PASSWORD from the repo-root .env
    spark: SparkSession = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        properties: dict[str, str] = get_mysql_connection_properties()
        ratings: DataFrame = read_mysql_data(spark, JDBC_URL, RATINGS_TABLE, properties)
        series: DataFrame = read_json_data(spark, str(DEFAULT_JSON))

        joined: DataFrame = join_ratings_with_series(ratings, series)
        print("== joined data (ratings + series) ==")
        joined.select(
            "series_id", "name", "status", "imdb_rating", "viewers_millions", "number_of_episodes"
        ).orderBy(F.desc("imdb_rating")).show(truncate=False)

        by_status: DataFrame = aggregate_by_status(joined)
        print("== aggregation: grouped by status ==")
        by_status.show(truncate=False)

        out: str = str(OUTPUT_DIR / "ratings_by_status")
        print(f"== writing aggregation to {out} ==")
        by_status.write.mode("overwrite").csv(out, header=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()