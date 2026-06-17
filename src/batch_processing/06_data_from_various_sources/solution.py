"""Spark batch application: ingest & parse hotel data from various sources.

Task 06 — *Read and parse data from various data sources*. The dataset holds, for
each city, a single nested-JSON file that bundles three sources with DIFFERENT
shapes under one object:

    {
      "airbnbHotels":    [ {title, subtitles[], price{value,…}, rating, link}, … ],
      "bookingHotels":   [ {title, location, highlights[], price{…}, rating{score,…}, link}, … ],
      "hotelsComHotels": [ {title, location, snippet{text}, price{…}, rating{score,…}, link}, … ]
    }

The three sources disagree on where the description lives (subtitles / highlights
/ snippet) and on how the rating is encoded (a bare number or the string
"No rating" for Airbnb vs. a nested {score, reviews} object on a 0–10 scale for
Booking / Hotels.com). One parser per source projects each onto ONE unified
schema (normalising the rating to a 0–5 scale), then we answer:

    Q1 (Rome)   : all properties mentioning a "Double Room" with price < $200.
    Q2 (Paris)  : all property links with a rating of 4.55+ (on the 0–5 scale).
    Q3 (Madrid) : the 10 cheapest properties with "Cozy" in the description.

A MySQL table can also be read as a relational source (see --with-mysql).

Error handling covers missing files (file errors), the mixed-type rating field
(data inconsistencies), and MySQL connection failures.

Run:
    # Java 17 must be on PATH (PySpark 4.x requirement):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/batch_processing/06_data_from_various_sources/solution.py
    # optionally also read a MySQL table (needs a running MySQL + .env):
    uv run python src/batch_processing/06_data_from_various_sources/solution.py --with-mysql
"""

import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, explode, lit

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S"
)
log = logging.getLogger("Data Ingestion")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "ingestion.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def get_spark_session(app_name: str) -> SparkSession:
    """Create a local SparkSession."""
    log.info("Creating Spark session...")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Force loopback networking so Spark's internal block transfers stay on
        # 127.0.0.1 (avoids the flaky LAN-IP shuffle failures seen on macOS).
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def parse_airbnb(raw: DataFrame) -> DataFrame:
    """Parse the Airbnb section onto the unified schema.

    Airbnb quirks: the description is the `subtitles` array, and `rating` is a
    mixed type -- a number, or the string "No rating". `try_cast` turns the
    numeric values into doubles and the "No rating" rows into NULL (instead of
    throwing, which a plain cast does under Spark's ANSI mode).
    """
    return raw.select(explode("airbnbHotels").alias("h")).select(
        lit("airbnb").alias("source"),
        col("h.title").alias("title"),
        concat_ws(" | ", col("h.subtitles")).alias("description"),
        col("h.price.value").alias("price"),
        col("h.rating").try_cast("double").alias("rating"),
        col("h.link").alias("link"),
    )


def parse_booking(raw: DataFrame) -> DataFrame:
    """Parse the Booking.com section onto the unified schema.

    Booking quirks: the description is the `highlights` array (where "Double
    Room" lives), and `rating.score` is on a 0-10 scale -> normalise to 0-5.
    """
    return raw.select(explode("bookingHotels").alias("h")).select(
        lit("booking").alias("source"),
        col("h.title").alias("title"),
        concat_ws(" | ", col("h.highlights")).alias("description"),
        col("h.price.value").alias("price"),
        # score is mixed-type ("No rating"); try_cast -> double (-> NULL), then 0-10 -> 0-5
        (col("h.rating.score").try_cast("double") / 2).alias("rating"),
        col("h.link").alias("link"),
    )


def parse_hotelscom(raw: DataFrame) -> DataFrame:
    """Parse the Hotels.com section onto the unified schema.

    Hotels.com quirks: the description is `snippet.text` (a nested struct), and
    `rating.score` is on a 0-10 scale -> normalise to 0-5.
    """
    return raw.select(explode("hotelsComHotels").alias("h")).select(
        lit("hotelsCom").alias("source"),
        col("h.title").alias("title"),
        col("h.snippet.text").alias("description"),
        col("h.price.value").alias("price"),
        # score is mixed-type ("No rating"); try_cast -> double (-> NULL), then 0-10 -> 0-5
        (col("h.rating.score").try_cast("double") / 2).alias("rating"),
        col("h.link").alias("link"),
    )


def load_city(spark: SparkSession, city: str) -> DataFrame:
    """Read one city file and union its three sources into one unified DataFrame.

    Handles file errors (a missing file -> a clear FileNotFoundError); the
    per-source parsers handle the structural differences between the sources.
    """
    path = INPUT_DIR / f"{city}.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing dataset file: {path}")

    log.info(f"Reading {path.name}")
    raw = spark.read.option("multiLine", True).json(str(path))  # one pretty-printed object

    # Cache: every query below re-scans this DataFrame, so materialise it once.
    unified = (
        parse_airbnb(raw).unionByName(parse_booking(raw)).unionByName(parse_hotelscom(raw)).cache()
    )

    rated = unified.filter(col("rating").isNotNull()).count()
    log.info(f"{city}: parsed {unified.count()} listings ({rated} with a usable rating)")
    return unified


def q1_double_under_200(df: DataFrame) -> DataFrame:
    """Q1: listings mentioning a 'Double Room' (case-insensitive) under $200."""
    log.info("Q1: Double Room under $200")

    return (
        df.filter(col("description").rlike("(?i)double room") & (col("price") < 200))
        .select("source", "title", "price", "description")
        .orderBy("price")
    )


def q2_links_rating_455(df: DataFrame) -> DataFrame:
    """Q2: links to properties with a normalised rating of 4.55+ (0-5 scale)."""
    log.info("Q2: links with rating >= 4.55")

    return (
        df.filter(col("rating") >= 4.55)
        .select("source", "link", "rating")
        .orderBy(col("rating").desc())
    )


def q3_cozy_cheapest(df: DataFrame) -> DataFrame:
    """Q3: the 10 cheapest listings with 'Cozy' (case-insensitive) in the description."""
    log.info("Q3: 10 cheapest 'Cozy' listings")

    return (
        df.filter(col("description").rlike("(?i)cozy") & col("price").isNotNull())
        .orderBy("price")
        .limit(10)
        .select("source", "title", "price", "description", "link")
    )


def read_mysql(spark: SparkSession, table: str = "series_ratings") -> None:
    """Optional relational source: read a MySQL table over JDBC.

    Demonstrates connection-failure handling -- a missing DB / driver /
    credentials is logged and the run continues (this step is optional).
    """
    import os

    from dotenv import load_dotenv

    load_dotenv()
    user, password = os.getenv("MYSQL_USER"), os.getenv("MYSQL_PASSWORD")
    if not (user and password):
        log.warning("MySQL skipped: MYSQL_USER / MYSQL_PASSWORD not set in .env")
        return
    try:
        jdbc = (
            spark.read.format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/pex")
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )
        log.info(f"MySQL '{table}': {jdbc.count()} rows")
        jdbc.show(truncate=False)
    except Exception as error:
        log.error(f"MySQL connection/read failed: {error}")


def main() -> None:
    """Read & parse the three city files and answer the required queries."""
    parser = argparse.ArgumentParser(description="Read & parse hotel data from various sources.")
    parser.add_argument(
        "--with-mysql", action="store_true", help="Also read a MySQL table (needs MySQL + .env)."
    )
    args = parser.parse_args()

    spark = get_spark_session("Data From Various Sources")
    try:
        rome = load_city(spark, "Rome")
        rome_q1 = q1_double_under_200(rome)
        rome_q1.show(truncate=False)
        rome_q1.write.mode("overwrite").csv(str(OUTPUT_DIR / "rome_double_under_200"), header=True)

        paris = load_city(spark, "Paris")
        paris_q2 = q2_links_rating_455(paris)
        log.info(f"Paris: {paris_q2.count()} links with rating >= 4.55")
        paris_q2.show(truncate=False)
        paris_q2.write.mode("overwrite").csv(
            str(OUTPUT_DIR / "paris_links_rating_455"), header=True
        )

        madrid = load_city(spark, "Madrid")
        madrid_q3 = q3_cozy_cheapest(madrid)
        madrid_q3.show(truncate=False)
        madrid_q3.write.mode("overwrite").csv(str(OUTPUT_DIR / "madrid_cozy_cheapest"), header=True)

        if args.with_mysql:
            read_mysql(spark)
    except Exception:
        log.exception("Ingestion failed")
        raise
    finally:
        spark.stop()
        log.info("Ingestion finished")


if __name__ == "__main__":
    main()
