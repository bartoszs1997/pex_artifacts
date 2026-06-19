"""PySpark analysis of movie ratings (MovieLens dataset).

Demonstrates core language constructs: data types, data structures, control
flow, exceptions, OOP principles, and PySpark DataFrame/SQL API.

Tasks performed:
    1. Calculate the average rating for each movie.
    2. Identify the top-rated movies based on average rating.
    3. Identify the movies with the highest rating.
    4. Filter the movies with rating less than 100.
    5. Calculate the average rating for each genre of movies.
    6. Identify the genre with the highest average rating.

Usage:
    uv run python solution.py
    (run from src/programming_languages/01_cinemagoer directory)
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR = SCRIPT_DIR / "data" / "input"
OUTPUT_DIR = SCRIPT_DIR / "data" / "output"
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
MOVIES_CSV = str(INPUT_DIR / "movies.csv")
RATINGS_CSV = str(INPUT_DIR / "ratings.csv")

# ---------------------------------------------------------------------------
# Logging: console (INFO) + file (INFO) — dual output like batch_processing
# ---------------------------------------------------------------------------
formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("Cinemagoer")
log.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "cinemagoer.log"))
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Schema definitions (explicit types instead of inferSchema for clarity)
# ---------------------------------------------------------------------------
MOVIES_SCHEMA = StructType([
    StructField("movieId", IntegerType(), nullable=False),
    StructField("title", StringType(), nullable=False),
    StructField("genres", StringType(), nullable=False),
])

RATINGS_SCHEMA = StructType([
    StructField("userId", IntegerType(), nullable=False),
    StructField("movieId", IntegerType(), nullable=False),
    StructField("rating", FloatType(), nullable=False),
    StructField("timestamp", IntegerType(), nullable=False),
])


# ---------------------------------------------------------------------------
# OOP: encapsulate analysis in a class
# ---------------------------------------------------------------------------
class MovieRatingAnalyzer:
    """Encapsulates PySpark operations on the MovieLens dataset.

    Demonstrates OOP principles: encapsulation (private methods), single
    responsibility (one class = one dataset analysis), and composition
    (Spark session as a dependency).
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._movies = self._load_movies()
        self._ratings = self._load_ratings()

    # --- Private: data loading with exception handling ---

    def _load_movies(self):
        """Load movies.csv with explicit schema."""
        try:
            df = self.spark.read.csv(MOVIES_CSV, header=True, schema=MOVIES_SCHEMA)
            log.info(f"Loaded movies: {df.count():,} rows")
            return df
        except Exception as e:
            log.error(f"Failed to load movies: {e}")
            raise

    def _load_ratings(self):
        """Load ratings.csv with explicit schema."""
        try:
            df = self.spark.read.csv(RATINGS_CSV, header=True, schema=RATINGS_SCHEMA)
            log.info(f"Loaded ratings: {df.count():,} rows")
            return df
        except Exception as e:
            log.error(f"Failed to load ratings: {e}")
            raise

    def _write_result(self, df, subdir: str) -> None:
        """Write DataFrame to CSV under data/output/<subdir>/ (overwrite)."""
        path = str(OUTPUT_DIR / subdir)
        df.write.mode("overwrite").csv(path, header=True)
        log.info(f"Results written to data/output/{subdir}/")

    # --- Public: analysis methods (one per task requirement) ---

    def avg_rating_per_movie(self, show: int = 10):
        """Task 1: Calculate the average rating for each movie."""
        log.info("=" * 70)
        log.info("TASK 1: Average rating per movie")
        log.info("=" * 70)

        avg_df = (
            self._ratings
            .groupBy("movieId")
            .agg(
                F.avg("rating").alias("avg_rating"),
                F.count("rating").alias("rating_count"),
            )
            .join(self._movies, on="movieId")
            .select("movieId", "title", "avg_rating", "rating_count")
            .orderBy(F.desc("rating_count"))
        )
        avg_df.show(show, truncate=False)
        self._write_result(avg_df, "avg_rating_per_movie")
        return avg_df

    def top_rated_movies(self, min_ratings: int = 50, top_n: int = 10):
        """Task 2: Identify the top-rated movies based on average rating.

        Only considers movies with at least min_ratings votes to avoid
        obscure films with a single 5.0 rating dominating.
        """
        log.info("=" * 70)
        log.info(f"TASK 2: Top {top_n} movies by avg rating (min {min_ratings} ratings)")
        log.info("=" * 70)

        top_df = (
            self._ratings
            .groupBy("movieId")
            .agg(
                F.avg("rating").alias("avg_rating"),
                F.count("rating").alias("rating_count"),
            )
            .filter(F.col("rating_count") >= min_ratings)
            .join(self._movies, on="movieId")
            .select("title", "avg_rating", "rating_count")
            .orderBy(F.desc("avg_rating"))
            .limit(top_n)
        )
        top_df.show(top_n, truncate=False)
        self._write_result(top_df, "top_rated_movies")
        return top_df

    def movies_with_highest_rating(self, top_n: int = 10):
        """Task 3: Identify the movies with the highest (max) rating."""
        log.info("=" * 70)
        log.info("TASK 3: Movies with the highest single rating (5.0)")
        log.info("=" * 70)

        # Find the maximum possible rating value
        max_rating = self._ratings.agg(F.max("rating")).collect()[0][0]

        highest_df = (
            self._ratings
            .filter(F.col("rating") == max_rating)
            .select("movieId")
            .distinct()
            .join(self._movies, on="movieId")
            .select("movieId", "title", "genres")
            .orderBy("title")
        )
        count = highest_df.count()
        log.info(f"Movies that received a {max_rating} rating: {count:,}")
        highest_df.show(top_n, truncate=False)
        self._write_result(highest_df, "movies_with_highest_rating")
        return highest_df

    def movies_with_fewer_than_100_ratings(self, top_n: int = 10):
        """Task 4: Filter the movies with rating count less than 100."""
        log.info("=" * 70)
        log.info("TASK 4: Movies with fewer than 100 ratings")
        log.info("=" * 70)

        filtered_df = (
            self._ratings
            .groupBy("movieId")
            .agg(F.count("rating").alias("rating_count"))
            .filter(F.col("rating_count") < 100)
            .join(self._movies, on="movieId")
            .select("title", "rating_count")
            .orderBy(F.desc("rating_count"))
        )
        count = filtered_df.count()
        log.info(f"Movies with < 100 ratings: {count:,}")
        filtered_df.show(top_n, truncate=False)
        self._write_result(filtered_df, "movies_fewer_than_100_ratings")
        return filtered_df

    def avg_rating_per_genre(self):
        """Task 5: Calculate the average rating for each genre.

        Movies can have multiple genres (pipe-delimited in movies.csv).
        We explode them so each genre is its own row before aggregating.
        """
        log.info("=" * 70)
        log.info("TASK 5: Average rating per genre")
        log.info("=" * 70)

        # Explode pipe-delimited genres into individual rows
        movies_exploded = (
            self._movies
            .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
            .select("movieId", "genre")
        )

        genre_avg_df = (
            self._ratings
            .join(movies_exploded, on="movieId")
            .groupBy("genre")
            .agg(
                F.avg("rating").alias("avg_rating"),
                F.count("rating").alias("rating_count"),
            )
            .orderBy(F.desc("avg_rating"))
        )
        genre_avg_df.show(30, truncate=False)
        self._write_result(genre_avg_df, "avg_rating_per_genre")
        return genre_avg_df

    def genre_with_highest_avg_rating(self):
        """Task 6: Identify the genre with the highest average rating."""
        log.info("=" * 70)
        log.info("TASK 6: Genre with the highest average rating")
        log.info("=" * 70)

        movies_exploded = (
            self._movies
            .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
            .select("movieId", "genre")
        )

        top_genre = (
            self._ratings
            .join(movies_exploded, on="movieId")
            .groupBy("genre")
            .agg(F.avg("rating").alias("avg_rating"))
            .orderBy(F.desc("avg_rating"))
            .first()
        )

        if top_genre:
            log.info(f"Highest-rated genre: {top_genre['genre']} "
                     f"(avg rating: {top_genre['avg_rating']:.3f})")
            # Write single-row result as CSV for consistency
            result_df = self.spark.createDataFrame([top_genre])
            self._write_result(result_df, "genre_with_highest_avg_rating")
        return top_genre


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    """Entry point: create Spark session, run all analyses, stop."""
    # Verify input files exist (control flow + exception)
    for path in (MOVIES_CSV, RATINGS_CSV):
        if not Path(path).exists():
            log.error(f"File not found: {path}")
            log.error("Run download_data.py first.")
            return 1

    spark = (
        SparkSession.builder
        .appName("Cinemagoer-MovieRatings")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        analyzer = MovieRatingAnalyzer(spark)

        analyzer.avg_rating_per_movie()
        analyzer.top_rated_movies()
        analyzer.movies_with_highest_rating()
        analyzer.movies_with_fewer_than_100_ratings()
        analyzer.avg_rating_per_genre()
        analyzer.genre_with_highest_avg_rating()

        log.info("All outputs written to data/output/")
    except Exception:
        log.exception("Movie ratings processing failed")
        raise
    finally:
        spark.stop()
        log.info("Spark session stopped. Done.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
