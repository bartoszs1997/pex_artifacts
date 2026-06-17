"""Spark application: process the English words dataset.

Reads the words list (one token per line) and:
    Q1: counts words starting with "abs",
    Q2: counts words whose third letter is "o",
    Q3: in words ending with "s", replaces "ou" with "uou".

Q1/Q2 results are logged; the Q3-modified words are written to CSV.

This app is meant to run on a Spark Standalone cluster via spark-submit: the
master is taken from --master and is NOT hard-coded in the session, so the same
file runs locally (default local[*]) or against a cluster. See the module
README for how to start the cluster and submit the job.

Run (Standalone cluster; Java 17 must be on PATH for PySpark 4.x):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    spark-submit --master spark://localhost:7077 \\
        src/batch_processing/03_english_words/solution.py
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    lower,
    regexp_replace,
    split,
    substring,
    when,
)

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "input" / "words.txt"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S")
log = logging.getLogger("Words Data Analysis")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "words.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def create_spark_session(app_name: str) -> SparkSession:
    """Create and return a SparkSession."""
    log.debug("Creating SparkSession...")

    return SparkSession.builder.appName(app_name).getOrCreate()


def read_text_file(file_path: str, session: SparkSession) -> DataFrame:
    """Read a text file into a Spark DataFrame."""
    log.debug(f"Reading text file from {file_path} into a Spark DataFrame...")

    return session.read.text(file_path)


def explode_words(df: DataFrame) -> DataFrame:
    """Explode multiple words in a row into individual rows."""
    log.debug("Exploding words into multiple rows...")

    return df.select(explode(split(col("value"), r"\s+")).alias("word"))


def count_words_starting_with_abs(df: DataFrame) -> int:
    """Count the number of words that start with 'abs'."""
    log.debug("Counting the number of words starting with 'abs'...")

    return df.filter(lower(col("word")).startswith("abs")).count()


def count_words_with_third_letter_o(df: DataFrame) -> int:
    """Count the number of words where the third letter is 'o'."""
    log.debug("Counting the number of words where the third letter is 'o'...")

    return df.filter(lower(substring(col("word"), 3, 1)) == "o").count()


def modify_words(df: DataFrame) -> DataFrame:
    """Modify words by replacing 'ou' with 'uou' if they end with 's'."""
    log.debug("Modifying words by replacing 'ou' with 'uou' if they end with 's'...")

    return df.withColumn(
        "new_word",
        when(
            lower(col("word")).endswith("s"), regexp_replace(col("word"), pattern="(?i)ou", replacement="uou")
        ).otherwise(col("word")),
    )


def write_to_csv(df: DataFrame, path: str) -> None:
    """Write a DataFrame to CSV (overwrite, with header)."""
    log.debug(f"Writing data to {path}...")

    df.write.mode("overwrite").csv(path, header=True)


def main() -> None:
    """Run the words analysis end to end."""
    spark: SparkSession = create_spark_session("WordsAnalysis")
    try:
        words: DataFrame = read_text_file(str(INPUT_FILE), spark).transform(explode_words)

        # Q1: words starting with "abs".
        abs_count: int = count_words_starting_with_abs(words)
        log.info(f"Number of words starting with 'abs': {abs_count}")

        # Q2: words whose third letter is "o".
        o_count: int = count_words_with_third_letter_o(words)
        log.info(f"Number of words where the third letter is 'o': {o_count}")

        # Q3: in words ending with "s", replace "ou" with "uou".
        modified: DataFrame = modify_words(words)
        changed: DataFrame = modified.filter(col("word") != col("new_word"))
        log.info(f"Number of words ending in 's' with 'ou' replaced by 'uou': {changed.count()}")

        print("== sample of modified words (word -> new_word) ==")
        changed.show(10, truncate=False)

        write_to_csv(modified, str(OUTPUT_DIR / "modified_words"))
    except Exception:
        log.exception("Words processing failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()