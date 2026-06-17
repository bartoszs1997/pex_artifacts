"""Spark application: analyze the Disease Symptoms and Patient Profile dataset.

Loads the patient dataset (CSV) into a Spark DataFrame and answers:
    Q1: How many 30-year-old males have Asthma?
    Q2: How many females have Hyperthyroidism with no Fever?
    Q3: For Sinusitis with Cough and Fatigue, is it predominant in males or females?

Each result is logged; the Q3 gender breakdown is also written to CSV.

Run:
    # Java 17 must be on PATH (PySpark 4.x requirement):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/batch_processing/04_disease_symptoms/solution.py
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "input" / "Disease_symptom_and_patient_profile_dataset.csv"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S")
log = logging.getLogger("Disease Data Analysis")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "disease.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def initialize_spark_session(app_name: str) -> SparkSession:
    """Initialize a SparkSession with the given application name."""
    log.info("Initializing SparkSession")

    return (
        SparkSession.builder.appName(app_name)
        # Run Spark locally using all available cores.
        .master("local[*]")
        # Force loopback networking so Spark's internal block transfers stay on
        # 127.0.0.1 (avoids the flaky LAN-IP shuffle failures seen on macOS).
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def load_dataset(spark: SparkSession, path: str) -> DataFrame:
    """Load the dataset from the given path into a DataFrame."""
    log.info(f"Loading dataset from path: {path}")

    return spark.read.csv(path, header=True, inferSchema=True)


def count_30_year_old_males_with_asthma(df: DataFrame) -> int:
    """Count the total number of 30-year-old men having Asthma."""
    log.info("Counting 30-year-old males with Asthma")

    return df.filter((col("Disease") == "Asthma") & (col("Age") == 30) & (col("Gender") == "Male")).count()


def count_females_with_hyperthyroidism_no_fever(df: DataFrame) -> int:
    """Count the total number of females with Hyperthyroidism with no Fever symptoms."""
    log.info("Counting females with Hyperthyroidism and no Fever symptoms")

    return df.filter(
        (col("Disease") == "Hyperthyroidism") & (col("Gender") == "Female") & (col("Fever") == "No")
    ).count()


def count_sinusitis_with_cough_fatigue(df: DataFrame) -> DataFrame:
    """Identify whether the Sinusitis with Cough and Fatigue symptoms is predominant for males or females."""
    log.info("Counting Sinusitis cases with Cough and Fatigue symptoms")

    return (
        df.filter((col("Disease") == "Sinusitis") & (col("Cough") == "Yes") & (col("Fatigue") == "Yes"))
        .groupBy("Gender")
        .agg(count("*").alias("Volume"))
    )


def identify_predominant_gender(df: DataFrame) -> str:
    """Identify the predominant gender for Sinusitis with Cough and Fatigue symptoms."""
    log.info("Identifying predominant gender for Sinusitis with Cough and Fatigue")
    predominant_gender = df.orderBy(col("Volume").desc()).first()

    return predominant_gender["Gender"]


def write_to_csv(df: DataFrame, path: str) -> None:
    """Write a DataFrame to CSV (overwrite, with header)."""
    log.info(f"Writing data to {path}")

    df.write.mode("overwrite").csv(path, header=True)


def main() -> None:
    """Run the disease symptoms analysis end to end."""
    spark: SparkSession = initialize_spark_session("Disease Data Analysis")
    try:
        df: DataFrame = load_dataset(spark, str(INPUT_FILE))
        df.show()

        # Q1: 30-year-old males with Asthma.
        asthma_30_male_count: int = count_30_year_old_males_with_asthma(df)
        log.info(f"Total number of 30-year-old Males having Asthma: {asthma_30_male_count}")

        # Q2: females with Hyperthyroidism and no Fever.
        hyperthyroidism_female_no_fever_count: int = count_females_with_hyperthyroidism_no_fever(df)
        log.info(f"Number of Females with Hyperthyroidism and No Fever symptoms: {hyperthyroidism_female_no_fever_count}")

        # Q3: Sinusitis with Cough and Fatigue — predominant gender.
        sinusitis_by_gender: DataFrame = count_sinusitis_with_cough_fatigue(df)
        sinusitis_by_gender.show()
        predominant_gender: str = identify_predominant_gender(sinusitis_by_gender)
        log.info(f"The predominant gender for Sinusitis with Cough and Fatigue is: {predominant_gender}")

        write_to_csv(sinusitis_by_gender, str(OUTPUT_DIR / "sinusitis_by_gender"))
    except Exception:
        log.exception("Disease data processing failed")
        raise
    finally:
        spark.stop()
        log.info("Data analysis script finished")


if __name__ == "__main__":
    main()