"""Spark DataFrame application: food orders — transformations & actions with performance tuning.

The task demonstrates Spark transformations and actions applied to a restaurant
order dataset with a focus on **performance optimization**: caching, reducing
shuffles, and choosing the right action for each question.

Pipeline:
    1. Read CSV into a Spark DataFrame.
    2. Filter out orders with no rating ("Not given").
    3. Cache the filtered DataFrame (acceptance criterion: cached for faster access).
    4. Calculate total cost_of_the_order per customer (groupBy customer_id).
    5. Sort restaurants by total rating descending.
    6. Display top 10 restaurants with highest total revenue.

Dataset (Kaggle "reenapinto/food-order"), columns:
    order_id, customer_id, restaurant_name, cuisine_type, cost_of_the_order,
    day_of_the_week, rating, food_preparation_time, delivery_time

Run (local; Java 17 must be on PATH for PySpark 4.x):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/batch_processing/08_food_orders/solution.py
"""

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "input" / "Resume_food_order_3.csv"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%y/%m/%d %H:%M:%S",
)
log = logging.getLogger("Food Orders")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "food_orders.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a SparkSession with performance-oriented settings."""
    log.debug("Creating SparkSession...")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def read_orders(spark: SparkSession, path: str) -> DataFrame:
    """Read the food orders CSV with schema inference and header."""
    log.debug(f"Reading orders from {path}...")

    return spark.read.csv(path, header=True, inferSchema=True)


def filter_rated_orders(df: DataFrame) -> DataFrame:
    """Remove orders where rating is 'Not given'."""
    log.debug("Filtering out orders with no rating...")

    return df.filter(col("rating") != "Not given")


def total_cost_per_customer(df: DataFrame) -> DataFrame:
    """Calculate total cost_of_the_order grouped by customer_id."""
    log.debug("Calculating total cost per customer...")

    return df.groupBy("customer_id").agg(
        spark_sum("cost_of_the_order").alias("total_cost")
    )


def restaurants_by_total_rating(df: DataFrame) -> DataFrame:
    """Sort restaurants by their total rating in descending order."""
    log.debug("Sorting restaurants by total rating (desc)...")

    return (
        df.withColumn("rating", col("rating").cast("int"))
        .groupBy("restaurant_name")
        .agg(spark_sum("rating").alias("total_rating"))
        .orderBy(col("total_rating").desc())
    )


def top_restaurants_by_revenue(df: DataFrame, n: int = 10) -> DataFrame:
    """Get top N restaurants by total revenue (sum of cost_of_the_order)."""
    log.debug(f"Getting top {n} restaurants by total revenue...")

    return (
        df.groupBy("restaurant_name")
        .agg(spark_sum("cost_of_the_order").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
        .limit(n)
    )


def main() -> None:
    """Run the food-orders analysis end to end."""
    spark: SparkSession = create_spark_session("FoodOrdersAnalysis")
    try:
        # --- Read ---
        raw: DataFrame = read_orders(spark, str(INPUT_FILE))
        log.info(f"Total orders loaded: {raw.count()}")

        # --- Step 1: Filter orders with no rating ---
        rated: DataFrame = filter_rated_orders(raw)

        # --- Cache for performance (acceptance criterion) ---
        # The filtered DataFrame is reused by 3 downstream queries (cost per
        # customer, rating sort, revenue top-10). Caching avoids re-reading and
        # re-filtering the source CSV for each action.
        rated.cache()

        rated_count: int = rated.count()  # materializes cache
        log.info(f"Orders with rating (cached): {rated_count}")

        # --- Step 2: Total cost per customer ---
        cost_df: DataFrame = total_cost_per_customer(rated)
        log.info(f"Distinct customers with rated orders: {cost_df.count()}")
        print("\n== Top 5 customers by total cost ==")
        cost_df.orderBy(col("total_cost").desc()).show(5, truncate=False)

        # --- Step 3: Restaurants sorted by total rating (desc) ---
        rating_df: DataFrame = restaurants_by_total_rating(rated)
        print("== Restaurants by total rating (top 10) ==")
        rating_df.show(10, truncate=False)

        # --- Step 4: Top 10 restaurants by total revenue ---
        revenue_df: DataFrame = top_restaurants_by_revenue(rated, n=10)
        print("== Top 10 restaurants by total revenue ==")
        revenue_df.show(10, truncate=False)

        # --- Write outputs ---
        cost_df.write.mode("overwrite").csv(
            str(OUTPUT_DIR / "cost_per_customer"), header=True
        )
        rating_df.write.mode("overwrite").csv(
            str(OUTPUT_DIR / "restaurants_by_rating"), header=True
        )
        revenue_df.write.mode("overwrite").csv(
            str(OUTPUT_DIR / "top_restaurants_revenue"), header=True
        )
        log.info("All outputs written to data/output/")

    except Exception:
        log.exception("Food orders processing failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
