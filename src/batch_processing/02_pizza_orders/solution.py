"""Spark batch application: process pizza sales data end to end.

Reads the normalized Pizza Sales dataset (four CSV tables: orders,
order_details, pizzas, pizza_types), joins them, and answers:
    Q1: How many cali_ckn pizzas were ordered on 2015-01-04?
    Q2: What ingredients does the pizza ordered on 2015-01-02 18:27:50 have?
    Q3: What is the most sold category between 2015-01-01 and 2015-01-08?

Results are logged and the joined dataset is written to Parquet.

Run:
    # Java 17 must be on PATH (PySpark 4.x requirement):
    export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
    uv run python src/batch_processing/02_pizza_orders/solution.py
"""

import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "input" / "pizza_sales"
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%y/%m/%d %H:%M:%S")
log = logging.getLogger("Pizza Sales")
log.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
log.addHandler(console_handler)

file_handler = logging.FileHandler(str(LOG_DIR / "pizza_sales.log"))
file_handler.setFormatter(formatter)
log.addHandler(file_handler)


@dataclass
class PizzaData:
    """
    A data class representing pizza-related data frames.

    Attributes:
        order_details (DataFrame): Contains details of individual pizza orders.
        orders (DataFrame): Contains information about overall orders.
        pizza_types (DataFrame): Contains information about different pizza types.
        pizzas (DataFrame): Contains information about individual pizzas.
    """

    order_details: DataFrame
    orders: DataFrame
    pizza_types: DataFrame
    pizzas: DataFrame


def create_spark_session() -> SparkSession:
    """Create and return a SparkSession for pizza sales analysis."""
    log.info("Creating SparkSession...")

    return (
        SparkSession.builder.appName("PizzaSales")
        # Run Spark locally using all available cores.
        .master("local[*]")
        # Force loopback networking so Spark's internal block transfers stay on
        # 127.0.0.1 (avoids the flaky LAN-IP shuffle failures seen on macOS).
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def read_csv_data(spark: SparkSession, file_path: str) -> DataFrame:
    """Read CSV data from the specified file path."""
    log.info(f"Reading data from {file_path}...")

    return spark.read.csv(file_path, encoding="iso-8859-1", header=True)


def load_pizza_data(spark: SparkSession) -> PizzaData:
    """Load all pizza-related data into a PizzaData instance."""
    log.info("Loading pizza data...")

    return PizzaData(
        order_details=read_csv_data(spark, str(INPUT_DIR / "order_details.csv")),
        orders=read_csv_data(spark, str(INPUT_DIR / "orders.csv")),
        pizza_types=read_csv_data(spark, str(INPUT_DIR / "pizza_types.csv")),
        pizzas=read_csv_data(spark, str(INPUT_DIR / "pizzas.csv")),
    )


def count_cali_ckn_pizzas(pizza_data: PizzaData, date: str) -> int:
    """Count the number of California Chicken pizzas ordered on a specific date."""
    log.info("Counting cali pizzas...")

    return (
        pizza_data.order_details.join(pizza_data.orders, "order_id")
        .filter((col("date") == date) & (col("pizza_id").like("cali_ckn%")))
        .agg(count("*").alias("cali_ckn_count"))
        .collect()[0]["cali_ckn_count"]
    )


def get_pizza_ingredients(pizza_data: PizzaData, date: str, time: str) -> str:
    """Get the ingredients of a pizza ordered on a specific date and time."""
    log.info("Getting pizza ingredients...")

    return (
        pizza_data.order_details.join(pizza_data.orders, "order_id")
        .join(pizza_data.pizzas, "pizza_id")
        .join(pizza_data.pizza_types, "pizza_type_id")
        .filter((col("date") == date) & (col("time") == time))
        .select("ingredients")
        .collect()[0]["ingredients"]
    )


def get_most_sold_category(pizza_data: PizzaData, start_date: str, end_date: str) -> str:
    """Get the most sold pizza category between two dates."""
    log.info("Getting most sold category...")

    return (
        pizza_data.order_details.join(pizza_data.orders, "order_id")
        .join(pizza_data.pizzas, "pizza_id")
        .join(pizza_data.pizza_types, "pizza_type_id")
        .filter(col("date").between(start_date, end_date))
        .groupBy("category")
        .agg(count("*").alias("category_count"))
        .orderBy(col("category_count").desc())
        .select("category")
        .first()["category"]
    )


def build_joined_data(pizza_data: PizzaData) -> DataFrame:
    """Join the four tables into a single enriched orders dataset."""
    log.info("Building joined dataset...")

    return (
        pizza_data.order_details.join(pizza_data.orders, "order_id")
        .join(pizza_data.pizzas, "pizza_id")
        .join(pizza_data.pizza_types, "pizza_type_id")
    )


def write_to_parquet(data: DataFrame, path: str) -> None:
    """Write a DataFrame to a Parquet file (overwrite)."""
    log.info(f"Writing data to {path}...")
    data.write.mode("overwrite").parquet(path)


def main() -> None:
    """Run the pizza sales analysis end to end."""
    spark: SparkSession = create_spark_session()
    try:
        pizza_data: PizzaData = load_pizza_data(spark)

        # 1. How many cali_ckn pizzas were ordered on 2015-01-04?
        cali_ckn_count: int = count_cali_ckn_pizzas(pizza_data, "2015-01-04")
        log.info(f"Number of cali_ckn pizzas ordered on 2015-01-04: {cali_ckn_count}")

        # 2. What ingredients does the pizza ordered on 2015-01-02 at 18:27:50 have?
        ingredients: str = get_pizza_ingredients(pizza_data, "2015-01-02", "18:27:50")
        log.info(f"Ingredients of the pizza ordered on 2015-01-02 at 18:27:50: {ingredients}")

        # 3. What is the most sold category of pizza between 2015-01-01 and 2015-01-08?
        most_sold_category: str = get_most_sold_category(pizza_data, "2015-01-01", "2015-01-08")
        log.info(f"Most sold category of pizza between 2015-01-01 and 2015-01-08: {most_sold_category}")

        # Persist the joined dataset to Parquet (task requirement).
        joined: DataFrame = build_joined_data(pizza_data)
        write_to_parquet(joined, str(OUTPUT_DIR / "pizza_sales_joined"))
    except Exception:
        log.exception("Pizza sales processing failed")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()