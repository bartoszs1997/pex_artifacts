# Food Orders — Batch Processing (Transformations & Actions, Performance Tuning)

Spark DataFrame application that processes restaurant order data, demonstrating
**transformations**, **actions**, and **caching** for performance optimization.

## Task

Apply Spark transformations and actions for performance optimization on a
dataset containing customer order data (order ID, customer ID, product,
restaurant, rating):

1. **Filter** — remove orders with no rating (`"Not given"`).
2. **Cache** — the filtered DataFrame is cached for faster subsequent access.
3. **GroupBy + Aggregate** — calculate total `cost_of_the_order` per customer.
4. **Sort** — restaurants sorted by total rating in descending order.
5. **Top-10** — display the top 10 restaurants with highest total revenue.

## Results

| Step | Result |
| --- | --- |
| Total orders loaded | 1 898 |
| Orders with a rating (after filter) | **1 162** |
| Distinct customers with rated orders | **859** |

**Top 10 restaurants by total rating:**

| # | Restaurant | Total rating |
|---|---|---|
| 1 | Shake Shack | 569 |
| 2 | The Meatball Shop | 379 |
| 3 | Blue Ribbon Sushi | 308 |
| 4 | Blue Ribbon Fried Chicken | 277 |
| 5 | RedFarm Broadway | 174 |
| 6 | Parm | 161 |
| 7 | RedFarm Hudson | 142 |
| 8 | TAO | 122 |
| 9 | Han Dynasty | 102 |
| 10 | Blue Ribbon Sushi Bar & Grill | 101 |

**Top 10 restaurants by total revenue:**

| # | Restaurant | Total revenue ($) |
|---|---|---|
| 1 | Shake Shack | 2 225.20 |
| 2 | The Meatball Shop | 1 495.65 |
| 3 | Blue Ribbon Sushi | 1 170.66 |
| 4 | Blue Ribbon Fried Chicken | 1 130.59 |
| 5 | RedFarm Broadway | 680.69 |
| 6 | Parm | 655.52 |
| 7 | RedFarm Hudson | 566.86 |
| 8 | Rubirosa | 450.66 |
| 9 | TAO | 421.16 |
| 10 | Momoya | 365.78 |

> Cross-checked with an independent pandas pass — identical numbers.

## Dataset

Kaggle [`reenapinto/food-order`](https://www.kaggle.com/datasets/reenapinto/food-order)
— `Resume_food_order_3.csv` (~124 KB, 1 898 rows). Columns:

```
order_id, customer_id, restaurant_name, cuisine_type, cost_of_the_order,
day_of_the_week, rating, food_preparation_time, delivery_time
```

`rating` is `3`, `4`, `5`, or `"Not given"`.

## Layout

```
08_food_orders/
├── download_data.py   # fetch CSV from Kaggle -> data/input/
├── solution.py        # Spark app: filter, cache, group, sort, top-10
├── data/              # gitignored
│   ├── input/Resume_food_order_3.csv
│   └── output/
│       ├── cost_per_customer/
│       ├── restaurants_by_rating/
│       └── top_restaurants_revenue/
└── logs/              # gitignored
    └── food_orders.log
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH`:
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```

## Run

```bash
# 0) Download dataset
uv run python src/batch_processing/08_food_orders/download_data.py

# 1) Run the analysis
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/batch_processing/08_food_orders/solution.py
```

## Performance optimizations applied

| Technique | Where | Why |
| --- | --- | --- |
| **`.cache()`** | after filter (line 120) | Filtered DF is reused by 3 downstream queries — avoids re-reading + re-filtering the CSV three times |
| **Filter early** | before any aggregation | Reduces rows from 1 898 → 1 162 so all downstream shuffles handle 39% less data |
| **Single groupBy per question** | cost, rating, revenue | Each aggregation does exactly one shuffle — no redundant wide transformations |
| **`.limit(n)`** | revenue top-10 | Spark can use a partial sort (TakeOrderedAndProject) instead of a full global sort |
| **`inferSchema=True`** | read | `cost_of_the_order` is read as double directly — no cast needed later |

In production at scale you would additionally consider:
- **`repartition`/`coalesce`** to match cluster parallelism before heavy groupBys.
- **Broadcast joins** if enriching with a small dimension table.
- **Predicate pushdown** (Parquet/ORC instead of CSV).
- **AQE** (Adaptive Query Execution, on by default in Spark 4.x) auto-coalesces shuffle partitions.

## `solution.py` — step by step

| Step | Transformation / Action | Purpose |
| --- | --- | --- |
| read | `spark.read.csv(..., inferSchema=True)` | Load CSV → DataFrame |
| filter | `.filter(col("rating") != "Not given")` | Narrow transformation — drop unrated |
| cache | `.cache()` then `.count()` | Persist in memory; count materializes it |
| cost/customer | `.groupBy("customer_id").agg(sum("cost_of_the_order"))` | Wide (shuffle) |
| rating sort | `.cast("int")` → `.groupBy(restaurant).agg(sum(rating))` → `.orderBy(desc)` | Wide + sort |
| top-10 revenue | `.groupBy(restaurant).agg(sum(cost))` → `.orderBy(desc).limit(10)` | Wide + partial sort |
| write | `.write.csv(...)` | Action — triggers execution, persists results |

## Acceptance criteria — status

| # | Criterion | Status |
| --- | --- | --- |
| 1 | Dataset read into a Spark DataFrame | `spark.read.csv` with header + inferSchema |
| 2 | Resulting DataFrame cached for faster access | `rated.cache()` before downstream queries |
| 3 | All necessary transformations applied | filter, groupBy, agg, orderBy, limit, cast |
| — | Filter out orders with no rating | `rating != "Not given"` → 1 162 rows |
| — | Total cost per customer | 859 customers |
| — | Restaurants sorted by total rating desc | Shake Shack #1 (569) |
| — | Top 10 restaurants by total revenue | Shake Shack #1 ($2 225.20) |

## Implementation notes

- **Loopback binding** (`spark.driver.host=127.0.0.1`) avoids macOS network issues.
- **`sum as spark_sum`** — avoids shadowing Python's built-in `sum`.
- **Data** (`data/input`, `data/output`) and **logs** are gitignored.
