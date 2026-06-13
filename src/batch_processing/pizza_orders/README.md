# Pizza Orders — Batch Processing (Spark + Parquet)

Spark batch application over the **Pizza Sales** dataset. It reads four
normalized CSV tables, joins them into a single enriched dataset, answers three
analytical questions, and writes the joined result to Parquet.

## Task

- Set up a Spark session running in **local** mode.
- Read CSV data into DataFrames.
- Apply transformations (filtering, aggregation, joins).
- Trigger actions to compute and collect results.
- Save the processed data to a **Parquet** file.
- Handle exceptions and log progress.

The three questions answered:

- **Q1:** How many `cali_ckn` (California Chicken) pizzas were ordered on `2015-01-04`?
- **Q2:** What ingredients does the pizza ordered on `2015-01-02 18:27:50` have?
- **Q3:** What is the most sold category between `2015-01-01` and `2015-01-08`?

## Dataset

The dataset is **normalized** into four CSV tables (like a small relational
schema). No single table holds everything, so answering the questions requires
joining them:

| File | Key columns | What it holds |
| --- | --- | --- |
| `orders.csv` | `order_id`, `date`, `time` | when each order was placed |
| `order_details.csv` | `order_details_id`, `order_id`, `pizza_id`, `quantity` | which pizzas are in each order |
| `pizzas.csv` | `pizza_id`, `pizza_type_id`, `size`, `price` | size + price per pizza |
| `pizza_types.csv` | `pizza_type_id`, `name`, `category`, `ingredients` | name, category, ingredients |

Join chain: `order_details → orders` (on `order_id`) `→ pizzas` (on `pizza_id`)
`→ pizza_types` (on `pizza_type_id`).

## Layout

```
pizza_orders/
├── download_data.py      # fetch the dataset from Kaggle -> data/input/
├── solution.py           # Spark app: read 4 CSVs, join, answer Q1-Q3, write Parquet
├── data/                 # gitignored
│   ├── input/pizza_sales/   # orders / order_details / pizzas / pizza_types .csv
│   └── output/              # pizza_sales_joined/ (Parquet)
└── logs/                 # gitignored
    └── pizza_sales.log      # run log (console output is mirrored here)
```

> `validation.py` is a local-only read-back helper (pandas) and is gitignored.

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH` (required by PySpark 4.x):
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```
- **Kaggle token** at `~/.kaggle/access_token` (or `kaggle.json`), `chmod 600`.

## Run

All commands are run from the repo root.

```bash
# 0) (once) sync the environment
uv sync

# 1) download the dataset -> data/input/pizza_sales/
uv run python src/batch_processing/pizza_orders/download_data.py

# 2) run the Spark app — read CSVs, answer Q1-Q3, write Parquet to data/output/
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/batch_processing/pizza_orders/solution.py
```

## `solution.py`

Reads the four CSVs (`encoding="iso-8859-1"`, because the ingredients contain
non-ASCII characters) and runs three queries, each logged to the console and to
`logs/pizza_sales.log`:

| Query | Logic | Result |
| --- | --- | --- |
| Q1 — cali_ckn count | join `order_details`+`orders` → filter `date` + `pizza_id LIKE cali_ckn%` → `count` | `5` |
| Q2 — ingredients | join all 4 → filter `date` + `time` → `select(ingredients)` | `Capocollo, Red Peppers, Tomatoes, Goat Cheese, Garlic, Oregano` |
| Q3 — top category | join all 4 → filter date range → `groupBy(category).count` → `orderBy desc` | `Classic` |

After answering the questions it builds the full four-table join
(`build_joined_data`) and writes it to `data/output/pizza_sales_joined/` as
Parquet (`mode("overwrite")`).

## Implementation notes

- **Lazy evaluation.** `join` / `filter` / `groupBy` / `select` are
  *transformations* — they only build a query plan. The work runs on the first
  *action* (`collect()`, `first()`, or `.write`). Each query therefore ends with
  exactly one action.
- **Exception handling.** `main()` wraps the pipeline in `try/except/finally`:
  on error it logs the full traceback (`log.exception`) and **re-raises** (no
  silent masking, non-zero exit); `finally` always calls `spark.stop()`.
- **Loopback networking.** The Spark driver is pinned to `127.0.0.1`
  (`spark.driver.host` / `spark.driver.bindAddress`). On macOS the driver may
  otherwise resolve to a LAN IP and intermittently fail to fetch its own
  shuffle/result blocks. This is an environment workaround, not a Spark
  requirement.
- **Data** (`data/input`, `data/output`) and **logs** are gitignored and never
  enter git.

## Verifying the Parquet output

`solution.py` writes the joined dataset to `data/output/pizza_sales_joined/`
(look for `_SUCCESS` + a `part-*.snappy.parquet` file). To prove the data is
stored correctly, read it back with an independent tool — the local-only
`validation.py` does this with pandas (no Spark/Java needed) and recomputes
Q1-Q3 straight from the saved file:

```bash
uv run python src/batch_processing/pizza_orders/validation.py
```

It reports **48 620 rows** (= number of `order_details` records, so the inner
join neither dropped nor duplicated rows) and the same Q1/Q2/Q3 answers as the
Spark run.

## Known limitations (intentional, for discussion)

- Q1 counts matching `order_details` **rows**, not `sum(quantity)`; for this
  dataset/date the orders are single-unit, so the row count equals the pizza
  count. A quantity-aware version would `sum("quantity")` instead.
- The questions use fixed dates/times as literal arguments in `main()`; a
  production version would take them as CLI parameters.
