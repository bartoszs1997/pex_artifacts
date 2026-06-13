# TV Series — Batch Processing (Spark + MySQL/JDBC)

Spark batch application over a TV Series dataset. It reads a semi-structured
JSON file, runs analytical queries, and (optionally) joins the data with a
MySQL table over JDBC and aggregates the joined result.

## Task

**Required (Phase 1):**
- Read a JSON file from the local file system with Spark.
- Extract and transform relevant fields into DataFrames.
- Display and write the results.

**Optional (Phase 2):**
- Connect to a MySQL database using the JDBC driver.
- Retrieve data from a table and join it with the processed JSON on a common key.
- Perform a basic aggregation (group by + count/sum) on the joined data.

## Layout

```
tv_series/
├── download_data.py                  # fetch the dataset from Kaggle -> data/input/
├── basic_solution.py                 # Phase 1: read JSON, 3 queries, show + write CSV
├── optional_data_load.py             # Phase 2 (load): run sql/setup_ratings.sql in MySQL
├── optional_data_read_and_transform.py  # Phase 2 (read): JDBC + join + aggregation
├── sql/
│   └── setup_ratings.sql             # creates `pex` DB + series_ratings table (+ seed rows)
└── data/                             # gitignored
    ├── input/                        # tvs.json (~151 MB), tvs.csv (~87 MB)
    └── output/                       # query results as CSV
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH` (required by PySpark 4.x):
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```
- **Kaggle token** at `~/.kaggle/access_token` (or `kaggle.json`), `chmod 600`.
- **MySQL** running locally on `localhost:3306` (Phase 2 only).
- **`.env`** at the repo root (Phase 2 only). Copy the template and fill it in:
  ```bash
  cp .env.example .env
  # then set MYSQL_USER / MYSQL_PASSWORD
  ```
  `.env` is gitignored; `.env.example` is committed as documentation.

## Run

All commands are run from the repo root.

```bash
# 0) (once) sync the environment
uv sync

# 1) download the dataset -> data/input/
uv run python src/batch_processing/tv_series/download_data.py

# 2) Phase 1 — read JSON, run 3 queries, write CSV to data/output/
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/batch_processing/tv_series/basic_solution.py
# add --pause to keep the Spark UI (http://localhost:4040) alive after the run
uv run python src/batch_processing/tv_series/basic_solution.py --pause

# 3) Phase 2 (load) — create the pex DB + series_ratings table in MySQL
uv run python src/batch_processing/tv_series/optional_data_load.py

# 4) Phase 2 (read) — JDBC read + join with JSON + aggregation
uv run python src/batch_processing/tv_series/optional_data_read_and_transform.py
```

## Phase 1 — `basic_solution.py`

Reads the JSON (`multiLine=True`, because the file is one big JSON array, not
JSONL) and runs three queries, each shown on screen and written as CSV under
`data/output/`:

| Query | Logic | Output |
| --- | --- | --- |
| Canceled creators | `status == "Canceled"` → explode `created_by.name` → distinct | `canceled_creators/` |
| Popular countries | `popularity > 5.0` → explode `origin_country` → distinct | `popular_countries/` |
| Short series | `number_of_episodes < 100` → `name` | `short_series/` |

## Phase 2 — MySQL + JDBC

**Load** (`optional_data_load.py`): connects with the native Python connector
(`mysql.connector`) and executes `sql/setup_ratings.sql`, which creates the
`pex` database and a small `series_ratings` table whose `series_id` joins to the
JSON series `id`. The script is idempotent and reads credentials from `.env`.

**Read + transform** (`optional_data_read_and_transform.py`):
1. Reads `series_ratings` from MySQL over **JDBC** (`spark.read.jdbc`). The
   driver is pulled from Maven via `spark.jars.packages`, so there is no jar to
   download or commit.
2. Reads the JSON with Spark.
3. **Joins** on `series_ratings.series_id == series.id` (inner).
4. **Aggregates** the joined data: `groupBy("status")` with
   `count` / `avg(imdb_rating)` / `sum(viewers_millions)` / `sum(number_of_episodes)`.
5. Displays the result and writes it to `data/output/ratings_by_status/`.

Example aggregation output:

```
+----------------+------------+---------------+----------------------+--------------+
|status          |series_count|avg_imdb_rating|total_viewers_millions|total_episodes|
+----------------+------------+---------------+----------------------+--------------+
|Returning Series|5           |8.70           |19.50                 |221           |
|Ended           |5           |8.94           |35.80                 |215           |
+----------------+------------+---------------+----------------------+--------------+
```

## Implementation notes

- **Loopback networking.** Both Spark scripts pin the driver to `127.0.0.1`
  (`spark.driver.host` / `spark.driver.bindAddress`). On macOS the driver may
  otherwise resolve to a LAN IP and intermittently fail to fetch its own
  shuffle/result blocks (`Broken pipe`, `TaskResultLost`). This is an
  environment workaround, not a Spark requirement; for a single-node app
  loopback is the most reliable choice.
- **Secrets** stay in the gitignored `.env`; only `.env.example` is committed.
- **Data** (`data/input`, `data/output`) is gitignored and never enters git.

## Known limitations (intentional, for discussion)

- `setup_ratings.sql` seeds a handful of hand-picked rows to make the join
  meaningful; it is not a full load of the dataset into MySQL.
- `split_statements()` in the loader is a naive `;` splitter (fine for this
  controlled script; a production version would use `sqlparse` or `multi=True`).
- The short-series query does not de-duplicate names (the task asks for all
  names), unlike the other two queries which use `distinct()`.
```