# Task 01 — Read Structured & Semi-Structured Data with PySpark

## What This Demonstrates

PySpark reading the TV Series JSON dataset (152,970 series with nested arrays of
structs) and answering the three required queries:

1. **Q1** — Names of `created_by` whose series status = `"Canceled"`.
2. **Q2** — All `origin_country` values with `popularity > 5.0`.
3. **Q3** — Names of series with `number_of_episodes < 100`.

Optional: JDBC join with a PostgreSQL `series_ratings` table (Docker, port 5433).

## Files

| File | Purpose |
|---|---|
| `solution.py` | PySpark job: schema, three queries, optional JDBC join |
| `download_data.py` | Fetches the dataset from Kaggle |
| `setup_pg.sql` | Creates the `series_ratings` table for the optional join |
| `qa.md` | Reviewer Q&A |

Dataset: [TV Series Dataset](https://www.kaggle.com/datasets/bourdier/all-tv-series-details-dataset)
(`tvs.json`, 151 MB — gitignored, fetched via `download_data.py`).

## How to Run

```bash
# From the repo root (UV environment + Java 17 required)

# 1. Fetch the dataset from Kaggle (needs ~/.kaggle/access_token)
python download_data.py

# 2. Required: the three queries
python solution.py

# Optional: include the JDBC join
docker-compose up -d   # PostgreSQL on port 5433
psql -h localhost -p 5433 -U peex -d peex -f setup_pg.sql
python solution.py --postgres --jdbc-jar /path/to/postgresql-42.7.4.jar
```

## Key Design Decisions

- **`multiLine=True`** is required because the dataset is a JSON array (not JSONL).
- **`F.explode("created_by")`** is the canonical way to unnest an array-of-struct
  column; we then dot-walk into `creator.name`.
- **`distinct().orderBy(...)`** keeps the result deterministic for screenshots/PRs.
- **JDBC kept optional** — the dataset alone satisfies all three required queries;
  the join shows JDBC competence without requiring a running container for graders.
- **`spark.jars` config** is the runtime way to add the PostgreSQL driver — keeps
  the Python environment free of Java jars.
