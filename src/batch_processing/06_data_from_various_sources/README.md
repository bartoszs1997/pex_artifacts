# Data From Various Sources — Batch Processing (multi-source nested JSON)

Spark batch application that ingests and parses hotel data from **various
sources**. For each city the dataset bundles three sources — Airbnb, Booking.com,
and Hotels.com — into a single nested-JSON file, each with a **different shape**.
The module parses all three onto one unified schema and answers three queries.

## Task

> NEBo task — *Read and parse data from various data sources*: build a data
> ingestion module that reads and parses data and stores it in appropriate data
> structures.

The three required queries:

- **Q1 (Rome):** all listings mentioning a **Double Room** with price **< $200**.
- **Q2 (Paris):** all property **links** with a rating of **4.55+**.
- **Q3 (Madrid):** the **10 cheapest** listings with the word **Cozy** in the description.

Optional: read a MySQL table as a relational source (`--with-mysql`).

## Results

| Query | Result |
| --- | --- |
| Q1 — Rome, Double Room < $200 | **12** listings (10 Airbnb + 2 Booking) |
| Q2 — Paris, rating ≥ 4.55 | **301** links |
| Q3 — Madrid, 10 cheapest "Cozy" | prices `37, 53, 57, 64, 66, 70, 75, 75, 78, 80` |

All three were cross-checked with an independent pure-Python pass over the same
files — identical numbers.

## Dataset

[`mykhailozub/500-hotels-from-airbnb-booking-and-hotelscom`](https://www.kaggle.com/datasets/mykhailozub/500-hotels-from-airbnb-booking-and-hotelscom)
— one JSON file per city (`Berlin`, `London`, `Madrid`, `Paris`, `Rome`). Each
file is a single object bundling three differently-shaped source arrays:

```json
{
  "airbnbHotels":    [ { "title", "subtitles": [...], "price": {"value"}, "rating", "link" } ],
  "bookingHotels":   [ { "title", "location", "highlights": [...], "price": {"value"}, "rating": {"score"}, "link" } ],
  "hotelsComHotels": [ { "title", "location", "snippet": {"text"}, "price": {"value"}, "rating": {"score"}, "link" } ]
}
```

The three sources disagree on **two** things that drive the whole design:

| | Airbnb | Booking.com | Hotels.com |
| --- | --- | --- | --- |
| description lives in | `subtitles[]` | `highlights[]` | `snippet.text` |
| rating encoded as | a number, or `"No rating"` | `{score}` on 0–10 | `{score}` on 0–10 |

So there is **one parser per source**, each projecting onto a shared schema
(`source, title, description, price, rating, link`) with the rating normalised
to a 0–5 scale. The parsed sources are then `unionByName`-ed into one DataFrame
the queries run against.

## The data-inconsistency catch (worth understanding)

`rating` / `rating.score` is genuinely **mixed-type**: most values are numbers,
but some rows carry the string `"No rating"`. Spark's JSON inference therefore
types the column as **STRING**. Under Spark 4.x (ANSI mode **on** by default), a
plain `cast("double")` on `"No rating"` **throws**
(`CAST_INVALID_INPUT`) rather than returning NULL — which aborts the whole job.

The fix is `try_cast("double")`: numeric strings become doubles, and `"No rating"`
becomes **NULL** (correctly excluded from the rating filter) without crashing.
This is exactly the "data inconsistency" the task asks us to handle.

## Layout

```
06_data_from_various_sources/
├── download_data.py   # fetch the 5 city JSONs from Kaggle -> data/input/
├── solution.py        # parse 3 sources/city -> unified schema -> Q1/Q2/Q3
├── data/              # gitignored
│   ├── input/         # Berlin / London / Madrid / Paris / Rome .json
│   └── output/        # rome_double_under_200 / paris_links_rating_455 / madrid_cozy_cheapest (CSV)
└── logs/              # gitignored
    └── ingestion.log
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH` (required by PySpark 4.x):
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```
- **Kaggle token** at `~/.kaggle/access_token` (or `kaggle.json`), `chmod 600`.
- *(Optional, only for `--with-mysql`)* a running MySQL with the `pex` DB and a
  `.env` providing `MYSQL_USER` / `MYSQL_PASSWORD` (see module 01).

## Run

All commands are run from the repo root.

```bash
# 0) (once) sync the environment
uv sync

# 1) download the dataset -> data/input/
uv run python src/batch_processing/06_data_from_various_sources/download_data.py

# 2) run the Spark app — parse 3 sources/city, answer Q1-Q3, write CSV to data/output/
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/batch_processing/06_data_from_various_sources/solution.py

# optional: also read a MySQL table as a relational source
uv run python src/batch_processing/06_data_from_various_sources/solution.py --with-mysql
```

## `solution.py`

| Step | Function | Logic |
| --- | --- | --- |
| read | `load_city` | `read.json(multiLine=True)` → union the three parsed sources, `cache()` |
| parse | `parse_airbnb` / `parse_booking` / `parse_hotelscom` | one parser per source → shared schema; `try_cast` on the mixed rating |
| Q1 | `q1_double_under_200` | `description rlike "(?i)double room" & price < 200`, ordered by price |
| Q2 | `q2_links_rating_455` | `rating >= 4.55`, select links, ordered by rating desc |
| Q3 | `q3_cozy_cheapest` | `description rlike "(?i)cozy" & price not null`, cheapest 10 |
| (opt) | `read_mysql` | JDBC read of a MySQL table; connection failure is logged, not fatal |

## Implementation notes

- **Case-insensitive matching.** "Double Room" and "Cozy" are matched with
  `rlike("(?i)…")`, so `DOUBLE ROOM`, `double room`, and `Double Room` all count
  (the data mixes cases). A plain `.contains("Cozy")` would miss most rows.
- **Rating normalisation.** Booking/Hotels.com score on 0–10, Airbnb on 0–5, so
  the 0–10 sources are divided by 2 to make the 4.55 threshold comparable across
  all three.
- **Error handling.** Missing files raise a clear `FileNotFoundError`; the
  mixed-type rating is handled with `try_cast`; the optional MySQL read catches
  connection failures and continues. `main()` wraps the pipeline in
  `try/except/finally`, logging the traceback and always stopping Spark.
- **Cache.** Each city DataFrame is reused by multiple actions (the stats log +
  each query), so it is `cache()`-d once after the union.
- **Data** (`data/input`, `data/output`) and **logs** are gitignored.

## Acceptance criteria — status

| # | Criterion | Status |
| --- | --- | --- |
| 1 | JSON parsed, relevant fields extracted into structured objects | 3 parsers → unified schema |
| 2 | Parsed data accurate & complete (no missing values/errors) | verified vs pure-Python; `try_cast` keeps malformed ratings as explicit NULL |
| 3 | Error handling for connection failures, file parsing errors, data inconsistencies | `FileNotFoundError` + `try_cast` + MySQL `try/except` |
