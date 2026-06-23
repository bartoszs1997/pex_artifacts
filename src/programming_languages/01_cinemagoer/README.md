# Cinemagoer — Movie Ratings Analysis (PySpark)

PySpark application that processes and analyses a movie ratings dataset,
demonstrating core Python/PySpark constructs: data types, data structures,
control flow, exception handling, OOP, and DataFrame API operations.

## Data source

The task originally references the **Cinemagoer** (formerly IMDbPY) library and
a Kaggle dataset (`thedevastator/imdb-cinemagoer-movie-ratings`). Neither source
is usable as of 2025:

- The Kaggle dataset returns HTTP 404 (removed from platform).
- The `cinemagoer` library is broken — IMDb changed its HTML structure after the
  last release (May 2023) and all API methods return empty results.

This solution uses the **MovieLens Latest Small** dataset from GroupLens
(University of Minnesota) instead. It covers the same domain (movie ratings),
provides real user ratings at scale (100k+), and is downloadable via plain HTTPS
without authentication. The `cinemagoer` package remains listed in
`pyproject.toml` as the task reference dependency.

## Task

Use PySpark to process and analyse a movie ratings dataset:

1. Calculate the average rating for each movie.
2. Identify the top-rated movies based on average rating (min 50 ratings).
3. Identify the movies with the highest rating (5.0).
4. Filter the movies with fewer than 100 ratings.
5. Calculate the average rating for each genre.
6. Identify the genre with the highest average rating.

## Layout

```
01_cinemagoer/
├── download_data.py   # fetch MovieLens zip via HTTPS -> data/input/
├── solution.py        # PySpark analysis (6 tasks, OOP, logging)
├── README.md
├── logs/              # gitignored — runtime logs
│   └── cinemagoer.log
└── data/              # gitignored
    ├── input/         # movies.csv, ratings.csv, tags.csv, links.csv
    └── output/        # CSV results per task (written by solution.py)
        ├── avg_rating_per_movie/
        ├── top_rated_movies/
        ├── movies_with_highest_rating/
        ├── movies_fewer_than_100_ratings/
        ├── avg_rating_per_genre/
        └── genre_with_highest_avg_rating/
```

## Prerequisites

- **Python** managed by `uv` (`uv sync` at the repo root).
- **Java 17** on `PATH` (required by PySpark 4.x):
  ```bash
  export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  ```

No database, Docker, or API tokens required.

## Run

All commands from the repo root:

```bash
# 0) sync the environment (once)
uv sync

# 1) download the dataset -> data/input/
uv run python src/programming_languages/01_cinemagoer/download_data.py

# 2) run analysis
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/programming_languages/01_cinemagoer/solution.py
```

## Output

The script produces three layers of output:

1. **Console (stdout)** — timestamped log lines + Spark table previews (`show()`).
2. **Log file** (`logs/cinemagoer.log`) — full run log with timestamps, persisted across runs.
3. **CSV files** (`data/output/<task>/`) — each task's result written as partitioned CSV with header.

| Task | Method | Output directory |
| --- | --- | --- |
| 1 | `avg_rating_per_movie()` | `data/output/avg_rating_per_movie/` |
| 2 | `top_rated_movies(min_ratings=50)` | `data/output/top_rated_movies/` |
| 3 | `movies_with_highest_rating()` | `data/output/movies_with_highest_rating/` |
| 4 | `movies_with_fewer_than_100_ratings()` | `data/output/movies_fewer_than_100_ratings/` |
| 5 | `avg_rating_per_genre()` | `data/output/avg_rating_per_genre/` |
| 6 | `genre_with_highest_avg_rating()` | `data/output/genre_with_highest_avg_rating/` |

## Core constructs demonstrated

- **Data types**: explicit PySpark schema (`StructType`, `IntegerType`, `FloatType`, `StringType`).
- **Data structures**: DataFrames, Row objects, Python dicts for config.
- **Control flow**: conditional min-ratings filter, loop over task methods.
- **Exception handling**: `try/except` around Spark session creation and data loading with meaningful error messages.
- **OOP**: `MovieRatingAnalyzer` class encapsulating SparkSession and all analysis methods.
- **PySpark constructs**: `groupBy`, `agg`, `join`, `filter`, `orderBy`, `explode`, `split`, window functions awareness.

## Dataset details

**MovieLens Latest Small** (GroupLens, September 2018):
- `movies.csv` — 9,742 movies (`movieId`, `title`, `genres` pipe-delimited)
- `ratings.csv` — 100,836 ratings (`userId`, `movieId`, `rating`, `timestamp`)
- `tags.csv` — 3,683 tags (not used in analysis)
- `links.csv` — 9,742 external IDs (not used in analysis)

Source: https://grouplens.org/datasets/movielens/latest/
