# Task 03 - Define data cleansing rules (bronze -> silver)

Create a data cleansing task that moves data from the **bronze** layer to the
**silver** layer, fixing common data quality issues. The data lake is imitated
with **LocalStack S3** (a local, free stand-in for AWS S3).

```
bronze : s3a://pex-datalake/bronze/covid   (raw, as ingested)
silver : s3a://pex-datalake/silver/covid   (cleansed)
```

## Dataset

US COVID-19 Cases and Deaths by State over Time
(`davidbroberts/us-covid-deaths-by-state-over-time`), a single CSV of 31,680
rows and 15 columns (`submission_date`, `state`, `tot_cases`, `conf_cases`,
`new_death`, `consent_cases`, ...).

## Identified data quality issues

| Issue | Evidence in the dataset |
|---|---|
| Missing values | Thousands of empty cells: `conf_cases` (16,605), `prob_cases` (16,676), `conf_death`/`prob_death` (16,138 each), `pnew_case`/`pnew_death` (~4,400), `consent_cases` (5,808), `consent_deaths` (4,752). |
| Outliers | `new_death` ranges from `-1453` to `1644`; negative daily deaths are impossible. The IQR fence is `[-27, 45]`, flagging 3,601 rows. |
| Duplicates | The natural grain is one record per `(submission_date, state)`; the pipeline enforces it with a deduplication step. |

## Cleansing rules (applied in order)

1. **Impute missing values** - numeric columns are filled with `0`, text columns
   with `"default"`. Implemented with `DataFrame.fillna`, split by column type.
2. **Remove outliers** - Tukey's IQR rule on `new_death`: compute
   `Q1`, `Q3`, `IQR = Q3 - Q1`, and drop rows outside
   `[Q1 - 1.5*IQR, Q3 + 1.5*IQR]` (here `[-27, 45]`). The fence is computed once
   on the bronze data so removal and verification use the same bounds.
3. **Deduplicate** - keep one record per `(submission_date, state)` with
   `dropDuplicates`.

The three rules are wrapped in a single `cleanse(df, low, high)` function, so the
whole transformation is one reusable call that drops straight into any
bronze -> silver pipeline.

## How to run

```bash
# 1. Download the dataset (Kaggle credentials read from ~/.kaggle/)
uv run python src/data_lake/03_data_cleansing/download_data.py

# 2. Bring up the data lake (LocalStack S3)
docker compose -f src/data_lake/03_data_cleansing/docker-compose.yml up -d

# 3. Run the cleansing pipeline (Java 17 must be on PATH for PySpark)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/data_lake/03_data_cleansing/cleanse.py

# 4. Tear the data lake down
docker compose -f src/data_lake/03_data_cleansing/docker-compose.yml down -v
```

## How to verify

The final line of the run prints:

```
VERIFIED: silver has 28079 rows, 0 missing values, 0 residual outliers, 0 duplicates
```

`verify_silver` reads the silver table back and asserts:
- total nulls across all columns is `0` (missing values imputed),
- no rows lie outside the applied outlier fence (outliers removed),
- row count equals distinct-by-key count (no duplicates).

## Dependencies and configuration

- **PySpark 4.1.1** (host), **Java 17** on PATH.
- Spark package pulled from Maven on first run (cached in `~/.ivy2`):
  `org.apache.hadoop:hadoop-aws:3.4.2` - the `s3a://` filesystem.
- **boto3** to create the S3 bucket on LocalStack.
- **LocalStack S3 3.8** (pinned; `latest` is a Pro build that fails activation).

## Acceptance criteria mapping

- *Data lake set up using a cloud-based solution* -> LocalStack S3 (bronze and
  silver layers under `s3a://`).
- *Cleansing task tested; silver is free of the identified issues* -> the
  verification step asserts 0 nulls, 0 outliers, 0 duplicates.
- *Rules and transformations documented* -> this README plus the module docstring
  and per-rule function docstrings.
- *Easily integrated into a bronze -> silver pipeline* -> the pipeline reads from
  bronze and writes to silver, and all rules are a single `cleanse()` call.
