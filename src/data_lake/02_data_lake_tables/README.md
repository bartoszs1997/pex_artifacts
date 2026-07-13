# Task 02 - Work with tables in a data lake

Create different table formats (Parquet, CSV, Avro), perform CRUD operations on
them, and merge two tables into a new one. The data lake is imitated with
**LocalStack S3** (a local, free stand-in for AWS S3); every table lives under
`s3a://pex-datalake/tables/`.

## Dataset

Powerlifting Database (`dansbecker/powerlifting-database`), file
`openpowerlifting.csv`. It is the single source table for every operation. To
keep local Spark fast we only use the meets with `MeetID <= 4` (~300 rows); this
slice still contains `meet_id == 2`, which step 5 deletes.

Column mapping (source -> our tables):

| Source column | Our column |
|---|---|
| `MeetID` | `meet_id` |
| `Name` | `user_name` |
| `Age` | `user_age` |
| `Division` | `division` / `devision` |
| `Equipment` | `equipment` |
| `BestSquatKg` | `best_squad_kg` |
| `WeightClassKg` | used only for the average query |

## Tables produced

| Table | Format | Path | Columns |
|---|---|---|---|
| `user_data` | Parquet | `s3a://pex-datalake/tables/user_data` | `meet_id, user_name, user_age (+ division)` |
| `characteristic_data` | CSV | `s3a://pex-datalake/tables/characteristic_data` | `equipment, devision, best_squad_kg` |
| `merged_data` | Avro | `s3a://pex-datalake/tables/merged_data` | all columns from both tables |

## What the script does (the eight required steps)

1. **Create** an empty Parquet table `user_data` with columns `meet_id`,
   `user_name`, `user_age`.
2. **Insert** rows into `user_data` from the source table.
3. **Query** the average `WeightClassKg` from the CSV (source) table.
4. **Update** `user_data` by adding the column `division`.
5. **Delete** the rows where `meet_id == 2`.
6. **Create** a CSV table `characteristic_data` with columns `equipment`,
   `devision`, `best_squad_kg` (one row per division).
7. **Merge** `user_data` and `characteristic_data` into a new Avro table
   `merged_data`, joining on `division == devision`.
8. **Verify** `merged_data` holds the combined columns from both tables with no
   duplicate rows.

## How to run

```bash
# 1. Download the dataset (Kaggle credentials read from ~/.kaggle/)
uv run python src/data_lake/02_data_lake_tables/download_data.py

# 2. Bring up the data lake (LocalStack S3)
docker compose -f src/data_lake/02_data_lake_tables/docker-compose.yml up -d

# 3. Run all table operations (Java 17 must be on PATH for PySpark)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/data_lake/02_data_lake_tables/table_operations.py

# 4. Tear the data lake down
docker compose -f src/data_lake/02_data_lake_tables/docker-compose.yml down -v
```

## How to verify

The final line of the run prints, for example:

```
VERIFIED: merged_data has 167 rows, no duplicates, and columns from both tables
```

The verification step asserts that `merged_data` contains every column from both
source tables (`meet_id, user_name, user_age, division` and `equipment,
devision, best_squad_kg`) and that the total row count equals the distinct row
count (no duplicates).

## Dependencies and configuration

- **PySpark 4.1.1** (host), **Java 17** on PATH.
- Spark packages pulled from Maven on first run (cached in `~/.ivy2`):
  - `org.apache.hadoop:hadoop-aws:3.4.2` - the `s3a://` filesystem.
  - `org.apache.spark:spark-avro_2.13:4.1.1` - the Avro table format.
- **boto3** to create the S3 bucket on LocalStack.
- **LocalStack S3 3.8** (pinned; `latest` is a Pro build that fails activation).

## Acceptance criteria mapping

- *Data lake environment set up using a cloud-based data lake solution* ->
  LocalStack S3 imitates AWS S3; all tables are stored there via `s3a://`.
- *Usage instructions, dependencies and configurations documented* -> this
  README (run steps, packages, versions, table layout).
