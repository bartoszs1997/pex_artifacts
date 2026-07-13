# Task 05 - Silver and gold layer pipelines (Delta Lake)

Two Delta Lake pipelines over the e-commerce Customers dataset, then a
demonstration of Delta features on the gold table. The data lake is imitated with
**LocalStack S3**.

```
silver : s3a://pex-datalake/silver/customer_silver   (Delta)
gold   : s3a://pex-datalake/gold/customer_gold       (Delta)
```

## Dataset

Customers (`datascientistanna/customers-dataset`), a single `Customers.csv` of
2,000 rows. Source columns: `CustomerID, Gender, Age, Annual Income ($),
Spending Score (1-100), Profession, Work Experience, Family Size`.

Data quality issues found: 35 missing `Profession`, 24 rows with `Age = 0`, 2
rows with `Annual Income = 0` (impossible values).

## Pipeline 1 - Silver (ETL: clean and conform)

Reads the raw CSV and produces `customer_silver`:

- Rename columns to snake_case and cast to target types.
- **Standardize** `gender` to title case (`initcap(trim(...))`).
- **Handle missing values**: `profession` nulls filled with `"Unknown"`.
- **Drop invalid rows**: `age <= 0` or `annual_income <= 0`.

Result: 1,974 clean rows (26 invalid dropped).

### Silver schema (`SILVER_SCHEMA`)

| Column | Type |
|---|---|
| `customer_id` | long |
| `gender` | string |
| `age` | int |
| `annual_income` | int |
| `spending_score` | int |
| `profession` | string |
| `work_experience` | int |
| `family_size` | int |

## Pipeline 2 - Gold (narrow for customer segmentation)

Loads `customer_silver`, filters and aggregates into `customer_gold`:

- **Filter**: keep customers aged 18-70 (segmentation-relevant).
- **Derive dimensions**:
  - `age_group`: `Young` (<30), `Adult` (30-49), `Senior` (50+).
  - `spending_level`: `Low` (<34), `Medium` (34-66), `High` (67+).
- **Aggregate per segment**: customer count, average income, average spending
  score, average family size.

Result: 9 segments (3 age groups x 3 spending levels).

### Gold schema (`GOLD_SCHEMA`)

| Column | Type |
|---|---|
| `age_group` | string |
| `spending_level` | string |
| `customer_count` | long |
| `avg_income` | double |
| `avg_spending_score` | double |
| `avg_family_size` | double |
| `high_value` | boolean (added later by schema evolution) |

## Delta Lake feature demonstration (on `customer_gold`)

1. **UPDATE** - increment `customer_count` for the Young/High segment.
2. **INSERT via MERGE** - add a new `Unknown/Unknown` segment
   (`whenNotMatchedInsertAll`).
3. **DELETE** - remove the placeholder `Unknown/Unknown` segment.
4. **SCHEMA EVOLUTION** - add a `high_value` boolean column via a write with
   `mergeSchema=true`.

## How to run

```bash
# 1. Download the dataset (Kaggle credentials read from ~/.kaggle/)
uv run python src/data_lake/05_silver_gold_layers/download_data.py

# 2. Bring up the data lake (LocalStack S3)
docker compose -f src/data_lake/05_silver_gold_layers/docker-compose.yml up -d

# 3. Run the pipelines (Java 17 must be on PATH for PySpark)
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/data_lake/05_silver_gold_layers/silver_gold.py

# 4. Tear the data lake down
docker compose -f src/data_lake/05_silver_gold_layers/docker-compose.yml down -v
```

## How to verify

The final lines print the gold table and:

```
VERIFIED: customer_silver has 1974 clean rows; customer_gold has 9 segments with
evolved schema [...'high_value']
```

`verify()` asserts:
- the silver schema equals `SILVER_SCHEMA`,
- silver has no invalid ages/incomes and no null professions,
- the gold table has the `high_value` column (schema evolution succeeded).

## Dependencies and configuration

- **PySpark 4.1.1** (host), **Java 17** on PATH.
- Spark packages pulled from Maven on first run (cached in `~/.ivy2`):
  - `io.delta:delta-spark_2.13:4.3.0` - Delta Lake format + `DeltaTable` API.
  - `org.apache.hadoop:hadoop-aws:3.4.2` - the `s3a://` filesystem.
- **boto3** to create the S3 bucket on LocalStack.
- **LocalStack S3 3.8** (pinned; `latest` is a Pro build that fails activation).

## Acceptance criteria mapping

- *ETL pipeline extracts raw data, transforms (missing values, standardize
  formats), cleanses* -> the silver pipeline (`build_silver`).
- *Demonstrate Delta features on customer_gold (update, insert, delete, evolve
  schema)* -> `demonstrate_delta_features`.
- *Document design decisions, source, transformations, both schemas* -> this
  README plus the module and function docstrings.
