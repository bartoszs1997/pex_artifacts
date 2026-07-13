# Task 10 - Define and follow schema enforcement rules

Define a strict schema for customer records and enforce it on a Delta table so
bad records can never be committed. Incoming raw data is validated against the
schema; non-compliant rows are handled with all three strategies the task lists
(reject, transform, discard). The task outcome is "Documentation, program code",
so the deliverables are [`SCHEMA.md`](./SCHEMA.md) (the schema, the chosen
mechanism, the strategy) and `schema_enforcement.py` (the implementation).

## Dataset

[Customers](https://www.kaggle.com/datasets/datascientistanna/customers-dataset)
(`datascientistanna/customers-dataset`, `Customers.csv`, 2000 rows). It contains
real schema violations - 35 blank `Profession` values and 24 rows with `Age = 0`
- which the enforcement pipeline catches. Downloaded into `data/input/`
(gitignored).

```bash
uv run python src/data_lake/10_schema_enforcement/download_data.py
```

## What it does

1. **Defines the schema** - a Spark `StructType` (typed, NOT NULL fields) plus
   five named Delta `CHECK` constraints (see `SCHEMA.md`).
2. **Creates a schema-enforced Delta table** - the schema and constraints are
   stored in the transaction log; Delta rejects any non-conforming write.
3. **Validates incoming data** and applies the strategies:
   - **Reject**: a deliberate bad row (`age = 0`) is blocked by Delta (error
     shown).
   - **Transform**: blank professions become `"Unknown"`; null numerics default
     to `0`.
   - **Discard**: rows still invalid after transform go to a quarantine table.
4. **Verifies** the clean table has zero violations, the constraints are active,
   the reject fired, and no rows were lost.

## How to run

Java 17 must be on PATH for PySpark.

```bash
docker compose -f src/data_lake/10_schema_enforcement/docker-compose.yml up -d

export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
uv run python src/data_lake/10_schema_enforcement/schema_enforcement.py

docker compose -f src/data_lake/10_schema_enforcement/docker-compose.yml down -v
```

## How to verify

`verify()` asserts: the clean table has 0 constraint violations; all 5 CHECK
constraints exist in the table metadata; the reject actually raised a constraint
error; and `loaded + quarantined = total`. Expected final line:

```
VERIFIED: 1976 rows in the schema-enforced table (0 violations, 5 CHECK
constraints active); REJECT blocked a bad write; 24 non-compliant rows
quarantined; loaded + quarantined = 2000 total
```

## Acceptance criteria mapping

| Criterion | Where it is met |
|---|---|
| Schema defined (types, field names, constraints) | `CUSTOMER_SCHEMA` + `CONSTRAINTS` in code; table in `SCHEMA.md` |
| Enforcement mechanism chosen | Delta Lake schema + CHECK/NOT NULL constraints (`create_enforced_table`) |
| Code/tools written to perform enforcement | `schema_enforcement.py` |
| Validation identifies inconsistencies/errors | `all_constraints_expr` + `demonstrate_reject` |
| Strategy for non-compliant data implemented | Reject + Transform + Discard (`demonstrate_reject`, `transform_to_schema`, quarantine) |

## Dependencies

`pyspark==4.1.1`, `delta-spark==4.3.0`, `boto3` (root `pyproject.toml`).
LocalStack S3 via Docker. Spark pulls `delta-spark` and `hadoop-aws` jars on
first run (cached in `~/.ivy2`).
