# Schema Definition and Enforcement

This document is the documentation deliverable for task 10 (outcome:
"Documentation, program code"). It defines the schema, states the enforcement
mechanism we chose and why, and describes the strategy for non-compliant data.
The companion `schema_enforcement.py` implements exactly what is written here.

## 1. Schema definition

The customer record schema fixes field names, data types, and constraints.

### Fields and types (structural contract)

| Field | Type | Nullable | Source column |
|---|---|---|---|
| `customer_id` | INT | no | CustomerID |
| `gender` | STRING | no | Gender |
| `age` | INT | no | Age |
| `annual_income` | INT | no | Annual Income ($) |
| `spending_score` | INT | no | Spending Score (1-100) |
| `profession` | STRING | no | Profession |
| `work_experience` | INT | no | Work Experience |
| `family_size` | INT | no | Family Size |

This is declared once as a Spark `StructType` (with `nullable=False`) and as the
column list of the Delta `CREATE TABLE ... NOT NULL`.

### Constraints (business rules)

Enforced as named Delta `CHECK` constraints stored in the table metadata:

| Constraint | Rule |
|---|---|
| `valid_age` | `age > 0 AND age < 120` |
| `non_negative_income` | `annual_income >= 0` |
| `valid_spending_score` | `spending_score BETWEEN 0 AND 100` |
| `non_blank_profession` | `length(trim(profession)) > 0` |
| `non_negative_experience` | `work_experience >= 0` |

## 2. Enforcement mechanism (chosen)

**Delta Lake schema enforcement.** The task offers two options: file formats
with built-in schema enforcement (Parquet/Avro) or an external metadata system
like Databricks. We use **Delta Lake OSS**, which is the open-source form of the
Databricks option: the schema and the named `CHECK` / `NOT NULL` constraints are
persisted in the Delta transaction log, and Delta **rejects at write time** any
data that does not match the column schema or violates a constraint. This is
stronger than plain Parquet/Avro (which validate structure but not value-level
business rules).

> Databricks -> OSS: Databricks Delta and Delta Lake OSS enforce schema and
> constraints identically; this code runs unchanged on Databricks.

## 3. Validation and the non-compliant-data strategy

Incoming raw data is validated against the full contract (types + NOT NULL +
CHECK). We implement **all three** strategies the task lists:

1. **Reject** - appending a row that breaks a constraint (we test `age = 0`)
   causes Delta to abort the entire transaction with a
   `DELTA_VIOLATE_CONSTRAINT` error. This proves enforcement is real.
2. **Transform** - fixable violations are repaired so the row fits the schema:
   a blank/null `profession` becomes `"Unknown"`; null `work_experience` /
   `family_size` default to `0`.
3. **Discard (quarantine)** - rows that are still non-compliant after transform
   (e.g. `age = 0`, which cannot be sensibly repaired) are written to a separate
   **quarantine** Delta table instead of the clean table. Nothing is silently
   dropped, so every rejected row remains auditable.

The clean table therefore contains only fully compliant rows, and Delta
re-checks every constraint on that final write as a second line of defense.

## 4. How to navigate the result

- `s3a://pex-datalake/customers` - the schema-enforced clean table.
- `s3a://pex-datalake/customers_quarantine` - rows rejected by validation.
- `SHOW TBLPROPERTIES delta.`.../customers`` lists the active
  `delta.constraints.*` entries (the enforcement rules in force).

With this dataset: 2000 raw rows -> 1976 loaded (35 blank professions repaired by
Transform) and 24 quarantined (`age = 0`, unfixable).
