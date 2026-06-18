# 13 — Slowly Changing Dimension (SCD Type 2)

## Goal

Demonstrate a **Type 2 Slowly Changing Dimension** in pure SQL on an RDBMS
(PostgreSQL). The script keeps both the **current** and the **historical** state
of a dimension: when a tracked attribute changes, the old row is expired and a
new row is inserted, so nothing is ever overwritten.

This is a **dry-run / reference script** — read it top to bottom to follow the
SCD2 logic. It is self-contained and idempotent (re-running it rebuilds
everything from scratch).

---

## Why SCD Type 2

| Type | Behaviour | History kept |
|------|-----------|--------------|
| Type 1 | Overwrite the old value | No |
| **Type 2** | **Expire old row + insert a new one** | **Full history** |
| Type 3 | Keep previous value in an extra column | Partial (one step back) |

Type 2 is the standard choice when you need to answer *"what did this record
look like at a given point in time?"* — e.g. which membership tier a customer
had when an order was placed.

---

## The dimension

`dim_customer` — one row per **customer version**.

| Column | Role | Notes |
|--------|------|-------|
| `customer_sk` | **Surrogate key** | `BIGSERIAL` PK, unique per version |
| `customer_id` | **Business key** | Stable; identifies the real customer |
| `email`, `city`, `membership_tier` | **Tracked attributes** | A change here creates a new version |
| `effective_start_date` | SCD2 metadata | When the version became valid |
| `effective_end_date` | SCD2 metadata | When it stopped being valid (`9999-12-31` = still current) |
| `is_current` | SCD2 metadata | `1` = active version, `0` = expired |

---

## What the script does

| Step | Section | Action |
|------|---------|--------|
| 1 | DDL | Create `dim_customer` |
| 2 | seed | Initial load — 3 customers, each one current version |
| 3 | staging | Load an incoming source snapshot into `stg_customer` |
| 4 | **DML #1 — UPDATE** | **Expire** current rows whose tracked attributes changed |
| 5 | **DML #2 — INSERT** | **Insert** new current versions (changed + brand-new) |
| 6 | verify | `SELECT` the full history |

Steps 4 and 5 are the heart of SCD2 (the **expire-then-insert** pattern) and
are wrapped in a transaction so the change is applied atomically.

### The incoming batch (Step 3)

| Business key | Change vs current | SCD2 outcome |
|---|---|---|
| `C001` | none | ignored (still current) |
| `C002` | city `Krakow` → `Warsaw` | expire old + insert new version |
| `C003` | email + tier changed | expire old + insert new version |
| `C004` | did not exist | insert as first version |

### The two-statement pattern

1. **Expire (UPDATE).** Join staging to the *current* dimension row on the
   business key and end-date every row where at least one tracked attribute
   differs. Comparison uses MySQL's NULL-safe operator: `NOT (a <=> b)` means
   "a differs from b", correctly handling `NULL`s.

2. **Insert (INSERT … LEFT JOIN).** After the expire step, a business key has
   **no current row** when it is either *brand new* or *just changed*. A single
   `LEFT JOIN … WHERE current row IS NULL` therefore covers **both** cases,
   while unchanged customers (which still have a current row) are skipped.

---

## Expected result

```
sk  id    email               city    tier    start       end         current
--  ----  ------------------  ------  ------  ----------  ----------  -------
1   C001  alice@example.com   Warsaw  SILVER  2024-01-01  9999-12-31  1   untouched
2   C002  bob@example.com     Krakow  GOLD    2024-01-01  2026-06-18  0   expired
4   C002  bob@example.com     Warsaw  GOLD    2026-06-18  9999-12-31  1   new version
3   C003  carol@example.com   Gdansk  SILVER  2024-01-01  2026-06-18  0   expired
5   C003  carol@newmail.com   Gdansk  GOLD    2026-06-18  9999-12-31  1   new version
6   C004  dave@example.com    Poznan  BRONZE  2026-06-18  9999-12-31  1   brand new
```

6 rows total: **4 current + 2 expired**. The history of `C002` and `C003` is
preserved instead of being overwritten.

A **point-in-time** query is what makes this worthwhile:

```sql
SELECT * FROM dim_customer
WHERE '2025-03-01' BETWEEN effective_start_date AND effective_end_date;
```

returns each customer as they were on that date.

---

## How to run

This is a dry-run script; no data download or cluster is needed. Against any
PostgreSQL instance:

```bash
psql -U <user> -d <database> -f scd_type2.sql
```

(or paste it into any SQL client). Re-running it is safe — it drops and
recreates the tables each time.

---

## Acceptance criteria coverage

| NEBo step / criterion | Where |
|---|---|
| Determine a dimension, its keys, and attributes | Header comment + "The dimension" table |
| Create SQL script for the table | Step 1 (DDL `CREATE TABLE dim_customer`) |
| Choose a type of SCD to implement | Type 2 (expire + insert) |
| Create SQL script with DML | Steps 4–5 (`UPDATE` + `INSERT`) |
| SQL script contains DML that modifies the dimension table | `UPDATE` (expire) and `INSERT` (new versions) |
| Dimension table modified according to the chosen SCD type | History preserved via `is_current` + effective dates |
