# 13 — Slowly Changing Dimension (SCD Type 2)

## Goal

Demonstrate a **Type 2 Slowly Changing Dimension** in pure SQL on an RDBMS
(PostgreSQL 15+). The script keeps both the **current** and the **historical**
state of a dimension: when a tracked attribute changes, the old row is expired
and a new row is inserted.

This is a **dry-run / reference script** — self-contained, idempotent, run top
to bottom.

---

## Why SCD Type 2

| Type | Behaviour | History kept |
|------|-----------|--------------|
| Type 1 | Overwrite the old value | No |
| **Type 2** | **Expire old row + insert a new one** | **Full history** |
| Type 3 | Keep previous value in an extra column | Partial (one step back) |

---

## The dimension

`historical_shipping_data` — one row per **package version**.

| Column | Role | Notes |
|--------|------|-------|
| `id` | Surrogate key | `BIGSERIAL` PK, unique per version |
| `package_id` | **Business key** | Stable identifier of the package |
| `status`, `shipping_dt`, `delivery_dt`, `source_address`, `destination_address` | **Tracked attributes** | A change in any of these creates a new version |
| `effective_start_date` | SCD2 metadata | When this version became valid |
| `effective_end_date` | SCD2 metadata | When it stopped being valid (`9999-12-31` = still current) |
| `is_deleted` | SCD2 metadata | `0` = current version, `1` = expired |

---

## What the script does (3-step pattern)

| Step | What | How |
|------|------|-----|
| 1 | Filter out unchanged rows | `EXCEPT` between new data and historical data |
| 2 | Expire old versions | `MERGE` — match on `package_id`, set `is_deleted = 1` and close `effective_end_date` |
| 3 | Insert new current versions | `INSERT ... SELECT` all filtered rows with `is_deleted = 0` |

Steps 2 and 3 are wrapped in `BEGIN / COMMIT` so they succeed or fail together
(no expired row without its replacement, no orphaned insert without the expire).

### The incoming batch

| Package | Change vs current | SCD2 outcome |
|---|---|---|
| `PKG-001` | none | filtered out by EXCEPT (ignored) |
| `PKG-002` | status + delivery_dt changed | expire old + insert new version |
| `PKG-003` | destination changed | expire old + insert new version |
| `PKG-004` | did not exist before | insert as first version |

---

## Expected result

```
id | package_id | status     | ship_dt    | deliv_dt   | src      | dst      | start      | end        | del
---+------------+------------+------------+------------+----------+----------+------------+------------+----
1  | PKG-001    | delivered  | 2025-01-10 | 2025-01-15 | Warsaw   | Krakow   | 2025-01-10 | 9999-12-31 | 0   untouched
2  | PKG-002    | in_transit | 2025-02-01 | NULL       | Gdansk   | Poznan   | 2025-02-01 | 2026-06-19 | 1   expired
5  | PKG-002    | delivered  | 2025-02-01 | 2025-02-08 | Gdansk   | Poznan   | 2026-06-19 | 9999-12-31 | 0   new version
3  | PKG-003    | shipped    | 2025-03-05 | NULL       | Wroclaw  | Lublin   | 2025-03-05 | 2026-06-19 | 1   expired
6  | PKG-003    | in_transit | 2025-03-05 | NULL       | Wroclaw  | Katowice | 2026-06-19 | 9999-12-31 | 0   new version
4  | PKG-004    | shipped    | 2025-04-01 | NULL       | Szczecin | Olsztyn  | 2026-06-19 | 9999-12-31 | 0   brand new
```

6 rows: **4 current + 2 expired**. History of PKG-002 and PKG-003 preserved.

Point-in-time query:

```sql
SELECT * FROM historical_shipping_data
WHERE '2025-02-15' BETWEEN effective_start_date AND effective_end_date;
```

---

## How to run

```bash
psql -U <user> -d <database> -f scd_type2.sql
```

Re-running is safe (drops and recreates tables).

---

## Acceptance criteria coverage

| NEBo requirement | Where in code |
|---|---|
| Determine a dimension, its keys, and attributes | Header comment in SQL |
| Create SQL script for the table (DDL) | `CREATE TABLE historical_shipping_data` |
| Choose a type of SCD | Type 2 (expire + insert) |
| Create SQL script with DML | Step 2 (`MERGE` expire) + Step 3 (`INSERT` new versions) |
| Dimension table modified according to the chosen SCD type | History preserved via `is_deleted` + effective dates |
