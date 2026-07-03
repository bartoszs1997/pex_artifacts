# 08 — Perform Insert, Update, and Delete in a Single Statement

## Goal

Demonstrate the MERGE statement to handle insert, update, and delete operations in one atomic statement.

---

## Concepts used

| Category | Feature |
|---|---|
| Single-statement DML | `MERGE ... USING ... ON` |
| Update existing rows | `WHEN MATCHED THEN UPDATE` |
| Insert new rows | `WHEN NOT MATCHED BY TARGET THEN INSERT` |
| Delete stale rows | `WHEN NOT MATCHED BY SOURCE THEN DELETE` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| Setup | Target table (Alice, Bob, Carol) and source table (Bob, Carol, Dave) |
| MERGE | All three operations in one statement: Alice deleted, Bob updated, Dave inserted |
| Verify | SELECT to confirm final state |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| MERGE statement | Main query |
| INSERT operation | `WHEN NOT MATCHED BY TARGET` — Dave inserted |
| UPDATE operation | `WHEN MATCHED` — Bob's salary updated |
| DELETE operation | `WHEN NOT MATCHED BY SOURCE` — Alice deleted |
