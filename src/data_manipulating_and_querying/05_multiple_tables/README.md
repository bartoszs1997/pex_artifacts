# 05 — Select Data from Multiple Tables (Set Operators)

## Goal

Combine results of two or more SELECT statements using T-SQL set operators.

---

## Concepts used

| Category | Operator |
|---|---|
| All rows including duplicates | `UNION ALL` |
| All rows without duplicates | `UNION` |
| Common rows | `INTERSECT` |
| Difference | `EXCEPT` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | `UNION ALL` — merges both tables, Carol appears twice (6 rows) |
| 2 | `UNION` — merges both tables, duplicate Carol removed (5 rows) |
| 3 | `INTERSECT` — only rows present in both tables (1 row: Carol) |
| 4 | `EXCEPT` — rows in table A that are not in table B (2 rows: Alice, Bob) |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| UNION ALL | Section 1 |
| UNION | Section 2 |
| INTERSECT | Section 3 |
| EXCEPT | Section 4 |
