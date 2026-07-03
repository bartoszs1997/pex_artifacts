# 06 — Use Logical Join Operators in Queries

## Goal

Demonstrate all five logical join types to combine rows from multiple tables.

---

## Concepts used

| Category | Operator |
|---|---|
| Matching rows only | `INNER JOIN` |
| All left + matching right | `LEFT OUTER JOIN` |
| All right + matching left | `RIGHT OUTER JOIN` |
| All rows from both sides | `FULL OUTER JOIN` |
| Cartesian product | `CROSS JOIN` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | `INNER JOIN` — only customers with orders (3 rows) |
| 2 | `LEFT OUTER JOIN` — all customers, NULLs for Carol (no orders) |
| 3 | `RIGHT OUTER JOIN` — all orders, NULL name for orphan order (customer_id=9) |
| 4 | `FULL OUTER JOIN` — both unmatched sides shown (Carol + orphan Monitor) |
| 5 | `CROSS JOIN` — every customer x every color (3 x 2 = 6 rows) |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| Inner Join | Section 1 |
| Left Outer Join | Section 2 |
| Right Outer Join | Section 3 |
| Full Outer Join | Section 4 |
| Cross Join | Section 5 |
