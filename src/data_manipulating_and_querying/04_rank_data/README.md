# 04 — Rank the Data in Database Queries

## Goal

Demonstrate ranking functions with OVER and PARTITION BY to assign ranks within partitions.

---

## Concepts used

| Category | Functions |
|---|---|
| Ranking with gaps | `RANK` |
| Ranking without gaps | `DENSE_RANK` |
| Bucket distribution | `NTILE` |
| Sequential numbering | `ROW_NUMBER` |
| Windowing clauses | `OVER`, `PARTITION BY`, `ORDER BY` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | `RANK` — ties get same rank, next rank is skipped (1,2,2,4) |
| 2 | `DENSE_RANK` — ties get same rank, no gaps (1,2,2,3) |
| 3 | `NTILE(2)` — splits rows into 2 roughly equal buckets per partition |
| 4 | `ROW_NUMBER` — unique sequential number per partition, no ties |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| RANK | Section 1 |
| DENSE_RANK | Section 2 |
| NTILE | Section 3 |
| ROW_NUMBER | Section 4 |
| OVER / PARTITION BY | All sections |
