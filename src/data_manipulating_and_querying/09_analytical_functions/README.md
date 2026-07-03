# 09 — Use Analytical Functions in Database Queries

## Goal

Demonstrate analytical (window) functions for row-level calculations across partitions.

---

## Concepts used

| Category | Functions |
|---|---|
| Row offset | `LAG`, `LEAD` |
| Boundary values | `FIRST_VALUE`, `LAST_VALUE` |
| Distribution / percentile | `CUME_DIST`, `PERCENTILE_CONT`, `PERCENTILE_DISC` |
| Relative ranking | `PERCENT_RANK` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | `LAG` — salary from the previous row in partition |
| 2 | `LEAD` — salary from the next row in partition |
| 3 | `CUME_DIST` — cumulative distribution position (0–1) |
| 4 | `FIRST_VALUE` — highest earner per department |
| 5 | `LAST_VALUE` — lowest earner per department (with ROWS frame) |
| 6 | `PERCENTILE_CONT` — interpolated median salary |
| 7 | `PERCENTILE_DISC` — discrete median salary (actual row value) |
| 8 | `PERCENT_RANK` — relative rank within partition (0–1) |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| LAG | Section 1 |
| LEAD | Section 2 |
| CUME_DIST | Section 3 |
| FIRST_VALUE | Section 4 |
| LAST_VALUE | Section 5 |
| PERCENTILE_CONT | Section 6 |
| PERCENTILE_DISC | Section 7 |
| PERCENT_RANK | Section 8 |
