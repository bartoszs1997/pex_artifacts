# 02 — Manipulate String Values in Database Queries

## Goal

Demonstrate string manipulation in T-SQL using scalar functions from the task requirements.

---

## Concepts used

| Function | Purpose |
|---|---|
| `CHARINDEX` | Starting position of expression in a string |
| `LEFT` | Left part of string with N characters |
| `LEN` | Length of string (excluding trailing spaces) |
| `LTRIM` | Remove leading blanks |
| `RTRIM` | Remove trailing blanks |
| `PATINDEX` | Starting position using wildcard pattern |
| `REPLACE` | Replace occurrences of text |
| `REPLICATE` | Repeat expression N times |
| `REVERSE` | Reverse a string |
| `SUBSTRING` | Extract portion of string |
| `UPPER` | Convert to uppercase |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | `CHARINDEX` — find position of space, dot, @ in strings |
| 2 | `LEFT` — extract first N chars, extract email username |
| 3 | `LEN` — string lengths (ignores trailing spaces) |
| 4 | `LTRIM` — strip leading spaces |
| 5 | `RTRIM` — strip trailing spaces |
| 6 | `PATINDEX` — wildcard pattern matching positions |
| 7 | `REPLACE` — replace spaces with underscores, swap domain |
| 8 | `REPLICATE` — mask names with asterisks, build separator |
| 9 | `REVERSE` — reverse names and emails |
| 10 | `SUBSTRING` — extract first 3 chars, extract email domain |
| 11 | `UPPER` — convert to uppercase |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| CHARINDEX | Section 1 |
| LEFT | Section 2 |
| LEN | Section 3 |
| LTRIM | Section 4 |
| RTRIM | Section 5 |
| PATINDEX | Section 6 |
| REPLACE | Section 7 |
| REPLICATE | Section 8 |
| REVERSE | Section 9 |
| SUBSTRING | Section 10 |
| UPPER | Section 11 |
