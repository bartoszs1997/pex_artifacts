# 03 — Explicit Data Type Conversion

## Goal

Demonstrate manual (explicit) conversion of data types in T-SQL using CAST, CONVERT, PARSE, TRY_PARSE, TRY_CAST, and TRY_CONVERT.

---

## Concepts used

| Category | Functions |
|---|---|
| Standard conversion | `CAST`, `CONVERT` |
| Culture-aware parsing | `PARSE`, `TRY_PARSE` |
| Safe conversion (NULL on failure) | `TRY_CAST`, `TRY_CONVERT` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | `CAST` — string→int, decimal→int (truncation), decimal→string, string→date |
| 2 | `CONVERT` — basic conversion + style codes for date formatting (101, 104, 121) |
| 3 | `PARSE` — culture-aware parsing: US date, German date, US number format |
| 4 | `TRY_PARSE` — safe parsing returning NULL for invalid strings |
| 5 | `TRY_CAST` — safe cast returning NULL for unconvertible values |
| 6 | `TRY_CONVERT` — safe convert returning NULL for unconvertible values |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| CAST | Section 1 |
| CONVERT | Section 2 |
| PARSE | Section 3 |
| TRY_PARSE | Section 4 |
| TRY_CAST | Section 5 |
| TRY_CONVERT | Section 6 |
