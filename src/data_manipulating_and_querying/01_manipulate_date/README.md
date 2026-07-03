# 01 — Manipulate Date and Time Data

## Goal

Demonstrate date/time manipulation in T-SQL using functions from the task requirements.

---

## Concepts used

| Category | Functions |
|---|---|
| System date/time | `SYSDATETIME`, `SYSDATETIMEOFFSET` |
| Date/time parts | `DATEPART`, `DATENAME`, `YEAR`, `MONTH`, `DAY` |
| Construct from parts | `DATEFROMPARTS`, `DATETIME2FROMPARTS`, `DATETIMEFROMPARTS`, `DATETIMEOFFSETFROMPARTS`, `SMALLDATETIMEFROMPARTS`, `TIMEFROMPARTS` |
| Differences | `DATEDIFF` |
| Modify values | `DATEADD`, `EOMONTH`, `SWITCHOFFSET` |
| Validate | `ISDATE` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | `SYSDATETIME`, `SYSDATETIMEOFFSET` |
| 2 | `DATEPART` (numeric), `DATENAME` (text), `YEAR`/`MONTH`/`DAY` shorthands |
| 3 | All six `*FROMPARTS` functions |
| 4 | `DATEDIFF` in DAY, MONTH, YEAR, HOUR |
| 5 | `DATEADD`, `EOMONTH` (with offset), `SWITCHOFFSET` |
| 6 | `ISDATE` on valid/invalid strings |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| Functions that return system date and time values | Section 1 |
| Functions that return date and time parts | Section 2 |
| Functions that return date/time values from their parts | Section 3 |
| Function that returns date and time difference values | Section 4 |
| Functions that modify date and time values | Section 5 |
| Function that validates date and time values | Section 6 |
