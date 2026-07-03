# 10 — Select Semi-Structured Data

## Goal

Demonstrate XML and JSON manipulation in T-SQL using built-in functions and clauses.

---

## Concepts used

| Category | Functions / clauses |
|---|---|
| XML import | `OPENROWSET` |
| Relational to XML | `FOR XML PATH`, `ROOT` |
| XML to relational | `OPENXML`, `sp_xml_preparedocument` |
| JSON validation | `ISJSON` |
| JSON scalar extraction | `JSON_VALUE` |
| JSON object/array extraction | `JSON_QUERY` |
| JSON modification | `JSON_MODIFY` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | `OPENROWSET` — import XML from file as SINGLE_BLOB |
| 2 | `FOR XML` — convert relational rows to XML document |
| 3 | `OPENXML` — shred XML attributes into relational columns |
| 4 | `ISJSON` — validate JSON strings (1 = valid, 0 = invalid) |
| 5 | `JSON_VALUE` — extract scalar values (age, active) |
| 6 | `JSON_QUERY` — extract array (skills) |
| 7 | `JSON_MODIFY` — update a value in JSON string |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| OPENROWSET (import XML) | Section 1 |
| FOR XML (relational to XML) | Section 2 |
| OPENXML (XML to relational) | Section 3 |
| ISJSON | Section 4 |
| JSON_VALUE | Section 5 |
| JSON_QUERY | Section 6 |
| JSON_MODIFY | Section 7 |
