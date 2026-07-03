# 12 — Run Full-Text Queries Against Character-Based Data

## Goal

Demonstrate the Full-Text Search feature: installation, catalog, index creation, and querying with CONTAINS and FREETEXT.

---

## Concepts used

| Category | Feature |
|---|---|
| Installation check | `FULLTEXTSERVICEPROPERTY` |
| Catalog | `CREATE FULLTEXT CATALOG` |
| Index | `CREATE FULLTEXT INDEX` |
| Exact word/phrase search | `CONTAINS` |
| Natural language search | `FREETEXT` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | Verify Full-Text Search is installed |
| 2 | Create a full-text catalog |
| 3 | Create a full-text index on title and content columns |
| 4a | `CONTAINS` — exact word, phrase, and OR boolean search |
| 4b | `FREETEXT` — natural language semantic matching |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| Install / verify Full-Text Search | Section 1 |
| Create full-text catalog | Section 2 |
| Create full-text index | Section 3 |
| CONTAINS queries | Section 4a |
| FREETEXT queries | Section 4b |
