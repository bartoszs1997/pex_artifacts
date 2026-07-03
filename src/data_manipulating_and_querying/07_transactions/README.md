# 07 — Implement and Manage Transactions and Isolation Levels

## Goal

Demonstrate explicit transactions, isolation levels, savepoints, and rollback logic in T-SQL.

---

## Concepts used

| Category | Feature |
|---|---|
| Isolation level | `SET TRANSACTION ISOLATION LEVEL READ COMMITTED` |
| Explicit transaction | `BEGIN TRANSACTION` / `COMMIT TRANSACTION` |
| Savepoint | `SAVE TRANSACTION` |
| Partial rollback | `ROLLBACK TRANSACTION <savepoint>` |
| Error handling | `TRY` / `CATCH` with full `ROLLBACK` |

---

## What the script does

| Section | What it demonstrates |
|---------|----------------------|
| 1 | Set isolation level to READ COMMITTED |
| 2 | Explicit transaction — atomic transfer between two accounts |
| 3 | Savepoint — partial rollback undoes one update while keeping another |
| 4 | TRY/CATCH — full rollback on error to maintain consistency |

---

## Acceptance criteria

| Requirement | Where |
|---|---|
| Choosing and setting isolation level | Section 1 |
| Explicit transaction with multiple DML | Sections 2, 3, 4 |
| Rollback logic | Sections 3, 4 |
| Save point feature | Section 3 |
