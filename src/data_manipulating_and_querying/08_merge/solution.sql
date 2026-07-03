-- ============================================================================
-- Perform Insert, Update, and Delete in a Single Statement (MERGE)
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;
IF OBJECT_ID('dbo.pex_sample_source', 'U') IS NOT NULL DROP TABLE dbo.pex_sample_source;

CREATE TABLE dbo.pex_sample (
    id      INT          PRIMARY KEY,
    name    NVARCHAR(50) NOT NULL,
    salary  INT          NOT NULL
);

CREATE TABLE dbo.pex_sample_source (
    id      INT          PRIMARY KEY,
    name    NVARCHAR(50) NOT NULL,
    salary  INT          NOT NULL
);

INSERT INTO dbo.pex_sample (id, name, salary) VALUES
    (1, 'Alice', 5000),
    (2, 'Bob',   4000),
    (3, 'Carol', 3000);

INSERT INTO dbo.pex_sample_source (id, name, salary) VALUES
    (2, 'Bob',   4500),   -- exists in target → UPDATE salary 4000 -> 4500
    (3, 'Carol', 3000),   -- exists, same data → UPDATE (no-op in practice)
    (4, 'Dave',  6000);   -- not in target     → INSERT


-- MERGE — insert, update, and delete in a single statement
MERGE dbo.pex_sample AS target
USING dbo.pex_sample_source AS source
ON target.id = source.id

WHEN MATCHED THEN
    UPDATE SET
        target.name   = source.name,
        target.salary = source.salary

WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, name, salary)
    VALUES (source.id, source.name, source.salary)

WHEN NOT MATCHED BY SOURCE THEN
    DELETE;
-- Result:
--   Alice (id=1) → DELETED      (not in source)
--   Bob   (id=2) → UPDATED      (salary 4000 -> 4500)
--   Carol (id=3) → UPDATED      (same values, no visible change)
--   Dave  (id=4) → INSERTED     (new row from source)


-- Verify final state
SELECT * FROM dbo.pex_sample;
-- id=2, Bob, 4500
-- id=3, Carol, 3000
-- id=4, Dave, 6000
