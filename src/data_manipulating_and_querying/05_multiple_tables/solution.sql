-- ============================================================================
-- Select Data from Multiple Tables — Set Operators
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample_a', 'U') IS NOT NULL DROP TABLE dbo.pex_sample_a;
IF OBJECT_ID('dbo.pex_sample_b', 'U') IS NOT NULL DROP TABLE dbo.pex_sample_b;

CREATE TABLE dbo.pex_sample_a (
    id    INT          PRIMARY KEY,
    name  NVARCHAR(50) NOT NULL,
    city  NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.pex_sample_b (
    id    INT          PRIMARY KEY,
    name  NVARCHAR(50) NOT NULL,
    city  NVARCHAR(50) NOT NULL
);

INSERT INTO dbo.pex_sample_a (id, name, city) VALUES
    (1, 'Alice',  'Warsaw'),
    (2, 'Bob',    'Berlin'),
    (3, 'Carol',  'Prague');

INSERT INTO dbo.pex_sample_b (id, name, city) VALUES
    (3, 'Carol',  'Prague'),
    (4, 'Dave',   'Berlin'),
    (5, 'Eve',    'Paris');


-- 1. UNION ALL — all rows from both queries, duplicates kept
SELECT id, name, city FROM dbo.pex_sample_a
UNION ALL
SELECT id, name, city FROM dbo.pex_sample_b;
-- 6 rows: Alice, Bob, Carol, Carol, Dave, Eve  (Carol appears twice)


-- 2. UNION — all rows from both queries, duplicates removed
SELECT id, name, city FROM dbo.pex_sample_a
UNION
SELECT id, name, city FROM dbo.pex_sample_b;
-- 5 rows: Alice, Bob, Carol, Dave, Eve  (Carol once)


-- 3. INTERSECT — only rows common to both queries
SELECT id, name, city FROM dbo.pex_sample_a
INTERSECT
SELECT id, name, city FROM dbo.pex_sample_b;
-- 1 row: Carol (id=3, Prague)


-- 4. EXCEPT — rows in first query that are NOT in second
SELECT id, name, city FROM dbo.pex_sample_a
EXCEPT
SELECT id, name, city FROM dbo.pex_sample_b;
-- 2 rows: Alice (Warsaw), Bob (Berlin)
