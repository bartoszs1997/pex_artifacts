-- ============================================================================
-- Rank the Data in Database Queries
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    dept       NVARCHAR(20) NOT NULL,
    employee   NVARCHAR(50) NOT NULL,
    salary     INT          NOT NULL
);

INSERT INTO dbo.pex_sample (dept, employee, salary) VALUES
    ('Engineering', 'Alice',  9000),
    ('Engineering', 'Bob',    8500),
    ('Engineering', 'Carol',  8500),
    ('Engineering', 'Dave',   7000),
    ('Sales',       'Eve',    7500),
    ('Sales',       'Frank',  7500),
    ('Sales',       'Grace',  6000);


-- 1. RANK — assigns rank; ties get the same rank, next rank is skipped
SELECT
    dept,
    employee,
    salary,
    RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
    -- Engineering: Alice=1, Bob=2, Carol=2, Dave=4
    -- Sales:       Eve=1, Frank=1, Grace=3
FROM dbo.pex_sample;


-- 2. DENSE_RANK — same as RANK but without gaps after ties
SELECT
    dept,
    employee,
    salary,
    DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dense_rnk
    -- Engineering: Alice=1, Bob=2, Carol=2, Dave=3  (no gap!)
    -- Sales:       Eve=1, Frank=1, Grace=2
FROM dbo.pex_sample;


-- 3. NTILE — distributes rows into N roughly equal buckets
SELECT
    dept,
    employee,
    salary,
    NTILE(2) OVER (PARTITION BY dept ORDER BY salary DESC) AS bucket
    -- Engineering (4 rows, 2 buckets): Alice=1, Bob=1, Carol=2, Dave=2
    -- Sales       (3 rows, 2 buckets): Eve=1, Frank=1, Grace=2
FROM dbo.pex_sample;


-- 4. ROW_NUMBER — sequential number, no ties (deterministic with unique order)
SELECT
    dept,
    employee,
    salary,
    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS row_num
    -- Engineering: Alice=1, Bob=2, Carol=3, Dave=4
    -- Sales:       Eve=1, Frank=2, Grace=3
FROM dbo.pex_sample;
