-- ============================================================================
-- Transpose a Dataset from Rows to Columns and Vice Versa
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    name     NVARCHAR(50) NOT NULL,
    quarter  NVARCHAR(10) NOT NULL,
    revenue  INT          NOT NULL
);

INSERT INTO dbo.pex_sample (name, quarter, revenue) VALUES
    ('Alice', 'Q1', 100),
    ('Alice', 'Q2', 150),
    ('Alice', 'Q3', 200),
    ('Bob',   'Q1', 300),
    ('Bob',   'Q2', 250),
    ('Bob',   'Q3', 400);


-- 1. PIVOT — transpose rows into columns with aggregation
SELECT name, [Q1], [Q2], [Q3]
FROM dbo.pex_sample
PIVOT (
    SUM(revenue) FOR quarter IN ([Q1], [Q2], [Q3])
) AS pvt;
-- Alice: Q1=100, Q2=150, Q3=200
-- Bob:   Q1=300, Q2=250, Q3=400


-- 2. UNPIVOT — transpose columns back into rows
-- First create a pivoted table to unpivot
IF OBJECT_ID('dbo.pex_sample_pivoted', 'U') IS NOT NULL DROP TABLE dbo.pex_sample_pivoted;

SELECT name, [Q1], [Q2], [Q3]
INTO dbo.pex_sample_pivoted
FROM dbo.pex_sample
PIVOT (
    SUM(revenue) FOR quarter IN ([Q1], [Q2], [Q3])
) AS pvt;

SELECT name, quarter, revenue
FROM dbo.pex_sample_pivoted
UNPIVOT (
    revenue FOR quarter IN ([Q1], [Q2], [Q3])
) AS unpvt;
-- Alice, Q1, 100
-- Alice, Q2, 150
-- Alice, Q3, 200
-- Bob,   Q1, 300
-- Bob,   Q2, 250
-- Bob,   Q3, 400
