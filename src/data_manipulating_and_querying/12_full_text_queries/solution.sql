-- ============================================================================
-- Run Full-Text Queries Against Character-Based Data
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    id      INT           PRIMARY KEY,
    title   NVARCHAR(100) NOT NULL,
    content NVARCHAR(MAX) NOT NULL
);

INSERT INTO dbo.pex_sample (id, title, content) VALUES
    (1, 'SQL Server Performance',   'Indexing strategies improve query performance in large databases.'),
    (2, 'Python Data Processing',   'Python is widely used for data processing and machine learning tasks.'),
    (3, 'Database Administration',  'Backup and recovery procedures are essential for database management.'),
    (4, 'Big Data Engineering',     'Processing large datasets requires distributed computing frameworks.');


-- 1. Install the Full-Text Search feature
-- Full-Text Search is installed via SQL Server Setup or added to existing instance:
--   SQL Server Setup → Feature Selection → check "Full-Text and Semantic Extractions for Search"
-- Verify installation:
SELECT FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') AS is_installed;  -- 1 = installed


-- 2. Create a full-text catalog
IF EXISTS (SELECT 1 FROM sys.fulltext_catalogs WHERE name = 'pex_catalog')
    DROP FULLTEXT CATALOG pex_catalog;

CREATE FULLTEXT CATALOG pex_catalog AS DEFAULT;


-- 3. Create a full-text index on the table
-- Requires a unique index on the table first
CREATE UNIQUE INDEX UX_pex_sample_id ON dbo.pex_sample(id);

CREATE FULLTEXT INDEX ON dbo.pex_sample (
    title,
    content
)
KEY INDEX UX_pex_sample_id
ON pex_catalog;


-- 4a. CONTAINS — search for specific words or phrases
SELECT id, title
FROM dbo.pex_sample
WHERE CONTAINS(content, 'performance');
-- id=1, SQL Server Performance

SELECT id, title
FROM dbo.pex_sample
WHERE CONTAINS(content, '"data processing"');
-- id=2, Python Data Processing  (exact phrase match)

SELECT id, title
FROM dbo.pex_sample
WHERE CONTAINS(content, 'database OR computing');
-- id=3, Database Administration
-- id=4, Big Data Engineering


-- 4b. FREETEXT — natural language search (looser matching than CONTAINS)
SELECT id, title
FROM dbo.pex_sample
WHERE FREETEXT(content, 'managing databases');
-- id=3, Database Administration  (matches "database management" semantically)

SELECT id, title
FROM dbo.pex_sample
WHERE FREETEXT(content, 'working with large data');
-- id=1, SQL Server Performance   (matches "large databases")
-- id=4, Big Data Engineering      (matches "large datasets")
