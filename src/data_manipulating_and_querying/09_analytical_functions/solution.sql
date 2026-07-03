-- ============================================================================
-- Use Analytical Functions in Database Queries
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    id      INT          PRIMARY KEY,
    dept    NVARCHAR(20) NOT NULL,
    name    NVARCHAR(50) NOT NULL,
    salary  INT          NOT NULL
);

INSERT INTO dbo.pex_sample (id, dept, name, salary) VALUES
    (1, 'Engineering', 'Alice', 9000),
    (2, 'Engineering', 'Bob',   7000),
    (3, 'Engineering', 'Carol', 6000),
    (4, 'Sales',       'Dave',  8000),
    (5, 'Sales',       'Eve',   5000),
    (6, 'Sales',       'Frank', 4000);


-- 1. LAG — fetch data from the previous row
SELECT name, salary,
    LAG(salary) OVER (PARTITION BY dept ORDER BY salary DESC)  AS prev_salary
    -- Alice=NULL, Bob=9000, Carol=7000, Dave=NULL, Eve=8000, Frank=5000
FROM dbo.pex_sample;


-- 2. LEAD — fetch data from the subsequent row
SELECT name, salary,
    LEAD(salary) OVER (PARTITION BY dept ORDER BY salary DESC) AS next_salary
    -- Alice=7000, Bob=6000, Carol=NULL, Dave=5000, Eve=4000, Frank=NULL
FROM dbo.pex_sample;


-- 3. CUME_DIST — cumulative distribution (relative position in group)
SELECT name, salary,
    CUME_DIST() OVER (PARTITION BY dept ORDER BY salary)       AS cume
    -- Carol=0.333, Bob=0.667, Alice=1.0, Frank=0.333, Eve=0.667, Dave=1.0
FROM dbo.pex_sample;


-- 4. FIRST_VALUE — first value in the ordered partition
SELECT name, salary,
    FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) AS top_earner
    -- Engineering: Alice, Alice, Alice
    -- Sales:       Dave, Dave, Dave
FROM dbo.pex_sample;


-- 5. LAST_VALUE — last value in the ordered partition
SELECT name, salary,
    LAST_VALUE(name) OVER (
        PARTITION BY dept ORDER BY salary DESC
        ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS lowest_earner
    -- Engineering: Carol, Carol, Carol
    -- Sales:       Frank, Frank, Frank
FROM dbo.pex_sample;


-- 6. PERCENTILE_CONT — percentile based on continuous distribution
SELECT DISTINCT dept,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)
        OVER (PARTITION BY dept) AS median_cont
    -- Engineering: 7000.0  (interpolated median)
    -- Sales:       5000.0
FROM dbo.pex_sample;


-- 7. PERCENTILE_DISC — percentile picking an actual discrete value
SELECT DISTINCT dept,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY salary)
        OVER (PARTITION BY dept) AS median_disc
    -- Engineering: 7000  (actual row value)
    -- Sales:       5000
FROM dbo.pex_sample;


-- 8. PERCENT_RANK — relative rank within the group (0 to 1)
SELECT name, salary,
    PERCENT_RANK() OVER (PARTITION BY dept ORDER BY salary)    AS pct_rank
    -- Carol=0.0, Bob=0.5, Alice=1.0, Frank=0.0, Eve=0.5, Dave=1.0
FROM dbo.pex_sample;
