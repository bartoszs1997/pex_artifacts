-- ============================================================================
-- Use Logical Join Operators in Queries
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample_orders', 'U') IS NOT NULL DROP TABLE dbo.pex_sample_orders;
IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;
IF OBJECT_ID('dbo.pex_sample_colors', 'U') IS NOT NULL DROP TABLE dbo.pex_sample_colors;

CREATE TABLE dbo.pex_sample (
    id    INT          PRIMARY KEY,
    name  NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.pex_sample_orders (
    id          INT PRIMARY KEY,
    customer_id INT NULL,
    product     NVARCHAR(50) NOT NULL
);

CREATE TABLE dbo.pex_sample_colors (
    color NVARCHAR(20) PRIMARY KEY
);

INSERT INTO dbo.pex_sample (id, name) VALUES
    (1, 'Alice'),
    (2, 'Bob'),
    (3, 'Carol');

INSERT INTO dbo.pex_sample_orders (id, customer_id, product) VALUES
    (10, 1, 'Laptop'),
    (11, 1, 'Mouse'),
    (12, 2, 'Keyboard'),
    (13, 9, 'Monitor');   -- customer_id=9 has no matching customer

INSERT INTO dbo.pex_sample_colors (color) VALUES
    ('Red'),
    ('Blue');


-- 1. INNER JOIN — only matching rows from both tables
SELECT c.id, c.name, o.product
FROM dbo.pex_sample c
INNER JOIN dbo.pex_sample_orders o ON o.customer_id = c.id;
-- 3 rows: Alice-Laptop, Alice-Mouse, Bob-Keyboard
-- Carol excluded (no orders), Monitor excluded (customer_id=9 not found)


-- 2. LEFT OUTER JOIN — all rows from left + matching from right
SELECT c.id, c.name, o.product
FROM dbo.pex_sample c
LEFT OUTER JOIN dbo.pex_sample_orders o ON o.customer_id = c.id;
-- 4 rows: Alice-Laptop, Alice-Mouse, Bob-Keyboard, Carol-NULL
-- Carol kept with NULL product (no matching order)


-- 3. RIGHT OUTER JOIN — all rows from right + matching from left
SELECT c.name, o.id AS order_id, o.product
FROM dbo.pex_sample c
RIGHT OUTER JOIN dbo.pex_sample_orders o ON o.customer_id = c.id;
-- 4 rows: Alice-Laptop, Alice-Mouse, Bob-Keyboard, NULL-Monitor
-- Monitor kept with NULL name (customer_id=9 not found)


-- 4. FULL OUTER JOIN — all rows from both, NULLs where no match
SELECT c.name, o.id AS order_id, o.product
FROM dbo.pex_sample c
FULL OUTER JOIN dbo.pex_sample_orders o ON o.customer_id = c.id;
-- 5 rows: Alice-Laptop, Alice-Mouse, Bob-Keyboard, Carol-NULL, NULL-Monitor


-- 5. CROSS JOIN — cartesian product (every row x every row)
SELECT c.name, cl.color
FROM dbo.pex_sample c
CROSS JOIN dbo.pex_sample_colors cl;
-- 6 rows: Alice-Red, Alice-Blue, Bob-Red, Bob-Blue, Carol-Red, Carol-Blue
