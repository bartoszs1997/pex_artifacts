-- ============================================================================
-- Explicit Data Type Conversion in T-SQL
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    id        INT IDENTITY(1,1) PRIMARY KEY,
    str_num   NVARCHAR(50)  NOT NULL,
    str_date  NVARCHAR(50)  NOT NULL,
    amount    DECIMAL(10,2) NOT NULL
);

INSERT INTO dbo.pex_sample (str_num, str_date, amount) VALUES
    ('250',  '2025-07-04',  99.95),
    ('100',  '2025-12-31', 150.00),
    ('42',   '2025-01-15',   0.50);


-- 1. CAST — convert an expression to another data type
SELECT
    CAST(str_num AS INT)                AS to_int,       -- 250, 100, 42
    CAST(amount AS INT)                 AS trunc_amt,    -- 99, 150, 0
    CAST(amount AS NVARCHAR(20))        AS amt_str,      -- '99.95', '150.00', '0.50'
    CAST(str_date AS DATE)              AS to_date       -- 2025-07-04, 2025-12-31, 2025-01-15
FROM dbo.pex_sample;


-- 2. CONVERT — convert with optional style code
SELECT
    CONVERT(INT, str_num)                                  AS to_int,  -- 250, 100, 42
    CONVERT(NVARCHAR(10), CAST(str_date AS DATE), 101)     AS us_fmt, -- '07/04/2025', '12/31/2025', '01/15/2025'
    CONVERT(NVARCHAR(10), CAST(str_date AS DATE), 104)     AS de_fmt, -- '04.07.2025', '31.12.2025', '15.01.2025'
    CONVERT(NVARCHAR(23), CAST(str_date AS DATETIME), 121) AS iso_ts  -- '2025-07-04 00:00:00.000', ...
FROM dbo.pex_sample;


-- 3. PARSE — culture-aware string to date/number
SELECT
    PARSE('04/07/2025' AS DATE USING 'en-US')              AS us_date, -- 2025-04-07
    PARSE('04.07.2025' AS DATE USING 'de-DE')              AS de_date, -- 2025-07-04
    PARSE('1,234.56'   AS DECIMAL(10,2) USING 'en-US')     AS us_num;  -- 1234.56


-- 4. TRY_PARSE — safe PARSE, returns NULL on failure
SELECT
    TRY_PARSE('2025-07-04' AS DATE USING 'en-US')          AS ok_date,  -- 2025-07-04
    TRY_PARSE('not-a-date' AS DATE USING 'en-US')          AS bad_date, -- NULL
    TRY_PARSE('123.45' AS DECIMAL(10,2) USING 'en-US')     AS ok_num,   -- 123.45
    TRY_PARSE('xyz'    AS DECIMAL(10,2) USING 'en-US')     AS bad_num;  -- NULL


-- 5. TRY_CAST — safe CAST, returns NULL on failure
SELECT
    TRY_CAST(str_num AS INT)            AS ok_int,       -- 250, 100, 42
    TRY_CAST('abc' AS INT)              AS bad_int,      -- NULL
    TRY_CAST(str_date AS DATE)          AS ok_date,      -- 2025-07-04, 2025-12-31, 2025-01-15
    TRY_CAST('nope' AS DATE)            AS bad_date      -- NULL
FROM dbo.pex_sample;


-- 6. TRY_CONVERT — safe CONVERT, returns NULL on failure
SELECT
    TRY_CONVERT(INT, str_num)           AS ok_int,       -- 250, 100, 42
    TRY_CONVERT(INT, 'abc')             AS bad_int,      -- NULL
    TRY_CONVERT(DATE, str_date)         AS ok_date,      -- 2025-07-04, 2025-12-31, 2025-01-15
    TRY_CONVERT(DATE, 'nope')           AS bad_date      -- NULL
FROM dbo.pex_sample;
