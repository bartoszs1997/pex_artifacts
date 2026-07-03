-- ============================================================================
-- Manipulate Date and Time Data in Database Queries
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    order_name  NVARCHAR(50)      NOT NULL,
    order_date  DATE              NOT NULL,
    created_at  DATETIME2(3)      NOT NULL,
    deadline    DATETIMEOFFSET(0)  NOT NULL
);

INSERT INTO dbo.pex_sample (order_name, order_date, created_at, deadline) VALUES
    ('ORD-001', '2025-01-15', '2025-01-10 09:30:00.000', '2025-01-15 17:00:00 +01:00'),
    ('ORD-002', '2025-06-15', '2025-06-01 14:30:45.123', '2025-06-30 18:00:00 +02:00'),
    ('ORD-003', '2025-02-10', '2025-02-05 08:15:00.456', '2025-03-01 09:00:00 -05:00');


-- 1. Functions that return system date and time values
SELECT
    SYSDATETIME()           AS sys_datetime,         -- 2025-07-04 14:30:45.1234567
    SYSDATETIMEOFFSET()     AS sys_datetime_offset;  -- 2025-07-04 14:30:45.1234567 +02:00


-- 2. Functions that return date and time parts
SELECT
    order_name,
    DATEPART(YEAR,    order_date)  AS part_year,     -- 2025
    DATEPART(MONTH,   order_date)  AS part_month,    -- 1, 6, 2
    DATEPART(DAY,     order_date)  AS part_day,      -- 15, 15, 10
    DATEPART(QUARTER, order_date)  AS part_quarter,  -- 1, 2, 1
    DATEPART(WEEKDAY, order_date)  AS part_weekday,  -- 4 (Wed), 1 (Sun), 2 (Mon)
    DATEPART(HOUR,    created_at)  AS part_hour,     -- 9, 14, 8
    DATEPART(MINUTE,  created_at)  AS part_minute,   -- 30, 30, 15
    DATEPART(SECOND,  created_at)  AS part_second,   -- 0, 45, 0
    YEAR(order_date)               AS fn_year,       -- 2025
    MONTH(order_date)              AS fn_month,      -- 1, 6, 2
    DAY(order_date)                AS fn_day,        -- 15, 15, 10
    DATENAME(WEEKDAY, order_date)  AS name_weekday,  -- 'Wednesday', 'Sunday', 'Monday'
    DATENAME(MONTH,   order_date)  AS name_month     -- 'January', 'June', 'February'
FROM dbo.pex_sample;


-- 3. Functions that return date and time values from their parts
SELECT
    DATEFROMPARTS(2025, 7, 4)                                    AS from_date,      -- 2025-07-04
    DATETIME2FROMPARTS(2025, 7, 4, 14, 30, 0, 0, 0)             AS from_datetime2, -- 2025-07-04 14:30:00
    DATETIMEFROMPARTS(2025, 7, 4, 14, 30, 0, 0)                  AS from_datetime,  -- 2025-07-04 14:30:00.000
    DATETIMEOFFSETFROMPARTS(2025, 7, 4, 14, 30, 0, 0, -5, 0, 0)  AS from_dto,       -- 2025-07-04 14:30:00 -05:00
    SMALLDATETIMEFROMPARTS(2025, 7, 4, 14, 30)                   AS from_smalldt,   -- 2025-07-04 14:30:00
    TIMEFROMPARTS(14, 30, 45, 0, 0)                              AS from_time;      -- 14:30:45


-- 4. Function that returns date and time difference values
SELECT
    order_name,
    DATEDIFF(DAY,   order_date, CAST(SYSDATETIME() AS DATE)) AS diff_days,   -- days since order
    DATEDIFF(MONTH, order_date, CAST(SYSDATETIME() AS DATE)) AS diff_months, -- months since order
    DATEDIFF(YEAR,  order_date, CAST(SYSDATETIME() AS DATE)) AS diff_years,  -- years since order
    DATEDIFF(HOUR,  created_at, SYSDATETIME())               AS diff_hours   -- hours since creation
FROM dbo.pex_sample;


-- 5. Functions that modify date and time values
SELECT
    order_name,
    DATEADD(DAY,   7, order_date)  AS plus_7_days,   -- order_date + 7 days
    DATEADD(MONTH, 3, order_date)  AS plus_3_months,  -- order_date + 3 months
    DATEADD(YEAR, -1, order_date)  AS minus_1_year,   -- order_date - 1 year
    DATEADD(HOUR,  6, created_at)  AS plus_6_hours    -- created_at + 6 hours
FROM dbo.pex_sample;

SELECT
    order_name,
    EOMONTH(order_date)      AS end_of_month,      -- last day of order's month
    EOMONTH(order_date, 1)   AS end_of_next_month,  -- last day of next month
    EOMONTH(order_date, -1)  AS end_of_prev_month   -- last day of previous month
FROM dbo.pex_sample;

SELECT
    order_name,
    deadline                                    AS original,
    SWITCHOFFSET(deadline, '+00:00')             AS in_utc,      -- converted to UTC
    SWITCHOFFSET(deadline, '-05:00')             AS in_eastern,  -- converted to US/Eastern
    SWITCHOFFSET(deadline, '+09:00')             AS in_tokyo     -- converted to Asia/Tokyo
FROM dbo.pex_sample;


-- 6. Function that validates date and time values
SELECT
    ISDATE('2025-07-04')          AS valid_date,       -- 1
    ISDATE('2025-02-30')          AS invalid_feb30,    -- 0
    ISDATE('2025-13-01')          AS invalid_month13,  -- 0
    ISDATE('not-a-date')          AS invalid_text,     -- 0
    ISDATE('14:30:00')            AS valid_time,       -- 1
    ISDATE('')                    AS invalid_empty;    -- 0
