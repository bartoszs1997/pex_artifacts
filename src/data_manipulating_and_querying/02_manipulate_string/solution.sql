-- ============================================================================
-- Manipulate String Values in Database Queries
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    full_name  NVARCHAR(100) NOT NULL,
    email      NVARCHAR(100) NOT NULL,
    notes      NVARCHAR(200) NOT NULL
);

INSERT INTO dbo.pex_sample (full_name, email, notes) VALUES
    ('John Smith',    'john.smith@company.com',   '  Senior developer at HQ  '),
    ('Anna Kowalska', 'anna.k@startup.io',        ' Junior analyst '),
    ('Bob Martinez',  'bob.martinez@corp.net',     '  Team lead - remote  ');


-- 1. CHARINDEX — starting position of expression in a string
SELECT
    full_name,
    CHARINDEX(' ', full_name)          AS space_pos,     -- 5, 5, 4
    CHARINDEX('.', email)              AS dot_pos,       -- 5, 5, 4
    CHARINDEX('@', email)              AS at_pos         -- 11, 7, 13
FROM dbo.pex_sample;


-- 2. LEFT — left part of string with N characters
SELECT
    full_name,
    LEFT(full_name, 3)                 AS first_3,       -- 'Joh', 'Ann', 'Bob'
    LEFT(email, CHARINDEX('@', email) - 1) AS email_user -- 'john.smith', 'anna.k', 'bob.martinez'
FROM dbo.pex_sample;


-- 3. LEN — length of string (excluding trailing spaces)
SELECT
    full_name,
    LEN(full_name)                     AS name_len,      -- 10, 14, 12
    LEN(notes)                         AS notes_len,     -- 25, 15, 20 (no trailing spaces)
    LEN(email)                         AS email_len      -- 22, 18, 22
FROM dbo.pex_sample;


-- 4. LTRIM — remove leading blanks
SELECT
    notes,
    LTRIM(notes)                       AS ltrimmed       -- leading spaces removed
FROM dbo.pex_sample;


-- 5. RTRIM — remove trailing blanks
SELECT
    notes,
    RTRIM(notes)                       AS rtrimmed       -- trailing spaces removed
FROM dbo.pex_sample;


-- 6. PATINDEX — starting position of pattern in string (supports wildcards)
SELECT
    email,
    PATINDEX('%@%', email)             AS at_pos,        -- 11, 7, 13
    PATINDEX('%.com', email)           AS com_pos,       -- 19, 0, 0
    PATINDEX('%[0-9]%', notes)         AS digit_pos      -- 0, 0, 0
FROM dbo.pex_sample;


-- 7. REPLACE — replace occurrences of text
SELECT
    full_name,
    REPLACE(full_name, ' ', '_')       AS underscored,   -- 'John_Smith', 'Anna_Kowalska', 'Bob_Martinez'
    REPLACE(email, '.com', '.org')     AS new_domain     -- 'john.smith@company.org', unchanged, unchanged
FROM dbo.pex_sample;


-- 8. REPLICATE — repeat expression N times
SELECT
    full_name,
    REPLICATE('*', LEN(full_name))     AS masked,        -- '**********', etc.
    REPLICATE('-', 20)                 AS separator       -- '--------------------'
FROM dbo.pex_sample;


-- 9. REVERSE — reverse a string
SELECT
    full_name,
    REVERSE(full_name)                 AS reversed,      -- 'htimS nhoJ', etc.
    REVERSE(email)                     AS email_rev       -- 'moc.ynapmoc@htims.nhoj', etc.
FROM dbo.pex_sample;


-- 10. SUBSTRING — extract portion of string
SELECT
    full_name,
    SUBSTRING(full_name, 1, 3)         AS first_3,       -- 'Joh', 'Ann', 'Bob'
    SUBSTRING(email, CHARINDEX('@', email) + 1, LEN(email)) AS domain -- 'company.com', 'startup.io', 'corp.net'
FROM dbo.pex_sample;


-- 11. UPPER — convert to uppercase
SELECT
    full_name,
    UPPER(full_name)                   AS uppered,       -- 'JOHN SMITH', etc.
    UPPER(LEFT(full_name, 1))          AS first_upper    -- 'J', 'A', 'B'
FROM dbo.pex_sample;
