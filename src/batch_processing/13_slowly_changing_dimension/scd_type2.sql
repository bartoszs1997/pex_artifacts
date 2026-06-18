-- ============================================================================
-- SCD Type 2 — PostgreSQL
-- ----------------------------------------------------------------------------
-- Two simple statements in a transaction:
--   1. UPDATE  - expire current rows where tracked attributes changed
--   2. INSERT  - add new current versions (for changed + brand-new customers)
--
-- Same fake data: C001 unchanged, C002 city changed, C003 email+tier changed,
-- C004 brand new customer.
-- ============================================================================


-- ============================================================================
-- STEP 1 - Create the dimension table
-- ============================================================================
DROP TABLE IF EXISTS dim_customer CASCADE;

CREATE TABLE dim_customer (
    customer_sk          BIGSERIAL    PRIMARY KEY,        -- surrogate key (per version)
    customer_id          VARCHAR(10)  NOT NULL,           -- business key (stable)
    email                VARCHAR(100) NOT NULL,           -- tracked attribute
    city                 VARCHAR(50)  NOT NULL,           -- tracked attribute
    membership_tier      VARCHAR(20)  NOT NULL,           -- tracked attribute
    effective_start_date DATE         NOT NULL,           -- version valid from
    effective_end_date   DATE         NOT NULL,           -- version valid until (9999-12-31 = current)
    is_current           BOOLEAN      NOT NULL DEFAULT TRUE
);


-- ============================================================================
-- STEP 2 - Initial load (3 customers, all current)
-- ============================================================================
INSERT INTO dim_customer
    (customer_id, email, city, membership_tier, effective_start_date, effective_end_date, is_current)
VALUES
    ('C001', 'alice@example.com', 'Warsaw', 'SILVER', '2024-01-01', '9999-12-31', TRUE),
    ('C002', 'bob@example.com',   'Krakow', 'GOLD',   '2024-01-01', '9999-12-31', TRUE),
    ('C003', 'carol@example.com', 'Gdansk', 'SILVER', '2024-01-01', '9999-12-31', TRUE);


-- ============================================================================
-- STEP 3 - Staging table (incoming batch from source system)
-- ============================================================================
DROP TABLE IF EXISTS stg_customer;

CREATE TABLE stg_customer (
    customer_id     VARCHAR(10)  NOT NULL,
    email           VARCHAR(100) NOT NULL,
    city            VARCHAR(50)  NOT NULL,
    membership_tier VARCHAR(20)  NOT NULL
);

INSERT INTO stg_customer (customer_id, email, city, membership_tier) VALUES
    ('C001', 'alice@example.com', 'Warsaw', 'SILVER'),  -- unchanged
    ('C002', 'bob@example.com',   'Warsaw', 'GOLD'),    -- city changed
    ('C003', 'carol@newmail.com', 'Gdansk', 'GOLD'),    -- email + tier changed
    ('C004', 'dave@example.com',  'Poznan', 'BRONZE');  -- brand new


-- ============================================================================
-- STEP 4 - Apply SCD2 (two statements, one transaction)
-- ============================================================================
BEGIN;

-- (A) EXPIRE current rows where any tracked attribute changed
UPDATE dim_customer AS d
SET    effective_end_date = CURRENT_DATE,
       is_current         = FALSE
FROM   stg_customer AS s
WHERE  s.customer_id = d.customer_id
  AND  d.is_current  = TRUE
  AND  (s.email           IS DISTINCT FROM d.email
     OR s.city            IS DISTINCT FROM d.city
     OR s.membership_tier IS DISTINCT FROM d.membership_tier);

-- (B) INSERT new current versions (changed customers + brand-new customers)
INSERT INTO dim_customer
    (customer_id, email, city, membership_tier, effective_start_date, effective_end_date, is_current)
SELECT s.customer_id,
       s.email,
       s.city,
       s.membership_tier,
       CURRENT_DATE,
       '9999-12-31',
       TRUE
FROM   stg_customer s
LEFT JOIN dim_customer d
       ON d.customer_id = s.customer_id
      AND d.is_current  = TRUE
WHERE  d.customer_id IS NULL;

COMMIT;


-- ============================================================================
-- STEP 5 - Verify
-- ============================================================================
SELECT customer_sk, customer_id, email, city, membership_tier,
       effective_start_date, effective_end_date, is_current
FROM   dim_customer
ORDER  BY customer_id, effective_start_date;

-- Expected (assuming CURRENT_DATE = 2026-06-18):
--
-- sk | id   | email              | city   | tier   | start      | end        | current
-- ---+------+--------------------+--------+--------+------------+------------+--------
-- 1  | C001 | alice@example.com  | Warsaw | SILVER | 2024-01-01 | 9999-12-31 | true   <- untouched
-- 2  | C002 | bob@example.com    | Krakow | GOLD   | 2024-01-01 | 2026-06-18 | false  <- expired
-- 5  | C002 | bob@example.com    | Warsaw | GOLD   | 2026-06-18 | 9999-12-31 | true   <- new version
-- 3  | C003 | carol@example.com  | Gdansk | SILVER | 2024-01-01 | 2026-06-18 | false  <- expired
-- 6  | C003 | carol@newmail.com  | Gdansk | GOLD   | 2026-06-18 | 9999-12-31 | true   <- new version
-- 4  | C004 | dave@example.com   | Poznan | BRONZE | 2026-06-18 | 9999-12-31 | true   <- brand new
