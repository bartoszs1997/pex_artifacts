-- ============================================================================
-- SCD Type 2 — Shipping Data
-- ----------------------------------------------------------------------------
-- Based on the 3-step pattern: EXCEPT -> MERGE -> INSERT
-- Dialect: PostgreSQL 15+ (supports MERGE)
--
-- Dimension : historical_shipping_data
--   Business key     : package_id
--   Tracked attrs    : status, shipping_dt, delivery_dt, source_address, destination_address
--   SCD2 metadata    : effective_start_date, effective_end_date, is_deleted
-- ============================================================================


-- ============================================================================
-- STEP 0 - Create the dimension table
-- ============================================================================
DROP TABLE IF EXISTS historical_shipping_data CASCADE;

CREATE TABLE historical_shipping_data (
    id                   BIGSERIAL    PRIMARY KEY,
    package_id           VARCHAR(20)  NOT NULL,         -- business key
    status               VARCHAR(30)  NOT NULL,         -- tracked attribute
    shipping_dt          DATE,                          -- tracked attribute
    delivery_dt          DATE,                          -- tracked attribute
    source_address       VARCHAR(200) NOT NULL,         -- tracked attribute
    destination_address  VARCHAR(200) NOT NULL,         -- tracked attribute
    effective_start_date DATE         NOT NULL,         -- when this version became valid
    effective_end_date   DATE         NOT NULL,         -- when it stopped being valid
    is_deleted           SMALLINT     NOT NULL DEFAULT 0  -- 0 = current, 1 = expired
);


-- ============================================================================
-- STEP 0b - Seed initial data (the dimension as it looks before the batch)
-- ============================================================================
INSERT INTO historical_shipping_data
    (package_id, status, shipping_dt, delivery_dt, source_address, destination_address,
     effective_start_date, effective_end_date, is_deleted)
VALUES
    ('PKG-001', 'delivered',  '2025-01-10', '2025-01-15', 'Warsaw',  'Krakow',  '2025-01-10', '9999-12-31', 0),
    ('PKG-002', 'in_transit', '2025-02-01', NULL,         'Gdansk',  'Poznan',  '2025-02-01', '9999-12-31', 0),
    ('PKG-003', 'shipped',    '2025-03-05', NULL,         'Wroclaw', 'Lublin',  '2025-03-05', '9999-12-31', 0);


-- ============================================================================
-- Incoming batch (fresh data from source system)
-- ============================================================================
DROP TABLE IF EXISTS new_shipping_data;

CREATE TABLE new_shipping_data (
    package_id          VARCHAR(20)  NOT NULL,
    status              VARCHAR(30)  NOT NULL,
    shipping_dt         DATE,
    delivery_dt         DATE,
    source_address      VARCHAR(200) NOT NULL,
    destination_address VARCHAR(200) NOT NULL
);

INSERT INTO new_shipping_data (package_id, status, shipping_dt, delivery_dt, source_address, destination_address) VALUES
    ('PKG-001', 'delivered',  '2025-01-10', '2025-01-15', 'Warsaw',  'Krakow'),   -- unchanged
    ('PKG-002', 'delivered',  '2025-02-01', '2025-02-08', 'Gdansk',  'Poznan'),   -- status + delivery_dt changed
    ('PKG-003', 'in_transit', '2025-03-05', NULL,         'Wroclaw', 'Katowice'), -- destination changed
    ('PKG-004', 'shipped',    '2025-04-01', NULL,         'Szczecin','Olsztyn');   -- brand new


-- ============================================================================
-- STEP 1 - Filter out unchanged rows using EXCEPT
-- ----------------------------------------------------------------------------
-- EXCEPT removes rows that are identical in both tables. What remains in
-- filtered_new_shipping_data are ONLY the rows that are new or changed.
-- ============================================================================
DROP TABLE IF EXISTS filtered_new_shipping_data;

CREATE TEMPORARY TABLE filtered_new_shipping_data AS
SELECT package_id, status, shipping_dt, delivery_dt, source_address, destination_address
FROM new_shipping_data
EXCEPT
SELECT package_id, status, shipping_dt, delivery_dt, source_address, destination_address
FROM historical_shipping_data;


-- ============================================================================
-- Steps 2 and 3 must succeed or fail together (no expired row without its
-- replacement, no orphaned insert without the expire).
-- ============================================================================
BEGIN;

-- ============================================================================
-- STEP 2 - MERGE: mark existing rows as expired where package_id matches
-- ----------------------------------------------------------------------------
-- If a package_id from the filtered set already exists in the dimension,
-- it means something changed. Expire the old row.
-- ============================================================================
MERGE INTO historical_shipping_data AS target
USING filtered_new_shipping_data AS source
ON target.package_id = source.package_id
   AND target.is_deleted = 0
WHEN MATCHED THEN
    UPDATE SET is_deleted          = 1,
               effective_end_date  = CURRENT_DATE;


-- ============================================================================
-- STEP 3 - INSERT all filtered rows as new current versions
-- ----------------------------------------------------------------------------
-- Every row in filtered_new_shipping_data is either changed or brand new.
-- Insert them all as the current version (is_deleted = 0).
-- ============================================================================
INSERT INTO historical_shipping_data
    (package_id, status, shipping_dt, delivery_dt, source_address, destination_address,
     effective_start_date, effective_end_date, is_deleted)
SELECT
    source.package_id,
    source.status,
    source.shipping_dt,
    source.delivery_dt,
    source.source_address,
    source.destination_address,
    CURRENT_DATE,
    '9999-12-31',
    0
FROM filtered_new_shipping_data AS source;

COMMIT;


-- ============================================================================
-- VERIFY
-- ============================================================================
SELECT id, package_id, status, shipping_dt, delivery_dt,
       source_address, destination_address,
       effective_start_date, effective_end_date, is_deleted
FROM historical_shipping_data
ORDER BY package_id, effective_start_date;

-- Expected (assume CURRENT_DATE = 2026-06-19):
--
-- id | package_id | status     | ship_dt    | deliv_dt   | src      | dst      | start      | end        | del
-- ---+------------+------------+------------+------------+----------+----------+------------+------------+----
-- 1  | PKG-001    | delivered  | 2025-01-10 | 2025-01-15 | Warsaw   | Krakow   | 2025-01-10 | 9999-12-31 | 0   <- untouched
-- 2  | PKG-002    | in_transit | 2025-02-01 | NULL       | Gdansk   | Poznan   | 2025-02-01 | 2026-06-19 | 1   <- expired
-- 5  | PKG-002    | delivered  | 2025-02-01 | 2025-02-08 | Gdansk   | Poznan   | 2026-06-19 | 9999-12-31 | 0   <- new version
-- 3  | PKG-003    | shipped    | 2025-03-05 | NULL       | Wroclaw  | Lublin   | 2025-03-05 | 2026-06-19 | 1   <- expired
-- 6  | PKG-003    | in_transit | 2025-03-05 | NULL       | Wroclaw  | Katowice | 2026-06-19 | 9999-12-31 | 0   <- new version
-- 4  | PKG-004    | shipped    | 2025-04-01 | NULL       | Szczecin | Olsztyn  | 2026-06-19 | 9999-12-31 | 0   <- brand new
