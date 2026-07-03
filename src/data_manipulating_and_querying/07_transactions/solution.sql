-- ============================================================================
-- Implement and Manage Transactions and Isolation Levels
-- Dialect: T-SQL (SQL Server)
-- ============================================================================

IF OBJECT_ID('dbo.pex_sample', 'U') IS NOT NULL DROP TABLE dbo.pex_sample;

CREATE TABLE dbo.pex_sample (
    id      INT          PRIMARY KEY,
    name    NVARCHAR(50) NOT NULL,
    balance DECIMAL(10,2) NOT NULL
);

INSERT INTO dbo.pex_sample (id, name, balance) VALUES
    (1, 'Alice', 1000.00),
    (2, 'Bob',    500.00),
    (3, 'Carol',  750.00);


-- 1. Setting transaction isolation level
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
-- prevents dirty reads; most common default in SQL Server


-- 2. Explicit transaction with multiple DML statements
BEGIN TRANSACTION;

    UPDATE dbo.pex_sample SET balance = balance - 200.00 WHERE id = 1;  -- Alice: 1000 -> 800
    UPDATE dbo.pex_sample SET balance = balance + 200.00 WHERE id = 2;  -- Bob:   500  -> 700

COMMIT TRANSACTION;
-- both updates succeed atomically; if either fails, neither is applied


-- 3. Rollback logic with SAVE POINT
BEGIN TRANSACTION;

    UPDATE dbo.pex_sample SET balance = balance - 100.00 WHERE id = 2;  -- Bob: 700 -> 600

    SAVE TRANSACTION before_carol;

    UPDATE dbo.pex_sample SET balance = balance + 999.00 WHERE id = 3;  -- Carol: 750 -> 1749 (oops)

    ROLLBACK TRANSACTION before_carol;
    -- Carol's update is undone (back to 750), Bob's update (600) is preserved

    UPDATE dbo.pex_sample SET balance = balance + 100.00 WHERE id = 3;  -- Carol: 750 -> 850

COMMIT TRANSACTION;
-- final state: Alice=800, Bob=600, Carol=850


-- 4. Rollback with error handling (TRY/CATCH)
BEGIN TRY
    BEGIN TRANSACTION;

        UPDATE dbo.pex_sample SET balance = balance - 50.00 WHERE id = 1;   -- Alice: 800 -> 750
        UPDATE dbo.pex_sample SET balance = balance + 50.00 WHERE id = 2;   -- Bob:   600 -> 650

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    -- on any error: both updates are rolled back, data stays consistent
END CATCH;
