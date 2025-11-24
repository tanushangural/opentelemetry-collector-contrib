-- =============================================================================
-- QUICK START: Blocking Session Demo for AdventureWorks2022
-- =============================================================================
-- Run these 3 scripts in SEPARATE query windows to generate blocking metrics
-- =============================================================================

-- =============================================================================
-- WINDOW 1: BLOCKER SESSION (Run this FIRST)
-- =============================================================================
/*
Copy the block below into a NEW query window and execute:
*/

USE AdventureWorks2022;
GO

PRINT '============================================';
PRINT 'BLOCKER SESSION - Starting...';
PRINT 'This session will hold locks for 5 minutes';
PRINT '============================================';
PRINT '';

DECLARE @EndTime DATETIME = DATEADD(MINUTE, 5, GETDATE());
DECLARE @CycleCount INT = 0;

WHILE GETDATE() < @EndTime
BEGIN
    SET @CycleCount = @CycleCount + 1;

    BEGIN TRANSACTION;

    -- Scenario 1: Exclusive lock on Product table (LCK_M_X)
    UPDATE Production.Product
    SET ListPrice = ListPrice * 1.00001
    WHERE ProductID = 680;

    PRINT 'Cycle ' + CAST(@CycleCount AS VARCHAR) + ': Holding EXCLUSIVE LOCK on Product 680...';
    PRINT '  → blocker_status: running';
    PRINT '  → blocker_open_transaction_count: 1';
    PRINT '  → Time remaining: ' + CAST(DATEDIFF(SECOND, GETDATE(), @EndTime) AS VARCHAR) + ' seconds';
    PRINT '';

    WAITFOR DELAY '00:00:30';  -- Hold lock for 30 seconds

    ROLLBACK;  -- Release lock

    -- Brief pause to let blocked queries complete
    WAITFOR DELAY '00:00:05';
END

PRINT '============================================';
PRINT 'BLOCKER SESSION - Complete!';
PRINT 'Total cycles: ' + CAST(@CycleCount AS VARCHAR);
PRINT '============================================';
GO

-- =============================================================================
-- WINDOW 2: BLOCKED SESSION #1 (Run this SECOND, immediately after WINDOW 1)
-- =============================================================================
/*
Copy the block below into a SECOND query window and execute:
*/

USE AdventureWorks2022;
GO

PRINT '============================================';
PRINT 'BLOCKED SESSION #1 - Starting...';
PRINT 'Will be blocked by WINDOW 1';
PRINT '============================================';
PRINT '';

DECLARE @EndTime DATETIME = DATEADD(MINUTE, 5, GETDATE());
DECLARE @AttemptCount INT = 0;

WHILE GETDATE() < @EndTime
BEGIN
    SET @AttemptCount = @AttemptCount + 1;

    PRINT 'Attempt #' + CAST(@AttemptCount AS VARCHAR) + ': Trying to UPDATE Product 680...';

    BEGIN TRY
        -- This will BLOCK waiting for WINDOW 1's exclusive lock
        -- wait_type: LCK_M_X (Exclusive Lock)
        UPDATE Production.Product
        SET StandardCost = StandardCost * 1.00001
        WHERE ProductID = 680;

        PRINT '  → SUCCESS! Lock was released.';
        PRINT '';
    END TRY
    BEGIN CATCH
        PRINT '  → ERROR: ' + ERROR_MESSAGE();
        PRINT '';
    END CATCH

    WAITFOR DELAY '00:00:02';  -- Wait between attempts
END

PRINT '============================================';
PRINT 'BLOCKED SESSION #1 - Complete!';
PRINT 'Total attempts: ' + CAST(@AttemptCount AS VARCHAR);
PRINT '============================================';
GO

-- =============================================================================
-- WINDOW 3: BLOCKED SESSION #2 - Shared Lock (Run this THIRD)
-- =============================================================================
/*
Copy the block below into a THIRD query window and execute:
*/

USE AdventureWorks2022;
GO

PRINT '============================================';
PRINT 'BLOCKED SESSION #2 - Starting...';
PRINT 'Waiting for SHARED lock (will also block)';
PRINT '============================================';
PRINT '';

DECLARE @EndTime DATETIME = DATEADD(MINUTE, 5, GETDATE());
DECLARE @AttemptCount INT = 0;

WHILE GETDATE() < @EndTime
BEGIN
    SET @AttemptCount = @AttemptCount + 1;

    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

    PRINT 'Attempt #' + CAST(@AttemptCount AS VARCHAR) + ': Trying to SELECT Product 680 (REPEATABLE READ)...';

    BEGIN TRY
        BEGIN TRANSACTION;

        -- This will BLOCK if WINDOW 1 holds exclusive lock
        -- wait_type: LCK_M_S (Shared Lock)
        SELECT * FROM Production.Product
        WHERE ProductID = 680;

        COMMIT;

        PRINT '  → SUCCESS! Got shared lock.';
        PRINT '';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        PRINT '  → ERROR: ' + ERROR_MESSAGE();
        PRINT '';
    END CATCH

    WAITFOR DELAY '00:00:03';
END

SET TRANSACTION ISOLATION LEVEL READ COMMITTED;  -- Reset to default

PRINT '============================================';
PRINT 'BLOCKED SESSION #2 - Complete!';
PRINT 'Total attempts: ' + CAST(@AttemptCount AS VARCHAR);
PRINT '============================================';
GO

-- =============================================================================
-- BONUS: FORGOTTEN TRANSACTION SCENARIO
-- =============================================================================
/*
To simulate a FORGOTTEN TRANSACTION (most critical RCA scenario):

STEP 1: Run this in a NEW window:
*/

USE AdventureWorks2022;
GO

PRINT '============================================';
PRINT 'FORGOTTEN TRANSACTION - Starting...';
PRINT 'Simulating application crash/forgotten txn';
PRINT '============================================';

BEGIN TRANSACTION;

UPDATE Production.ProductInventory
SET Quantity = Quantity - 1
WHERE ProductID = 1 AND LocationID = 1;

PRINT 'Transaction started but NOT committed...';
PRINT 'blocker_status will show: sleeping';
PRINT 'blocker_open_transaction_count: 1';
PRINT '';
PRINT 'DO NOT CLOSE THIS WINDOW!';
PRINT 'DO NOT COMMIT OR ROLLBACK!';
PRINT 'Leave this session IDLE (sleeping)';
PRINT '';
PRINT 'This simulates a forgotten transaction!';
PRINT '============================================';
-- DO NOT RUN GO HERE - Keep transaction open!

/*
STEP 2: Then run this in ANOTHER window:
*/

USE AdventureWorks2022;
GO

DECLARE @AttemptCount INT = 0;
WHILE @AttemptCount < 10
BEGIN
    SET @AttemptCount = @AttemptCount + 1;

    PRINT 'Attempt #' + CAST(@AttemptCount AS VARCHAR) + ': Trying to read ProductInventory...';

    -- This will BLOCK waiting for the forgotten transaction
    SELECT * FROM Production.ProductInventory
    WHERE ProductID = 1 AND LocationID = 1;

    PRINT '  → Read successful';
    WAITFOR DELAY '00:00:05';
END
GO

-- =============================================================================
-- POST-TEST: Check Blocking Metrics in New Relic
-- =============================================================================
/*
After running the above scenarios, wait 60-90 seconds for metrics to be scraped.

Then query New Relic with:

SELECT
    latest(blocking_spid) AS 'Blocker SPID',
    latest(blocked_spid) AS 'Blocked SPID',
    latest(wait_type) AS 'Wait Type',
    latest(wait_time_seconds) AS 'Wait Time (s)',
    latest(blocker_status) AS 'Blocker Status',
    latest(blocker_open_transaction_count) AS 'Open Txns',
    latest(blocker_program_name) AS 'Blocker App',
    latest(wait_resource) AS 'Wait Resource',
    latest(database_name) AS 'Database'
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
FACET blocking_spid, blocked_spid
SINCE 10 minutes ago
LIMIT 50

Expected results:
- Multiple blocking sessions with LCK_M_X wait type
- blocker_status: 'running' or 'sleeping' (for forgotten txn)
- blocker_open_transaction_count: 1 (indicates open transaction)
- wait_resource: Shows locked resource (e.g., "PAGE: 5:1:680")
- database_name: AdventureWorks2022
*/
