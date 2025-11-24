-- =============================================================================
-- AdventureWorks2022 Comprehensive Demo Script
-- =============================================================================
-- Purpose: Generate realistic SQL Server workload for OpenTelemetry monitoring
-- Covers: Slow queries, blocking, wait types, RCA scenarios, execution plans
-- Database: AdventureWorks2022
-- =============================================================================

USE AdventureWorks2022;
GO

-- =============================================================================
-- SECTION 1: SLOW QUERIES - Different Performance Patterns
-- =============================================================================
PRINT '=== SECTION 1: Generating Slow Queries ==='

-- 1.1. CPU-Intensive Query (Complex Aggregation)
PRINT 'Running CPU-intensive aggregation query...'
DECLARE @StartLoop1 INT = 1;
WHILE @StartLoop1 <= 50
BEGIN
    SELECT
        p.Name AS ProductName,
        COUNT(*) AS OrderCount,
        SUM(sod.LineTotal) AS TotalRevenue,
        AVG(sod.OrderQty) AS AvgQuantity,
        MAX(sod.UnitPrice) AS MaxPrice,
        MIN(sod.UnitPrice) AS MinPrice
    FROM Production.Product p
    JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
    WHERE p.ProductID > 100
    GROUP BY p.Name
    ORDER BY TotalRevenue DESC;

    SET @StartLoop1 = @StartLoop1 + 1;
    WAITFOR DELAY '00:00:01'; -- 1 second between executions
END
GO

-- 1.2. I/O-Intensive Query (Large Table Scan)
PRINT 'Running I/O-intensive table scan...'
DECLARE @StartLoop2 INT = 1;
WHILE @StartLoop2 <= 30
BEGIN
    SELECT
        soh.SalesOrderID,
        soh.OrderDate,
        soh.TotalDue,
        c.FirstName,
        c.LastName,
        a.City,
        a.StateProvinceID
    FROM Sales.SalesOrderHeader soh
    JOIN Sales.Customer cust ON soh.CustomerID = cust.CustomerID
    JOIN Person.Person c ON cust.PersonID = c.BusinessEntityID
    JOIN Person.BusinessEntityAddress bea ON c.BusinessEntityID = bea.BusinessEntityID
    JOIN Person.Address a ON bea.AddressID = a.AddressID
    WHERE soh.OrderDate > DATEADD(YEAR, -5, GETDATE())
    ORDER BY soh.TotalDue DESC;

    SET @StartLoop2 = @StartLoop2 + 1;
    WAITFOR DELAY '00:00:02';
END
GO

-- 1.3. Memory-Intensive Query (Hash Aggregation with Spills)
PRINT 'Running memory-intensive query (potential spills)...'
DECLARE @StartLoop3 INT = 1;
WHILE @StartLoop3 <= 20
BEGIN
    SELECT
        ProductID,
        COUNT(DISTINCT SalesOrderID) AS UniqueOrders,
        COUNT(*) AS TotalLines,
        SUM(LineTotal) AS Revenue,
        AVG(OrderQty) AS AvgQty,
        STDEV(UnitPrice) AS PriceVariance
    FROM Sales.SalesOrderDetail
    WHERE ProductID > 1
    GROUP BY ProductID
    HAVING COUNT(*) > 10
    ORDER BY Revenue DESC
    OPTION (MAXDOP 1, HASH GROUP); -- Force hash aggregation

    SET @StartLoop3 = @StartLoop3 + 1;
    WAITFOR DELAY '00:00:02';
END
GO

-- 1.4. Parallel Query with CXPACKET Waits
PRINT 'Running parallel query with CXPACKET waits...'
DECLARE @StartLoop4 INT = 1;
WHILE @StartLoop4 <= 25
BEGIN
    SELECT
        ProductID,
        OrderQty,
        ROW_NUMBER() OVER (PARTITION BY ProductID ORDER BY ModifiedDate DESC) AS RowNum,
        RANK() OVER (PARTITION BY ProductID ORDER BY LineTotal DESC) AS RevenueRank,
        SUM(LineTotal) OVER (PARTITION BY ProductID) AS TotalProductRevenue
    FROM Sales.SalesOrderDetail
    WHERE ProductID > 100
    ORDER BY ProductID, RowNum
    OPTION (MAXDOP 4); -- Explicit parallelism

    SET @StartLoop4 = @StartLoop4 + 1;
    WAITFOR DELAY '00:00:01';
END
GO

-- =============================================================================
-- SECTION 2: BLOCKING SCENARIOS - Different Lock Types
-- =============================================================================
PRINT '=== SECTION 2: Generating Blocking Scenarios ==='

-- 2.1. Exclusive Lock Blocking (LCK_M_X) - UPDATE Blocking
PRINT 'Scenario 2.1: Exclusive Lock Blocking (UPDATE)'
PRINT 'Open a NEW query window and run this BLOCKER session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'BEGIN TRANSACTION;'
PRINT 'UPDATE Production.Product SET ListPrice = ListPrice * 1.01 WHERE ProductID = 680;'
PRINT '-- DO NOT COMMIT - Keep transaction open for 2 minutes'
PRINT 'WAITFOR DELAY ''00:02:00'';'
PRINT 'ROLLBACK;'
PRINT '-----------------------------------------------------'
PRINT ''
PRINT 'Then run this BLOCKED session (in another window):'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'UPDATE Production.Product SET StandardCost = StandardCost * 1.02 WHERE ProductID = 680;'
PRINT '-----------------------------------------------------'
PRINT ''

-- 2.2. Shared Lock Blocking (LCK_M_S) - SERIALIZABLE Isolation
PRINT 'Scenario 2.2: Shared Lock Blocking (SERIALIZABLE isolation)'
PRINT 'Open a NEW query window and run this BLOCKER session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;'
PRINT 'BEGIN TRANSACTION;'
PRINT 'SELECT * FROM Sales.Customer WHERE CustomerID = 11000;'
PRINT 'WAITFOR DELAY ''00:02:00'';'
PRINT 'ROLLBACK;'
PRINT '-----------------------------------------------------'
PRINT ''
PRINT 'Then run this BLOCKED session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'UPDATE Sales.Customer SET AccountNumber = AccountNumber WHERE CustomerID = 11000;'
PRINT '-----------------------------------------------------'
PRINT ''

-- 2.3. Update Lock Blocking (LCK_M_U)
PRINT 'Scenario 2.3: Update Lock Blocking'
PRINT 'Open a NEW query window and run this BLOCKER session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'BEGIN TRANSACTION;'
PRINT 'SELECT * FROM Sales.SalesOrderHeader WITH (UPDLOCK) WHERE SalesOrderID = 43659;'
PRINT 'WAITFOR DELAY ''00:02:00'';'
PRINT 'ROLLBACK;'
PRINT '-----------------------------------------------------'
PRINT ''
PRINT 'Then run this BLOCKED session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'UPDATE Sales.SalesOrderHeader SET SubTotal = SubTotal WHERE SalesOrderID = 43659;'
PRINT '-----------------------------------------------------'
PRINT ''

-- 2.4. Page Lock Blocking (LCK_M_X on PAGE)
PRINT 'Scenario 2.4: Page Lock Blocking'
PRINT 'Open a NEW query window and run this BLOCKER session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'BEGIN TRANSACTION;'
PRINT 'UPDATE Sales.SalesOrderDetail SET OrderQty = OrderQty WHERE SalesOrderDetailID BETWEEN 1 AND 100;'
PRINT 'WAITFOR DELAY ''00:02:00'';'
PRINT 'ROLLBACK;'
PRINT '-----------------------------------------------------'
PRINT ''
PRINT 'Then run this BLOCKED session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'UPDATE Sales.SalesOrderDetail SET UnitPrice = UnitPrice WHERE SalesOrderDetailID BETWEEN 50 AND 150;'
PRINT '-----------------------------------------------------'
PRINT ''

-- 2.5. FORGOTTEN TRANSACTION (Most Critical RCA Scenario)
PRINT 'Scenario 2.5: FORGOTTEN TRANSACTION (sleeping blocker with open txn)'
PRINT 'Open a NEW query window and run this BLOCKER session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'BEGIN TRANSACTION;'
PRINT 'UPDATE Production.ProductInventory SET Quantity = Quantity - 1 WHERE ProductID = 1;'
PRINT '-- Simulate application crash/forgotten transaction - DO NOT COMMIT!'
PRINT '-- Leave this session open and idle (sleeping)'
PRINT '-----------------------------------------------------'
PRINT ''
PRINT 'Then run this BLOCKED session (in another window):'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'SELECT * FROM Production.ProductInventory WHERE ProductID = 1;'
PRINT '-----------------------------------------------------'
PRINT 'This will show: blocker_status = ''sleeping'', blocker_open_transaction_count > 0'
PRINT ''

-- 2.6. Key Lock Blocking (Row-Level Lock)
PRINT 'Scenario 2.6: Key Lock Blocking (Row-Level)'
PRINT 'Open a NEW query window and run this BLOCKER session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'BEGIN TRANSACTION;'
PRINT 'UPDATE Person.Person SET MiddleName = ''X'' WHERE BusinessEntityID = 1;'
PRINT 'WAITFOR DELAY ''00:02:00'';'
PRINT 'ROLLBACK;'
PRINT '-----------------------------------------------------'
PRINT ''
PRINT 'Then run this BLOCKED session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'DELETE FROM Person.Person WHERE BusinessEntityID = 1;'
PRINT '-----------------------------------------------------'
PRINT ''

-- =============================================================================
-- SECTION 3: WAIT TYPE SCENARIOS - Comprehensive Coverage
-- =============================================================================
PRINT '=== SECTION 3: Generating Different Wait Types ==='

-- 3.1. PAGEIOLATCH_SH (Disk I/O Read Waits)
PRINT 'Scenario 3.1: PAGEIOLATCH_SH (Disk I/O - Reading)'
PRINT 'First, clear buffer cache to force physical reads:'
PRINT 'CHECKPOINT; DBCC DROPCLEANBUFFERS;'
PRINT 'Then run this query in a new window:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'SELECT * FROM Sales.SalesOrderDetail ORDER BY SalesOrderDetailID;'
PRINT '-----------------------------------------------------'
PRINT ''

-- 3.2. WRITELOG (Transaction Log Waits)
PRINT 'Scenario 3.2: WRITELOG (Transaction Log Writes)'
DECLARE @StartLoop5 INT = 1;
WHILE @StartLoop5 <= 30
BEGIN
    BEGIN TRANSACTION;

    INSERT INTO Sales.SalesOrderDetail
        (SalesOrderID, CarrierTrackingNumber, OrderQty, ProductID, SpecialOfferID, UnitPrice, UnitPriceDiscount, LineTotal, rowguid, ModifiedDate)
    VALUES
        (43659, 'DEMO-' + CAST(NEWID() AS VARCHAR(36)), 1, 707, 1, 34.99, 0.00, 34.99, NEWID(), GETDATE());

    COMMIT;

    SET @StartLoop5 = @StartLoop5 + 1;
    WAITFOR DELAY '00:00:00.100'; -- 100ms between inserts
END
GO

-- 3.3. ASYNC_NETWORK_IO (Client Not Consuming Results Fast Enough)
PRINT 'Scenario 3.3: ASYNC_NETWORK_IO (Network/Client Waits)'
PRINT 'Run this query in a new window and DO NOT scroll through results:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'SELECT sod.*, soh.*, p.*, c.* '
PRINT 'FROM Sales.SalesOrderDetail sod'
PRINT 'JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID'
PRINT 'JOIN Production.Product p ON sod.ProductID = p.ProductID'
PRINT 'JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID;'
PRINT '-- DO NOT scroll or fetch results - this causes ASYNC_NETWORK_IO wait'
PRINT '-----------------------------------------------------'
PRINT ''

-- 3.4. RESOURCE_SEMAPHORE (Memory Grant Waits)
PRINT 'Scenario 3.4: RESOURCE_SEMAPHORE (Waiting for Memory Grant)'
PRINT 'Run multiple instances of this query simultaneously in different windows:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'SELECT '
PRINT '    sod.ProductID,'
PRINT '    COUNT(*) AS OrderCount,'
PRINT '    SUM(sod.LineTotal) AS Revenue'
PRINT 'FROM Sales.SalesOrderDetail sod'
PRINT 'CROSS JOIN Sales.SalesOrderDetail sod2  -- Cartesian join for large result'
PRINT 'GROUP BY sod.ProductID'
PRINT 'ORDER BY Revenue DESC'
PRINT 'OPTION (MAXDOP 1);'
PRINT '-----------------------------------------------------'
PRINT ''

-- 3.5. SOS_SCHEDULER_YIELD (CPU Pressure)
PRINT 'Scenario 3.5: SOS_SCHEDULER_YIELD (CPU Pressure)'
DECLARE @StartLoop6 INT = 1;
WHILE @StartLoop6 <= 20
BEGIN
    -- CPU-intensive calculation
    SELECT
        p1.ProductID,
        p2.ProductID,
        ABS(CHECKSUM(NEWID())) % 1000000 AS RandomCalc
    FROM Production.Product p1
    CROSS JOIN Production.Product p2
    WHERE p1.ProductID < 100 AND p2.ProductID < 100;

    SET @StartLoop6 = @StartLoop6 + 1;
END
GO

-- =============================================================================
-- SECTION 4: COMPLEX EXECUTION PLANS - For Execution Plan Analysis
-- =============================================================================
PRINT '=== SECTION 4: Generating Complex Execution Plans ==='

-- 4.1. Nested Loops Join
PRINT 'Running query with Nested Loops join...'
DECLARE @StartLoop7 INT = 1;
WHILE @StartLoop7 <= 15
BEGIN
    SELECT TOP 100
        c.CustomerID,
        c.AccountNumber,
        p.FirstName,
        p.LastName,
        soh.SalesOrderID,
        soh.TotalDue
    FROM Sales.Customer c
    INNER LOOP JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
    INNER LOOP JOIN Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
    WHERE c.CustomerID < 1000
    ORDER BY soh.TotalDue DESC
    OPTION (FORCE ORDER);

    SET @StartLoop7 = @StartLoop7 + 1;
    WAITFOR DELAY '00:00:01';
END
GO

-- 4.2. Hash Match Join (Large Table Join)
PRINT 'Running query with Hash Match join...'
DECLARE @StartLoop8 INT = 1;
WHILE @StartLoop8 <= 15
BEGIN
    SELECT
        p.Name,
        COUNT(sod.SalesOrderID) AS OrderCount,
        SUM(sod.LineTotal) AS TotalRevenue
    FROM Production.Product p
    INNER HASH JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
    GROUP BY p.Name
    HAVING SUM(sod.LineTotal) > 10000
    ORDER BY TotalRevenue DESC
    OPTION (HASH GROUP);

    SET @StartLoop8 = @StartLoop8 + 1;
    WAITFOR DELAY '00:00:01';
END
GO

-- 4.3. Merge Join (Sorted Inputs)
PRINT 'Running query with Merge join...'
DECLARE @StartLoop9 INT = 1;
WHILE @StartLoop9 <= 15
BEGIN
    SELECT
        soh.SalesOrderID,
        soh.OrderDate,
        sod.ProductID,
        sod.OrderQty
    FROM Sales.SalesOrderHeader soh
    INNER MERGE JOIN Sales.SalesOrderDetail sod
        ON soh.SalesOrderID = sod.SalesOrderID
    WHERE soh.OrderDate > '2013-01-01'
    ORDER BY soh.SalesOrderID
    OPTION (FORCE ORDER, MERGE JOIN);

    SET @StartLoop9 = @StartLoop9 + 1;
    WAITFOR DELAY '00:00:01';
END
GO

-- 4.4. Index Scan vs Index Seek
PRINT 'Running query forcing Index Scan...'
DECLARE @StartLoop10 INT = 1;
WHILE @StartLoop10 <= 20
BEGIN
    -- Force index scan with function on indexed column
    SELECT
        ProductID,
        Name,
        ListPrice
    FROM Production.Product
    WHERE UPPER(Name) LIKE '%BIKE%'
    ORDER BY ListPrice DESC;

    SET @StartLoop10 = @StartLoop10 + 1;
    WAITFOR DELAY '00:00:01';
END
GO

-- =============================================================================
-- SECTION 5: TRANSACTION SCENARIOS - Different Isolation Levels
-- =============================================================================
PRINT '=== SECTION 5: Transaction Scenarios ==='

-- 5.1. READ COMMITTED (Default)
PRINT 'Scenario 5.1: READ COMMITTED isolation'
DECLARE @StartLoop11 INT = 1;
WHILE @StartLoop11 <= 10
BEGIN
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    BEGIN TRANSACTION;

    SELECT TOP 100 * FROM Sales.Customer WHERE CustomerID > 10000;

    COMMIT;
    SET @StartLoop11 = @StartLoop11 + 1;
END
GO

-- 5.2. REPEATABLE READ
PRINT 'Scenario 5.2: REPEATABLE READ isolation'
DECLARE @StartLoop12 INT = 1;
WHILE @StartLoop12 <= 10
BEGIN
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    BEGIN TRANSACTION;

    SELECT TOP 50 * FROM Production.Product WHERE ProductID > 500;

    COMMIT;
    SET @StartLoop12 = @StartLoop12 + 1;
    WAITFOR DELAY '00:00:01';
END
GO

-- 5.3. SERIALIZABLE (Strictest - Causes Most Blocking)
PRINT 'Scenario 5.3: SERIALIZABLE isolation (causes blocking)'
PRINT 'Run this BLOCKER in new window:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;'
PRINT 'BEGIN TRANSACTION;'
PRINT 'SELECT * FROM Sales.SalesOrderHeader WHERE SalesOrderID BETWEEN 43000 AND 44000;'
PRINT 'WAITFOR DELAY ''00:02:00'';'
PRINT 'ROLLBACK;'
PRINT '-----------------------------------------------------'
PRINT ''
PRINT 'Then run this BLOCKED session:'
PRINT '-----------------------------------------------------'
PRINT 'USE AdventureWorks2022;'
PRINT 'INSERT INTO Sales.SalesOrderHeader (RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, CustomerID, SalesPersonID, BillToAddressID, ShipToAddressID, ShipMethodID, SubTotal, TaxAmt, Freight, TotalDue, rowguid, ModifiedDate)'
PRINT 'VALUES (1, GETDATE(), DATEADD(DAY, 7, GETDATE()), DATEADD(DAY, 5, GETDATE()), 1, 1, 11000, 279, 985, 985, 5, 1000.00, 80.00, 50.00, 1130.00, NEWID(), GETDATE());'
PRINT '-----------------------------------------------------'
PRINT ''

-- =============================================================================
-- SECTION 6: AUTOMATED BLOCKING TEST (Run in Separate Windows)
-- =============================================================================
PRINT '=== SECTION 6: Automated Blocking Test ==='
PRINT ''
PRINT 'To generate AUTOMATIC blocking for 5 minutes, follow these steps:'
PRINT ''
PRINT 'STEP 1: Open a NEW query window and run this BLOCKER (Session 1):'
PRINT '==============================================================================='
PRINT '-- BLOCKER SESSION - Holds locks for 5 minutes'
PRINT 'USE AdventureWorks2022;'
PRINT 'PRINT ''BLOCKER: Starting 5-minute blocking scenario...'';'
PRINT ''
PRINT 'DECLARE @EndTime DATETIME = DATEADD(MINUTE, 5, GETDATE());'
PRINT 'WHILE GETDATE() < @EndTime'
PRINT 'BEGIN'
PRINT '    BEGIN TRANSACTION;'
PRINT '    '
PRINT '    -- Hold exclusive lock on Product table'
PRINT '    UPDATE Production.Product '
PRINT '    SET ListPrice = ListPrice * 1.001 '
PRINT '    WHERE ProductID = 680;'
PRINT '    '
PRINT '    PRINT ''BLOCKER: Holding lock on ProductID 680... '' + CAST(DATEDIFF(SECOND, GETDATE(), @EndTime) AS VARCHAR) + '' seconds remaining'';'
PRINT '    '
PRINT '    WAITFOR DELAY ''00:00:30'';  -- Hold lock for 30 seconds'
PRINT '    '
PRINT '    ROLLBACK;'
PRINT '    '
PRINT '    WAITFOR DELAY ''00:00:05'';  -- Brief pause between cycles'
PRINT 'END'
PRINT ''
PRINT 'PRINT ''BLOCKER: Completed 5-minute blocking scenario'';'
PRINT 'GO'
PRINT '==============================================================================='
PRINT ''
PRINT 'STEP 2: Open ANOTHER query window and run this BLOCKED (Session 2):'
PRINT '==============================================================================='
PRINT '-- BLOCKED SESSION - Repeatedly tries to access locked resource'
PRINT 'USE AdventureWorks2022;'
PRINT 'PRINT ''BLOCKED: Starting attempts to access locked resource...'';'
PRINT ''
PRINT 'DECLARE @EndTime DATETIME = DATEADD(MINUTE, 5, GETDATE());'
PRINT 'DECLARE @AttemptCount INT = 0;'
PRINT ''
PRINT 'WHILE GETDATE() < @EndTime'
PRINT 'BEGIN'
PRINT '    SET @AttemptCount = @AttemptCount + 1;'
PRINT '    PRINT ''BLOCKED: Attempt #'' + CAST(@AttemptCount AS VARCHAR) + '' - Trying to access ProductID 680...'';'
PRINT '    '
PRINT '    BEGIN TRY'
PRINT '        -- This will block waiting for Session 1''s lock'
PRINT '        UPDATE Production.Product '
PRINT '        SET StandardCost = StandardCost * 1.001 '
PRINT '        WHERE ProductID = 680;'
PRINT '        '
PRINT '        PRINT ''BLOCKED: SUCCESS! Lock was released.'';'
PRINT '    END TRY'
PRINT '    BEGIN CATCH'
PRINT '        PRINT ''BLOCKED: Error - '' + ERROR_MESSAGE();'
PRINT '    END CATCH'
PRINT '    '
PRINT '    WAITFOR DELAY ''00:00:02'';  -- Wait 2 seconds between attempts'
PRINT 'END'
PRINT ''
PRINT 'PRINT ''BLOCKED: Completed 5-minute test'';'
PRINT 'GO'
PRINT '==============================================================================='
PRINT ''
PRINT 'After running both sessions, check New Relic for blocking metrics!'
PRINT ''

-- =============================================================================
-- SECTION 7: CLEANUP AND ROLLBACK TEST DATA
-- =============================================================================
PRINT '=== SECTION 7: Cleanup ==='
PRINT 'Removing test data inserted during demo...'

-- Remove demo inserts from SalesOrderDetail
DELETE FROM Sales.SalesOrderDetail
WHERE CarrierTrackingNumber LIKE 'DEMO-%';

PRINT 'Cleanup complete!'
PRINT ''
PRINT '=============================================================================';
PRINT 'AdventureWorks2022 Demo Script Complete!';
PRINT '=============================================================================';
PRINT '';
PRINT 'SUMMARY OF SCENARIOS COVERED:';
PRINT '- CPU-intensive queries (complex aggregations)';
PRINT '- I/O-intensive queries (large table scans, PAGEIOLATCH waits)';
PRINT '- Memory-intensive queries (hash aggregations, potential spills)';
PRINT '- Parallel queries (CXPACKET, CXSYNC_PORT, EXECSYNC waits)';
PRINT '- Exclusive lock blocking (LCK_M_X)';
PRINT '- Shared lock blocking (LCK_M_S)';
PRINT '- Update lock blocking (LCK_M_U)';
PRINT '- Page lock blocking';
PRINT '- Key lock blocking (row-level)';
PRINT '- FORGOTTEN TRANSACTION (sleeping blocker with open txns) - CRITICAL RCA!';
PRINT '- Transaction log waits (WRITELOG)';
PRINT '- Network waits (ASYNC_NETWORK_IO)';
PRINT '- Memory grant waits (RESOURCE_SEMAPHORE)';
PRINT '- CPU pressure (SOS_SCHEDULER_YIELD)';
PRINT '- Complex execution plans (Nested Loops, Hash Match, Merge Join)';
PRINT '- Different isolation levels (READ COMMITTED, REPEATABLE READ, SERIALIZABLE)';
PRINT '';
PRINT 'Check New Relic for comprehensive monitoring data!';
PRINT '=============================================================================';
GO
