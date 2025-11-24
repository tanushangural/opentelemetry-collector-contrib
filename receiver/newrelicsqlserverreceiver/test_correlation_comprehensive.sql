-- ============================================================================
-- COMPREHENSIVE SQL SERVER QUERY MONITORING TEST SCRIPT
-- Tests all edge cases and correlation scenarios for OpenTelemetry Collector
-- ============================================================================

USE AdventureWorks2022;
GO

-- ============================================================================
-- SCENARIO 1: PARAMETERIZED QUERIES (Perfect Correlation ✅)
-- Same query_hash across all parameter values
-- ============================================================================
PRINT '=== SCENARIO 1: Parameterized Queries (Best Practice) ===';
GO

-- Execute with different parameters - all get SAME query_hash
EXEC sp_executesql N'
SELECT p.ProductID, p.Name, p.StandardCost, p.ListPrice
FROM Production.Product p
WHERE p.ProductID = @ProductID
AND p.SafetyStockLevel > @MinStock',
N'@ProductID INT, @MinStock INT',
@ProductID = 1, @MinStock = 100;

EXEC sp_executesql N'
SELECT p.ProductID, p.Name, p.StandardCost, p.ListPrice
FROM Production.Product p
WHERE p.ProductID = @ProductID
AND p.SafetyStockLevel > @MinStock',
N'@ProductID INT, @MinStock INT',
@ProductID = 999, @MinStock = 50;

PRINT 'Parameterized queries executed - Check query_hash consistency';
GO

-- ============================================================================
-- SCENARIO 2: AD-HOC QUERIES WITH LITERALS (Different Hashes per Literal)
-- Each literal combination gets DIFFERENT query_hash
-- ============================================================================
PRINT '=== SCENARIO 2: Ad-hoc Queries with Literals (Anti-Pattern) ===';
GO

-- Each of these gets a DIFFERENT query_hash
SELECT * FROM Production.Product WHERE ProductID = 1;
SELECT * FROM Production.Product WHERE ProductID = 2;
SELECT * FROM Production.Product WHERE ProductID = 3;

PRINT 'Ad-hoc queries executed - Each has DIFFERENT query_hash';
GO

-- ============================================================================
-- SCENARIO 3: OPTION(RECOMPILE) - NULL query_hash ❌
-- Forces plan to not be cached, query_hash will be NULL
-- ============================================================================
PRINT '=== SCENARIO 3: OPTION(RECOMPILE) - Not Correlatable ===';
GO

SELECT p.ProductID, p.Name, COUNT(*) as OrderCount
FROM Production.Product p
JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
WHERE p.ProductID > 100
GROUP BY p.ProductID, p.Name
OPTION(RECOMPILE);

PRINT 'OPTION(RECOMPILE) query executed - query_hash will be NULL';
GO

-- ============================================================================
-- SCENARIO 4: PARALLEL WORKERS (Same query_hash, multiple sessions)
-- Long-running query that spawns parallel workers
-- ============================================================================
PRINT '=== SCENARIO 4: Parallel Execution (Multiple Workers, Same Hash) ===';
GO

-- Heavy aggregation that will trigger parallel execution
SELECT
    p.ProductSubcategoryID,
    COUNT(DISTINCT sod.SalesOrderID) as UniqueOrders,
    SUM(sod.LineTotal) as TotalSales,
    AVG(sod.OrderQty) as AvgQuantity,
    MAX(sod.UnitPrice) as MaxPrice,
    STRING_AGG(CAST(p.ProductID AS VARCHAR), ',') as ProductList
FROM Production.Product p
CROSS JOIN Sales.SalesOrderDetail sod
WHERE p.ProductSubcategoryID IS NOT NULL
GROUP BY p.ProductSubcategoryID
OPTION (MAXDOP 4);  -- Force parallel execution with 4 workers

PRINT 'Parallel query executed - Check for multiple sessions with same query_hash';
GO

-- ============================================================================
-- SCENARIO 5: FAST QUERIES (Complete quickly, move to plan cache)
-- These execute in <100ms and disappear from dm_exec_requests quickly
-- ============================================================================
PRINT '=== SCENARIO 5: Fast Queries (Quickly move to plan cache) ===';
GO

-- Rapid-fire fast queries
DECLARE @i INT = 1;
WHILE @i <= 10
BEGIN
    SELECT TOP 10 ProductID, Name FROM Production.Product WHERE ProductID > @i;
    SET @i = @i + 1;
END

PRINT 'Fast queries executed - Will be in dm_exec_query_stats, not dm_exec_requests';
GO

-- ============================================================================
-- SCENARIO 6: LONG-RUNNING QUERIES (Stay in dm_exec_requests for minutes)
-- Simulates production long-running queries
-- ============================================================================
PRINT '=== SCENARIO 6: Long-Running Queries (Active for minutes) ===';
GO

-- Intentionally slow cross-join
SELECT TOP 100
    p1.ProductID as Product1,
    p2.ProductID as Product2,
    REPLICATE(p1.Name, 50) as ReplicatedName,
    (SELECT COUNT(*) FROM Sales.SalesOrderDetail WHERE ProductID = p1.ProductID) as P1_OrderCount,
    (SELECT COUNT(*) FROM Sales.SalesOrderDetail WHERE ProductID = p2.ProductID) as P2_OrderCount
FROM Production.Product p1
CROSS JOIN Production.Product p2
WHERE p1.ProductID < 100 AND p2.ProductID < 100;

PRINT 'Long-running query executed - Should be visible in dm_exec_requests';
GO

-- ============================================================================
-- SCENARIO 7: BLOCKING SCENARIO (Blocker and Blocked queries)
-- Creates blocking chain for testing
-- ============================================================================
PRINT '=== SCENARIO 7: Blocking Scenario (Lock Contention) ===';
GO

-- Session 1: Start a transaction and hold locks
BEGIN TRANSACTION;
UPDATE Production.Product
SET StandardCost = StandardCost * 1.01
WHERE ProductID = 1;
-- DON'T COMMIT YET - keeps locks

PRINT 'Blocker session started - Run blocking query in another session';
PRINT 'In another session, run: SELECT * FROM Production.Product WITH (TABLOCKX);';
PRINT 'Then run: ROLLBACK; in this session to release locks';
-- Uncommitted transaction holds locks
GO

-- ============================================================================
-- SCENARIO 8: CPU-INTENSIVE QUERIES (High CPU time)
-- Generates high CPU usage for testing
-- ============================================================================
PRINT '=== SCENARIO 8: CPU-Intensive Queries (High CPU Usage) ===';
GO

-- CPU-bound computation
DECLARE @result BIGINT = 0;
DECLARE @j INT = 1;

WHILE @j < 100000
BEGIN
    SET @result = @result + @j * @j;
    SET @j = @j + 1;
END

SELECT @result as ComputationResult;

PRINT 'CPU-intensive query executed - Check cpu_time_ms metric';
GO

-- ============================================================================
-- SCENARIO 9: I/O-INTENSIVE QUERIES (High reads/writes)
-- Generates disk I/O for testing
-- ============================================================================
PRINT '=== SCENARIO 9: I/O-Intensive Queries (High Reads) ===';
GO

-- Force physical reads by clearing buffer cache (ADMIN ONLY)
-- DBCC DROPCLEANBUFFERS;  -- Uncomment if you have admin rights

-- Large table scan
SELECT
    soh.SalesOrderID,
    soh.OrderDate,
    sod.ProductID,
    sod.OrderQty,
    p.Name,
    p.ProductNumber
FROM Sales.SalesOrderHeader soh
JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
JOIN Production.Product p ON sod.ProductID = p.ProductID
WHERE soh.OrderDate >= '2011-01-01'
ORDER BY soh.OrderDate DESC;

PRINT 'I/O-intensive query executed - Check reads and logical_reads metrics';
GO

-- ============================================================================
-- SCENARIO 10: QUERIES WITH WAITS (ASYNC_NETWORK_IO, etc.)
-- Simulates client not fetching results fast enough
-- ============================================================================
PRINT '=== SCENARIO 10: Queries with Wait States ===';
GO

-- Large result set that client might not fetch quickly
SELECT
    sod.*,
    p.Name,
    p.Color,
    p.Size,
    REPLICATE('X', 1000) as LargeColumn
FROM Sales.SalesOrderDetail sod
JOIN Production.Product p ON sod.ProductID = p.ProductID;

PRINT 'Large result set query executed - May show ASYNC_NETWORK_IO wait';
GO

-- ============================================================================
-- SCENARIO 11: STORED PROCEDURE EXECUTION (Consistent query_hash)
-- Same query_hash across all stored procedure executions
-- ============================================================================
PRINT '=== SCENARIO 11: Stored Procedure Execution ===';
GO

-- Create temp stored procedure
CREATE OR ALTER PROCEDURE #TestCorrelation
    @MinSales MONEY = 1000
AS
BEGIN
    SELECT
        p.ProductID,
        p.Name,
        SUM(sod.LineTotal) as TotalSales,
        COUNT(sod.SalesOrderDetailID) as SaleCount
    FROM Production.Product p
    JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
    GROUP BY p.ProductID, p.Name
    HAVING SUM(sod.LineTotal) > @MinSales
    ORDER BY TotalSales DESC;
END
GO

-- Execute multiple times with different parameters
EXEC #TestCorrelation @MinSales = 1000;
EXEC #TestCorrelation @MinSales = 5000;
EXEC #TestCorrelation @MinSales = 10000;

PRINT 'Stored procedure executed - All executions share same query_hash';
GO

-- ============================================================================
-- SCENARIO 12: TRANSACTION WITH MULTIPLE STATEMENTS
-- Tests transaction_id correlation
-- ============================================================================
PRINT '=== SCENARIO 12: Multi-Statement Transaction ===';
GO

BEGIN TRANSACTION;

-- Statement 1
UPDATE Production.Product
SET StandardCost = StandardCost * 1.001
WHERE ProductSubcategoryID = 1;

-- Wait a bit
WAITFOR DELAY '00:00:02';

-- Statement 2
SELECT ProductID, Name, StandardCost
FROM Production.Product
WHERE ProductSubcategoryID = 1;

-- Wait again
WAITFOR DELAY '00:00:02';

-- Statement 3
INSERT INTO Production.ProductDocument (ProductID, DocumentNode)
VALUES (1, CAST(NEWID() AS VARCHAR(36)));

COMMIT TRANSACTION;

PRINT 'Multi-statement transaction executed - Check transaction_id correlation';
GO

-- ============================================================================
-- SCENARIO 13: MEMORY-INTENSIVE QUERIES (High granted_query_memory)
-- Tests memory grant tracking
-- ============================================================================
PRINT '=== SCENARIO 13: Memory-Intensive Queries (Large Memory Grants) ===';
GO

-- Force large memory grant with OPTION(MAXDOP)
SELECT DISTINCT
    p1.ProductID,
    p2.ProductID,
    p1.Name,
    p2.Name,
    ROW_NUMBER() OVER (PARTITION BY p1.ProductSubcategoryID ORDER BY p1.ProductID) as RowNum
FROM Production.Product p1
CROSS JOIN Production.Product p2
WHERE p1.ProductSubcategoryID IS NOT NULL
OPTION (MAXDOP 1);  -- Forces larger memory grant

PRINT 'Memory-intensive query executed - Check granted_query_memory_pages metric';
GO

-- ============================================================================
-- SCENARIO 14: CTE AND SUBQUERY PATTERNS
-- Tests query_hash for complex query structures
-- ============================================================================
PRINT '=== SCENARIO 14: CTE and Subquery Patterns ===';
GO

WITH ProductSales AS (
    SELECT
        ProductID,
        SUM(LineTotal) as TotalSales,
        COUNT(*) as OrderCount
    FROM Sales.SalesOrderDetail
    GROUP BY ProductID
),
ProductRanking AS (
    SELECT
        ProductID,
        TotalSales,
        OrderCount,
        ROW_NUMBER() OVER (ORDER BY TotalSales DESC) as SalesRank
    FROM ProductSales
)
SELECT
    p.ProductID,
    p.Name,
    pr.TotalSales,
    pr.OrderCount,
    pr.SalesRank
FROM ProductRanking pr
JOIN Production.Product p ON pr.ProductID = p.ProductID
WHERE pr.SalesRank <= 100;

PRINT 'CTE query executed - Check query_hash for complex structures';
GO

-- ============================================================================
-- SCENARIO 15: QUERY WITH DIFFERENT EXECUTION PLANS (Parameter Sniffing)
-- Same query_hash but different performance based on parameters
-- ============================================================================
PRINT '=== SCENARIO 15: Parameter Sniffing Scenario ===';
GO

-- First execution with selective parameter (creates index seek plan)
EXEC sp_executesql N'
SELECT * FROM Sales.SalesOrderHeader
WHERE CustomerID = @CustomerID',
N'@CustomerID INT',
@CustomerID = 29825;  -- Customer with few orders

WAITFOR DELAY '00:00:01';

-- Second execution with non-selective parameter (uses cached plan, may perform poorly)
EXEC sp_executesql N'
SELECT * FROM Sales.SalesOrderHeader
WHERE CustomerID = @CustomerID',
N'@CustomerID INT',
@CustomerID = 1;  -- Customer with many orders (reuses same plan)

PRINT 'Parameter sniffing scenario executed - Same query_hash, different performance';
GO

-- ============================================================================
-- VERIFICATION QUERIES
-- Run these to check what the collector will see
-- ============================================================================
PRINT '=== VERIFICATION: Check Active Queries ===';
GO

SELECT
    r.session_id,
    r.status,
    r.command,
    r.query_hash,
    r.total_elapsed_time / 1000.0 AS elapsed_seconds,
    r.cpu_time / 1000.0 AS cpu_seconds,
    r.wait_type,
    r.blocking_session_id,
    DB_NAME(r.database_id) AS database_name,
    s.login_name,
    LEFT(t.text, 100) AS query_preview
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE s.is_user_process = 1
ORDER BY r.start_time DESC;
GO

PRINT '=== VERIFICATION: Check Plan Cache (Completed Queries) ===';
GO

SELECT TOP 20
    qs.query_hash,
    qs.execution_count,
    qs.total_elapsed_time / 1000000.0 AS total_elapsed_seconds,
    (qs.total_elapsed_time / qs.execution_count) / 1000.0 AS avg_elapsed_ms,
    qs.last_execution_time,
    DB_NAME(CONVERT(INT, pa.value)) AS database_name,
    LEFT(qt.text, 100) AS query_preview
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
WHERE pa.attribute = 'dbid'
    AND DB_NAME(CONVERT(INT, pa.value)) = 'AdventureWorks2022'
    AND qs.last_execution_time >= DATEADD(MINUTE, -10, GETUTCDATE())
ORDER BY qs.last_execution_time DESC;
GO

PRINT '=== VERIFICATION: Query Hash Summary ===';
GO

SELECT
    CASE
        WHEN query_hash IS NULL THEN 'NULL (Not Correlatable)'
        ELSE 'HAS HASH (Correlatable)'
    END AS correlation_status,
    COUNT(*) AS query_count
FROM sys.dm_exec_requests
WHERE session_id IN (SELECT session_id FROM sys.dm_exec_sessions WHERE is_user_process = 1)
GROUP BY CASE
        WHEN query_hash IS NULL THEN 'NULL (Not Correlatable)'
        ELSE 'HAS HASH (Correlatable)'
    END;
GO

PRINT '============================================================================';
PRINT 'TEST SCRIPT COMPLETE';
PRINT 'All edge cases and correlation scenarios have been executed';
PRINT 'The OpenTelemetry collector should now capture diverse query patterns';
PRINT '============================================================================';
GO
