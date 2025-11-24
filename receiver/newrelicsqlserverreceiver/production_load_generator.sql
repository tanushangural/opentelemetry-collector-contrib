-- ============================================================================
-- PRODUCTION-LIKE CONTINUOUS LOAD GENERATOR
-- Simulates realistic database workload with various query patterns
-- Run this in multiple sessions to generate concurrent load
-- ============================================================================

USE AdventureWorks2022;
GO

PRINT '============================================================================';
PRINT 'PRODUCTION LOAD GENERATOR - Starting...';
PRINT 'This script runs continuously to simulate production workload';
PRINT 'Press CTRL+C to stop';
PRINT '============================================================================';
GO

DECLARE @WorkerID INT = ABS(CHECKSUM(NEWID())) % 10;  -- Random worker ID 0-9
DECLARE @IterationCount INT = 0;
DECLARE @MaxIterations INT = 1000;  -- Run 1000 iterations, adjust as needed
DECLARE @QueryType INT;
DECLARE @RandomParam INT;
DECLARE @StartTime DATETIME;
DECLARE @ElapsedMs INT;

PRINT 'Worker ' + CAST(@WorkerID AS VARCHAR) + ' started';

WHILE @IterationCount < @MaxIterations
BEGIN
    SET @StartTime = GETDATE();
    SET @QueryType = ABS(CHECKSUM(NEWID())) % 10;  -- Random query type 0-9
    SET @RandomParam = ABS(CHECKSUM(NEWID())) % 1000 + 1;  -- Random value 1-1000
    SET @IterationCount = @IterationCount + 1;

    -- ========================================================================
    -- QUERY TYPE 0: Fast OLTP Query (Parameterized, Single Row)
    -- Simulates typical OLTP workload - same query_hash, fast execution
    -- ========================================================================
    IF @QueryType = 0
    BEGIN
        EXEC sp_executesql N'
            /* ProductLookup_Worker@WorkerID */
            SELECT p.ProductID, p.Name, p.ProductNumber, p.StandardCost, p.ListPrice
            FROM Production.Product p
            WHERE p.ProductID = @PID',
            N'@PID INT, @WorkerID INT',
            @PID = @RandomParam,
            @WorkerID = @WorkerID;
    END

    -- ========================================================================
    -- QUERY TYPE 1: Aggregation Query (CPU-Intensive)
    -- Simulates reporting/analytics workload
    -- ========================================================================
    ELSE IF @QueryType = 1
    BEGIN
        EXEC sp_executesql N'
            /* SalesAnalysis_Worker@WorkerID */
            SELECT
                p.ProductSubcategoryID,
                COUNT(DISTINCT sod.SalesOrderID) as UniqueOrders,
                SUM(sod.LineTotal) as TotalSales,
                AVG(sod.OrderQty) as AvgQuantity,
                MAX(sod.UnitPrice) as MaxPrice
            FROM Production.Product p
            JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
            WHERE p.ProductSubcategoryID = @SubcategoryID
            GROUP BY p.ProductSubcategoryID',
            N'@SubcategoryID INT, @WorkerID INT',
            @SubcategoryID = (@RandomParam % 37) + 1,  -- 37 subcategories in AdventureWorks
            @WorkerID = @WorkerID;
    END

    -- ========================================================================
    -- QUERY TYPE 2: Join-Heavy Query (I/O-Intensive)
    -- Simulates complex multi-table joins
    -- ========================================================================
    ELSE IF @QueryType = 2
    BEGIN
        EXEC sp_executesql N'
            /* CustomerOrderHistory_Worker@WorkerID */
            SELECT TOP 100
                soh.SalesOrderID,
                soh.OrderDate,
                c.CustomerID,
                c.AccountNumber,
                p.Name as ProductName,
                sod.OrderQty,
                sod.LineTotal
            FROM Sales.Customer c
            JOIN Sales.SalesOrderHeader soh ON c.CustomerID = soh.CustomerID
            JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
            JOIN Production.Product p ON sod.ProductID = p.ProductID
            WHERE c.CustomerID = @CustomerID
            ORDER BY soh.OrderDate DESC',
            N'@CustomerID INT, @WorkerID INT',
            @CustomerID = @RandomParam,
            @WorkerID = @WorkerID;
    END

    -- ========================================================================
    -- QUERY TYPE 3: Full Text Search Simulation (String Operations)
    -- Simulates search functionality
    -- ========================================================================
    ELSE IF @QueryType = 3
    BEGIN
        EXEC sp_executesql N'
            /* ProductSearch_Worker@WorkerID */
            SELECT
                p.ProductID,
                p.Name,
                p.ProductNumber,
                CHARINDEX(@SearchTerm, p.Name) as MatchPosition,
                LEN(p.Name) as NameLength
            FROM Production.Product p
            WHERE p.Name LIKE ''%'' + @SearchTerm + ''%''
            OR p.ProductNumber LIKE ''%'' + @SearchTerm + ''%''',
            N'@SearchTerm VARCHAR(50), @WorkerID INT',
            @SearchTerm = CHAR(65 + (@RandomParam % 26)),  -- Random letter A-Z
            @WorkerID = @WorkerID;
    END

    -- ========================================================================
    -- QUERY TYPE 4: Update Operation (Write-Heavy)
    -- Simulates transactional updates
    -- ========================================================================
    ELSE IF @QueryType = 4
    BEGIN
        BEGIN TRANSACTION;

        EXEC sp_executesql N'
            /* InventoryUpdate_Worker@WorkerID */
            UPDATE Production.Product
            SET ModifiedDate = GETDATE()
            WHERE ProductID = @PID',
            N'@PID INT, @WorkerID INT',
            @PID = @RandomParam,
            @WorkerID = @WorkerID;

        COMMIT TRANSACTION;
    END

    -- ========================================================================
    -- QUERY TYPE 5: Long-Running Analytical Query
    -- Simulates slow dashboard/report queries
    -- ========================================================================
    ELSE IF @QueryType = 5
    BEGIN
        EXEC sp_executesql N'
            /* MonthlyRevenueAnalysis_Worker@WorkerID */
            SELECT
                YEAR(soh.OrderDate) as OrderYear,
                MONTH(soh.OrderDate) as OrderMonth,
                COUNT(DISTINCT soh.CustomerID) as UniqueCustomers,
                COUNT(soh.SalesOrderID) as TotalOrders,
                SUM(soh.SubTotal) as MonthlyRevenue,
                AVG(soh.SubTotal) as AvgOrderValue
            FROM Sales.SalesOrderHeader soh
            WHERE soh.OrderDate >= DATEADD(MONTH, -@MonthsBack, GETDATE())
            GROUP BY YEAR(soh.OrderDate), MONTH(soh.OrderDate)
            ORDER BY OrderYear DESC, OrderMonth DESC',
            N'@MonthsBack INT, @WorkerID INT',
            @MonthsBack = (@RandomParam % 12) + 1,  -- 1-12 months
            @WorkerID = @WorkerID;
    END

    -- ========================================================================
    -- QUERY TYPE 6: Subquery Pattern (Nested Queries)
    -- Simulates complex business logic
    -- ========================================================================
    ELSE IF @QueryType = 6
    BEGIN
        EXEC sp_executesql N'
            /* TopProductsByCategory_Worker@WorkerID */
            SELECT
                p.ProductID,
                p.Name,
                (SELECT SUM(LineTotal)
                 FROM Sales.SalesOrderDetail
                 WHERE ProductID = p.ProductID) as TotalRevenue,
                (SELECT COUNT(*)
                 FROM Sales.SalesOrderDetail
                 WHERE ProductID = p.ProductID) as OrderCount
            FROM Production.Product p
            WHERE p.ProductSubcategoryID = @SubcategoryID
            AND (SELECT SUM(LineTotal)
                 FROM Sales.SalesOrderDetail
                 WHERE ProductID = p.ProductID) > @MinRevenue
            ORDER BY TotalRevenue DESC',
            N'@SubcategoryID INT, @MinRevenue MONEY, @WorkerID INT',
            @SubcategoryID = (@RandomParam % 37) + 1,
            @MinRevenue = @RandomParam * 10.0,
            @WorkerID = @WorkerID;
    END

    -- ========================================================================
    -- QUERY TYPE 7: CTE Pattern (Modern SQL)
    -- Simulates hierarchical or recursive queries
    -- ========================================================================
    ELSE IF @QueryType = 7
    BEGIN
        EXEC sp_executesql N'
            /* ProductProfitability_Worker@WorkerID */
            WITH ProductMetrics AS (
                SELECT
                    p.ProductID,
                    p.Name,
                    p.StandardCost,
                    p.ListPrice,
                    SUM(sod.LineTotal) as TotalRevenue,
                    COUNT(sod.SalesOrderDetailID) as SalesCount
                FROM Production.Product p
                LEFT JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
                WHERE p.ProductSubcategoryID = @SubcategoryID
                GROUP BY p.ProductID, p.Name, p.StandardCost, p.ListPrice
            )
            SELECT
                ProductID,
                Name,
                StandardCost,
                ListPrice,
                TotalRevenue,
                SalesCount,
                (ListPrice - StandardCost) as ProfitMargin,
                (TotalRevenue - (StandardCost * SalesCount)) as EstimatedProfit
            FROM ProductMetrics
            WHERE TotalRevenue > @MinRevenue
            ORDER BY EstimatedProfit DESC',
            N'@SubcategoryID INT, @MinRevenue MONEY, @WorkerID INT',
            @SubcategoryID = (@RandomParam % 37) + 1,
            @MinRevenue = @RandomParam * 5.0,
            @WorkerID = @WorkerID;
    END

    -- ========================================================================
    -- QUERY TYPE 8: Bulk Data Export Pattern
    -- Simulates ETL/export operations
    -- ========================================================================
    ELSE IF @QueryType = 8
    BEGIN
        EXEC sp_executesql N'
            /* BulkDataExport_Worker@WorkerID */
            SELECT TOP 1000
                soh.SalesOrderID,
                soh.OrderDate,
                soh.CustomerID,
                soh.SubTotal,
                soh.TaxAmt,
                soh.Freight,
                soh.TotalDue,
                sod.ProductID,
                sod.OrderQty,
                sod.UnitPrice,
                sod.LineTotal
            FROM Sales.SalesOrderHeader soh
            JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
            WHERE soh.OrderDate >= DATEADD(DAY, -@DaysBack, GETDATE())
            ORDER BY soh.OrderDate DESC',
            N'@DaysBack INT, @WorkerID INT',
            @DaysBack = (@RandomParam % 30) + 1,  -- 1-30 days
            @WorkerID = @WorkerID;
    END

    -- ========================================================================
    -- QUERY TYPE 9: Parameterized Stored Procedure Call
    -- Simulates application calling stored procedures
    -- ========================================================================
    ELSE IF @QueryType = 9
    BEGIN
        EXEC sp_executesql N'
            /* GetProductsByPrice_Worker@WorkerID */
            SELECT
                p.ProductID,
                p.Name,
                p.ProductNumber,
                p.StandardCost,
                p.ListPrice,
                p.Color,
                p.Size
            FROM Production.Product p
            WHERE p.ListPrice BETWEEN @MinPrice AND @MaxPrice
            AND p.SellEndDate IS NULL
            ORDER BY p.ListPrice DESC',
            N'@MinPrice MONEY, @MaxPrice MONEY, @WorkerID INT',
            @MinPrice = @RandomParam * 1.0,
            @MaxPrice = (@RandomParam + 500) * 1.0,
            @WorkerID = @WorkerID;
    END

    -- Calculate elapsed time for this iteration
    SET @ElapsedMs = DATEDIFF(MILLISECOND, @StartTime, GETDATE());

    -- Log progress every 100 iterations
    IF @IterationCount % 100 = 0
    BEGIN
        PRINT 'Worker ' + CAST(@WorkerID AS VARCHAR) +
              ': Iteration ' + CAST(@IterationCount AS VARCHAR) +
              ' | Last query type: ' + CAST(@QueryType AS VARCHAR) +
              ' | Elapsed: ' + CAST(@ElapsedMs AS VARCHAR) + 'ms';
    END

    -- Random delay between queries (10-500ms) to simulate realistic load
    DECLARE @DelayMs INT = (@RandomParam % 490) + 10;
    DECLARE @DelayString VARCHAR(12) = '00:00:00.' + RIGHT('000' + CAST(@DelayMs AS VARCHAR), 3);
    WAITFOR DELAY @DelayString;

END

PRINT '============================================================================';
PRINT 'Worker ' + CAST(@WorkerID AS VARCHAR) + ' completed ' + CAST(@IterationCount AS VARCHAR) + ' iterations';
PRINT 'PRODUCTION LOAD GENERATOR - Finished';
PRINT '============================================================================';
GO
