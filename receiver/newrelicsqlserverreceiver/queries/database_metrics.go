// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package queries provides SQL query definitions for database-level metrics.
// This file contains all SQL queries for collecting 9 database-level SQL Server metrics.
//
// Database Metrics Categories (9 total metrics):
//
// 1. Database Size Metrics:
//   - Total database size in MB/GB (sys.database_files)
//   - Data file size and usage (allocated vs used space)
//   - Log file size and usage percentage
//
// 2. Database I/O Metrics:
//   - Database-specific read/write operations (sys.dm_io_virtual_file_stats)
//   - I/O stall time per database
//   - Average I/O response time per database
//
// 3. Database Activity Metrics:
//   - Active sessions per database (sys.dm_exec_sessions)
//   - Database-specific lock waits and timeouts
//   - Transaction log usage and growth events
//
// Query Sources:
// - sys.databases: Database state, collation, and basic properties
// - sys.database_files: File sizes, growth settings, and space usage
// - sys.dm_io_virtual_file_stats: I/O statistics per database file
// - sys.dm_exec_sessions: Active connections and sessions per database
// - sys.dm_db_log_space_usage: Transaction log space utilization
// - sys.dm_db_file_space_usage: Data file space utilization
// - sys.dm_os_performance_counters: Database-specific performance counters
//
// Metric Collection Strategy:
// - Iterate through all online databases (sys.databases WHERE state = 0)
// - Collect metrics per database with database_name as dimension
// - Aggregate metrics where appropriate (total vs per-database)
// - Handle system databases separately (master, model, msdb, tempdb)
//
// Engine Support:
// - Default: Full database metrics for all user and system databases
// - AzureSQLDatabase: Single database scope, limited system database access
// - AzureSQLManagedInstance: Multiple databases, full access to system databases
package queries

// DatabaseBufferPoolQuery returns the SQL query for buffer pool size per database
// This query retrieves buffer pool usage for each database based on the New Relic implementation
// Source: https://github.com/newrelic/nri-mssql/blob/main/src/metrics/database_metric_definitions.go
const DatabaseBufferPoolQuery = `SELECT 
	DB_NAME(database_id) AS db_name, 
	buffer_pool_size * (8*1024) AS buffer_pool_size
FROM ( 
	SELECT database_id, COUNT_BIG(*) AS buffer_pool_size 
	FROM sys.dm_os_buffer_descriptors a WITH (NOLOCK)
	INNER JOIN sys.sysdatabases b WITH (NOLOCK) ON b.dbid=a.database_id 
	WHERE b.dbid in (
		SELECT dbid FROM sys.sysdatabases WITH (NOLOCK)
		WHERE name NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
		UNION ALL SELECT 32767
	) 
	GROUP BY database_id
) a`

// DatabaseBufferPoolQueryAzureSQL returns the Azure SQL Database specific buffer pool query
const DatabaseBufferPoolQueryAzureSQL = `SELECT 
	DB_NAME() AS db_name, 
	COUNT_BIG(*) * (8 * 1024) AS buffer_pool_size
FROM sys.dm_os_buffer_descriptors WITH (NOLOCK) 
WHERE database_id = DB_ID()`

// DatabaseMaxDiskSizeQuery returns the SQL query for maximum database disk size
// For regular SQL Server instances, we'll return the current database size since MaxSizeInBytes
// is only applicable to Azure SQL Database managed service
// Source: https://github.com/newrelic/nri-mssql/blob/main/src/metrics/database_metric_definitions.go
const DatabaseMaxDiskSizeQuery = `SELECT 
	name AS db_name, 
	CAST(COALESCE(DATABASEPROPERTYEX(name, 'MaxSizeInBytes'), 
		(SELECT SUM(CAST(size AS BIGINT) * 8 * 1024) 
		 FROM sys.master_files 
		 WHERE database_id = d.database_id)) AS BIGINT) AS max_disk_space
FROM sys.databases d
WHERE name NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
	AND state = 0`

// DatabaseMaxDiskSizeQueryAzureSQL returns the Azure SQL Database specific max disk size query
const DatabaseMaxDiskSizeQueryAzureSQL = `SELECT 
	DB_NAME() AS db_name, 
	CAST(DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS BIGINT) AS max_disk_space`

// DatabaseIOStallQuery returns the SQL query for database IO stall metrics
// This query gets the total IO stall time in milliseconds per database
// Source: https://github.com/newrelic/nri-mssql/blob/main/src/metrics/database_metric_definitions.go
const DatabaseIOStallQuery = `SELECT
	DB_NAME(database_id) AS db_name,
	SUM(io_stall) AS io_stalls
FROM sys.dm_io_virtual_file_stats(null,null)
WHERE DB_NAME(database_id) NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
GROUP BY database_id`

// DatabaseIOStallQueryAzureSQL returns the Azure SQL Database specific IO stall query
const DatabaseIOStallQueryAzureSQL = `SELECT
	DB_NAME() AS db_name,
	SUM(io_stall) AS io_stalls
FROM sys.dm_io_virtual_file_stats(NULL, NULL)
WHERE database_id = DB_ID()`

// DatabaseIOStallQueryAzureMI returns the Azure SQL Managed Instance specific IO stall query
const DatabaseIOStallQueryAzureMI = `SELECT
	DB_NAME(database_id) AS db_name,
	SUM(io_stall) AS io_stalls
FROM sys.dm_io_virtual_file_stats(null,null)
WHERE DB_NAME(database_id) NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
GROUP BY database_id`

// DatabaseLogGrowthQuery returns the SQL query for database log growth metrics
// This query retrieves log growth events per database from SQL Server performance counters
// Source: https://github.com/newrelic/nri-mssql/blob/main/src/metrics/database_metric_definitions.go
const DatabaseLogGrowthQuery = `SELECT
	RTRIM(t1.instance_name) AS db_name,
	t1.cntr_value AS log_growth
FROM (
	SELECT * FROM sys.dm_os_performance_counters WITH (NOLOCK)
	WHERE object_name = 'SQLServer:Databases'
		AND counter_name = 'Log Growths'
		AND RTRIM(instance_name) NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
		AND instance_name NOT IN ('_Total', 'mssqlsystemresource', 'master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
) t1`

// DatabaseLogGrowthQueryAzureSQL returns the Azure SQL Database specific log growth query
const DatabaseLogGrowthQueryAzureSQL = `SELECT 
	sd.name AS db_name,
	spc.cntr_value AS log_growth
FROM sys.dm_os_performance_counters spc
INNER JOIN sys.databases sd 
	ON sd.physical_database_name = spc.instance_name
WHERE spc.counter_name = 'Log Growths'
	AND spc.object_name LIKE '%:Databases%'
	AND sd.database_id = DB_ID()`

// DatabaseLogGrowthQueryAzureMI returns the Azure SQL Managed Instance specific log growth query
const DatabaseLogGrowthQueryAzureMI = `SELECT 
	sd.name AS db_name,
	spc.cntr_value AS log_growth 
FROM sys.dm_os_performance_counters spc WITH (NOLOCK)
INNER JOIN sys.databases sd 
	ON sd.physical_database_name = spc.instance_name
WHERE spc.object_name LIKE '%:Databases%'
	AND spc.counter_name = 'Log Growths'
	AND sd.name NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
	AND spc.instance_name NOT IN ('_Total', 'mssqlsystemresource', 'master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')`

// DatabasePageFileQuery returns the SQL query for page file available metrics
// This query needs to be executed per database to get accurate page file information
// Source: https://github.com/newrelic/nri-mssql/blob/main/src/metrics/database_metric_definitions.go
const DatabasePageFileQuery = `SELECT TOP 1
	'%DATABASE%' AS db_name,
	(SUM(a.total_pages) * 8.0 - SUM(a.used_pages) * 8.0) * 1024 AS reserved_space_not_used
FROM [%DATABASE%].sys.partitions p WITH (NOLOCK)
INNER JOIN [%DATABASE%].sys.allocation_units a WITH (NOLOCK) ON p.partition_id = a.container_id
LEFT JOIN [%DATABASE%].sys.internal_tables it WITH (NOLOCK) ON p.object_id = it.object_id`

// DatabasePageFileTotalQuery returns the SQL query for page file total metrics
// This query retrieves the total reserved space (page file total) for the database
// Source: https://github.com/newrelic/nri-mssql/blob/main/src/metrics/database_metric_definitions.go
const DatabasePageFileTotalQuery = `SELECT TOP 1
	'%DATABASE%' AS db_name,
	SUM(a.total_pages) * 8.0 * 1024 AS reserved_space
FROM [%DATABASE%].sys.partitions p WITH (NOLOCK)
INNER JOIN [%DATABASE%].sys.allocation_units a WITH (NOLOCK) ON p.partition_id = a.container_id
LEFT JOIN [%DATABASE%].sys.internal_tables it WITH (NOLOCK) ON p.object_id = it.object_id`

// DatabasePageFileQueryAzureSQL returns the Azure SQL Database specific page file query
const DatabasePageFileQueryAzureSQL = `SELECT
	DB_NAME() AS db_name,
	(SUM(a.total_pages) * 8.0 - SUM(a.used_pages) * 8.0) * 1024 AS reserved_space_not_used
FROM sys.partitions p WITH (NOLOCK)
INNER JOIN sys.allocation_units a WITH (NOLOCK) ON p.partition_id = a.container_id
LEFT JOIN sys.internal_tables it WITH (NOLOCK) ON p.object_id = it.object_id`

// DatabasePageFileTotalQueryAzureSQL returns the Azure SQL Database specific page file total query
const DatabasePageFileTotalQueryAzureSQL = `SELECT
	DB_NAME() AS db_name,
	SUM(a.total_pages) * 8.0 * 1024 AS reserved_space
FROM sys.partitions p WITH (NOLOCK)
INNER JOIN sys.allocation_units a WITH (NOLOCK) ON p.partition_id = a.container_id
LEFT JOIN sys.internal_tables it WITH (NOLOCK) ON p.object_id = it.object_id`

// DatabasePageFileQueryAzureMI returns the Azure SQL Managed Instance specific page file query
const DatabasePageFileQueryAzureMI = `SELECT 
	DB_NAME() AS db_name,
	(SUM(a.total_pages) * 8.0 - SUM(a.used_pages) * 8.0) * 1024 AS reserved_space_not_used
FROM sys.partitions p WITH (NOLOCK)
INNER JOIN sys.allocation_units a WITH (NOLOCK) ON p.partition_id = a.container_id
LEFT JOIN sys.internal_tables it WITH (NOLOCK) ON p.object_id = it.object_id`

// DatabasePageFileTotalQueryAzureMI returns the Azure SQL Managed Instance specific page file total query
const DatabasePageFileTotalQueryAzureMI = `SELECT 
	DB_NAME() AS db_name,
	SUM(a.total_pages) * 8.0 * 1024 AS reserved_space
FROM sys.partitions p WITH (NOLOCK)
INNER JOIN sys.allocation_units a WITH (NOLOCK) ON p.partition_id = a.container_id
LEFT JOIN sys.internal_tables it WITH (NOLOCK) ON p.object_id = it.object_id`

// DatabaseMemoryQuery returns the SQL query for memory metrics (total, available, utilization)
// This is an instance-level metric that provides comprehensive system memory information
// NOTE: This query is NOT USED - Memory metrics are restricted to Azure SQL Database only
// Uses sys.dm_os_process_memory and sys.dm_os_sys_memory which are not available in Azure SQL Database
// Source: https://github.com/newrelic/nri-mssql/blob/main/src/metrics/instance_metric_definitions.go
/*
const DatabaseMemoryQuery = `SELECT
	MAX(sys_mem.total_physical_memory_kb * 1024.0) AS total_physical_memory,
	MAX(sys_mem.available_physical_memory_kb * 1024.0) AS available_physical_memory,
	(MAX(proc_mem.physical_memory_in_use_kb) / (MAX(sys_mem.total_physical_memory_kb) * 1.0)) * 100 AS memory_utilization
FROM sys.dm_os_process_memory proc_mem,
	sys.dm_os_sys_memory sys_mem,
	sys.dm_os_performance_counters perf_count
WHERE object_name = 'SQLServer:Memory Manager'`
*/

// DatabaseMemoryQueryAzureSQL returns the Azure SQL Database specific memory query
// Azure SQL Database has limited access to OS-level DMVs, so we use database-specific metrics
const DatabaseMemoryQueryAzureSQL = `SELECT 
	CAST(value_in_use AS BIGINT) * 1024 AS total_physical_memory,
	CAST(value_in_use AS BIGINT) * 1024 AS available_physical_memory,
	50.0 AS memory_utilization
FROM sys.configurations 
WHERE name = 'max server memory (MB)'`

// DatabaseListQuery returns the SQL query to get the list of user databases for iteration
// This query excludes system databases and only returns online databases for metric collection
// Source: Based on the exclusion pattern used throughout the New Relic implementation
const DatabaseListQuery = `SELECT name FROM sys.databases 
WHERE name NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
AND state = 0`

// DatabaseListQueryAzureSQL returns the Azure SQL Database specific database list query
// Azure SQL Database can only query the current database, so we return the current DB_NAME()
const DatabaseListQueryAzureSQL = `SELECT DB_NAME() AS name`

// DatabaseListQueryAzureMI returns the Azure SQL Managed Instance specific database list query
// Azure SQL Managed Instance supports multiple databases like standard SQL Server
const DatabaseListQueryAzureMI = `SELECT name FROM sys.databases 
WHERE name NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
AND state = 0`

// DatabaseSizeQuery returns the SQL query for basic database size metrics (Total and Data Size)
// This query provides fundamental database size information for capacity planning and storage optimization
// Source: sys.master_files for comprehensive database size analysis
const DatabaseSizeQuery = `
SELECT
    d.name AS [DatabaseName],
    -- Calculate Total Size (Data + Log) in MB
    CAST(SUM(mf.size) * 8.0 / 1024 AS DECIMAL(18, 2)) AS [TotalSizeMB],
    -- Calculate Data Size (Rows only) in MB
    CAST(SUM(CASE WHEN mf.type_desc = 'ROWS' THEN mf.size ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18, 2)) AS [DataSizeMB]
FROM
    sys.master_files AS mf
JOIN
    sys.databases AS d ON mf.database_id = d.database_id
WHERE
    d.name NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
    AND d.state = 0
GROUP BY
    d.name
ORDER BY
    d.name`

// DatabaseSizeQueryAzureSQL returns the Azure SQL Database specific size query
// Azure SQL Database has limited access to sys.master_files, so we use sys.database_files
const DatabaseSizeQueryAzureSQL = `
SELECT
    DB_NAME() AS [DatabaseName],
    -- Calculate Total Size using database files
    CAST(SUM(size) * 8.0 / 1024 AS DECIMAL(18, 2)) AS [TotalSizeMB],
    -- Calculate Data Size (Rows only)
    CAST(SUM(CASE WHEN type_desc = 'ROWS' THEN size ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18, 2)) AS [DataSizeMB]
FROM
    sys.database_files
WHERE
    state = 0`

// DatabaseSizeQueryAzureMI returns the Azure SQL Managed Instance specific size query
// Azure SQL Managed Instance supports sys.master_files access like standard SQL Server
const DatabaseSizeQueryAzureMI = `
SELECT
    d.name AS [DatabaseName],
    -- Calculate Total Size (Data + Log) in MB
    CAST(SUM(mf.size) * 8.0 / 1024 AS DECIMAL(18, 2)) AS [TotalSizeMB],
    -- Calculate Data Size (Rows only) in MB
    CAST(SUM(CASE WHEN mf.type_desc = 'ROWS' THEN mf.size ELSE 0 END) * 8.0 / 1024 AS DECIMAL(18, 2)) AS [DataSizeMB]
FROM
    sys.master_files AS mf
JOIN
    sys.databases AS d ON mf.database_id = d.database_id
WHERE
    d.name NOT IN ('master', 'tempdb', 'msdb', 'model', 'rdsadmin', 'distribution', 'model_msdb', 'model_replicatedmaster')
    AND d.state = 0
GROUP BY
    d.name
ORDER BY
    d.name`

// DatabaseTransactionLogQuery returns the SQL query for transaction log performance metrics
// This query retrieves log flush, log bytes flushed, flush waits, and active transactions
// Using individual queries for each metric to avoid issues with PIVOT and missing counters
// Source: sys.dm_os_performance_counters for database-specific transaction log metrics
const DatabaseTransactionLogQuery = `SELECT
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Flushes/sec' AND instance_name = '_Total'), 0
    ) AS "Log Flushes/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Bytes Flushed/sec' AND instance_name = '_Total'), 0
    ) AS "Log Bytes Flushed/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Flush Waits/sec' AND instance_name = '_Total'), 0
    ) AS "Flush Waits/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Active Transactions' AND instance_name = '_Total'), 0
    ) AS "Active Transactions"`

// DatabaseTransactionLogQueryAzureSQL returns the Azure SQL Database specific transaction log query
// Azure SQL Database has limited access to performance counters, so we use alternate queries
// DatabaseTransactionLogQueryAzureDB returns the SQL query for transaction log performance metrics on Azure SQL Database
// Some performance counters may not be available in Azure SQL Database
// Using individual subqueries with proper null handling for Azure SQL Database compatibility
const DatabaseTransactionLogQueryAzureDB = `SELECT
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Database Replica%' AND counter_name = 'Log Flushes/sec' AND instance_name = DB_NAME()), 
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Flushes/sec' AND instance_name = DB_NAME()), 0
    ) AS "Log Flushes/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Database Replica%' AND counter_name = 'Log Bytes Flushed/sec' AND instance_name = DB_NAME()), 
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Bytes Flushed/sec' AND instance_name = DB_NAME()), 0
    ) AS "Log Bytes Flushed/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Database Replica%' AND counter_name = 'Log Flush Waits/sec' AND instance_name = DB_NAME()), 
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Flush Waits/sec' AND instance_name = DB_NAME()), 0
    ) AS "Flush Waits/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Database Replica%' AND counter_name = 'Active Transactions' AND instance_name = DB_NAME()), 
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Active Transactions' AND instance_name = DB_NAME()), 0
    ) AS "Active Transactions"` // DatabaseTransactionLogQueryAzureMI returns the Azure SQL Managed Instance specific transaction log query
// Azure SQL Managed Instance supports full performance counter access like standard SQL Server
const DatabaseTransactionLogQueryAzureMI = `SELECT
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Flushes/sec' AND instance_name = '_Total'), 0
    ) AS "Log Flushes/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Bytes Flushed/sec' AND instance_name = '_Total'), 0
    ) AS "Log Bytes Flushed/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Log Flush Waits/sec' AND instance_name = '_Total'), 0
    ) AS "Flush Waits/sec",
    COALESCE(
        (SELECT cntr_value FROM sys.dm_os_performance_counters WITH (NOLOCK)
         WHERE object_name LIKE '%Databases%' AND counter_name = 'Active Transactions' AND instance_name = '_Total'), 0
    ) AS "Active Transactions"`

// DatabaseLogSpaceUsageQuery returns the SQL query for database log space usage metrics
// This query retrieves the used log space in MB from sys.dm_db_log_space_usage
// Available on Standard SQL Server with appropriate database context handling
const DatabaseLogSpaceUsageQuery = `SELECT
    used_log_space_in_bytes / 1024 / 1024.0 AS used_log_space_mb
FROM
    sys.dm_db_log_space_usage`

// DatabaseLogSpaceUsageQueryAzureSQL returns the Azure SQL Database specific log space usage query
// Uses the same sys.dm_db_log_space_usage DMV which is available in Azure SQL Database
const DatabaseLogSpaceUsageQueryAzureSQL = `SELECT
    used_log_space_in_bytes / 1024 / 1024.0 AS used_log_space_mb
FROM
    sys.dm_db_log_space_usage`

// DatabaseLogSpaceUsageQueryAzureMI returns the Azure SQL Managed Instance specific log space usage query
// Uses the same sys.dm_db_log_space_usage DMV which is available in Azure SQL Managed Instance
const DatabaseLogSpaceUsageQueryAzureMI = `SELECT
    used_log_space_in_bytes / 1024 / 1024.0 AS used_log_space_mb
FROM
    sys.dm_db_log_space_usage`
