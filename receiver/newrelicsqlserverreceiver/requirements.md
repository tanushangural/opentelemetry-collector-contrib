# SQL Server Query Performance Monitoring - Requirements and Implementation

## Overview
This document defines the requirements for scraping OpenTelemetry metrics from SQL Server databases with a focus on query performance monitoring, active running queries, wait events, blocking sessions, and execution plans.

## Configuration Parameters

### query_monitoring_fetch_interval
**Location**: `config.yaml:86`
**Type**: `int` (seconds)
**Default**: `15`
**Description**: Lookback window for slow queries in seconds. Determines how far back in time to fetch query statistics from `sys.dm_exec_query_stats`.

**Implementation Impact**:
- Queries executed within the last `query_monitoring_fetch_interval` seconds are included in slow query metrics
- **Critical Filter**: `qs.last_execution_time >= DATEADD(SECOND, -@IntervalSeconds, GETUTCDATE())`
- Should match or exceed `collection_interval` (60s) to ensure queries aren't missed between collection cycles
- Lower values = faster queries, but may miss slower-running queries
- Higher values = captures more historical data, but increases query execution time

**Recommendation**: Set to 60 seconds (matching `collection_interval`) to ensure complete coverage of queries executed between scrape cycles.

**Related Configuration**:
- `collection_interval: 60s` - How often metrics are collected
- `query_monitoring_response_time_threshold: 1` - Minimum elapsed time (ms) for queries to be monitored
- `query_monitoring_count_threshold: 20` - Minimum execution count for queries to be monitored

---

## Implementation Requirements

### 1. Normalized Queries Scraping
- Scrap all the grouped queries from dm_query_stats. See if below query is optimised or else optmise it if not already.

DECLARE @IntervalSeconds INT = %d; 		-- Define the interval in seconds
DECLARE @TopN INT = %d; 				-- Number of top queries to retrieve
DECLARE @ElapsedTimeThreshold INT = %d;  -- Elapsed time threshold in milliseconds
DECLARE @TextTruncateLimit INT = %d; 	-- Truncate limit for query_text
				
WITH StatementDetails AS (
	SELECT
		qs.plan_handle,
		qs.sql_handle,
		-- Extract the query text for the specific statement within the batch
		LEFT(SUBSTRING(
			qt.text,
			(qs.statement_start_offset / 2) + 1,
			(
				CASE
					qs.statement_end_offset
					WHEN -1 THEN DATALENGTH(qt.text)
					ELSE qs.statement_end_offset
				END - qs.statement_start_offset
			) / 2 + 1
		), @TextTruncateLimit) AS query_text, 
		qs.query_hash AS query_id,
		qs.last_execution_time,
		qs.execution_count,
        -- Historical average metrics (reflecting all runs since caching)
		(qs.total_worker_time / qs.execution_count) / 1000.0 AS avg_cpu_time_ms,
		(qs.total_elapsed_time / qs.execution_count) / 1000.0 AS avg_elapsed_time_ms,
		(qs.total_logical_reads / qs.execution_count) AS avg_disk_reads,
		(qs.total_logical_writes / qs.execution_count) AS avg_disk_writes,
		-- Average rows processed (returned by query)
		(qs.total_rows / qs.execution_count) AS avg_rows_processed,
		-- Determine statement type (SELECT, INSERT, etc.)
		CASE
			WHEN UPPER(LTRIM(SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6))) LIKE 'SELECT' THEN 'SELECT'
			WHEN UPPER(LTRIM(SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6))) LIKE 'INSERT' THEN 'INSERT'
			WHEN UPPER(LTRIM(SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6))) LIKE 'UPDATE' THEN 'UPDATE'
			WHEN UPPER(LTRIM(SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6))) LIKE 'DELETE' THEN 'DELETE'
			ELSE 'OTHER'
		END AS statement_type,
		CONVERT(INT, pa.value) AS database_id,
		qt.objectid
	FROM
		sys.dm_exec_query_stats qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
		JOIN sys.dm_exec_cached_plans cp ON qs.plan_handle = cp.plan_handle
		CROSS APPLY sys.dm_exec_plan_attributes(cp.plan_handle) AS pa
	WHERE
		-- *** KEY FILTER: Only plans that ran in the last @IntervalSeconds (e.g., 15) ***
		qs.last_execution_time >= DATEADD(SECOND, -@IntervalSeconds, GETUTCDATE())
		AND qs.execution_count > 0
		AND pa.attribute = 'dbid'
		AND DB_NAME(CONVERT(INT, pa.value)) NOT IN ('master', 'model', 'msdb', 'tempdb')
		AND qt.text NOT LIKE '%%sys.%%'
		AND qt.text NOT LIKE '%%INFORMATION_SCHEMA%%'
		AND qt.text NOT LIKE '%%schema_name()%%'
		AND qt.text IS NOT NULL
		AND LTRIM(RTRIM(qt.text)) <> ''
		AND EXISTS (
			SELECT 1
			FROM sys.databases d
			WHERE d.database_id = CONVERT(INT, pa.value)
		)
)
-- Select the raw, non-aggregated statement data.
SELECT TOP (@TopN)
    s.query_id,
	s.plan_handle,
    s.query_text,
    DB_NAME(s.database_id) AS database_name,
    COALESCE(
        OBJECT_SCHEMA_NAME(s.objectid, s.database_id),
        'N/A'
    ) AS schema_name,
    FORMAT(
        s.last_execution_time AT TIME ZONE 'UTC',
        'yyyy-MM-ddTHH:mm:ssZ'
    ) AS last_execution_timestamp,
    s.execution_count,
    s.avg_cpu_time_ms,
    s.avg_elapsed_time_ms,
    s.avg_disk_reads,
    s.avg_disk_writes,
    s.avg_rows_processed,
    s.statement_type,
    FORMAT(
        SYSDATETIMEOFFSET() AT TIME ZONE 'UTC',
        'yyyy-MM-ddTHH:mm:ssZ'
    ) AS collection_timestamp
FROM
    StatementDetails s
WHERE
	    s.avg_elapsed_time_ms > @ElapsedTimeThreshold
ORDER BY
    s.last_execution_time DESC;

- Scrap the active running queries from dm_query_requests and then to map active running queries under a query_has you can map it first anonymising the query text present in dm_query_stats and then create hash of anonymised query and map it with the same anonymising the query text present in dm_query_requests and then create hash of anonymised query. So that we can get the active running queries for the anonymised query as query_hash. and scrap the metrics under active running queries.

ActiveRunning with Wait and Blocking details:
DECLARE @Limit INT = 1000; -- Set the maximum number of rows to return
DECLARE @TextTruncateLimit INT = 4096; -- Set the maximum length for query text

SELECT TOP (@Limit)
    -- A. CURRENT SESSION DETAILS
    r_wait.session_id AS Current_Session_ID,
    DB_NAME(r_wait.database_id) AS DatabaseName,
    s_wait.login_name AS LoginName,
    s_wait.host_name AS HostName,
    r_wait.command AS RequestCommand,
    
    -- B. WAIT DETAILS (Always present for sessions in dm_exec_requests)
    r_wait.wait_type AS WaitType,
    r_wait.wait_time / 1000.0 AS WaitTime_s,
    r_wait.wait_resource AS WaitResource,
    
    -- C. PERFORMANCE/EXECUTION METRICS
    r_wait.cpu_time AS CPUTime_ms,
    r_wait.total_elapsed_time AS TotalElapsedTime_ms,
    r_wait.start_time AS RequestStartTime,
    SYSDATETIME() AS CollectionTimestamp,
    
    -- D. BLOCKING DETAILS (Show 'N/A' if not blocked)
    CASE 
        WHEN r_wait.blocking_session_id = 0 THEN 'N/A' 
        ELSE CAST(r_wait.blocking_session_id AS NVARCHAR(10)) 
    END AS Blocking_Session_ID,
    
    ISNULL(s_blocker.login_name, 'N/A') AS Blocker_LoginName,
    ISNULL(s_blocker.host_name, 'N/A') AS Blocker_HostName,
   
    
    -- E. QUERY TEXT - Current Session (Blocked or Waiting)
    SUBSTRING(st_wait.text, (r_wait.statement_start_offset / 2) + 1,
        ((CASE r_wait.statement_end_offset
            WHEN -1 THEN DATALENGTH(st_wait.text)
            ELSE r_wait.statement_end_offset
        END - r_wait.statement_start_offset) / 2) + 1
    ) AS QueryStatementText,
    
    -- F. QUERY TEXT - Blocking Session (Show 'N/A' if not blocked)
    CASE
        WHEN r_wait.blocking_session_id = 0 THEN 'N/A'
        -- If blocking, use input_buffer if the blocker is idle (r_blocker.command is NULL), otherwise use the active request text
        WHEN r_blocker.command IS NULL THEN LEFT(ib_blocker.event_info, @TextTruncateLimit)
        ELSE SUBSTRING(st_blocker.text, (r_blocker.statement_start_offset / 2) + 1,
            ((CASE r_blocker.statement_end_offset
                WHEN -1 THEN DATALENGTH(st_blocker.text)
                ELSE r_blocker.statement_end_offset
            END - r_blocker.statement_start_offset) / 2) + 1
        )
    END AS Blocking_QueryStatementText
    
FROM
    sys.dm_exec_requests AS r_wait
INNER JOIN
    sys.dm_exec_sessions AS s_wait ON s_wait.session_id = r_wait.session_id
CROSS APPLY
    sys.dm_exec_sql_text(r_wait.sql_handle) AS st_wait
LEFT JOIN
    sys.dm_exec_requests AS r_blocker ON r_wait.blocking_session_id = r_blocker.session_id
LEFT JOIN
    sys.dm_exec_sessions AS s_blocker ON r_wait.blocking_session_id = s_blocker.session_id
OUTER APPLY
    sys.dm_exec_sql_text(r_blocker.sql_handle) AS st_blocker
OUTER APPLY
    sys.dm_exec_input_buffer(r_wait.blocking_session_id, NULL) AS ib_blocker
WHERE
    r_wait.session_id > 50
    AND r_wait.database_id > 4
    AND r_wait.wait_type IS NOT NULL -- Exclude certain system-only states

ORDER BY
    r_wait.wait_time DESC;

- Scrap the wait events(wait time, wait type and other metrics related to wait events) by session id of active running queries and then its associated blocked objects and blocking sessions.

- scrap the query execution plan of active running query by passing the query_hash and plan_handle, where plan handle can be used from the normalised query

ExecutionPlan with Query_Hash:
    DECLARE @TargetQueryHash BINARY(8) = %s;

    SELECT 
        qs.query_hash AS query_id,
        qs.plan_handle,
        qs.query_plan_hash AS query_plan_id,
        CAST(qp.query_plan AS NVARCHAR(MAX)) AS execution_plan_xml,
        qs.total_worker_time / 1000.0 AS total_cpu_ms,
        qs.total_elapsed_time / 1000.0 AS total_elapsed_ms,
        DATEDIFF(SECOND, '1970-01-01 00:00:00', qs.creation_time) AS creation_time,
        DATEDIFF(SECOND, '1970-01-01 00:00:00', qs.last_execution_time) AS last_execution_time,
        st.text AS sql_text
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
    WHERE qs.query_hash = @TargetQueryHash
        AND qp.query_plan IS NOT NULL;

All scrapped data should be ingested to NRDB which is already there in place for ingestion logic.

Cross above technical requirement as per the below user flow -

User comes on the landing page, and see the data coming from NRDB using NRQL -
- See the list of all the normalised queries with timestamp, query id, database, query, lock time, calls, rows examined List of normalised queries - We will be showing the one row per query_hash or normalised query.

If user clicks on the normalised query -
We will show the normalised query selected at the top.
2. We will show the chart with active running queries where each bar will represent the active running query.
3. We click on any bar, will show each active running queries as table.
       3.1 Click on query, We will show the wait time analysis for query hash + session_id contains -
           - wait type, wait time in ms
           - blocked objects
           - blocking sessions
	    - Execution plan for the session id + query_hash

---

## Implementation Status & Alignment Verification

### ✅ IMPLEMENTED: Normalized Queries from dm_exec_query_stats
**Files**:
- `queries/query_performance_monitoring_metrics.go:330-422` (SlowQuery)
- `scrapers/scraper_query_performance_montoring_metrics.go:158-244` (ScrapeSlowQueryMetrics)
- `models/query_performance_monitoring_metrics.go:128-145` (SlowQuery model)

**Status**: ✅ COMPLETE
- Fetches queries from `sys.dm_exec_query_stats` within the last `@IntervalSeconds`
- Includes: query_id (query_hash), query_text (anonymized), database, schema, timestamps (ISO 8601), execution stats
- Filters: Time window, elapsed time threshold, excludes system databases/queries
- **Alignment**: Fully aligned with requirements

**Key Metrics Ingested**:
- `query_id` (BINARY(8) - SQL Server query_hash in hex format 0x1A2B3C4D)
- `query_signature` (computed SHA256 hash for cross-instance correlation)
- `last_execution_timestamp` (ISO 8601 format: `2025-11-21T18:09:27Z`)
- `execution_count`, `avg_cpu_time_ms`, `avg_elapsed_time_ms`
- `avg_disk_reads`, `avg_disk_writes`, `avg_rows_processed`
- `statement_type` (SELECT, INSERT, UPDATE, DELETE, OTHER)
- `query_text` (anonymized - literals replaced with placeholders)

### ✅ IMPLEMENTED: Active Running Queries from dm_exec_requests
**Files**:
- `queries/query_performance_monitoring_metrics.go:424-543` (ActiveRunningQuery)
- `scrapers/scraper_query_performance_montoring_metrics_active.go:46-144` (ScrapeActiveRunningQueryMetrics)
- `models/query_performance_monitoring_metrics.go:147-168` (ActiveRunningQuery model)

**Status**: ✅ COMPLETE
- Fetches currently executing queries from `sys.dm_exec_requests`
- Joins with `sys.dm_exec_sql_text` for query text
- Joins with `sys.dm_exec_query_stats` to get `query_hash` for correlation
- Includes wait information: `wait_type`, `wait_time`, `wait_resource`
- Includes blocking information: `blocking_session_id`
- **Alignment**: Fully aligned with requirements

**Key Metrics Ingested**:
- `session_id` - Unique session identifier
- `query_id` (query_hash from dm_exec_query_stats for correlation)
- `query_signature` (SHA256 computed hash)
- `database_name`, `login_name`, `host_name`, `program_name`
- `request_start_time` (ISO 8601), `request_status`
- `wait_type`, `wait_time_s`, `wait_resource`
- `blocking_session_id` - ID of session blocking this query
- `cpu_time_ms`, `total_elapsed_time_ms`
- `query_text` (anonymized)
- `execution_plan_xml` - Fetched using dual-method approach

### ✅ IMPLEMENTED: Execution Plan Fetching with Dual-Method Approach
**Files**:
- `scrapers/scraper_query_performance_montoring_metrics_active.go:472-544` (fetchExecutionPlanForActiveQuery)
- `queries/query_performance_monitoring_metrics.go:545-595` (ActiveQueryExecutionPlanQuery, QueryExecutionPlan)

**Status**: ✅ COMPLETE - Dual-method fallback implemented
- **Method 1** (Preferred): Uses `plan_handle` from active query → `sys.dm_exec_query_plan(plan_handle)`
- **Method 2** (Fallback): Uses `query_hash` from normalized queries → Query from `sys.dm_exec_query_stats`
- Execution plan XML added as attribute to metrics and logs
- **Alignment**: Fully aligned with requirements

### ✅ IMPLEMENTED: ISO 8601 Timestamp Format
**Files**:
- `queries/query_performance_monitoring_metrics.go:581-582` (creation_time, last_execution_time)
- `queries/query_performance_monitoring_metrics.go:86-91` (collection_timestamp)

**Status**: ✅ COMPLETE
- All timestamps converted from Unix epoch to ISO 8601 format
- Format: `FORMAT(datetime AT TIME ZONE 'UTC', 'yyyy-MM-ddTHH:mm:ssZ')`
- Example: `2025-11-21T18:09:27Z`
- **Alignment**: Fully aligned with requirements

### ⚠️ PARTIAL: Wait Events and Blocking Analysis
**Current Implementation**:
- Active query includes: `wait_type`, `wait_time`, `wait_resource`, `blocking_session_id`
- BlockingSession metrics scraper exists: `scrapers/scraper_query_performance_montoring_metrics.go:246-368`

**Status**: ⚠️ NEEDS ENHANCEMENT
- Current implementation captures basic blocking information
- **Missing**: Detailed blocked objects (resource_type, object_name, index_name from sys.dm_tran_locks)
- **Missing**: Blocking session details (blocker login_name, host_name, query text)
- **Recommendation**: Enhance ActiveRunningQuery to include detailed blocking/locked resource information

**Required Enhancement**:
```sql
-- Add JOIN to sys.dm_tran_locks for blocked objects
LEFT JOIN sys.dm_tran_locks tl ON r.session_id = tl.request_session_id
-- Add JOIN to sys.partitions for object details
LEFT JOIN sys.partitions p ON tl.resource_associated_entity_id = p.hobt_id
-- Add JOIN to sys.objects for object names
LEFT JOIN sys.objects o ON p.object_id = o.object_id
```

### ❌ REMOVED: Query Store Dependencies
**Status**: ✅ COMPLETE
- Query Store views (`sys.query_store_*`) are NOT used in the implementation
- Implementation relies solely on DMVs: `dm_exec_query_stats`, `dm_exec_requests`, `dm_exec_query_plan`
- **Alignment**: Fully aligned with requirements (Query Store has performance impact)

---

## Test Scenarios Using dmv-populator-repo

### Overview
The `dmv-populator-repo` is a Go-based load generator that populates SQL Server DMVs with realistic query workloads for testing the OpenTelemetry receiver.

**Location**: `/Users/pkulkarni/workspace/mssql-otel/opentelemetry-collector-contrib/dmv-populator-repo/`

### Configuration
**SQL Server Connection** (from `config.yaml:69-74`):
```yaml
hostname: "74.225.3.34"
port: "1433"
username: "sa"
password: "AbAnTaPassword@123"
```

**Database**: `AdventureWorks2022`

**Test Parameters** (from `main.go:28-33`):
- Target Query Count: 350,000 queries
- Concurrent Workers: 52
- Run Duration: 25 minutes
- Progress Reporting: Every 5,000 queries

### Running the Test Scenario

#### Step 1: Configure dmv-populator-repo
Update `dmv-populator-repo/main.go:19-25`:
```go
const (
    DB_SERVER   = "74.225.3.34"
    DB_PORT     = "1433"
    DB_USER     = "sa"
    DB_PASSWORD = "AbAnTaPassword@123"
    DB_NAME     = "AdventureWorks2022"
)
```

#### Step 2: Run the DMV Populator
```bash
cd dmv-populator-repo
go mod download
go run main.go
```

**Expected Output**:
```
🚀 Starting DMV Populator for AdventureWorks2022
📋 Target: Generate 350,000+ diverse queries to populate SQL Server DMVs
⚡ Configuration: 52 workers, 25 minutes runtime
================================================================================
🔗 Connecting to SQL Server 74.225.3.34/AdventureWorks2022...
✅ Successfully connected to AdventureWorks2022!
📋 Configuring SQL Server to prevent query anonymization...
✅ SQL Server configured to prevent query anonymization
🔍 Initial DMV state:
✅ Query Stats Count: 1234
✅ Recent Queries (last hour): 567
✅ Unique Query Hashes: 890
✅ Execution Plans Count: 1234
🏭 Starting 52 concurrent workers...
```

#### Step 3: Run OpenTelemetry Collector
In a separate terminal, start the collector:
```bash
cd /Users/pkulkarni/workspace/mssql-otel/opentelemetry-collector-contrib
make clean && make otelcontribcol
./bin/otelcontribcol --config receiver/newrelicsqlserverreceiver/testdata/config.yaml
```

#### Step 4: Monitor Metrics Ingestion
Watch for metrics in the collector debug output:
```
2025-11-21T18:09:27Z info MetricsExporter {"#metrics": 2500}
  Metric #0
    -> Name: sqlserver.query.slow.avg_elapsed_time_ms
    -> Attributes:
      -> query_id: 0x1A2B3C4D5E6F7890
      -> query_signature: 0x1a2b3c4d5e6f7890abcdef1234567890abcdef1234567890abcdef1234567890
      -> database_name: AdventureWorks2022
      -> last_execution_timestamp: 2025-11-21T18:09:27Z
      -> query_text: SELECT TOP @p1 @p2, @p3 FROM Sales.SalesOrderHeader WHERE @p2 IS NOT NULL
```

#### Step 5: Verify DMV Population
After test completes, verify DMV population:
```sql
-- Check query stats count
SELECT COUNT(*) as query_count FROM sys.dm_exec_query_stats;
-- Expected: 350,000+ rows

-- Check active queries during load
SELECT
    session_id,
    wait_type,
    wait_time,
    blocking_session_id,
    DB_NAME(database_id) as database_name
FROM sys.dm_exec_requests
WHERE session_id > 50 AND database_id > 4;
```

### Test Scenarios by User Flow

#### Scenario 1: Landing Page - Normalized Queries List
**Goal**: Verify that all normalized queries appear in NRDB with correct attributes

**Test Query** (NRQL):
```nrql
SELECT
    query_id,
    query_signature,
    database_name,
    last_execution_timestamp,
    avg_elapsed_time_ms,
    execution_count,
    avg_rows_processed
FROM Metric
WHERE metricName LIKE 'sqlserver.query.slow%'
SINCE 1 hour ago
FACET query_id
LIMIT 1000
```

**Expected Result**:
- 350,000+ unique `query_id` values (query_hash)
- Each row represents one normalized query
- Attributes: database_name, query_text (anonymized), timestamps in ISO 8601 format
- Metrics: avg_cpu_time_ms, avg_elapsed_time_ms, execution_count

#### Scenario 2: Drill-Down - Active Running Queries for a Normalized Query
**Goal**: Click on a normalized query → See chart of active running queries with that query_hash

**Test Query** (NRQL):
```nrql
SELECT
    session_id,
    request_start_time,
    wait_type,
    wait_time_s,
    blocking_session_id,
    total_elapsed_time_ms
FROM Metric
WHERE metricName LIKE 'sqlserver.query.active%'
  AND query_id = '0x1A2B3C4D5E6F7890'  -- Selected normalized query
SINCE 1 hour ago
TIMESERIES AUTO
```

**Expected Result**:
- Multiple active query executions for the selected query_hash
- Chart shows bars for each active query execution over time
- Each bar contains: session_id, wait_type, wait_time, blocking_session_id

#### Scenario 3: Deep Dive - Wait Time Analysis + Execution Plan
**Goal**: Click on an active query bar → Show detailed wait analysis + execution plan

**Test Query** (NRQL):
```nrql
SELECT
    wait_type,
    wait_time_s,
    wait_resource,
    blocking_session_id,
    execution_plan_xml,
    cpu_time_ms,
    query_text
FROM Metric
WHERE metricName = 'sqlserver.query.active.total_elapsed_time_ms'
  AND query_id = '0x1A2B3C4D5E6F7890'
  AND session_id = 123
SINCE 1 hour ago
LIMIT 1
```

**Expected Result**:
- Detailed wait information: wait_type (e.g., LCK_M_X, PAGEIOLATCH_SH), wait_time_s
- Blocking information: blocking_session_id (if blocked)
- Execution plan: execution_plan_xml (full XML showplan)
- Query text: anonymized SQL statement

### Query Types Generated by dmv-populator-repo
The test generator creates diverse query patterns to simulate real workloads:

1. **Simple SELECT** queries with filters
2. **INNER JOIN** and **LEFT JOIN** queries across tables
3. **GROUP BY** aggregate queries
4. **Window functions** (ROW_NUMBER, RANK)
5. **Subqueries** (IN, EXISTS)
6. **Common Table Expressions (CTEs)**
7. **Queries with ORDER BY** and **DISTINCT**

**Tables Used** (AdventureWorks2022):
- Sales: SalesOrderHeader, SalesOrderDetail, Customer, SalesPerson, Store
- Production: Product, ProductCategory, ProductSubcategory, WorkOrder
- Person: Person, Address, StateProvince
- HumanResources: Employee, Department
- Purchasing: PurchaseOrderHeader, PurchaseOrderDetail, Vendor

---

## Key Metrics to Ingest to NRDB

### 1. Normalized Queries (SlowQuery) - One row per query_hash
**Metric Prefix**: `sqlserver.query.slow.*`

| Attribute/Metric | Type | Source | Format/Example |
|-----------------|------|--------|----------------|
| query_id | Attribute | sys.dm_exec_query_stats.query_hash | 0x1A2B3C4D5E6F7890 |
| query_signature | Attribute | Computed SHA256 | 0x1a2b3c4d... |
| database_name | Attribute | DB_NAME(database_id) | AdventureWorks2022 |
| schema_name | Attribute | OBJECT_SCHEMA_NAME | Sales |
| query_text | Attribute | Anonymized SQL | SELECT TOP @p1 @p2 FROM... |
| last_execution_timestamp | Attribute | FORMAT(UTC) | 2025-11-21T18:09:27Z |
| collection_timestamp | Attribute | FORMAT(SYSDATETIMEOFFSET()) | 2025-11-21T18:09:30Z |
| statement_type | Attribute | Parsed from query | SELECT, INSERT, UPDATE |
| execution_count | Metric (Gauge) | qs.execution_count | 1234 |
| avg_cpu_time_ms | Metric (Gauge) | qs.total_worker_time / count / 1000 | 45.67 |
| avg_elapsed_time_ms | Metric (Gauge) | qs.total_elapsed_time / count / 1000 | 123.45 |
| avg_disk_reads | Metric (Gauge) | qs.total_logical_reads / count | 567 |
| avg_disk_writes | Metric (Gauge) | qs.total_logical_writes / count | 23 |
| avg_rows_processed | Metric (Gauge) | qs.total_rows / count | 100 |

### 2. Active Running Queries (ActiveQuery) - Multiple rows per query_hash (one per session_id)
**Metric Prefix**: `sqlserver.query.active.*`

| Attribute/Metric | Type | Source | Format/Example |
|-----------------|------|--------|----------------|
| session_id | Attribute | dm_exec_requests.session_id | 123 |
| query_id | Attribute | dm_exec_query_stats.query_hash | 0x1A2B3C4D5E6F7890 |
| query_signature | Attribute | Computed SHA256 | 0x1a2b3c4d... |
| database_name | Attribute | DB_NAME(database_id) | AdventureWorks2022 |
| login_name | Attribute | dm_exec_sessions.login_name | sa |
| host_name | Attribute | dm_exec_sessions.host_name | APP-SERVER-01 |
| program_name | Attribute | dm_exec_sessions.program_name | .Net SqlClient |
| request_status | Attribute | dm_exec_requests.status | running, suspended |
| request_command | Attribute | dm_exec_requests.command | SELECT, INSERT |
| request_start_time | Attribute | FORMAT(UTC) | 2025-11-21T18:09:25Z |
| wait_type | Attribute | dm_exec_requests.wait_type | LCK_M_X, PAGEIOLATCH_SH |
| wait_time_s | Metric (Gauge) | wait_time / 1000.0 | 2.5 |
| wait_resource | Attribute | dm_exec_requests.wait_resource | KEY: 5:1234 (abcd1234) |
| blocking_session_id | Attribute | dm_exec_requests.blocking_session_id | 456 (or NULL if not blocked) |
| cpu_time_ms | Metric (Gauge) | dm_exec_requests.cpu_time | 1234 |
| total_elapsed_time_ms | Metric (Gauge) | dm_exec_requests.total_elapsed_time | 5678 |
| query_text | Attribute | Anonymized SQL | SELECT @p1 FROM @p2 WHERE... |
| execution_plan_xml | Attribute | dm_exec_query_plan XML | <ShowPlanXML>...</ShowPlanXML> |

### 3. Execution Plans (as Logs or Attributes)
**Log Signal**: `plog.Logs` with severity INFO

| Attribute | Type | Source | Format/Example |
|----------|------|--------|----------------|
| query_id | Attribute | query_hash | 0x1A2B3C4D5E6F7890 |
| query_plan_id | Attribute | query_plan_hash | 0xABCDEF1234567890 |
| execution_plan_xml | Body | dm_exec_query_plan XML | <ShowPlanXML xmlns="...">... |
| plan_handle | Attribute | plan_handle hex | 0x05000600... |
| creation_time | Attribute | FORMAT(UTC) | 2025-11-21T17:00:00Z |
| last_execution_time | Attribute | FORMAT(UTC) | 2025-11-21T18:09:27Z |

---

## User Flow to Implementation Mapping

### Flow 1: Landing Page - List of Normalized Queries
**User Action**: Opens monitoring dashboard

**Backend Query** (NRQL):
```nrql
FROM Metric
SELECT latest(avg_elapsed_time_ms),
       latest(execution_count),
       latest(avg_rows_processed),
       latest(last_execution_timestamp)
WHERE metricName LIKE 'sqlserver.query.slow%'
FACET query_id, database_name, query_text
SINCE 1 hour ago
LIMIT 100
```

**Implementation Source**:
- Scraper: `scrapers/scraper_query_performance_montoring_metrics.go:158-244` (ScrapeSlowQueryMetrics)
- Query: `queries/query_performance_monitoring_metrics.go:330-422` (SlowQuery)
- Data Source: `sys.dm_exec_query_stats` filtered by last_execution_time >= DATEADD(SECOND, -@IntervalSeconds, GETUTCDATE())

**Data Flow**:
1. Every 60 seconds (collection_interval), scraper executes SlowQuery
2. Fetches queries executed in last 15 seconds (query_monitoring_fetch_interval)
3. Anonymizes query text, computes SHA256 signature
4. Emits OTLP metrics with query_id, database_name, avg metrics as attributes
5. Metrics sent to NRDB via otlphttp exporter

### Flow 2: Drill-Down - Chart of Active Running Queries for Selected Query
**User Action**: Clicks on a normalized query (query_id = 0x1A2B3C4D)

**Backend Query** (NRQL):
```nrql
FROM Metric
SELECT session_id,
       wait_type,
       wait_time_s,
       blocking_session_id,
       total_elapsed_time_ms
WHERE metricName LIKE 'sqlserver.query.active%'
  AND query_id = '0x1A2B3C4D5E6F7890'
SINCE 1 hour ago
TIMESERIES AUTO
```

**Implementation Source**:
- Scraper: `scrapers/scraper_query_performance_montoring_metrics_active.go:46-144` (ScrapeActiveRunningQueryMetrics)
- Query: `queries/query_performance_monitoring_metrics.go:424-543` (ActiveRunningQuery)
- Data Source: `sys.dm_exec_requests` INNER JOIN `sys.dm_exec_query_stats` on query_hash

**Data Flow**:
1. Every 60 seconds, scraper executes ActiveRunningQuery
2. Fetches currently executing queries (status = running/suspended)
3. Joins with dm_exec_query_stats to get query_hash for correlation
4. Fetches execution plan using dual-method approach (plan_handle or query_hash)
5. Emits OTLP metrics with session_id, query_id, wait info, blocking info as attributes
6. Metrics sent to NRDB via otlphttp exporter

### Flow 3: Deep Dive - Wait Time Analysis + Execution Plan
**User Action**: Clicks on a specific active query (session_id = 123, query_id = 0x1A2B3C4D)

**Backend Query** (NRQL):
```nrql
FROM Metric
SELECT wait_type,
       wait_time_s,
       wait_resource,
       blocking_session_id,
       execution_plan_xml,
       query_text
WHERE metricName = 'sqlserver.query.active.total_elapsed_time_ms'
  AND query_id = '0x1A2B3C4D5E6F7890'
  AND session_id = 123
SINCE 1 hour ago
LIMIT 1
```

**Implementation Source**:
- Scraper: Same as Flow 2 - `ScrapeActiveRunningQueryMetrics`
- Execution Plan: `scrapers/scraper_query_performance_montoring_metrics_active.go:472-544` (fetchExecutionPlanForActiveQuery)
- Data Source: `sys.dm_exec_requests` + `sys.dm_exec_query_plan(plan_handle)` OR `sys.dm_exec_query_stats WHERE query_hash = ?`

**Data Flow**:
1. Active query metrics already include: wait_type, wait_time_s, wait_resource, blocking_session_id
2. Execution plan fetched using:
   - **Method 1**: `sys.dm_exec_query_plan(plan_handle)` if plan_handle available
   - **Method 2**: Query from `sys.dm_exec_query_stats WHERE query_hash = ?` if plan_handle is NULL
3. Execution plan XML added as attribute: `execution_plan_xml`
4. All data sent as metric attributes to NRDB
5. Frontend parses execution_plan_xml and displays graphical execution plan

---

## Summary of Alignment

### ✅ FULLY ALIGNED
1. ✅ Normalized queries from `sys.dm_exec_query_stats` - COMPLETE
2. ✅ Active running queries from `sys.dm_exec_requests` - COMPLETE
3. ✅ Execution plan fetching with dual-method fallback - COMPLETE
4. ✅ ISO 8601 timestamp format for all timestamps - COMPLETE
5. ✅ Query text anonymization - COMPLETE
6. ✅ SHA256 query signature computation - COMPLETE
7. ✅ Correlation via query_hash (query_id) - COMPLETE
8. ✅ No Query Store dependencies - COMPLETE

### ⚠️ NEEDS ENHANCEMENT
1. ⚠️ Detailed blocked objects (resource_type, object_name from sys.dm_tran_locks)
2. ⚠️ Blocking session details (blocker login_name, host_name, query text)

### ✅ TEST INFRASTRUCTURE READY
1. ✅ dmv-populator-repo configured with credentials from config.yaml
2. ✅ AdventureWorks2022 database ready for load testing
3. ✅ 350,000+ diverse query patterns available for testing
4. ✅ All user flows (Landing Page → Drill-Down → Deep Dive) can be tested with generated data

---

## Next Steps

### For Testing
1. **Update dmv-populator-repo credentials**: Set DB_SERVER, DB_USER, DB_PASSWORD in `main.go:19-24`
2. **Run load generator**: `cd dmv-populator-repo && go run main.go`
3. **Start collector**: `./bin/otelcontribcol --config receiver/newrelicsqlserverreceiver/testdata/config.yaml`
4. **Verify metrics in NRDB**: Use NRQL queries from test scenarios above
5. **Test user flows**: Landing Page → Drill-Down → Deep Dive

### For Enhancement (Optional)
1. **Add detailed blocking information**: Enhance ActiveRunningQuery to include sys.dm_tran_locks JOIN
2. **Add blocker session details**: Include blocking session login_name, host_name, query text
3. **Add lock resource details**: Parse wait_resource to extract object_name, index_name

---

## Configuration Recommendations

### For Production Workloads
```yaml
# Increase lookback window to match collection interval
query_monitoring_fetch_interval: 60  # seconds (matches collection_interval)

# Adjust thresholds based on workload
query_monitoring_response_time_threshold: 1000  # 1 second for slower queries
query_monitoring_count_threshold: 20  # Minimum 20 executions

# Collection frequency
collection_interval: 60s  # Every minute

# Timeout for slow queries on busy systems
timeout: 300s
```

### For Testing/Development
```yaml
# Shorter lookback window for faster iteration
query_monitoring_fetch_interval: 15  # seconds

# Lower thresholds to capture more queries
query_monitoring_response_time_threshold: 1  # 1 ms

# More frequent collection for real-time feedback
collection_interval: 30s

# Debug logging
service:
  telemetry:
    logs:
      level: debug
```

## Queries needs to be aligned as per the implementation and should be aligned when any changes happen

### ✅ VERIFIED - These queries align with the actual implementation

### 1. Landing Page: Fetching Slow/Normalized Queries
**Purpose**: Show list of all normalized queries with key metrics

```nrql
now 
```

**Key Attributes Available**: `query_id`, `database_name`, `schema_name`, `statement_type`, `query_text` (on query_text metric only), `query_signature`

---

### 2. Query Details Page - After Selecting a Normalized Query

#### 2.1 Show Normalized Query Text
**Purpose**: Display the SQL text for the selected query_id

```nrql
SELECT query_id, query_text, query_signature, database_name, schema_name, statement_type
FROM Metric
WHERE metricName = 'sqlserver.slowquery.query_text'
  AND query_id = '0x1A2B3C4D5E6F7890'
SINCE 6 hours ago UNTIL now
LIMIT 1
```

**Note**: `query_text` is an **attribute** on the `sqlserver.slowquery.query_text` metric, not a metric value itself.

---

#### 2.2 Chart: Count of Active Running Queries for Selected Query
**Purpose**: Show how many active executions exist for the selected normalized query over time

```nrql
SELECT uniqueCount(request_id) as 'Active Query Count'
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_time_seconds'
  AND query_id = '0x1A2B3C4D5E6F7890'
FACET query_id
TIMESERIES AUTO
SINCE 6 hours ago UNTIL now
```

**Fix Applied**: Changed `FACET query_hash` to `FACET query_id` (implementation uses `query_id` as attribute name)

---

#### 2.3 On Clicking Bar: Show Active Running Queries Table
**Purpose**: List all active query executions with their metrics

```nrql
SELECT
    latest(query_text) as 'Query Text',
    latest(request_start_time) as 'Start Time',
    latest(sqlserver.activequery.wait_time_seconds) as 'Wait Time (s)',
    latest(sqlserver.activequery.elapsed_time_ms) as 'Elapsed Time (ms)',
    latest(sqlserver.activequery.cpu_time_ms) as 'CPU Time (ms)',
    latest(wait_type) as 'Wait Type',
    latest(request_status) as 'Status'
FROM Metric
WHERE metricName in (
    'sqlserver.activequery.wait_time_seconds',
    'sqlserver.activequery.elapsed_time_ms',
    'sqlserver.activequery.cpu_time_ms'
)
  AND query_id = '0x1A2B3C4D5E6F7890'
FACET query_id, request_id, session_id
SINCE 6 hours ago UNTIL now
```

**Fixes Applied**:
- Changed `query_hash` to `query_id` (3 occurrences)
- Removed `as query_id` alias (redundant since attribute is already named query_id)

---

#### 2.4 Selected Active Query: Full Details
**Purpose**: Show comprehensive details for a specific active query execution

```nrql
SELECT
    latest(database_name) as 'Database',
    latest(request_start_time) as 'Start Time',
    latest(login_name) as 'User',
    latest(host_name) as 'Client Host',
    latest(request_command) as 'Command Type',
    latest(request_status) as 'Status',
    latest(sqlserver.activequery.elapsed_time_ms) as 'Elapsed Time (ms)',
    latest(sqlserver.activequery.cpu_time_ms) as 'CPU Time (ms)',
    latest(sqlserver.activequery.logical_reads) as 'Logical Reads',
    latest(sqlserver.activequery.writes) as 'Writes',
    latest(sqlserver.activequery.row_count) as 'Row Count'
FROM Metric
WHERE metricName in (
    'sqlserver.activequery.wait_time_seconds',
    'sqlserver.activequery.elapsed_time_ms',
    'sqlserver.activequery.cpu_time_ms'
)
  AND query_id = '0x1A2B3C4D5E6F7890'
  AND session_id = 123
FACET query_id, request_id, session_id
SINCE 6 hours ago UNTIL now
```

**Fixes Applied**: Changed `query_hash` to `query_id`

**Additional Metrics Available**: `sqlserver.activequery.reads`, `sqlserver.activequery.granted_query_memory_pages`

---

#### 2.5 Wait Time Analysis for Active Query
**Purpose**: Show wait events for the selected active query

```nrql
FROM Metric
SELECT
    latest(sqlserver.activequery.wait_time_seconds) as 'Wait Time (s)',
    latest(query_id) as 'Query Id',
    latest(wait_type) as 'Wait Type',
    latest(wait_resource) as 'Wait Resource',
    latest(database_name) as 'Database',
    latest(login_name) as 'User',
    latest(query_text) as 'Query Text',
    latest(sqlserver.activequery.elapsed_time_ms) as 'Total Elapsed (ms)',
    latest(blocking_session_id) as 'Blocking Session'
WHERE metricName in (
    'sqlserver.activequery.wait_time_seconds',
    'sqlserver.activequery.elapsed_time_ms'
)
  AND query_id = '0x1A2B3C4D5E6F7890'
  AND session_id = 123
  AND wait_type IS NOT NULL
  AND wait_type != 'N/A'
FACET session_id, wait_type
SINCE 6 hours ago UNTIL now
```

**Fixes Applied**: Changed `latest(query_hash)` to `latest(query_id)`

**Additional Attributes Available**: `last_wait_type`, `blocker_login_name`, `blocker_host_name`

---

#### 2.6 Blocking Queries Analysis
**Purpose**: Show blocking and blocked session details

```nrql
SELECT
    latest(blocking_query_text) as 'Blocking Query',
    latest(blocking_spid) as 'Blocking SPID',
    latest(blocking_status) as 'Blocking Status',
    latest(blocking_query_hash) as 'Blocking Query Hash',
    latest(sqlserver.blocking_query.wait_time_seconds) as 'Blocking Wait Time (s)',
    latest(blocked_spid) as 'Blocked SPID',
    latest(blocked_status) as 'Blocked Status',
    latest(blocked_query_text) as 'Blocked Query',
    latest(blocked_query_hash) as 'Blocked Query Hash',
    latest(blocked_query_start_time) as 'Blocked Start Time',
    latest(sqlserver.blocked_query.wait_time_seconds) as 'Blocked Wait Time (s)',
    latest(wait_type) as 'Wait Type',
    latest(database_name) as 'Database',
    latest(command_type) as 'Command'
FROM Metric
WHERE metricName in (
    'sqlserver.blocking_query.wait_time_seconds',
    'sqlserver.blocked_query.wait_time_seconds'
)
SINCE 6 hours ago UNTIL now
```

**Verified**: ✅ Query is correct - metric names match implementation

**Available Metrics**:
- `sqlserver.blocking.spid` - Blocking session ID (gauge)
- `sqlserver.blocked.spid` - Blocked session ID (gauge)
- `sqlserver.blocking_query.wait_time_seconds` - Wait time with blocking context
- `sqlserver.blocked_query.wait_time_seconds` - Wait time with blocked context

---

#### 2.7 Execution Plan for Active Running Query
**Purpose**: Get execution plan details from Log events

```nrql
SELECT
    latest(execution_plan_xml) as 'Execution Plan XML',
    latest(query_id) as 'Query ID',
    latest(query_text) as 'Query Text',
    latest(plan_handle) as 'Plan Handle',
    latest(collection_timestamp) as 'Collection Time'
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_time_seconds'
  AND query_id = '0x1A2B3C4D5E6F7890'
  AND session_id = 123
  AND execution_plan_xml IS NOT NULL
SINCE 6 hours ago UNTIL now
LIMIT 1
```

**Alternative - If using Log-based execution plans** (for parsed operator details):
```nrql
SELECT
    latest(plan_handle),
    latest(node_id),
    latest(parent_node_id),
    latest(avg_elapsed_time_ms),
    latest(avg_row_size),
    latest(estimate_cpu),
    latest(estimate_io),
    latest(estimate_rows),
    latest(estimated_execution_mode),
    latest(estimated_operator_cost),
    latest(execution_count),
    latest(granted_memory_kb),
    latest(input_type),
    latest(logical_op),
    latest(no_join_predicate),
    latest(physical_op),
    latest(spill_occurred),
    latest(total_elapsed_time),
    latest(total_logical_reads),
    latest(total_logical_writes),
    latest(total_subtree_cost),
    latest(total_worker_time),
    latest(last_execution_time),
    latest(collection_timestamp)
FROM Log
WHERE event.name like '%sqlserver.execution_plan_operator%'
  AND query_id = '0x1A2B3C4D5E6F7890'
FACET query_id
LIMIT 30
SINCE 6 hours ago UNTIL now
```

**Note**: Execution plan XML is available as an **attribute** (`execution_plan_xml`) on active query metrics. The parsed execution plan operators are available as Log events if execution plan logging is enabled.

---

## Summary of Fixes Applied

### ❌ Issues Fixed:
1. **query_hash → query_id**: Changed all occurrences of `query_hash` to `query_id` (implementation uses `query_id` as the attribute name)
2. **Query text access**: Clarified that `query_text` is an attribute on metrics, not retrieved via `latest()` alone
3. **Added missing attributes**: Included additional useful attributes like `query_signature`, `blocking_query_hash`, `blocked_query_hash`
4. **Execution plan location**: Clarified that `execution_plan_xml` is an attribute on active query metrics
5. **Filter improvements**: Added `query_id` filters to narrow down results

### ✅ Verified Correct:
1. All metric names match implementation exactly
2. Blocking queries use correct metric names: `sqlserver.blocking_query.wait_time_seconds` and `sqlserver.blocked_query.wait_time_seconds`
3. Attribute names align with implementation (from scrapers)

---

## Implementation Attribute Reference

### SlowQuery Attributes (on ALL sqlserver.slowquery.* metrics):
- `query_id` (String, hex format: 0x1A2B3C4D5E6F7890)
- `database_name`
- `schema_name`
- `statement_type`

**Additional on sqlserver.slowquery.query_text metric only**:
- `query_text` (anonymized SQL)
- `query_signature` (SHA256 hash)

### ActiveQuery Attributes (on ALL sqlserver.activequery.* metrics):
- `query_id` (String, hex format)
- `query_signature` (SHA256 hash)
- `session_id` (Int64)
- `request_id` (Int64)
- `database_name`
- `login_name`
- `host_name`
- `request_command`
- `request_status`
- `wait_type`
- `wait_resource`
- `last_wait_type`
- `request_start_time` (ISO 8601)
- `collection_timestamp` (ISO 8601)
- `blocking_session_id`
- `blocker_login_name`
- `blocker_host_name`
- `query_text` (anonymized SQL)
- `blocking_query_text` (anonymized SQL)
- `blocking_query_hash` (SHA256)
- `execution_plan_xml` (full XML showplan)

### Blocking Session Attributes:
- `wait_type`
- `database_name`
- `command_type`
- `blocking_spid` (Int64)
- `blocking_status`
- `blocking_query_text` (anonymized)
- `blocking_query_hash` (SHA256)
- `blocked_spid` (Int64)
- `blocked_status`
- `blocked_query_text` (anonymized)
- `blocked_query_hash` (SHA256)
- `blocked_query_start_time` (ISO 8601)

### dm_exec_requests doc -
sys.dm_exec_requests (Transact-SQL)
Applies to:  SQL Server  Azure SQL Database  Azure SQL Managed Instance  Azure Synapse Analytics  Analytics Platform System (PDW)  SQL analytics endpoint in Microsoft Fabric  Warehouse in Microsoft Fabric  SQL database in Microsoft Fabric

Returns information about each request that is executing in SQL Server. For more information about requests, see the Thread and task architecture guide.

 Note

To call this from dedicated SQL pool in Azure Synapse Analytics or Analytics Platform System (PDW), see sys.dm_pdw_exec_requests. For serverless SQL pool or Microsoft Fabric, use sys.dm_exec_requests.

Column name	Data type	Description
session_id	smallint	ID of the session to which this request is related. Not nullable.
request_id	int	ID of the request. Unique in the context of the session. Not nullable.
start_time	datetime	Timestamp when the request arrived. Not nullable.
status	nvarchar(30)	Status of the request. Can be one of the following values:

background
rollback
running
runnable
sleeping
suspended

Not nullable.
command	nvarchar(32)	Identifies the current type of command that is being processed. Common command types include the following values:

SELECT
INSERT
UPDATE
DELETE
BACKUP LOG
BACKUP DATABASE
DBCC
FOR

The text of the request can be retrieved by using sys.dm_exec_sql_text with the corresponding sql_handle for the request. Internal system processes set the command based on the type of task they perform. Tasks can include the following values:

LOCK MONITOR
CHECKPOINTLAZY
WRITER

Not nullable.
sql_handle	varbinary(64)	A token that uniquely identifies the batch or stored procedure that the query is part of. Nullable.
statement_start_offset	int	Indicates, in bytes, beginning with 0, the starting position of the currently executing statement for the currently executing batch or persisted object. Can be used together with the sql_handle, the statement_end_offset, and the sys.dm_exec_sql_text dynamic management function to retrieve the currently executing statement for the request. Nullable.
statement_end_offset	int	Indicates, in bytes, starting with 0, the ending position of the currently executing statement for the currently executing batch or persisted object. Can be used together with the sql_handle, the statement_start_offset, and the sys.dm_exec_sql_text dynamic management function to retrieve the currently executing statement for the request. Nullable.
plan_handle	varbinary(64)	A token that uniquely identifies a query execution plan for a batch that is currently executing. Nullable.
database_id	smallint	ID of the database the request is executing against. Not nullable.

In Azure SQL Database, the values are unique within a single database or an elastic pool, but not within a logical server.
user_id	int	ID of the user who submitted the request. Not nullable.
connection_id	uniqueidentifier	ID of the connection on which the request arrived. Nullable.
blocking_session_id	smallint	ID of the session that is blocking the request. If this column is NULL or 0, the request isn't blocked, or the session information of the blocking session isn't available (or can't be identified). For more information, see Understand and resolve SQL Server blocking problems.

-2 = The blocking resource is owned by an orphaned distributed transaction.

-3 = The blocking resource is owned by a deferred recovery transaction.

-4 = session_id of the blocking latch owner couldn't be determined at this time because of internal latch state transitions.

-5 = session_id of the blocking latch owner couldn't be determined because it isn't tracked for this latch type (for example, for an SH latch).

By itself, blocking_session_id -5 doesn't indicate a performance problem. -5 is an indication that the session is waiting on an asynchronous action to complete. Before -5 was introduced, the same session would have shown blocking_session_id 0, even though it was still in a wait state.

Depending on workload, observing blocking_session_id = -5 might be a common occurrence.
wait_type	nvarchar(60)	If the request is currently blocked, this column returns the type of wait. Nullable.

When a request uses multiple tasks, for example because of intra-query parallelism, tasks can wait on different resources with different wait types. A task can be blocked while other tasks of the same request continue execution. To find the wait type and duration for each task and whether it is blocked, use sys.dm_os_waiting_tasks.

For information about types of waits, see sys.dm_os_wait_stats.
wait_time	int	If the request is currently blocked, this column returns the duration in milliseconds, of the current wait. Not nullable.
last_wait_type	nvarchar(60)	If this request has previously been blocked, this column returns the type of the last wait. Not nullable.
wait_resource	nvarchar(256)	If the request is currently blocked, this column returns the resource for which the request is currently waiting. Not nullable.
open_transaction_count	int	Number of transactions that are open for this request. Not nullable.
open_resultset_count	int	Number of result sets that are open for this request. Not nullable.
transaction_id	bigint	ID of the transaction in which this request executes. Not nullable.
context_info	varbinary(128)	CONTEXT_INFO value of the session. Nullable.
percent_complete	real	Percentage of work completed for the following commands:

ALTER INDEX REORGANIZE
AUTO_SHRINK option with ALTER DATABASE
BACKUP DATABASE
DBCC CHECKDB
DBCC CHECKFILEGROUP
DBCC CHECKTABLE
DBCC INDEXDEFRAG
DBCC SHRINKDATABASE
DBCC SHRINKFILE
RECOVERY
RESTORE DATABASE
ROLLBACK
TDE ENCRYPTION

Not nullable.
estimated_completion_time	bigint	Internal only. Not nullable.
cpu_time	int	CPU time in milliseconds that is used by the request. Not nullable.
total_elapsed_time	int	Total time elapsed in milliseconds since the request arrived. Not nullable.
scheduler_id	int	ID of the scheduler that is scheduling this request. Nullable.
task_address	varbinary(8)	Memory address allocated to the task that is associated with this request. Nullable.
reads	bigint	Number of reads performed by this request. Not nullable.
writes	bigint	Number of writes performed by this request. Not nullable.
logical_reads	bigint	Number of logical reads that have been performed by the request. Not nullable.
text_size	int	TEXTSIZE setting for this request. Not nullable.
language	nvarchar(128)	Language setting for the request. Nullable.
date_format	nvarchar(3)	DATEFORMAT setting for the request. Nullable.
date_first	smallint	DATEFIRST setting for the request. Not nullable.
quoted_identifier	bit	1 = QUOTED_IDENTIFIER is ON for the request. Otherwise, it's 0.

Not nullable.
arithabort	bit	1 = ARITHABORT setting is ON for the request. Otherwise, it's 0.

Not nullable.
ansi_null_dflt_on	bit	1 = ANSI_NULL_DFLT_ON setting is ON for the request. Otherwise, it's 0.

Not nullable.
ansi_defaults	bit	1 = ANSI_DEFAULTS setting is ON for the request. Otherwise, it's 0.

Not nullable.
ansi_warnings	bit	1 = ANSI_WARNINGS setting is ON for the request. Otherwise, it's 0.

Not nullable.
ansi_padding	bit	1 = ANSI_PADDING setting is ON for the request.

Otherwise, it's 0.

Not nullable.
ansi_nulls	bit	1 = ANSI_NULLS setting is ON for the request. Otherwise, it's 0.

Not nullable.
concat_null_yields_null	bit	1 = CONCAT_NULL_YIELDS_NULL setting is ON for the request. Otherwise, it's 0.

Not nullable.
transaction_isolation_level	smallint	Isolation level with which the transaction for this request is created. Not nullable.
0 = Unspecified
1 = ReadUncommitted
2 = ReadCommitted
3 = Repeatable
4 = Serializable
5 = Snapshot
lock_timeout	int	Lock time-out period in milliseconds for this request. Not nullable.
deadlock_priority	int	DEADLOCK_PRIORITY setting for the request. Not nullable.
row_count	bigint	Number of rows that have been returned to the client by this request. Not nullable.
prev_error	int	Last error that occurred during the execution of the request. Not nullable.
nest_level	int	Current nesting level of code that is executing on the request. Not nullable.
granted_query_memory	int	Number of pages allocated to the execution of a query on the request. Not nullable.
executing_managed_code	bit	Indicates whether a specific request is currently executing common language runtime objects, such as routines, types, and triggers. it's set for the full time a common language runtime object is on the stack, even while running Transact-SQL from within common language runtime. Not nullable.
group_id	int	ID of the workload group to which this query belongs. Not nullable.
query_hash	binary(8)	Binary hash value calculated on the query and used to identify queries with similar logic. You can use the query hash to determine the aggregate resource usage for queries that differ only by literal values.
query_plan_hash	binary(8)	Binary hash value calculated on the query execution plan and used to identify similar query execution plans. You can use query plan hash to find the cumulative cost of queries with similar execution plans.
statement_sql_handle	varbinary(64)	Applies to: SQL Server 2014 (12.x) and later.

sql_handle of the individual query.

This column is NULL if Query Store isn't enabled for the database.
statement_context_id	bigint	Applies to: SQL Server 2014 (12.x) and later.

The optional foreign key to sys.query_context_settings.

This column is NULL if Query Store isn't enabled for the database.
dop	int	Applies to: SQL Server 2016 (13.x) and later.

The degree of parallelism of the query.
parallel_worker_count	int	Applies to: SQL Server 2016 (13.x) and later.

The number of reserved parallel workers if this is a parallel query.
external_script_request_id	uniqueidentifier	Applies to: SQL Server 2016 (13.x) and later.

The external script request ID associated with the current request.
is_resumable	bit	Applies to: SQL Server 2017 (14.x) and later.

Indicates whether the request is a resumable index operation.
page_resource	binary(8)	Applies to: SQL Server 2019 (15.x)

An 8-byte hexadecimal representation of the page resource if the wait_resource column contains a page. For more information, see sys.fn_PageResCracker.
page_server_reads	bigint	Applies to: Azure SQL Database Hyperscale

Number of page server reads performed by this request. Not nullable.
dist_statement_id	uniqueidentifier	Applies to: SQL Server 2022 and later versions, Azure SQL Database, Azure SQL Managed Instance, Azure Synapse Analytics (serverless pools only), and Microsoft Fabric

Unique ID for the statement for the request submitted. Not nullable.
Remarks
To execute code that is outside SQL Server (for example, extended stored procedures and distributed queries), a thread has to execute outside the control of the non-preemptive scheduler. To do this, a worker switches to preemptive mode. Time values returned by this dynamic management view don't include time spent in preemptive mode.

When executing parallel requests in row mode, SQL Server assigns a worker thread to coordinate the worker threads responsible for completing tasks assigned to them. In this DMV, only the coordinator thread is visible for the request. The columns reads, writes, logical_reads, and row_count are not updated for the coordinator thread. The columns wait_type, wait_time, last_wait_type, wait_resource, and granted_query_memory are only updated for the coordinator thread. For more information, see the Thread and task architecture guide.

The wait_resource column contains similar information to resource_description in sys.dm_tran_locks but is formatted differently.

Permissions
If the user has VIEW SERVER STATE permission on the server, the user sees all executing sessions on the instance of SQL Server; otherwise, the user sees only the current session. VIEW SERVER STATE can't be granted in Azure SQL Database so sys.dm_exec_requests is always limited to the current connection.

In availability group scenarios, if the secondary replica is set to read-intent only, the connection to the secondary must specify its application intent in connection string parameters by adding applicationintent=readonly. Otherwise, the access check for sys.dm_exec_requests doesn't pass for databases in the availability group, even if VIEW SERVER STATE permission is present.

For SQL Server 2022 (16.x) and later versions, sys.dm_exec_requests requires VIEW SERVER PERFORMANCE STATE permission on the server.

Examples
A. Find the query text for a running batch
The following example queries sys.dm_exec_requests to find the interesting query and copy its sql_handle from the output.

SQL
SELECT * FROM sys.dm_exec_requests;
GO
Then, to obtain the statement text, use the copied sql_handle with system function sys.dm_exec_sql_text(sql_handle).

SQL
SELECT * FROM sys.dm_exec_sql_text(< copied sql_handle >);
GO
B. Show active requests
This following example shows all currently running queries in your SQL Server data warehouse, excluding your own session (@@SPID). It uses CROSS APPLY with sys.dm_exec_sql_text to retrieve the full query text for each request, and joins with sys.dm_exec_sessions to include user any host info. The session_id <> @@SPID filter ensures that you don't see your own query in the results.

SQL
SELECT r.session_id,
       r.status,
       r.command,
       r.start_time,
       r.total_elapsed_time / 1000.00 AS elapsed_seconds,
       r.cpu_time / 1000.00 AS cpu_seconds,
       r.reads,
       r.writes,
       r.logical_reads,
       r.row_count,
       s.login_name,
       s.host_name,
       t.text AS query_text
FROM sys.dm_exec_requests AS r
     INNER JOIN sys.dm_exec_sessions AS s
         ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
WHERE r.session_id <> @@SPID
ORDER BY r.start_time DESC;
C. Find all locks that a running batch is holding
The following example queries sys.dm_exec_requests to find the interesting batch and copy its transaction_id from the output.

SQL
SELECT * FROM sys.dm_exec_requests;
GO
Then, to find lock information, use the copied transaction_id with the system function sys.dm_tran_locks.

SQL
SELECT * FROM sys.dm_tran_locks
WHERE request_owner_type = N'TRANSACTION'
    AND request_owner_id = < copied transaction_id >;
GO
D. Find all currently blocked requests
The following example queries sys.dm_exec_requests to find information about blocked requests.

SQL
SELECT session_id,
       status,
       blocking_session_id,
       wait_type,
       wait_time,
       wait_resource,
       transaction_id
FROM sys.dm_exec_requests
WHERE status = N'suspended';
GO
E. Order existing requests by CPU
SQL
SELECT [req].[session_id],
    [req].[start_time],
    [req].[cpu_time] AS [cpu_time_ms],
    OBJECT_NAME([ST].[objectid], [ST].[dbid]) AS [ObjectName],
    SUBSTRING(
        REPLACE(
            REPLACE(
                SUBSTRING(
                    [ST].[text], ([req].[statement_start_offset] / 2) + 1,
                    ((CASE [req].[statement_end_offset]
                            WHEN -1 THEN DATALENGTH([ST].[text])
                            ELSE [req].[statement_end_offset]
                        END - [req].[statement_start_offset]
                        ) / 2
                    ) + 1
                ), CHAR(10), ' '
            ), CHAR(13), ' '
        ), 1, 512
    ) AS [statement_text]
FROM
    [sys].[dm_exec_requests] AS [req]
    CROSS APPLY [sys].dm_exec_sql_text([req].[sql_handle]) AS [ST]
ORDER BY
    [req].[cpu_time] DESC;
GO

### sys.dmv_tran_locks
sys.dm_tran_locks (Transact-SQL)
Applies to:  SQL Server  Azure SQL Database  Azure SQL Managed Instance  Azure Synapse Analytics  Analytics Platform System (PDW)  Warehouse in Microsoft Fabric  SQL database in Microsoft Fabric

Returns information about currently active lock manager resources in SQL Server. Each row represents a currently active request to the lock manager for a lock that has been granted or is waiting to be granted.

The columns in the result set are divided into two main groups: resource and request. The resource group describes the resource on which the lock request is being made, and the request group describes the lock request.

 Note

To call this from Azure Synapse Analytics or Analytics Platform System (PDW), use the name sys.dm_pdw_nodes_tran_locks. This syntax is not supported by serverless SQL pool in Azure Synapse Analytics.

Column name	Data type	Description
resource_type	nvarchar(60)	Represents the resource type. The value can be:

DATABASE

FILE

OBJECT

PAGE

KEY

EXTENT

RID (Row ID)

APPLICATION

METADATA

HOBT (Heap or B-tree)

ALLOCATION_UNIT

XACT (Transaction)

OIB (Online index build)

ROW_GROUP
resource_subtype	nvarchar(60)	Represents a subtype of resource_type. Acquiring a subtype lock without holding a non-subtyped lock of the parent type is technically valid. Different subtypes do not conflict with each other or with the non-subtyped parent type. Not all resource types have subtypes.
resource_database_id	int	ID of the database under which this resource is scoped. All resources handled by the lock manager are scoped by the database ID.
resource_description	nvarchar(256)	Description of the resource that contains only information that is not available from other resource columns.
resource_associated_entity_id	bigint	ID of the entity in a database with which a resource is associated. This can be an object ID, HOBT ID, or an Allocation Unit ID, depending on the resource type.
resource_lock_partition	Int	ID of the lock partition for a partitioned lock resource. The value for nonpartitioned lock resources is 0.
request_mode	nvarchar(60)	Mode of the request. For granted requests, this is the granted mode; for waiting requests, this is the mode being requested.

NULL = No access is granted to the resource. Serves as a placeholder.

Sch-S (Schema stability) = Ensures that a schema element, such as a table or index, is not dropped while any session holds a schema stability lock on the schema element.

Sch-M (Schema modification) = Must be held by any session that wants to change the schema of the specified resource. Ensures that no other sessions are referencing the indicated object.

S (Shared) = The holding session is granted shared access to the resource.

U (Update) = Indicates an update lock acquired on resources that may eventually be updated. It is used to prevent a common form of deadlock that occurs when multiple sessions lock resources for potential update in the future.

X (Exclusive) = The holding session is granted exclusive access to the resource.

IS (Intent Shared) = Indicates the intention to place S locks on some subordinate resource in the lock hierarchy.

IU (Intent Update) = Indicates the intention to place U locks on some subordinate resource in the lock hierarchy.

IX (Intent Exclusive) = Indicates the intention to place X locks on some subordinate resource in the lock hierarchy.

SIU (Shared Intent Update) = Indicates shared access to a resource with the intent of acquiring update locks on subordinate resources in the lock hierarchy.

SIX (Shared Intent Exclusive) = Indicates shared access to a resource with the intent of acquiring exclusive locks on subordinate resources in the lock hierarchy.

UIX (Update Intent Exclusive) = Indicates an update lock hold on a resource with the intent of acquiring exclusive locks on subordinate resources in the lock hierarchy.

BU = Used by bulk operations.

RangeS_S (Shared Key-Range and Shared Resource lock) = Indicates serializable range scan.

RangeS_U (Shared Key-Range and Update Resource lock) = Indicates serializable update scan.

RangeI_N (Insert Key-Range and Null Resource lock) = Used to test ranges before inserting a new key into an index.

RangeI_S = Key-Range Conversion lock, created by an overlap of RangeI_N and S locks.

RangeI_U = Key-Range Conversion lock, created by an overlap of RangeI_N and U locks.

RangeI_X = Key-Range Conversion lock, created by an overlap of RangeI_N and X locks.

RangeX_S = Key-Range Conversion lock, created by an overlap of RangeI_N and RangeS_S. locks.

RangeX_U = Key-Range Conversion lock, created by an overlap of RangeI_N and RangeS_U locks.

RangeX_X (Exclusive Key-Range and Exclusive Resource lock) = This is a conversion lock used when updating a key in a range.
request_type	nvarchar(60)	Request type. The value is LOCK.
request_status	nvarchar(60)	Current status of this request. Possible values are GRANTED, CONVERT, WAIT, LOW_PRIORITY_CONVERT, LOW_PRIORITY_WAIT, or ABORT_BLOCKERS. For more information about low priority waits and abort blockers, see the low_priority_lock_wait section of ALTER INDEX (Transact-SQL).
request_reference_count	smallint	Returns an approximate number of times the same requestor has requested this resource.
request_lifetime	int	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
request_session_id	int	session_id that currently owns this request. The owning session_id can change for distributed and bound transactions. A value of -2 indicates that the request belongs to an orphaned distributed transaction. A value of -3 indicates that the request belongs to a deferred recovery transaction, such as, a transaction for which a rollback has been deferred at recovery because the rollback could not be completed successfully.
request_exec_context_id	int	Execution context ID of the process that currently owns this request.
request_request_id	int	request_id (batch ID) of the process that currently owns this request. This value changes every time that the active Multiple Active Result Set (MARS) connection for a transaction changes.
request_owner_type	nvarchar(60)	Entity type that owns the request. Lock manager requests can be owned by a variety of entities. Possible values are:

TRANSACTION = The request is owned by a transaction.

CURSOR = The request is owned by a cursor.

SESSION = The request is owned by a user session.

SHARED_TRANSACTION_WORKSPACE = The request is owned by the shared part of the transaction workspace.

EXCLUSIVE_TRANSACTION_WORKSPACE = The request is owned by the exclusive part of the transaction workspace.

NOTIFICATION_OBJECT = The request is owned by an internal SQL Server component. This component has requested the lock manager to notify it when another component is waiting to take the lock. The FileTable feature is a component that uses this value.

Note: Work spaces are used internally to hold locks for enlisted sessions.
request_owner_id	bigint	ID of the specific owner of this request.

When a transaction is the owner of the request, this value contains the transaction ID.

When a FileTable is the owner of the request, request_owner_id has one of the following values:
-4 : A FileTable has taken a database lock.
-3 : A FileTable has taken a table lock.
Other value : The value represents a file handle. This value also appears as fcb_id in the dynamic management view sys.dm_filestream_non_transacted_handles (Transact-SQL).
request_owner_guid	uniqueidentifier	GUID of the specific owner of this request. This value is only used by a distributed transaction where the value corresponds to the MS DTC GUID for that transaction.
request_owner_lockspace_id	nvarchar(32)	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed. This value represents the lockspace ID of the requestor. The lockspace ID determines whether two requestors are compatible with each other and can be granted locks in modes that would otherwise conflict with one another.
lock_owner_address	varbinary(8)	Memory address of the internal data structure that is used to track this request. This column can be joined the with resource_address column in sys.dm_os_waiting_tasks.
pdw_node_id	int	Applies to: Azure Synapse Analytics, Analytics Platform System (PDW)

The identifier for the node that this distribution is on.
Permissions
On SQL Server and SQL Managed Instance, requires VIEW SERVER STATE permission.

On SQL Database Basic, S0, and S1 service objectives, and for databases in elastic pools, the server admin account, the Microsoft Entra admin account, or membership in the ##MS_ServerStateReader## server role is required. On all other SQL Database service objectives, either the VIEW DATABASE STATE permission on the database, or membership in the ##MS_ServerStateReader## server role is required.

Permissions for SQL Server 2022 and later
Requires VIEW SERVER PERFORMANCE STATE permission on the server.

Remarks
A granted request status indicates that a lock has been granted on a resource to the requestor. A waiting request indicates that the request has not yet been granted. The following waiting-request types are returned by the request_status column:

A convert request status indicates that the requestor has already been granted a request for the resource and is currently waiting for an upgrade to the initial request to be granted.

A wait request status indicates that the requestor does not currently hold a granted request on the resource.

Because sys.dm_tran_locks is populated from internal lock manager data structures, maintaining this information does not add extra overhead to regular processing. Materializing the view does require access to the lock manager internal data structures. This can have minor effects on the regular processing in the server. These effects should be unnoticeable and should only affect heavily used resources. Because the data in this view corresponds to live lock manager state, the data can change at any time, and rows are added and removed as locks are acquired and released. Applications querying this view might experience unpredictable performance due to the nature of protecting the integrity of lock manager structures. This view has no historical information.

Two requests operate on the same resource only if all the resource-group columns are equal.

You can control the locking of read operations by using the following tools:

SET TRANSACTION ISOLATION LEVEL to specify the level of locking for a session. For more information, see SET TRANSACTION ISOLATION LEVEL (Transact-SQL).

Locking table hints to specify the level of locking for an individual reference of a table in a FROM clause. For syntax and restrictions, see Table Hints (Transact-SQL).

A resource that is running under one session_id can have more than one granted lock. Different entities that are running under one session can each own a lock on the same resource, and the information is displayed in the request_owner_type and request_owner_id columns that are returned by sys.dm_tran_locks. If multiple instances of the same request_owner_type exist, the request_owner_id column is used to distinguish each instance. For distributed transactions, the request_owner_type and the request_owner_guid columns show the different entity information.

For example, Session S1 owns a shared lock on Table1; and transaction T1, which is running under session S1, also owns a shared lock on Table1. In this case, the resource_description column that is returned by sys.dm_tran_locks shows two instances of the same resource. The request_owner_type column shows one instance as a session and the other as a transaction. Also, the resource_owner_id column has different values.

Multiple cursors that run under one session are indistinguishable and are treated as one entity.

Distributed transactions that are not associated with a session_id value are orphaned transactions and are assigned the session_id value of -2. For more information, see KILL (Transact-SQL).

Locks
Locks are held on SQL Server resources, such as rows read or modified during a transaction, to prevent concurrent use of resources by different transactions. For example, if an exclusive (X) lock is held on a row within a table by a transaction, no other transaction can modify that row until the lock is released. Minimizing locks increases concurrency, which can improve performance.

Resource details
The following table lists the resources that are represented in the resource_associated_entity_id column.

Resource type	Resource description	resource_associated_entity_id
DATABASE	Represents a database.	Not applicable
FILE	Represents a database file. This file can be either a data or a log file.	Not applicable
OBJECT	Represents an object in a database. This object can be a data table, view, stored procedure, extended stored procedure, or any object that has an object ID.	Object ID
PAGE	Represents a single page in a data file.	HoBt ID. This value corresponds to sys.partitions.hobt_id. The HoBt ID is not always available for PAGE resources because the HoBt ID is extra information that can be provided by the caller, and not all callers can provide this information.
KEY	Represents a row in an index.	HoBt ID. This value corresponds to sys.partitions.hobt_id.
EXTENT	Represents a data file extent. An extent is a group of eight contiguous pages.	Not applicable
RID	Represents a physical row in a heap.	HoBt ID. This value corresponds to sys.partitions.hobt_id. The HoBt ID is not always available for RID resources because the HoBt ID is extra information that can be provided by the caller, and not all callers can provide this information.
APPLICATION	Represents an application specified resource.	Not applicable
METADATA	Represents metadata information.	Not applicable
HOBT	Represents a heap or a B-tree. These are the basic access path structures.	HoBt ID. This value corresponds to sys.partitions.hobt_id.
OIB	Represents online index (re)build.	HoBt ID. This value corresponds to sys.partitions.hobt_id.
ALLOCATION_UNIT	Represents a set of related pages, such as an index partition. Each allocation unit covers a single Index Allocation Map (IAM) chain.	Allocation Unit ID. This value corresponds to sys.allocation_units.allocation_unit_id.
ROW_GROUP	Represents a columnstore row group.	
XACT	Represents a transaction. Occurs when optimized locking is enabled.	There are two scenarios:

Scenario 1 (Owner)
- Resource type: XACT.
- Resource description: When a TID lock is held, the resource_description is the XACT resource.
- Resource associated entity ID: resource_associated_entity_id is 0.

Scenario 2 (Waiter)
- Resource type: XACT.
- Resource description: When a request waits for a TID lock, the resource_description is the XACT resource followed by the underlying KEY or RID resource.
- Resource associated entity ID: resource_associated_entity_id is the underlying HoBt ID.
 Note

Documentation uses the term B-tree generally in reference to indexes. In rowstore indexes, the Database Engine implements a B+ tree. This does not apply to columnstore indexes or indexes on memory-optimized tables. For more information, see the SQL Server and Azure SQL index architecture and design guide.

The following table lists the subtypes that are associated with each resource type.

ResourceSubType	Synchronizes
ALLOCATION_UNIT.BULK_OPERATION_PAGE	Pre-allocated pages used for bulk operations.
ALLOCATION_UNIT.PAGE_COUNT	Allocation unit page count statistics during deferred drop operations.
DATABASE.BULKOP_BACKUP_DB	Database backups with bulk operations.
DATABASE.BULKOP_BACKUP_LOG	Database log backups with bulk operations.
DATABASE.CHANGE_TRACKING_CLEANUP	Change tracking cleanup tasks.
DATABASE.CT_DDL	Database and table-level change tracking DDL operations.
DATABASE.CONVERSATION_PRIORITY	Service Broker conversation priority operations such as CREATE BROKER PRIORITY.
DATABASE.DDL	Data definition language (DDL) operations with filegroup operations, such as drop.
DATABASE.ENCRYPTION_SCAN	TDE encryption synchronization.
DATABASE.PLANGUIDE	Plan guide synchronization.
DATABASE.RESOURCE_GOVERNOR_DDL	DDL operations for resource governor operations such as ALTER RESOURCE POOL.
DATABASE.SHRINK	Database shrink operations.
DATABASE.STARTUP	Used for database startup synchronization.
FILE.SHRINK	File shrink operations.
HOBT.BULK_OPERATION	Heap-optimized bulk load operations with concurrent scan, under these isolation levels: snapshot, read uncommitted, and read committed using row versioning.
HOBT.INDEX_REORGANIZE	Heap or index reorganization operations.
OBJECT.COMPILE	Stored procedure compile.
OBJECT.INDEX_OPERATION	Index operations.
OBJECT.UPDSTATS	Statistics updates on a table.
METADATA.ASSEMBLY	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ASSEMBLY_CLR_NAME	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ASSEMBLY_TOKEN	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ASYMMETRIC_KEY	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.AUDIT	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.AUDIT_ACTIONS	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.AUDIT_SPECIFICATION	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.AVAILABILITY_GROUP	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CERTIFICATE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CHILD_INSTANCE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.COMPRESSED_FRAGMENT	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.COMPRESSED_ROWSET	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CONVERSTATION_ENDPOINT_RECV	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CONVERSTATION_ENDPOINT_SEND	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CONVERSATION_GROUP	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CONVERSATION_PRIORITY	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CREDENTIAL	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CRYPTOGRAPHIC_PROVIDER	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DATA_SPACE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DATABASE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DATABASE_PRINCIPAL	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DB_MIRRORING_SESSION	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DB_MIRRORING_WITNESS	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DB_PRINCIPAL_SID	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ENDPOINT	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ENDPOINT_WEBMETHOD	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.EXPR_COLUMN	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.EXPR_HASH	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.FULLTEXT_CATALOG	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.FULLTEXT_INDEX	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.FULLTEXT_STOPLIST	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.INDEX_EXTENSION_SCHEME	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.INDEXSTATS	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.INSTANTIATED_TYPE_HASH	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.MESSAGE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.METADATA_CACHE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PARTITION_FUNCTION	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PASSWORD_POLICY	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PERMISSIONS	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PLAN_GUIDE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PLAN_GUIDE_HASH	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PLAN_GUIDE_SCOPE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.QNAME	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.QNAME_HASH	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.REMOTE_SERVICE_BINDING	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ROUTE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SCHEMA	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SECURITY_CACHE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SECURITY_DESCRIPTOR	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SEQUENCE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVER_EVENT_SESSIONS	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVER_PRINCIPAL	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVICE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVICE_BROKER_GUID	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVICE_CONTRACT	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVICE_MESSAGE_TYPE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.STATS	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SYMMETRIC_KEY	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.USER_TYPE	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.XML_COLLECTION	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.XML_COMPONENT	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.XML_INDEX_QNAME	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
The following table provides the format of the resource_description column for each resource type.

Resource	Format	Description
DATABASE	Not applicable	Database ID is already available in the resource_database_id column.
FILE	<file_id>	ID of the file that is represented by this resource.
OBJECT	<object_id>	ID of the object that is represented by this resource. This object can be any object listed in sys.objects, not just a table.
PAGE	<file_id>:<page_in_file>	Represents the file and page ID of the page that is represented by this resource.
KEY	<hash_value>	Represents a hash of the key columns from the row that is represented by this resource.
EXTENT	<file_id>:<page_in_files>	Represents the file and page ID of the extent that is represented by this resource. The extent ID is the same as the page ID of the first page in the extent.
RID	<file_id>:<page_in_file>:<row_on_page>	Represents the page ID and row ID of the row that is represented by this resource. If the associated object ID is 99, this resource represents one of the eight mixed page slots on the first IAM page of an IAM chain.
APPLICATION	<DbPrincipalId>:<up to 32 characters>:(<hash_value>)	Represents the ID of the database principal that is used for scoping this application lock resource. Also included are up to 32 characters from the resource string that corresponds to this application lock resource. In certain cases, only two characters can be displayed due to the full string no longer being available. This behavior occurs only at database recovery time for application locks that are reacquired as part of the recovery process. The hash value represents a hash of the full resource string that corresponds to this application lock resource.
HOBT	Not applicable	HoBt ID is included as the resource_associated_entity_id.
ALLOCATION_UNIT	Not applicable	Allocation Unit ID is included as the resource_associated_entity_id.
XACT	<dbid>:<XdesId low>:<XdesId high>	The TID (transaction ID) resource. Occurs when optimized locking is enabled.
XACT KEY	[XACT <dbid>:<XdesId low>:<XdesId High>] KEY (<hash_value>)	The underlying resource the transaction is waiting on, with an index KEY object. Occurs when optimized locking is enabled.
XACT RID	[XACT <dbid>:<XdesId low>:<XdesId High>] RID (<file_id>:<page_in_file>:<row_on_page>)	The underlying resource the transaction is waiting on, with a heap RID object. Occurs when optimized locking is enabled.
METADATA.ASSEMBLY	assembly_id = A	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ASSEMBLY_CLR_NAME	$qname_id = Q	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ASSEMBLY_TOKEN	assembly_id = A, $token_id	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ASSYMMETRIC_KEY	asymmetric_key_id = A	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.AUDIT	audit_id = A	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.AUDIT_ACTIONS	device_id = D, major_id = M	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.AUDIT_SPECIFICATION	audit_specification_id = A	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.AVAILABILITY_GROUP	availability_group_id = A	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CERTIFICATE	certificate_id = C	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CHILD_INSTANCE	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.COMPRESSED_FRAGMENT	object_id = O , compressed_fragment_id = C	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.COMPRESSED_ROW	object_id = O	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CONVERSTATION_ENDPOINT_RECV	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CONVERSTATION_ENDPOINT_SEND	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CONVERSATION_GROUP	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CONVERSATION_PRIORITY	conversation_priority_id = C	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CREDENTIAL	credential_id = C	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.CRYPTOGRAPHIC_PROVIDER	provider_id = P	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DATA_SPACE	data_space_id = D	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DATABASE	database_id = D	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DATABASE_PRINCIPAL	principal_id = P	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DB_MIRRORING_SESSION	database_id = D	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DB_MIRRORING_WITNESS	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.DB_PRINCIPAL_SID	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ENDPOINT	endpoint_id = E	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ENDPOINT_WEBMETHOD	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.FULLTEXT_CATALOG	fulltext_catalog_id = F	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.FULLTEXT_INDEX	object_id = O	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.EXPR_COLUMN	object_id = O, column_id = C	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.EXPR_HASH	object_id = O, $hash = H	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.FULLTEXT_CATALOG	fulltext_catalog_id = F	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.FULLTEXT_INDEX	object_id = O	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.FULLTEXT_STOPLIST	fulltext_stoplist_id = F	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.INDEX_EXTENSION_SCHEME	index_extension_id = I	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.INDEXSTATS	object_id = O, index_id or stats_id = I	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.INSTANTIATED_TYPE_HASH	user_type_id = U, hash = H	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.MESSAGE	message_id = M	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.METADATA_CACHE	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PARTITION_FUNCTION	function_id = F	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PASSWORD_POLICY	principal_id = P	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PERMISSIONS	class = C	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PLAN_GUIDE	plan_guide_id = P	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PLAN_GUIDE_HASH	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.PLAN_GUIDE_SCOPE	scope_id = S	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.QNAME	$qname_id = Q	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.QNAME_HASH	$qname_scope_id = Q, $qname_hash = H	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.REMOTE_SERVICE_BINDING	remote_service_binding_id = R	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.ROUTE	route_id = R	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SCHEMA	schema_id = S	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SECURITY_CACHE	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SECURITY_DESCRIPTOR	sd_id = S	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SEQUENCE	$seq_type = S, object_id = O	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVER	server_id = S	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVER_EVENT_SESSIONS	event_session_id = E	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVER_PRINCIPAL	principal_id = P	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVICE	service_id = S	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVICE_BROKER_GUID	$hash = H1:H2:H3	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVICE_CONTRACT	service_contract_id = S	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SERVICE_MESSAGE_TYPE	message_type_id = M	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.STATS	object_id = O, stats_id = S	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.SYMMETRIC_KEY	symmetric_key_id = S	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.USER_TYPE	user_type_id = U	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.XML_COLLECTION	xml_collection_id = X	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.XML_COMPONENT	xml_component_id = X	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
METADATA.XML_INDEX_QNAME	object_id = O, $qname_id = Q	Identified for informational purposes only. Not supported. Future compatibility is not guaranteed.
Examples
A. Use sys.dm_tran_locks with other tools
The following example works with a scenario in which an update operation is blocked by another transaction. By using sys.dm_tran_locks and other tools, information about locking resources is provided.

SQL
USE tempdb;
GO

-- Create test table and index.
CREATE TABLE t_lock
    (
    c1 int, c2 int
    );
GO

CREATE INDEX t_lock_ci on t_lock(c1);
GO

-- Insert values into test table
INSERT INTO t_lock VALUES (1, 1);
INSERT INTO t_lock VALUES (2, 2);
INSERT INTO t_lock VALUES (3, 3);
INSERT INTO t_lock VALUES (4, 4);
INSERT INTO t_lock VALUES (5, 5);
INSERT INTO t_lock VALUES (6, 6);
GO

-- Session 1
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

BEGIN TRAN
    SELECT c1
        FROM t_lock
        WITH(holdlock, rowlock);

-- Session 2
BEGIN TRAN
    UPDATE t_lock SET c1 = 10;
The following query displays lock information. The value for <dbid> should be replaced with the database_id from sys.databases.

SQL
SELECT resource_type, resource_associated_entity_id,
    request_status, request_mode,request_session_id,
    resource_description
    FROM sys.dm_tran_locks
    WHERE resource_database_id = <dbid>;
The following query returns object information by using resource_associated_entity_id from the previous query. This query must be executed while you are connected to the database that contains the object.

SQL
SELECT object_name(object_id), *
    FROM sys.partitions
    WHERE hobt_id=<resource_associated_entity_id> ;
The following query shows blocking information.

SQL
SELECT
    t1.resource_type,
    t1.resource_database_id,
    t1.resource_associated_entity_id,
    t1.request_mode,
    t1.request_session_id,
    t2.blocking_session_id
FROM sys.dm_tran_locks as t1
INNER JOIN sys.dm_os_waiting_tasks as t2
    ON t1.lock_owner_address = t2.resource_address;
Release the resources by rolling back the transactions.

SQL
-- Session 1
ROLLBACK;
GO

-- Session 2
ROLLBACK;
GO
B. Link session information to operating system threads
The following example returns information that associates a session_id with a Windows thread ID. The performance of the thread can be monitored in the Windows Performance Monitor. This query does not return a session_id that is currently sleeping.

SQL
SELECT STasks.session_id, SThreads.os_thread_id
FROM sys.dm_os_tasks AS STasks
INNER JOIN sys.dm_os_threads AS SThreads
    ON STasks.worker_address = SThreads.worker_address
WHERE STasks.session_id IS NOT NULL
ORDER BY STasks.session_id;
GO

### sys.dm_exec_requests
sys.dm_exec_requests (Transact-SQL)
Applies to:  SQL Server  Azure SQL Database  Azure SQL Managed Instance  Azure Synapse Analytics  Analytics Platform System (PDW)  SQL analytics endpoint in Microsoft Fabric  Warehouse in Microsoft Fabric  SQL database in Microsoft Fabric

Returns information about each request that is executing in SQL Server. For more information about requests, see the Thread and task architecture guide.

 Note

To call this from dedicated SQL pool in Azure Synapse Analytics or Analytics Platform System (PDW), see sys.dm_pdw_exec_requests. For serverless SQL pool or Microsoft Fabric, use sys.dm_exec_requests.

Column name	Data type	Description
session_id	smallint	ID of the session to which this request is related. Not nullable.
request_id	int	ID of the request. Unique in the context of the session. Not nullable.
start_time	datetime	Timestamp when the request arrived. Not nullable.
status	nvarchar(30)	Status of the request. Can be one of the following values:

background
rollback
running
runnable
sleeping
suspended

Not nullable.
command	nvarchar(32)	Identifies the current type of command that is being processed. Common command types include the following values:

SELECT
INSERT
UPDATE
DELETE
BACKUP LOG
BACKUP DATABASE
DBCC
FOR

The text of the request can be retrieved by using sys.dm_exec_sql_text with the corresponding sql_handle for the request. Internal system processes set the command based on the type of task they perform. Tasks can include the following values:

LOCK MONITOR
CHECKPOINTLAZY
WRITER

Not nullable.
sql_handle	varbinary(64)	A token that uniquely identifies the batch or stored procedure that the query is part of. Nullable.
statement_start_offset	int	Indicates, in bytes, beginning with 0, the starting position of the currently executing statement for the currently executing batch or persisted object. Can be used together with the sql_handle, the statement_end_offset, and the sys.dm_exec_sql_text dynamic management function to retrieve the currently executing statement for the request. Nullable.
statement_end_offset	int	Indicates, in bytes, starting with 0, the ending position of the currently executing statement for the currently executing batch or persisted object. Can be used together with the sql_handle, the statement_start_offset, and the sys.dm_exec_sql_text dynamic management function to retrieve the currently executing statement for the request. Nullable.
plan_handle	varbinary(64)	A token that uniquely identifies a query execution plan for a batch that is currently executing. Nullable.
database_id	smallint	ID of the database the request is executing against. Not nullable.

In Azure SQL Database, the values are unique within a single database or an elastic pool, but not within a logical server.
user_id	int	ID of the user who submitted the request. Not nullable.
connection_id	uniqueidentifier	ID of the connection on which the request arrived. Nullable.
blocking_session_id	smallint	ID of the session that is blocking the request. If this column is NULL or 0, the request isn't blocked, or the session information of the blocking session isn't available (or can't be identified). For more information, see Understand and resolve SQL Server blocking problems.

-2 = The blocking resource is owned by an orphaned distributed transaction.

-3 = The blocking resource is owned by a deferred recovery transaction.

-4 = session_id of the blocking latch owner couldn't be determined at this time because of internal latch state transitions.

-5 = session_id of the blocking latch owner couldn't be determined because it isn't tracked for this latch type (for example, for an SH latch).

By itself, blocking_session_id -5 doesn't indicate a performance problem. -5 is an indication that the session is waiting on an asynchronous action to complete. Before -5 was introduced, the same session would have shown blocking_session_id 0, even though it was still in a wait state.

Depending on workload, observing blocking_session_id = -5 might be a common occurrence.
wait_type	nvarchar(60)	If the request is currently blocked, this column returns the type of wait. Nullable.

When a request uses multiple tasks, for example because of intra-query parallelism, tasks can wait on different resources with different wait types. A task can be blocked while other tasks of the same request continue execution. To find the wait type and duration for each task and whether it is blocked, use sys.dm_os_waiting_tasks.

For information about types of waits, see sys.dm_os_wait_stats.
wait_time	int	If the request is currently blocked, this column returns the duration in milliseconds, of the current wait. Not nullable.
last_wait_type	nvarchar(60)	If this request has previously been blocked, this column returns the type of the last wait. Not nullable.
wait_resource	nvarchar(256)	If the request is currently blocked, this column returns the resource for which the request is currently waiting. Not nullable.
open_transaction_count	int	Number of transactions that are open for this request. Not nullable.
open_resultset_count	int	Number of result sets that are open for this request. Not nullable.
transaction_id	bigint	ID of the transaction in which this request executes. Not nullable.
context_info	varbinary(128)	CONTEXT_INFO value of the session. Nullable.
percent_complete	real	Percentage of work completed for the following commands:

ALTER INDEX REORGANIZE
AUTO_SHRINK option with ALTER DATABASE
BACKUP DATABASE
DBCC CHECKDB
DBCC CHECKFILEGROUP
DBCC CHECKTABLE
DBCC INDEXDEFRAG
DBCC SHRINKDATABASE
DBCC SHRINKFILE
RECOVERY
RESTORE DATABASE
ROLLBACK
TDE ENCRYPTION

Not nullable.
estimated_completion_time	bigint	Internal only. Not nullable.
cpu_time	int	CPU time in milliseconds that is used by the request. Not nullable.
total_elapsed_time	int	Total time elapsed in milliseconds since the request arrived. Not nullable.
scheduler_id	int	ID of the scheduler that is scheduling this request. Nullable.
task_address	varbinary(8)	Memory address allocated to the task that is associated with this request. Nullable.
reads	bigint	Number of reads performed by this request. Not nullable.
writes	bigint	Number of writes performed by this request. Not nullable.
logical_reads	bigint	Number of logical reads that have been performed by the request. Not nullable.
text_size	int	TEXTSIZE setting for this request. Not nullable.
language	nvarchar(128)	Language setting for the request. Nullable.
date_format	nvarchar(3)	DATEFORMAT setting for the request. Nullable.
date_first	smallint	DATEFIRST setting for the request. Not nullable.
quoted_identifier	bit	1 = QUOTED_IDENTIFIER is ON for the request. Otherwise, it's 0.

Not nullable.
arithabort	bit	1 = ARITHABORT setting is ON for the request. Otherwise, it's 0.

Not nullable.
ansi_null_dflt_on	bit	1 = ANSI_NULL_DFLT_ON setting is ON for the request. Otherwise, it's 0.

Not nullable.
ansi_defaults	bit	1 = ANSI_DEFAULTS setting is ON for the request. Otherwise, it's 0.

Not nullable.
ansi_warnings	bit	1 = ANSI_WARNINGS setting is ON for the request. Otherwise, it's 0.

Not nullable.
ansi_padding	bit	1 = ANSI_PADDING setting is ON for the request.

Otherwise, it's 0.

Not nullable.
ansi_nulls	bit	1 = ANSI_NULLS setting is ON for the request. Otherwise, it's 0.

Not nullable.
concat_null_yields_null	bit	1 = CONCAT_NULL_YIELDS_NULL setting is ON for the request. Otherwise, it's 0.

Not nullable.
transaction_isolation_level	smallint	Isolation level with which the transaction for this request is created. Not nullable.
0 = Unspecified
1 = ReadUncommitted
2 = ReadCommitted
3 = Repeatable
4 = Serializable
5 = Snapshot
lock_timeout	int	Lock time-out period in milliseconds for this request. Not nullable.
deadlock_priority	int	DEADLOCK_PRIORITY setting for the request. Not nullable.
row_count	bigint	Number of rows that have been returned to the client by this request. Not nullable.
prev_error	int	Last error that occurred during the execution of the request. Not nullable.
nest_level	int	Current nesting level of code that is executing on the request. Not nullable.
granted_query_memory	int	Number of pages allocated to the execution of a query on the request. Not nullable.
executing_managed_code	bit	Indicates whether a specific request is currently executing common language runtime objects, such as routines, types, and triggers. it's set for the full time a common language runtime object is on the stack, even while running Transact-SQL from within common language runtime. Not nullable.
group_id	int	ID of the workload group to which this query belongs. Not nullable.
query_hash	binary(8)	Binary hash value calculated on the query and used to identify queries with similar logic. You can use the query hash to determine the aggregate resource usage for queries that differ only by literal values.
query_plan_hash	binary(8)	Binary hash value calculated on the query execution plan and used to identify similar query execution plans. You can use query plan hash to find the cumulative cost of queries with similar execution plans.
statement_sql_handle	varbinary(64)	Applies to: SQL Server 2014 (12.x) and later.

sql_handle of the individual query.

This column is NULL if Query Store isn't enabled for the database.
statement_context_id	bigint	Applies to: SQL Server 2014 (12.x) and later.

The optional foreign key to sys.query_context_settings.

This column is NULL if Query Store isn't enabled for the database.
dop	int	Applies to: SQL Server 2016 (13.x) and later.

The degree of parallelism of the query.
parallel_worker_count	int	Applies to: SQL Server 2016 (13.x) and later.

The number of reserved parallel workers if this is a parallel query.
external_script_request_id	uniqueidentifier	Applies to: SQL Server 2016 (13.x) and later.

The external script request ID associated with the current request.
is_resumable	bit	Applies to: SQL Server 2017 (14.x) and later.

Indicates whether the request is a resumable index operation.
page_resource	binary(8)	Applies to: SQL Server 2019 (15.x)

An 8-byte hexadecimal representation of the page resource if the wait_resource column contains a page. For more information, see sys.fn_PageResCracker.
page_server_reads	bigint	Applies to: Azure SQL Database Hyperscale

Number of page server reads performed by this request. Not nullable.
dist_statement_id	uniqueidentifier	Applies to: SQL Server 2022 and later versions, Azure SQL Database, Azure SQL Managed Instance, Azure Synapse Analytics (serverless pools only), and Microsoft Fabric

Unique ID for the statement for the request submitted. Not nullable.
Remarks
To execute code that is outside SQL Server (for example, extended stored procedures and distributed queries), a thread has to execute outside the control of the non-preemptive scheduler. To do this, a worker switches to preemptive mode. Time values returned by this dynamic management view don't include time spent in preemptive mode.

When executing parallel requests in row mode, SQL Server assigns a worker thread to coordinate the worker threads responsible for completing tasks assigned to them. In this DMV, only the coordinator thread is visible for the request. The columns reads, writes, logical_reads, and row_count are not updated for the coordinator thread. The columns wait_type, wait_time, last_wait_type, wait_resource, and granted_query_memory are only updated for the coordinator thread. For more information, see the Thread and task architecture guide.

The wait_resource column contains similar information to resource_description in sys.dm_tran_locks but is formatted differently.

Permissions
If the user has VIEW SERVER STATE permission on the server, the user sees all executing sessions on the instance of SQL Server; otherwise, the user sees only the current session. VIEW SERVER STATE can't be granted in Azure SQL Database so sys.dm_exec_requests is always limited to the current connection.

In availability group scenarios, if the secondary replica is set to read-intent only, the connection to the secondary must specify its application intent in connection string parameters by adding applicationintent=readonly. Otherwise, the access check for sys.dm_exec_requests doesn't pass for databases in the availability group, even if VIEW SERVER STATE permission is present.

For SQL Server 2022 (16.x) and later versions, sys.dm_exec_requests requires VIEW SERVER PERFORMANCE STATE permission on the server.

Examples
A. Find the query text for a running batch
The following example queries sys.dm_exec_requests to find the interesting query and copy its sql_handle from the output.

SQL
SELECT * FROM sys.dm_exec_requests;
GO
Then, to obtain the statement text, use the copied sql_handle with system function sys.dm_exec_sql_text(sql_handle).

SQL
SELECT * FROM sys.dm_exec_sql_text(< copied sql_handle >);
GO
B. Show active requests
This following example shows all currently running queries in your SQL Server data warehouse, excluding your own session (@@SPID). It uses CROSS APPLY with sys.dm_exec_sql_text to retrieve the full query text for each request, and joins with sys.dm_exec_sessions to include user any host info. The session_id <> @@SPID filter ensures that you don't see your own query in the results.

SQL
SELECT r.session_id,
       r.status,
       r.command,
       r.start_time,
       r.total_elapsed_time / 1000.00 AS elapsed_seconds,
       r.cpu_time / 1000.00 AS cpu_seconds,
       r.reads,
       r.writes,
       r.logical_reads,
       r.row_count,
       s.login_name,
       s.host_name,
       t.text AS query_text
FROM sys.dm_exec_requests AS r
     INNER JOIN sys.dm_exec_sessions AS s
         ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
WHERE r.session_id <> @@SPID
ORDER BY r.start_time DESC;
C. Find all locks that a running batch is holding
The following example queries sys.dm_exec_requests to find the interesting batch and copy its transaction_id from the output.

SQL
SELECT * FROM sys.dm_exec_requests;
GO
Then, to find lock information, use the copied transaction_id with the system function sys.dm_tran_locks.

SQL
SELECT * FROM sys.dm_tran_locks
WHERE request_owner_type = N'TRANSACTION'
    AND request_owner_id = < copied transaction_id >;
GO
D. Find all currently blocked requests
The following example queries sys.dm_exec_requests to find information about blocked requests.

SQL
SELECT session_id,
       status,
       blocking_session_id,
       wait_type,
       wait_time,
       wait_resource,
       transaction_id
FROM sys.dm_exec_requests
WHERE status = N'suspended';
GO
E. Order existing requests by CPU
SQL
SELECT [req].[session_id],
    [req].[start_time],
    [req].[cpu_time] AS [cpu_time_ms],
    OBJECT_NAME([ST].[objectid], [ST].[dbid]) AS [ObjectName],
    SUBSTRING(
        REPLACE(
            REPLACE(
                SUBSTRING(
                    [ST].[text], ([req].[statement_start_offset] / 2) + 1,
                    ((CASE [req].[statement_end_offset]
                            WHEN -1 THEN DATALENGTH([ST].[text])
                            ELSE [req].[statement_end_offset]
                        END - [req].[statement_start_offset]
                        ) / 2
                    ) + 1
                ), CHAR(10), ' '
            ), CHAR(13), ' '
        ), 1, 512
    ) AS [statement_text]
FROM
    [sys].[dm_exec_requests] AS [req]
    CROSS APPLY [sys].dm_exec_sql_text([req].[sql_handle]) AS [ST]
ORDER BY
    [req].[cpu_time] DESC;
GO

### sys.dm_db_partition_stats

Returns page and row-count information for every partition in the current database.

 Note

To call this from Azure Synapse Analytics or Analytics Platform System (PDW), use the name sys.dm_pdw_nodes_db_partition_stats. The partition_id in sys.dm_pdw_nodes_db_partition_stats differs from the partition_id in the sys.partitions catalog view for Azure Synapse Analytics. This syntax is not supported by serverless SQL pool in Azure Synapse Analytics.

Column name	Data type	Description
partition_id	bigint	ID of the partition. This is unique within a database. This is the same value as the partition_id in the sys.partitions catalog view except for Azure Synapse Analytics.
object_id	int	Object ID of the table or indexed view that the partition is part of.
index_id	int	ID of the heap or index the partition is part of.

0 = Heap
1 = Clustered index.
> 1 = Nonclustered index
partition_number	int	1-based partition number within the index or heap.
in_row_data_page_count	bigint	Number of pages in use for storing in-row data in this partition. If the partition is part of a heap, the value is the number of data pages in the heap. If the partition is part of an index, the value is the number of pages in the leaf level. (Nonleaf pages in the B+ tree are not included in the count.) IAM (Index Allocation Map) pages are not included in either case. Always 0 for an xVelocity memory optimized columnstore index.
in_row_used_page_count	bigint	Total number of pages in use to store and manage the in-row data in this partition. This count includes nonleaf B+ tree pages, IAM pages, and all pages included in the in_row_data_page_count column. Always 0 for a columnstore index.
in_row_reserved_page_count	bigint	Total number of pages reserved for storing and managing in-row data in this partition, regardless of whether the pages are in use or not. Always 0 for a columnstore index.
lob_used_page_count	bigint	Number of pages in use for storing and managing out-of-row text, ntext, image, varchar(max), nvarchar(max), varbinary(max), and xml columns within the partition. IAM pages are included.

Total number of LOBs used to store and manage columnstore index in the partition.
lob_reserved_page_count	bigint	Total number of pages reserved for storing and managing out-of-row text, ntext, image, varchar(max), nvarchar(max), varbinary(max), and xml columns within the partition, regardless of whether the pages are in use or not. IAM pages are included.

Total number of LOBs reserved for storing and managing a columnstore index in the partition.
row_overflow_used_page_count	bigint	Number of pages in use for storing and managing row-overflow varchar, nvarchar, varbinary, and sql_variant columns within the partition. IAM pages are included.

Always 0 for a columnstore index.
row_overflow_reserved_page_count	bigint	Total number of pages reserved for storing and managing row-overflow varchar, nvarchar, varbinary, and sql_variant columns within the partition, regardless of whether the pages are in use or not. IAM pages are included.

Always 0 for a columnstore index.
used_page_count	bigint	Total number of pages used for the partition. Computed as in_row_used_page_count + lob_used_page_count + row_overflow_used_page_count.
reserved_page_count	bigint	Total number of pages reserved for the partition. Computed as in_row_reserved_page_count + lob_reserved_page_count + row_overflow_reserved_page_count.
row_count	bigint	The approximate number of rows in the partition.
pdw_node_id	int	Applies to: Azure Synapse Analytics, Analytics Platform System (PDW)

The identifier for the node that this distribution is on.
distribution_id	int	Applies to: Azure Synapse Analytics, Analytics Platform System (PDW)

The unique numeric ID associated with the distribution.
Remarks
The sys.dm_db_partition_stats dynamic management view (DMV) displays information about the space used to store and manage in-row data LOB data, and row-overflow data for all partitions in a database. One row is displayed per partition.

The counts on which the output are based are cached in memory or stored on disk in various system tables.

In-row data, LOB data, and row-overflow data represent the three allocation units that make up a partition. The sys.allocation_units catalog view can be queried for metadata about each allocation unit in the database.

If a heap or index is not partitioned, it is made up of one partition (with partition number = 1); therefore, only one row is returned for that heap or index. The sys.partitions catalog view can be queried for metadata about each partition of all the tables and indexes in a database.

The total count for an individual table or an index can be obtained by adding the counts for all relevant partitions.

Permissions
Requires VIEW DATABASE STATE and VIEW DEFINITION permissions to query the sys.dm_db_partition_stats dynamic management view. For more information about permissions on dynamic management views, see Dynamic Management Views and Functions (Transact-SQL).

Permissions for SQL Server 2022 and later
Requires VIEW DATABASE PERFORMANCE STATE and VIEW SECURITY DEFINITION permissions on the database.

Examples
A. Return all counts for all partitions of all indexes and heaps in a database
The following example shows all counts for all partitions of all indexes and heaps in the AdventureWorks2022 database.

SQL
USE AdventureWorks2022;  
GO  
SELECT * FROM sys.dm_db_partition_stats;  
GO  
B. Return all counts for all partitions of a table and its indexes
The following example shows all counts for all partitions of the HumanResources.Employee table and its indexes.

SQL
USE AdventureWorks2022;  
GO  
SELECT * FROM sys.dm_db_partition_stats   
WHERE object_id = OBJECT_ID('HumanResources.Employee');  
GO  
C. Return total used pages and total number of rows for a heap or clustered index
The following example returns total used pages and total number of rows for the heap or clustered index of the HumanResources.Employee table. Because the Employee table is not partitioned by default, note the sum includes only one partition.

SQL
USE AdventureWorks2022;  
GO  
SELECT SUM(used_page_count) AS total_number_of_used_pages,   
    SUM (row_count) AS total_number_of_rows   
FROM sys.dm_db_partition_stats  
WHERE object_id=OBJECT_ID('HumanResources.Employee')    AND (index_id=0 or index_id=1);  
GO  