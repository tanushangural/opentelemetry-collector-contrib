# User Flow Analysis & Implementation Gap Report

## Required User Flow (from requirements.md)

### Step 1: Landing Page - Normalized Queries List
**User sees**: List of all normalized queries with:
- ✅ Timestamp (last_execution_timestamp)
- ❌ Query ID (query_hash) - **MISSING LINK**
- ✅ Database name
- ✅ Query text (normalized/anonymized)
- ⚠️ Lock time - **PARTIALLY AVAILABLE** (as wait_time in blocking sessions)
- ✅ Calls (execution_count)
- ✅ Rows examined (avg_rows_processed)

**Expected**: One row per query_hash or normalized query

### Step 2: Drill-down - Click on Normalized Query
When user clicks on a normalized query:
- ✅ Show the normalized query at the top
- ❌ Show chart with active running queries (each bar = active query) - **MISSING CORRELATION**
- ❌ Click on any bar to show active running queries table - **MISSING CORRELATION**

### Step 3: Deep Drill-down - Click on Active Query
When user clicks on an active query, show for (query_hash + session_id):
- ✅ Wait type
- ✅ Wait time in ms
- ❌ Blocked objects - **MISSING**
- ⚠️ Blocking sessions - **PARTIALLY AVAILABLE** (only blocker session_id, login, host)
- ❌ Execution plan - **NOT IMPLEMENTED**

## Current Implementation Status

### ✅ What Works

#### 1. Slow Queries (Step 1 - Partial)
**Query**: `SlowQuery` in `queries/query_performance_monitoring_metrics.go`

**Data Available**:
```sql
- query_id (query_hash) ✅
- query_text (anonymized) ✅
- database_name ✅
- last_execution_timestamp ✅
- execution_count ✅
- avg_rows_processed ✅
- avg_cpu_time_ms ✅
- avg_elapsed_time_ms ✅
- avg_disk_reads ✅
- avg_disk_writes ✅
- statement_type ✅
```

**Metrics Emitted**:
- `sqlserver.slowquery.avg_cpu_time_ms`
- `sqlserver.slowquery.avg_elapsed_time_ms`
- `sqlserver.slowquery.avg_disk_reads`
- `sqlserver.slowquery.avg_disk_writes`
- `sqlserver.slowquery.rows_processed`

**Attributes**: query_id, query_text, database_name, schema_name, execution_count, statement_type, timestamps

#### 2. Active Running Queries (Step 2 - Partial)
**Query**: `ActiveRunningQueriesQuery` in `queries/query_performance_monitoring_metrics.go`

**Data Available**:
```sql
- session_id ✅
- database_name ✅
- query_statement_text (anonymized) ✅
- wait_type ✅
- wait_time_s ✅
- cpu_time_ms ✅
- total_elapsed_time_ms ✅
- blocking_session_id ✅
- blocker_login_name ✅
- blocker_host_name ✅
- blocking_query_statement_text ✅
```

**Metrics Emitted**:
- `sqlserver.activequery.wait_time_seconds`
- `sqlserver.activequery.cpu_time_ms`
- `sqlserver.activequery.elapsed_time_ms`

**Attributes**: session_id, database_name, login_name, host_name, request_command, wait_type, wait_resource, blocking_session_id, query_text, blocking_query_text, timestamps

#### 3. Wait Time Analysis (Step 3 - Partial)
**Query**: `WaitQuery` in `queries/query_performance_monitoring_metrics.go`

**Data Available**:
```sql
- query_id (query_hash) ✅
- database_name ✅
- query_text ✅
- wait_category ✅
- total_wait_time_ms ✅
- avg_wait_time_ms ✅
- wait_event_count ✅
```

**Note**: Uses Query Store, not dm_exec_requests, so different data source

## ❌ Critical Gaps for User Flow

### Gap 1: Query Hash Correlation ⚠️ CRITICAL
**Problem**: No way to correlate slow queries with active running queries

**Current State**:
- Slow queries have `query_id` (query_hash from dm_exec_query_stats)
- Active queries have `query_statement_text` (raw text from dm_exec_requests)
- NO common identifier to link them!

**Required**:
1. Compute query_hash from anonymized query text in active running queries
2. Add `query_hash` attribute to active running queries metrics
3. This enables NRQL correlation:
```sql
FROM Metric
SELECT *
WHERE query_hash = '<selected_query_hash>'
  AND metricName LIKE 'sqlserver.activequery%'
```

**Solution**:
```go
// In processActiveRunningQueryMetrics()
if result.QueryStatementText != nil {
    anonymizedQuery := helpers.AnonymizeQueryText(*result.QueryStatementText)
    queryHash := computeQueryHash(anonymizedQuery)  // NEW FUNCTION NEEDED
    attrs.PutStr("query_hash", queryHash)
}
```

### Gap 2: Blocked Objects ❌ MISSING
**Problem**: User flow requires showing "blocked objects" but we don't capture them

**Current State**:
- We capture `wait_resource` (e.g., "PAGE: 6:1:123")
- We capture `blocking_session_id`
- We DO NOT parse or expose the actual blocked object details

**Required Data**:
- Object name (table/index name)
- Object type (table, index, page, row, key)
- Database name (already have this)
- Lock type (from wait_type)

**Solution**: Enhance `ActiveRunningQueriesQuery` to parse wait_resource:
```sql
-- Add to ActiveRunningQueriesQuery
CASE
    WHEN r_wait.wait_resource LIKE 'PAGE:%' THEN
        OBJECT_NAME(
            SUBSTRING(r_wait.wait_resource,
                CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1) + 1,
                LEN(r_wait.wait_resource))
        )
    WHEN r_wait.wait_resource LIKE 'OBJECT:%' THEN
        OBJECT_NAME(CAST(SUBSTRING(r_wait.wait_resource, 8, LEN(r_wait.wait_resource)) AS INT))
    ELSE NULL
END AS blocked_object_name,

CASE
    WHEN r_wait.wait_resource LIKE 'PAGE:%' THEN 'PAGE'
    WHEN r_wait.wait_resource LIKE 'OBJECT:%' THEN 'OBJECT'
    WHEN r_wait.wait_resource LIKE 'KEY:%' THEN 'KEY'
    WHEN r_wait.wait_resource LIKE 'RID:%' THEN 'ROW'
    ELSE 'OTHER'
END AS blocked_resource_type
```

### Gap 3: Blocking Chain Details ⚠️ INCOMPLETE
**Problem**: Only capture immediate blocker, not full blocking chain

**Current State**:
- We have `blocking_session_id`
- We have `blocker_login_name` and `blocker_host_name`
- We DO NOT capture blocking chain depth or head blocker

**User Flow Expectation**: Show complete blocking hierarchy

**Solution**: Add blocking chain traversal:
```sql
-- Add recursive CTE to trace blocking chain
WITH BlockingChain AS (
    SELECT
        session_id,
        blocking_session_id,
        0 AS blocking_level,
        CAST(session_id AS VARCHAR(MAX)) AS blocking_path
    FROM sys.dm_exec_requests
    WHERE blocking_session_id != 0

    UNION ALL

    SELECT
        r.session_id,
        r.blocking_session_id,
        bc.blocking_level + 1,
        bc.blocking_path + ' <- ' + CAST(r.blocking_session_id AS VARCHAR(MAX))
    FROM sys.dm_exec_requests r
    INNER JOIN BlockingChain bc ON r.session_id = bc.blocking_session_id
    WHERE bc.blocking_level < 10  -- Prevent infinite loops
)
```

### Gap 4: Execution Plan by Session + Query Hash ❌ NOT IMPLEMENTED
**Problem**: User flow requires execution plan for specific session_id + query_hash

**Current State**:
- `QueryExecutionPlan` query exists but takes only query_hash
- Does NOT filter by session_id
- Has a bug in the scraper (wrong parameter count)
- Currently disabled for performance reasons

**User Flow Requirement**: Show execution plan for the SPECIFIC active running query instance

**Solution**: Create new query `ActiveQueryExecutionPlan`:
```sql
DECLARE @TargetSessionID INT = %d;
DECLARE @TargetQueryHash BINARY(8) = %s;

SELECT
    r.session_id,
    qs.query_hash AS query_id,
    qs.plan_handle,
    CAST(qp.query_plan AS NVARCHAR(MAX)) AS execution_plan_xml,
    r.cpu_time AS current_cpu_ms,
    r.total_elapsed_time AS current_elapsed_ms,
    r.wait_type,
    r.wait_time / 1000.0 AS current_wait_time_s,
    st.text AS sql_text
FROM sys.dm_exec_requests AS r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
INNER JOIN sys.dm_exec_query_stats AS qs ON r.sql_handle = qs.sql_handle
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE r.session_id = @TargetSessionID
    AND qs.query_hash = @TargetQueryHash
    AND qp.query_plan IS NOT NULL;
```

## Data Flow for Complete User Experience

### Phase 1: Landing Page Query (NRQL)
```sql
FROM Metric
SELECT
    latest(execution_count) as 'Calls',
    latest(sqlserver.slowquery.avg_elapsed_time_ms) as 'Avg Time',
    latest(sqlserver.slowquery.rows_processed) as 'Rows',
    latest(query_text) as 'Query',
    latest(database_name) as 'Database',
    latest(last_execution_timestamp) as 'Last Executed'
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
FACET query_id, query_text
SINCE 1 hour ago
LIMIT 100
```

### Phase 2: Active Queries for Selected Query (NRQL) - ❌ BROKEN
```sql
-- THIS WILL NOT WORK - query_hash not available in active queries!
FROM Metric
SELECT
    latest(sqlserver.activequery.wait_time_seconds) as 'Wait Time',
    latest(sqlserver.activequery.elapsed_time_ms) as 'Elapsed',
    latest(wait_type) as 'Wait Type',
    latest(blocking_session_id) as 'Blocker'
WHERE query_hash = '${selectedQueryHash}'  -- ❌ NOT AVAILABLE!
  AND metricName LIKE 'sqlserver.activequery%'
FACET session_id
SINCE 5 minutes ago
```

**Fix Required**: Add `query_hash` to active query metrics attributes!

### Phase 3: Drill-down Details (NRQL) - ⚠️ INCOMPLETE
```sql
FROM Metric
SELECT
    latest(wait_type) as 'Wait Type',
    latest(sqlserver.activequery.wait_time_seconds) as 'Wait Time',
    latest(wait_resource) as 'Resource',
    latest(blocking_session_id) as 'Blocker',
    latest(blocked_object_name) as 'Object',  -- ❌ NOT AVAILABLE!
    latest(blocking_query_text) as 'Blocking Query'
WHERE session_id = ${selectedSessionId}
  AND query_hash = '${selectedQueryHash}'  -- ❌ NOT AVAILABLE!
SINCE 5 minutes ago
```

**Fix Required**:
1. Add `query_hash` attribute
2. Add `blocked_object_name` attribute
3. Add `blocked_resource_type` attribute

## Implementation Priority

### 🔴 P0 - Critical for User Flow
1. **Add Query Hash to Active Running Queries**
   - Compute hash from anonymized query text
   - Add as attribute to all active query metrics
   - Enables correlation between slow queries and active queries

2. **Fix Blocked Objects Parsing**
   - Parse `wait_resource` to extract object details
   - Add `blocked_object_name` and `blocked_resource_type` attributes
   - Required for "blocked objects" in user flow step 3.1

### 🟡 P1 - Important for Complete User Flow
3. **Implement Active Query Execution Plan**
   - Create `ActiveQueryExecutionPlan` query
   - Filter by session_id + query_hash
   - Add scraper method `ScrapeActiveQueryExecutionPlan()`
   - Required for user flow step 3.1

4. **Enhance Blocking Chain Details**
   - Add blocking level (depth in chain)
   - Add head blocker identification
   - Add blocking path visualization

### 🟢 P2 - Nice to Have
5. **Add Blocked Session Details**
   - Additional details about blocked sessions
   - Lock mode information
   - Transaction details

6. **Performance Optimization**
   - Cache query hash computations
   - Optimize query parsing
   - Add cardinality limits

## Code Changes Required

### 1. Add Query Hash Helper Function
**File**: `helpers/query_hash.go` (NEW FILE)
```go
package helpers

import (
    "crypto/md5"
    "encoding/hex"
)

// ComputeQueryHash computes a consistent hash for a query text
// This hash should match SQL Server's query_hash for anonymized queries
func ComputeQueryHash(queryText string) string {
    // Normalize the query text
    normalized := AnonymizeQueryText(queryText)

    // Compute MD5 hash (matches SQL Server approach)
    hash := md5.Sum([]byte(normalized))
    return "0x" + hex.EncodeToString(hash[:8]) // First 8 bytes as hex
}
```

### 2. Update Active Running Query Model
**File**: `models/query_performance_monitoring_metrics.go`
```go
type ActiveRunningQuery struct {
    // ... existing fields ...

    // NEW FIELDS for user flow
    QueryHash              *string `db:"query_hash" metric_name:"query_hash" source_type:"attribute"`
    BlockedObjectName      *string `db:"blocked_object_name" metric_name:"blocked_object_name" source_type:"attribute"`
    BlockedResourceType    *string `db:"blocked_resource_type" metric_name:"blocked_resource_type" source_type:"attribute"`
    BlockingLevel          *int    `db:"blocking_level" metric_name:"blocking_level" source_type:"attribute"`
}
```

### 3. Enhance Active Running Queries SQL
**File**: `queries/query_performance_monitoring_metrics.go`

Add to SELECT clause:
```sql
-- Compute query hash for correlation
CONVERT(VARCHAR(20), HASHBYTES('MD5', <anonymized_query_text>), 1) AS query_hash,

-- Parse blocked object details
CASE
    WHEN r_wait.wait_resource LIKE 'OBJECT:%' THEN
        OBJECT_NAME(CAST(SUBSTRING(r_wait.wait_resource, 8, LEN(r_wait.wait_resource)) AS INT))
    ELSE NULL
END AS blocked_object_name,

CASE
    WHEN r_wait.wait_resource LIKE 'PAGE:%' THEN 'PAGE'
    WHEN r_wait.wait_resource LIKE 'OBJECT:%' THEN 'OBJECT'
    WHEN r_wait.wait_resource LIKE 'KEY:%' THEN 'KEY'
    WHEN r_wait.wait_resource LIKE 'RID:%' THEN 'ROW'
    ELSE 'OTHER'
END AS blocked_resource_type
```

### 4. Update Scraper to Add Query Hash
**File**: `scrapers/scraper_query_performance_montoring_metrics.go`
```go
func (s *QueryPerformanceScraper) addActiveQueryAttributes(attrs pcommon.Map, result models.ActiveRunningQuery) {
    // ... existing code ...

    // Add query hash for correlation with slow queries
    if result.QueryStatementText != nil {
        queryHash := helpers.ComputeQueryHash(*result.QueryStatementText)
        attrs.PutStr("query_hash", queryHash)
    }

    // Add blocked object details
    if result.BlockedObjectName != nil {
        attrs.PutStr("blocked_object_name", *result.BlockedObjectName)
    }
    if result.BlockedResourceType != nil {
        attrs.PutStr("blocked_resource_type", *result.BlockedResourceType)
    }
}
```

## Updated User Flow (After Fixes)

### ✅ Step 1: Landing Page
```sql
SELECT query_id, query_text, database_name, execution_count, avg_elapsed_time_ms
FROM slow_queries
ORDER BY avg_elapsed_time_ms DESC
```
→ User clicks on a query with `query_id = '0xABCD1234'`

### ✅ Step 2: Active Queries for Selected Query
```sql
SELECT session_id, wait_time_s, wait_type, elapsed_time_ms, query_text
FROM active_running_queries
WHERE query_hash = '0xABCD1234'  -- ✅ NOW AVAILABLE!
ORDER BY wait_time_s DESC
```
→ User clicks on an active query bar with `session_id = 52`

### ✅ Step 3: Drill-down for Selected Active Query
```sql
SELECT
    wait_type,              -- ✅ AVAILABLE
    wait_time_s,           -- ✅ AVAILABLE
    blocked_object_name,   -- ✅ AVAILABLE (after fix)
    blocked_resource_type, -- ✅ AVAILABLE (after fix)
    blocking_session_id,   -- ✅ AVAILABLE
    blocker_login_name,    -- ✅ AVAILABLE
    execution_plan_xml     -- ⚠️ NEEDS IMPLEMENTATION
FROM active_query_details
WHERE session_id = 52 AND query_hash = '0xABCD1234'
```

## Conclusion

### Current State: 60% Complete ⚠️

**What Works**:
- ✅ Slow queries scraping with query_hash
- ✅ Active running queries scraping with wait details
- ✅ Blocking session identification
- ✅ Query text anonymization
- ✅ Configuration and integration

**What's Missing for User Flow**:
- ❌ Query hash correlation (CRITICAL)
- ❌ Blocked objects parsing (HIGH)
- ❌ Execution plan by session+query_hash (MEDIUM)
- ❌ Blocking chain depth (LOW)

**Estimated Effort to Complete**:
- Query hash correlation: 2-3 hours
- Blocked objects parsing: 2-3 hours
- Execution plan implementation: 3-4 hours
- Testing and validation: 2-3 hours
- **Total: ~10-15 hours**

The foundation is solid, but the critical correlation mechanism (query_hash) is missing to enable the complete drill-down user experience!
