# Root Cause Analysis (RCA) Implementation Review

## Executive Summary

This document analyzes the current SQL Server query performance monitoring implementation against the DMV documentation and provides RCA-oriented recommendations for improvement.

---

## 🚨 CRITICAL ISSUES FOUND

### 1. Query Store Dependencies (Performance Impact)

**❌ REMOVE IMMEDIATELY** - Query Store has significant performance overhead and was explicitly mentioned to be removed.

#### Locations Using Query Store:

1. **WaitQuery** (`queries/query_performance_monitoring_metrics.go:463-569`)
   - Line 488: `WHERE is_query_store_on = 1`
   - Lines 502-546: Uses `sys.query_store_wait_stats`, `sys.query_store_plan`, `sys.query_store_query`
   - **Impact**: Iterates through ALL databases with Query Store enabled, causing performance issues

2. **BlockingSessionsQuery** (`queries/query_performance_monitoring_metrics.go:458`)
   - Line 458: `WHERE db.is_query_store_on = 1`
   - **Impact**: Only monitors databases with Query Store enabled, missing critical blocking issues

3. **SlowQuery** (`queries/query_performance_monitoring_metrics.go:270-271`, 373-377)
   - Lines 373-377: Filters databases based on Query Store status
   - **Impact**: Reduces coverage of slow query detection

---

## 📊 RCA-Oriented Data Flow Issues

### Current Flow (Incomplete for RCA)
```
Slow Queries (dm_exec_query_stats)
                ❌ NO CORRELATION
Active Queries (dm_exec_requests)
                ✅ HAS CORRELATION
Blocking Analysis
```

### Missing Correlations for RCA

#### 1. **No query_hash Correlation Between Slow and Active Queries**

**Problem**: Cannot answer "Is this slow query currently running?"

**Solution**: Add query_hash to ActiveRunningQueriesQuery

```sql
-- ADD TO ActiveRunningQueriesQuery (line 599)
SELECT TOP (@Limit)
    -- ... existing columns ...
    r_wait.query_hash AS query_id,  -- ✅ ADD THIS
    r_wait.plan_handle AS plan_handle,
```

**RCA Benefit**: Can now correlate:
- Slow query with query_hash X → Is it currently running? → Yes, session 52 with blocking

---

#### 2. **Wait Stats from Query Store Instead of DMVs**

**Problem**: Wait stats come from historical Query Store, not real-time DMVs

**Current** (WaitQuery): Uses `sys.query_store_wait_stats` (aggregated historical data)
**Should Use**: `sys.dm_os_wait_stats` or extract wait info from `sys.dm_exec_requests`

**Replacement Query for Wait Stats:**

```sql
-- NEW: Real-time wait stats from dm_exec_requests (per active query)
SELECT
    r.session_id,
    r.query_hash AS query_id,
    r.wait_type,
    r.wait_time / 1000.0 AS wait_time_s,
    r.last_wait_type,
    r.wait_resource,
    DB_NAME(r.database_id) AS database_name,
    LEFT(SUBSTRING(st.text, (r.statement_start_offset / 2) + 1,
        ((CASE r.statement_end_offset
            WHEN -1 THEN DATALENGTH(st.text)
            ELSE r.statement_end_offset
        END - r.statement_start_offset) / 2) + 1
    ), @TextTruncateLimit) AS query_text,
    FORMAT(r.start_time AT TIME ZONE 'UTC', 'yyyy-MM-ddTHH:mm:ssZ') AS request_start_time,
    FORMAT(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC', 'yyyy-MM-ddTHH:mm:ssZ') AS collection_timestamp
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
WHERE r.session_id > 50
    AND r.wait_type IS NOT NULL
    AND r.wait_time > 0
ORDER BY r.wait_time DESC;
```

**RCA Benefit**: Real-time wait analysis instead of historical averages

---

#### 3. **Missing Session Context from dm_exec_sessions**

**Problem**: Limited session details for RCA (no full context about who/what/when)

**Available in dm_exec_sessions but NOT captured:**
- `client_interface_name` - Which driver/tool is being used
- `program_name` - Application name
- `status` - Session status (running/sleeping/dormant)
- `cpu_time` - Total CPU time for session
- `memory_usage` - Memory pages used by session
- `total_elapsed_time` - Session lifetime
- `last_request_start_time` - When last request started
- `last_request_end_time` - When last request completed
- `is_user_process` - User vs system session

**ADD TO ActiveRunningQueriesQuery:**

```sql
-- ADD session-level context
s_wait.program_name,
s_wait.status AS session_status,
s_wait.cpu_time AS session_total_cpu_ms,
s_wait.memory_usage AS session_memory_pages,
FORMAT(s_wait.last_request_start_time AT TIME ZONE 'UTC', 'yyyy-MM-ddTHH:mm:ssZ') AS session_last_request_start,
s_wait.client_interface_name
```

**RCA Benefit**: Can answer:
- "Which application is causing this issue?" (program_name)
- "Is this a pattern or one-off?" (session_last_request_start)
- "Which driver is being used?" (client_interface_name)

---

#### 4. **Missing Transaction Context**

**Problem**: Cannot identify if blocking is due to long-running transactions

**Available in dm_exec_requests but NOT captured:**
- `transaction_id` - Link to sys.dm_tran_active_transactions
- `open_transaction_count` - Number of open transactions

**Available in dm_tran_active_transactions:**
- `transaction_begin_time` - When transaction started
- `transaction_type` - Read/write transaction
- `transaction_state` - Active/prepared/committed

**ADD TO ActiveRunningQueriesQuery:**

```sql
-- ADD transaction context
r_wait.transaction_id,
r_wait.open_transaction_count,
FORMAT(ta.transaction_begin_time AT TIME ZONE 'UTC', 'yyyy-MM-ddTHH:mm:ssZ') AS transaction_begin_time,
ta.transaction_state
-- JOIN with:
LEFT JOIN sys.dm_tran_active_transactions ta ON r_wait.transaction_id = ta.transaction_id
```

**RCA Benefit**: Can answer:
- "Is blocking due to a long-running transaction?" (transaction_begin_time)
- "How many transactions does this session have open?" (open_transaction_count)

---

#### 5. **Missing Parallel Query Execution Details**

**Problem**: Cannot diagnose parallel query issues (CXPACKET waits, parallel worker issues)

**Available in dm_exec_requests but NOT captured:**
- `dop` (Degree of Parallelism) - How many parallel workers
- `parallel_worker_count` - Reserved parallel workers

**ADD TO ActiveRunningQueriesQuery:**

```sql
-- ADD parallel execution details
r_wait.dop AS degree_of_parallelism,
r_wait.parallel_worker_count AS parallel_workers
```

**RCA Benefit**: Can answer:
- "Is this CXPACKET wait due to parallel query execution?"
- "Is parallelism too high for this query?"

---

## 🔍 Enhanced Correlation Algorithm

### Recommended Implementation Flow

```sql
-- STEP 1: Fetch Normalized Queries (Historical Slow Queries)
-- Source: sys.dm_exec_query_stats
-- Key: query_hash (renamed to query_id)
-- Time Window: last @IntervalSeconds (e.g., 15s)
SELECT
    qs.query_hash AS query_id,
    qs.query_plan_hash AS query_plan_id,
    qs.plan_handle,
    AVG(qs.total_elapsed_time / qs.execution_count) AS avg_elapsed_time_ms,
    SUM(qs.execution_count) AS execution_count,
    -- ... other aggregates
FROM sys.dm_exec_query_stats qs
WHERE qs.last_execution_time >= DATEADD(SECOND, -@IntervalSeconds, GETUTCDATE())
GROUP BY qs.query_hash, qs.query_plan_hash, qs.plan_handle;

-- STEP 2: Fetch Active Running Queries (Current Execution State)
-- Source: sys.dm_exec_requests + sys.dm_exec_sessions
-- Key: query_hash (same as query_id)
-- Correlation: JOIN with Step 1 on query_hash
SELECT
    r.session_id,
    r.query_hash AS query_id,  -- ✅ CORRELATES WITH STEP 1
    r.plan_handle,
    r.wait_type,
    r.wait_time,
    r.blocking_session_id,
    r.cpu_time,
    r.total_elapsed_time,
    r.reads,
    r.writes,
    r.logical_reads,
    r.transaction_id,
    r.open_transaction_count,
    r.dop,
    r.parallel_worker_count,
    s.login_name,
    s.host_name,
    s.program_name,
    s.status AS session_status,
    -- Query text
    SUBSTRING(st.text, (r.statement_start_offset / 2) + 1, ...) AS query_text
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
WHERE r.session_id > 50
    AND r.query_hash IN (SELECT query_id FROM Step1)  -- ✅ CORRELATION
ORDER BY r.total_elapsed_time DESC;

-- STEP 3: For Each Blocked Active Query, Get Blocking Chain
-- Source: sys.dm_exec_requests (recursive for blocking chains)
-- Key: session_id → blocking_session_id
WITH BlockingChain AS (
    -- Anchor: Blocked sessions
    SELECT
        session_id AS blocked_spid,
        blocking_session_id AS blocking_spid,
        0 AS blocking_level,
        CAST(session_id AS VARCHAR(MAX)) AS blocking_chain
    FROM sys.dm_exec_requests
    WHERE blocking_session_id != 0

    UNION ALL

    -- Recursive: Find the head blocker
    SELECT
        bc.blocked_spid,
        r.blocking_session_id,
        bc.blocking_level + 1,
        CAST(bc.blocking_chain + ' ← ' + CAST(r.blocking_session_id AS VARCHAR) AS VARCHAR(MAX))
    FROM BlockingChain bc
    INNER JOIN sys.dm_exec_requests r ON bc.blocking_spid = r.session_id
    WHERE r.blocking_session_id != 0
)
SELECT * FROM BlockingChain;

-- STEP 4: For Each Blocking Session, Get Locked Objects
-- Source: sys.dm_tran_locks + sys.partitions
-- Key: session_id
SELECT
    l.request_session_id AS session_id,
    DB_NAME(l.resource_database_id) AS database_name,
    OBJECT_NAME(p.object_id, l.resource_database_id) AS locked_object_name,
    l.resource_type,
    l.request_mode AS lock_mode,
    l.request_status AS lock_status
FROM sys.dm_tran_locks l
LEFT JOIN sys.partitions p ON l.resource_associated_entity_id = p.hobt_id
WHERE l.request_session_id IN (SELECT blocking_spid FROM BlockingChain);

-- STEP 5: Fetch Execution Plans (on-demand)
-- Source: sys.dm_exec_query_plan()
-- Key: plan_handle from Step 1 or Step 2
SELECT
    CAST(qp.query_plan AS NVARCHAR(MAX)) AS execution_plan_xml
FROM sys.dm_exec_query_plan(@PlanHandle) qp;
```

---

## 📐 Dimensional Metrics Design (NRDB-Optimized)

### Metric Naming Convention

```
sqlserver.<scope>.<metric_name>
```

**Scopes:**
- `slowquery` - Historical query stats from dm_exec_query_stats
- `activequery` - Real-time execution from dm_exec_requests
- `blocking` - Blocking session info
- `wait` - Wait event info
- `lock` - Lock resource info

### Attributes vs Metrics

#### **Metrics** (Values - Dimensions in NRDB)
- `sqlserver.slowquery.avg_elapsed_time_ms` - gauge
- `sqlserver.slowquery.execution_count` - gauge
- `sqlserver.activequery.cpu_time_ms` - gauge
- `sqlserver.activequery.wait_time_ms` - gauge
- `sqlserver.blocking.wait_time_seconds` - gauge

#### **Attributes** (Tags - Facets in NRDB)
- `query_id` - Correlation key (query_hash)
- `database_name`
- `schema_name`
- `statement_type`
- `wait_type`
- `blocking_session_id`
- `session_id`
- `login_name`
- `host_name`
- `program_name`
- `transaction_id`
- `query_text` - Anonymized
- `execution_plan_xml` - On-demand

---

## 🎯 RCA User Flow Implementation

### User Flow 1: Landing Page (Slow Queries)

**NRQL Query:**
```nrql
SELECT
    latest(sqlserver.slowquery.avg_elapsed_time_ms),
    latest(database_name),
    latest(query_text)
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
FACET query_id
SINCE 1 hour ago
ORDER BY latest(sqlserver.slowquery.avg_elapsed_time_ms) DESC
```

**Drill-Down:** Click on query_id → Go to User Flow 2

---

### User Flow 2: Active Query Analysis

**Question:** "Is this slow query currently running?"

**NRQL Query:**
```nrql
SELECT
    latest(session_id),
    latest(sqlserver.activequery.elapsed_time_ms),
    latest(wait_type),
    latest(blocking_session_id),
    latest(request_status)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
    AND query_id = '<selected_query_id>'  -- ✅ CORRELATION
SINCE 10 minutes ago
```

**RCA Answer:**
- ✅ YES → Session 52 is running this query with wait_type=LCK_M_X
- ❌ NO → Query completed or not currently executing

**Drill-Down:** If blocking_session_id != 'N/A' → Go to User Flow 3

---

### User Flow 3: Blocking Chain Analysis

**Question:** "What is blocking my query?"

**NRQL Query:**
```nrql
SELECT
    latest(blocking_spid),
    latest(blocked_spid),
    latest(sqlserver.blocking.wait_time_seconds),
    latest(blocking_query_text),
    latest(blocked_query_text)
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
    AND (blocked_spid = 52 OR blocking_spid = 52)
SINCE 10 minutes ago
```

**RCA Answer:**
- Session 52 (blocked) is waiting for Session 48 (blocker)
- Blocker is holding a lock for 45 seconds
- Blocker query: `UPDATE Product SET...`

**Drill-Down:** Click on blocking_spid → Go to User Flow 4

---

### User Flow 4: Locked Object Analysis

**Question:** "What objects is the blocker holding locks on?"

**NRQL Query:**
```nrql
SELECT
    latest(database_name),
    latest(schema_name),
    latest(locked_object_name),
    latest(lock_mode),
    latest(lock_granularity)
FROM Metric
WHERE metricName = 'sqlserver.lock.count'
    AND session_id = 48  -- blocker session
FACET locked_object_name, lock_mode
SINCE 10 minutes ago
```

**RCA Answer:**
- Blocker holds X (exclusive) lock on `Production.Product` table
- Lock granularity: Table Lock (escalated from row locks)

---

### User Flow 5: Root Cause Identification

**RCA Chain:**
```
Slow Query (query_hash X, avg_elapsed_time=5000ms)
    ↓ CORRELATION
Active Query (session 52, query_hash X, wait_type=LCK_M_X, wait_time=45s)
    ↓ BLOCKING_SESSION_ID
Blocker Session (session 48, transaction_begin_time=3 minutes ago)
    ↓ LOCKED_OBJECTS
Locked Table (Production.Product, lock_mode=X, lock_granularity=TABLE)
```

**Root Cause:** Session 48 started a long-running transaction 3 minutes ago and escalated to a table lock on Production.Product, blocking all queries trying to access that table.

**Resolution Steps:**
1. Identify session 48 owner: `login_name=app_user`, `program_name=MyApp`
2. Kill session 48: `KILL 48` (if necessary)
3. Fix application code to use shorter transactions
4. Review lock escalation settings

---

## 🔧 Implementation Recommendations

### Priority 1: REMOVE Query Store Dependencies (Critical)

**Files to Update:**
1. `queries/query_performance_monitoring_metrics.go`
   - Remove `WaitQuery` (lines 463-569)
   - Remove `is_query_store_on` checks from `SlowQuery` (lines 373-377)
   - Remove `is_query_store_on` check from `BlockingSessionsQuery` (line 458)

**Replacement:**
- Use `sys.dm_exec_requests` for real-time wait stats per query
- Use `sys.dm_os_wait_stats` for server-wide wait stats (optional)

---

### Priority 2: Add query_hash to Active Queries (High)

**File:** `queries/query_performance_monitoring_metrics.go`
**Line:** 599 (ActiveRunningQueriesQuery)

**Add:**
```sql
r_wait.query_hash AS query_id,  -- Correlation key
```

**Model Update:** Already exists in `models.ActiveRunningQuery.QueryID`

---

### Priority 3: Enhance Session Context (Medium)

**Add to ActiveRunningQueriesQuery:**
```sql
s_wait.program_name,
s_wait.status AS session_status,
s_wait.client_interface_name
```

**Model Update:** Add to `models.ActiveRunningQuery`

---

### Priority 4: Add Transaction Context (Medium)

**Add to ActiveRunningQueriesQuery:**
```sql
r_wait.transaction_id,
r_wait.open_transaction_count,
ta.transaction_begin_time,
ta.transaction_state
-- JOIN:
LEFT JOIN sys.dm_tran_active_transactions ta ON r_wait.transaction_id = ta.transaction_id
```

**Model Update:** Add to `models.ActiveRunningQuery`

---

### Priority 5: Add Parallel Execution Details (Low)

**Add to ActiveRunningQueriesQuery:**
```sql
r_wait.dop AS degree_of_parallelism,
r_wait.parallel_worker_count
```

**Model Update:** Add to `models.ActiveRunningQuery`

---

## 📋 Testing Checklist

After implementing changes:

- [ ] Remove all Query Store dependencies
- [ ] Verify query_hash correlation between slow and active queries
- [ ] Test blocking chain resolution (3-level deep blocking)
- [ ] Test locked object resolution for blockers
- [ ] Verify transaction context in long-running transactions
- [ ] Test parallel query details with CXPACKET waits
- [ ] Validate NRDB ingestion with dimensional metrics
- [ ] Test all user flows end-to-end
- [ ] Verify no performance impact from changes
- [ ] Run all 20 dmv-populator scenarios

---

## 📊 Performance Impact Assessment

### Current Issues:
- ❌ Query Store iteration across all databases: **HIGH IMPACT**
- ❌ Filtering only Query Store enabled databases: **COVERAGE LOSS**

### After Fixes:
- ✅ Direct DMV queries: **LOW IMPACT**
- ✅ Full database coverage: **COMPLETE COVERAGE**
- ✅ Real-time data: **BETTER RCA**

---

## 🎓 Key RCA Principles Applied

1. **Correlation over Causation** - Link related data through keys (query_hash, session_id, transaction_id)
2. **Context is King** - Capture who, what, when, where (session details, transaction context)
3. **Drill-down Path** - Design metrics for progressive investigation
4. **Real-time > Historical** - Use DMVs for current state, not aggregated history
5. **Dimensional Thinking** - Metrics (values) + Attributes (context) = Powerful queries

---

## 📚 Reference Documentation

- **sys.dm_exec_requests**: Real-time executing queries
- **sys.dm_exec_sessions**: Session-level context
- **sys.dm_exec_query_stats**: Historical query statistics
- **sys.dm_tran_locks**: Lock information
- **sys.dm_tran_active_transactions**: Transaction details
- **sys.dm_exec_sql_text()**: Query text retrieval
- **sys.dm_exec_query_plan()**: Execution plan retrieval

---

**Document Version:** 1.0
**Last Updated:** 2025-11-22
**Status:** REQUIRES IMMEDIATE ACTION (Query Store removal)
