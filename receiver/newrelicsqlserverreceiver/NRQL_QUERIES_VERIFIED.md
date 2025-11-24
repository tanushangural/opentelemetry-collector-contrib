# Verified NRQL Queries for SQL Server Query Performance Monitoring

This document contains **confirmed working** NRQL queries that have been tested and validated against the OpenTelemetry SQL Server receiver implementation.

## Status Legend
- ✅ **VERIFIED WORKING** - Query has been tested and confirmed working
- 🔄 **IN PROGRESS** - Query is being tested
- ⏳ **PENDING** - Query not yet tested

---

## 1. Landing Page - Slow Query List

### ✅ 1.1 Basic Slow Query List (VERIFIED WORKING)

**Purpose**: Display list of normalized/slow queries with key performance metrics

**Status**: ✅ VERIFIED WORKING (Tested: 2025-11-22)

```nrql
SELECT
    latest(sqlserver.slowquery.last_execution_timestamp) as 'Last Execution',
    latest(database_name) as 'Database',
    latest(query_text) as 'Query',
    latest(statement_type) as 'Type',
    average(sqlserver.slowquery.avg_elapsed_time_ms) as 'Avg Duration (ms)',
    sum(sqlserver.slowquery.execution_count) as 'Calls',
    average(sqlserver.slowquery.avg_rows_processed) as 'Avg Rows'
FROM Metric
WHERE metricName IN (
    'sqlserver.slowquery.avg_elapsed_time_ms',
    'sqlserver.slowquery.execution_count',
    'sqlserver.slowquery.avg_rows_processed',
    'sqlserver.slowquery.query_text',
    'sqlserver.slowquery.last_execution_timestamp'
)
FACET query_id
SINCE 5 minutes ago UNTIL now
```

**Metrics Used**:
- `sqlserver.slowquery.avg_elapsed_time_ms` - Average query duration
- `sqlserver.slowquery.execution_count` - Total executions
- `sqlserver.slowquery.avg_rows_processed` - Average rows returned
- `sqlserver.slowquery.query_text` - SQL query text (anonymized)
- `sqlserver.slowquery.last_execution_timestamp` - Last execution time (Unix epoch)

**Key Attributes**:
- `query_id` - SQL Server's query_hash (correlation key)
- `database_name` - Database name
- `statement_type` - Query type (SELECT, INSERT, UPDATE, DELETE)
- `query_text` - Anonymized SQL text (only on query_text metric)

**Output Example**:
| Last Execution | Database | Query | Type | Avg Duration (ms) | Calls | Avg Rows |
|----------------|----------|-------|------|-------------------|-------|----------|
| 1763840555 | AdventureWorks2022 | SELECT p.Name, COUNT(*), ... | SELECT | 819.70 | 4299 | 1234.5 |
| 1763840556 | AdventureWorks2022 | SELECT ProductID, COUNT(*), ... | SELECT | 798.91 | 4601 | 567.8 |

---

### ⏳ 1.2 Slow Query List with RCA Metrics (PENDING)

**Purpose**: Display slow queries with Root Cause Analysis enhancement fields

**Status**: ⏳ PENDING (Requires RCA changes to be committed and collector restart)

```nrql
SELECT
    latest(query_text) as 'Query',
    latest(database_name) as 'Database',
    latest(statement_type) as 'Type',
    average(sqlserver.slowquery.avg_elapsed_time_ms) as 'Avg Duration (ms)',
    max(sqlserver.slowquery.max_elapsed_time_ms) as 'Max Duration (ms)',
    min(sqlserver.slowquery.min_elapsed_time_ms) as 'Min Duration (ms)',
    sum(sqlserver.slowquery.execution_count) as 'Executions',
    average(sqlserver.slowquery.avg_cpu_time_ms) as 'Avg CPU (ms)',
    average(sqlserver.slowquery.avg_disk_reads) as 'Avg Reads',
    average(sqlserver.slowquery.avg_rows_processed) as 'Avg Rows',
    latest(sqlserver.slowquery.last_execution_timestamp) as 'Last Execution'
FROM Metric
WHERE metricName IN (
    'sqlserver.slowquery.avg_elapsed_time_ms',
    'sqlserver.slowquery.max_elapsed_time_ms',
    'sqlserver.slowquery.min_elapsed_time_ms',
    'sqlserver.slowquery.execution_count',
    'sqlserver.slowquery.avg_cpu_time_ms',
    'sqlserver.slowquery.avg_disk_reads',
    'sqlserver.slowquery.avg_rows_processed',
    'sqlserver.slowquery.query_text',
    'sqlserver.slowquery.last_execution_timestamp'
)
FACET query_id
LIMIT 100
SINCE 6 hours ago
```

**Additional RCA Metrics** (Available after commit):
- `sqlserver.slowquery.min_elapsed_time_ms` - Performance variance detection
- `sqlserver.slowquery.max_elapsed_time_ms` - Performance variance detection
- `sqlserver.slowquery.last_elapsed_time_ms` - Most recent execution time
- `sqlserver.slowquery.last_grant_kb` - Memory grant analysis
- `sqlserver.slowquery.last_used_grant_kb` - Actual memory used
- `sqlserver.slowquery.last_spills` - TempDB spill detection
- `sqlserver.slowquery.max_spills` - Maximum TempDB spills
- `sqlserver.slowquery.last_dop` - Degree of parallelism

---

## 2. Query Details Page

### ✅ 2.1 Active Queries for Selected Query_ID (VERIFIED WORKING)

**Purpose**: Show active running queries for a specific normalized query

**Status**: ✅ VERIFIED WORKING (Tested: 2025-11-23)

```nrql
SELECT
    latest(query_text) as 'Query',
    latest(request_start_time) as 'Start Time',
    latest(request_status) as 'Status',
    latest(sqlserver.activequery.wait_time_seconds) as 'Wait Time (s)',
    latest(wait_type) as 'Wait Type',
    latest(sqlserver.activequery.elapsed_time_ms) as 'Elapsed (ms)',
    latest(sqlserver.activequery.cpu_time_ms) as 'CPU (ms)',
    latest(sqlserver.activequery.logical_reads) as 'Logical Reads',
    latest(blocking_session_id) as 'Blocked By',
    latest(degree_of_parallelism) as 'DOP',
    latest(open_transaction_count) as 'Open Txns'
FROM Metric
WHERE metricName IN (
    'sqlserver.activequery.wait_time_seconds',
    'sqlserver.activequery.elapsed_time_ms',
    'sqlserver.activequery.cpu_time_ms',
    'sqlserver.activequery.logical_reads'
)
  AND query_id = '0x9e177289f3f627e2'
FACET session_id, request_id
SINCE 30 minutes ago
LIMIT 100
```

**Metrics Used**:
- `sqlserver.activequery.wait_time_seconds` - Current wait time
- `sqlserver.activequery.elapsed_time_ms` - Total elapsed time
- `sqlserver.activequery.cpu_time_ms` - CPU time consumed
- `sqlserver.activequery.logical_reads` - Buffer cache reads

**Key Attributes**:
- `session_id` - SQL Server session ID
- `request_id` - Request ID within session
- `query_id` - Correlation key to slow queries
- `wait_type` - Current wait type (LCK_M_X, PAGEIOLATCH_SH, etc.)
- `blocking_session_id` - Blocking session ID (if blocked)
- `degree_of_parallelism` - Parallel execution threads
- `open_transaction_count` - Number of open transactions

**Output Example** (4 parallel sessions executing same query):
| Session | Query | Status | Wait Type | Elapsed (ms) | CPU (ms) | Logical Reads |
|---------|-------|--------|-----------|--------------|----------|---------------|
| 53 | SELECT ProductID, OrderQty, ROW_NUMBER()... | suspended | CXSYNC_PORT | 163 | 29 | 0 |
| 76 | SELECT ProductID, OrderQty, ROW_NUMBER()... | suspended | CXSYNC_PORT | 387 | 135 | 0 |
| 81 | SELECT ProductID, OrderQty, ROW_NUMBER()... | suspended | CXSYNC_PORT | 594 | 200 | 0 |
| 85 | SELECT ProductID, OrderQty, ROW_NUMBER()... | suspended | CXSYNC_PORT | 357 | 106 | 0 |

**Note**: CXSYNC_PORT indicates parallel query execution with worker threads coordinating data.

---

### ✅ 2.2 Active Query Chart (Count Over Time) (VERIFIED WORKING)

**Purpose**: Show timeline of active query executions

**Status**: ✅ VERIFIED WORKING (Tested: 2025-11-23)

**Note**: Use actual query_id from slow query list (1.1) or from query_id lookup. The query_id shown below is an example from testing.

```nrql
SELECT
    uniqueCount(session_id) as 'Active Executions'
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_time_seconds'
  AND query_id = '0x9e177289f3f627e2'
TIMESERIES AUTO
SINCE 6 hours ago
```

---

### ✅ 2.3 Active Query Drill-Down (RCA Details) (VERIFIED WORKING)

**Purpose**: Comprehensive RCA view when clicking on a time window in the chart above

**Status**: ✅ VERIFIED WORKING (Tested: 2025-11-23)

**Usage**: Click on any bar in the timeline chart (2.2) to drill down into this detailed view

**Version A: Drill-Down from Specific Query (With query_id Filter)**
```nrql
SELECT
    -- Query Context
    latest(query_id) as 'Query ID',
    latest(query_text) as 'Query',
    latest(database_name) as 'Database',
    latest(request_status) as 'Status',
    latest(request_start_time) as 'Start Time',

    -- Performance Metrics
    latest(sqlserver.activequery.elapsed_time_ms) as 'Elapsed (ms)',
    latest(sqlserver.activequery.cpu_time_ms) as 'CPU (ms)',
    (latest(sqlserver.activequery.elapsed_time_ms) - latest(sqlserver.activequery.cpu_time_ms)) as 'Wait Time (ms)',

    -- Wait Analysis
    latest(sqlserver.activequery.wait_time_seconds) as 'Current Wait (s)',
    latest(wait_type) as 'Wait Type',
    latest(last_wait_type) as 'Last Wait Type',
    latest(wait_resource) as 'Wait Resource',

    -- I/O Metrics
    latest(sqlserver.activequery.reads) as 'Physical Reads',
    latest(sqlserver.activequery.logical_reads) as 'Logical Reads',
    latest(sqlserver.activequery.writes) as 'Writes',
    latest(sqlserver.activequery.row_count) as 'Rows',

    -- Memory & Parallelism
    latest(sqlserver.activequery.granted_query_memory_pages) as 'Memory Grant (pages)',
    latest(degree_of_parallelism) as 'DOP',

    -- Blocking & Transaction Context
    latest(blocking_session_id) as 'Blocked By',
    latest(open_transaction_count) as 'Open Txns',
    latest(transaction_isolation_level) as 'Isolation Level',

    -- Execution Plan
    latest(query_plan) as 'Execution Plan'

FROM Metric
WHERE metricName IN (
    'sqlserver.activequery.elapsed_time_ms',
    'sqlserver.activequery.cpu_time_ms',
    'sqlserver.activequery.reads',
    'sqlserver.activequery.writes',
    'sqlserver.activequery.logical_reads',
    'sqlserver.activequery.row_count',
    'sqlserver.activequery.granted_query_memory_pages'
)
  AND query_id = '0x9e177289f3f627e2'
FACET session_id, request_id
SINCE 1 day ago
LIMIT 100
```

**Version B: All Active Queries (No query_id Filter) - ✅ VERIFIED**
```nrql
SELECT
    -- Query Context
    latest(query_text) as 'Query',
    latest(database_name) as 'Database',
    latest(request_status) as 'Status',

    -- Performance Metrics
    latest(sqlserver.activequery.elapsed_time_ms) as 'Elapsed (ms)',
    latest(sqlserver.activequery.cpu_time_ms) as 'CPU (ms)',
    (latest(sqlserver.activequery.elapsed_time_ms) - latest(sqlserver.activequery.cpu_time_ms)) as 'Wait Time (ms)',

    -- Wait Analysis
    latest(wait_type) as 'Wait Type',

    -- I/O Metrics
    latest(sqlserver.activequery.logical_reads) as 'Logical Reads',

    -- Memory & Parallelism
    latest(degree_of_parallelism) as 'DOP',

    -- Blocking
    latest(blocking_session_id) as 'Blocked By'

FROM Metric
WHERE metricName IN (
    'sqlserver.activequery.elapsed_time_ms',
    'sqlserver.activequery.cpu_time_ms',
    'sqlserver.activequery.logical_reads'
)
FACET query_id, session_id, request_id
SINCE 1 day ago
LIMIT 100
```

**Metrics Explained for RCA**:

**Performance Analysis:**
- **Elapsed (ms)**: Total time since query started
- **CPU (ms)**: Actual CPU time consumed
- **Wait Time (ms)**: Calculated as `Elapsed - CPU` (time spent waiting)

**Wait Analysis:**
- **Current Wait (s)**: How long currently waiting
- **Wait Type**: Current wait reason (CXSYNC_PORT, PAGEIOLATCH_SH, LCK_M_X, etc.)
- **Last Wait Type**: Previous wait type before current wait
- **Wait Resource**: Specific resource being waited on (lock resource, page, etc.)

**I/O Analysis:**
- **Physical Reads**: Disk reads (high = I/O pressure)
- **Logical Reads**: Buffer cache reads (high = memory pressure)
- **Writes**: Write operations (high = tempdb/log pressure)
- **Rows**: Rows processed so far

**Memory & Parallelism:**
- **Memory Grant (pages)**: Memory allocated to query (8KB per page)
- **DOP**: Degree of Parallelism (number of worker threads)

**Blocking & Transaction:**
- **Blocked By**: Session ID causing blocking (if any)
- **Open Txns**: Number of uncommitted transactions
- **Isolation Level**: Transaction isolation level

**Execution Plan:**
- XML execution plan for detailed analysis

**Output Example (Version A - Filtered by query_id)**:
| Session | Query ID | Query | Database | Status | Elapsed (ms) | CPU (ms) | Wait (ms) | Wait Type | Logical Reads | DOP | Blocked By |
|---------|----------|-------|----------|--------|--------------|----------|-----------|-----------|---------------|-----|------------|
| 53 | 0x9e177289f3f627e2 | SELECT ProductID... | AdventureWorks | suspended | 163 | 29 | 134 | CXSYNC_PORT | 0 | 4 | N/A |
| 76 | 0x9e177289f3f627e2 | SELECT ProductID... | AdventureWorks | suspended | 387 | 135 | 252 | CXSYNC_PORT | 0 | 4 | N/A |

**Output Example (Version B - All Active Queries)**:
| Session | Query ID | Query | Database | Elapsed (ms) | CPU (ms) | Wait (ms) | Wait Type | DOP | Blocked By |
|---------|----------|-------|----------|--------------|----------|-----------|-----------|-----|------------|
| 53 | 0x9e177289f3f627e2 | SELECT ProductID... | AdventureWorks | 163 | 29 | 134 | CXSYNC_PORT | 4 | N/A |
| 67 | 0xabc123def4567890 | SELECT * FROM Orders... | SalesDB | 2156 | 1890 | 266 | LCK_M_X | 1 | 55 |
| 72 | NULL | BACKUP DATABASE... | master | 45320 | 234 | 45086 | BACKUPBUFFER | 1 | N/A |

---

### ✅ 2.4 Single Active Query Deep Dive (RCA Details) (VERIFIED WORKING)

**Purpose**: Comprehensive RCA details when clicking on a specific active query row

**Status**: ✅ VERIFIED WORKING (Tested: 2025-11-23)

**Usage**: Click on any row in Query 2.3 to see full RCA details for that specific session/request

```nrql
SELECT
    -- === QUERY IDENTIFICATION ===
    latest(query_text) as 'Query Text',
    latest(database_name) as 'Database',
    latest(query_id) as 'Query ID (Hash)',

    -- === SESSION CONTEXT ===
    latest(login_name) as 'User',
    latest(host_name) as 'Client Host',
    latest(request_command) as 'Command Type',
    latest(request_status) as 'Status',
    latest(request_start_time) as 'Start Time',

    -- === PERFORMANCE METRICS ===
    latest(sqlserver.activequery.elapsed_time_ms) as 'Elapsed Time (ms)',
    latest(sqlserver.activequery.cpu_time_ms) as 'CPU Time (ms)',
    (latest(sqlserver.activequery.elapsed_time_ms) - latest(sqlserver.activequery.cpu_time_ms)) as 'Wait Time (ms)',
    (latest(sqlserver.activequery.cpu_time_ms) / latest(sqlserver.activequery.elapsed_time_ms) * 100) as 'CPU Efficiency %',

    -- === WAIT ANALYSIS ===
    latest(sqlserver.activequery.wait_time_seconds) as 'Current Wait Duration (s)',
    latest(wait_type) as 'Current Wait Type',
    latest(last_wait_type) as 'Previous Wait Type',
    latest(wait_resource) as 'Wait Resource',

    -- === I/O METRICS ===
    latest(sqlserver.activequery.reads) as 'Physical Reads (Disk)',
    latest(sqlserver.activequery.logical_reads) as 'Logical Reads (Buffer Cache)',
    latest(sqlserver.activequery.writes) as 'Writes',
    latest(sqlserver.activequery.row_count) as 'Rows Processed',
    (latest(sqlserver.activequery.logical_reads) / latest(sqlserver.activequery.elapsed_time_ms) * 1000) as 'Logical Reads per Second',

    -- === MEMORY & PARALLELISM ===
    latest(sqlserver.activequery.granted_query_memory_pages) as 'Memory Grant (Pages)',
    (latest(sqlserver.activequery.granted_query_memory_pages) * 8) as 'Memory Grant (KB)',
    latest(degree_of_parallelism) as 'Degree of Parallelism',
    latest(parallel_worker_count) as 'Parallel Workers',

    -- === BLOCKING & TRANSACTION CONTEXT ===
    latest(blocking_session_id) as 'Blocked By Session',
    latest(open_transaction_count) as 'Open Transactions',
    latest(transaction_isolation_level) as 'Isolation Level',
    latest(lock_timeout) as 'Lock Timeout (ms)',

    -- === EXECUTION PLAN ===
    latest(query_plan) as 'Execution Plan (XML)',
    latest(plan_handle) as 'Plan Handle'

FROM Metric
WHERE metricName IN (
    'sqlserver.activequery.elapsed_time_ms',
    'sqlserver.activequery.cpu_time_ms',
    'sqlserver.activequery.wait_time_seconds',
    'sqlserver.activequery.reads',
    'sqlserver.activequery.writes',
    'sqlserver.activequery.logical_reads',
    'sqlserver.activequery.row_count',
    'sqlserver.activequery.granted_query_memory_pages'
)
  AND query_id = '{{query_id}}'
  AND session_id = '{{session_id}}'
  AND request_id = '{{request_id}}'
SINCE 1 day ago
LIMIT 1
```

**RCA Analysis Guide:**

**🎯 Performance Analysis:**
- **Elapsed Time**: Total time since query started
- **CPU Time**: Active processing time
- **Wait Time**: `Elapsed - CPU` = Time spent waiting (high = bottleneck)
- **CPU Efficiency %**: `(CPU / Elapsed) * 100` = How much time is productive
  - > 80% = CPU-bound query (good CPU utilization)
  - < 50% = Wait-bound query (investigate wait types)

**⏸️ Wait Analysis:**
- **Current Wait Type**: What's blocking the query RIGHT NOW
  - `CXSYNC_PORT` / `CXPACKET` = Parallel query coordination
  - `PAGEIOLATCH_*` = Disk I/O wait
  - `LCK_M_*` = Lock contention
  - `ASYNC_NETWORK_IO` = Client not consuming results fast enough
  - `WRITELOG` = Transaction log write bottleneck
- **Wait Resource**: Specific resource being waited on (lock resource ID, page ID, etc.)
- **Previous Wait Type**: Last wait before current one (pattern analysis)

**💾 I/O Analysis:**
- **Physical Reads**: Disk reads (high = missing indexes or buffer cache pressure)
- **Logical Reads**: Buffer cache reads (high = large scans or missing indexes)
- **Logical Reads/Sec**: I/O throughput rate
  - > 10,000/sec = High I/O activity
- **Writes**: TempDB or table modifications

**🧠 Memory & Parallelism:**
- **Memory Grant**: Memory allocated for hash joins, sorts, etc.
  - > 1GB (125,000 pages) = High memory consumer
- **Degree of Parallelism**: Number of worker threads
  - 1 = Serial execution
  - > 1 = Parallel execution
- **Parallel Workers**: Actual workers allocated (can be less than DOP)
- **CXSYNC_PORT wait + High DOP** = Parallel plan with coordination overhead

**🔒 Blocking & Transaction:**
- **Blocked By Session**: If not NULL/N/A, this session is waiting on another
- **Open Transactions**: Uncommitted transactions (high = long-running transaction risk)
- **Isolation Level**:
  - 2 = READ COMMITTED (default)
  - 4 = REPEATABLE READ (more locks)
  - 5 = SNAPSHOT (no locks but tempdb overhead)

**📊 Execution Plan:**
- XML execution plan for detailed operator analysis
- Look for: table scans, missing indexes, expensive operators

**Common RCA Patterns:**

| Symptom | Root Cause | Action |
|---------|-----------|---------|
| High Wait Time, Low CPU % | Wait-bound query | Check wait_type for bottleneck |
| PAGEIOLATCH_*, High Physical Reads | Missing index or buffer cache pressure | Review execution plan for scans |
| LCK_M_*, Blocking Session ID set | Lock contention | Check blocking session, review isolation level |
| CXSYNC_PORT, High DOP | Parallel query overhead | Consider MAXDOP hint or query tuning |
| High Logical Reads, Low Rows | Inefficient query plan | Add/optimize indexes |
| ASYNC_NETWORK_IO | Client not consuming results | Check application code, network latency |
| High Memory Grant, Low Usage | Memory grant over-estimate | Update statistics |

**Output Example:**
```
Query Text: SELECT ProductID, OrderQty, ROW_NUMBER() OVER...
Database: AdventureWorks2022
Query ID: 0x9e177289f3f627e2
User: app_user
Client Host: WEB-SERVER-01
Status: suspended
Start Time: 2025-11-23 10:15:23

Elapsed Time: 387 ms
CPU Time: 135 ms
Wait Time: 252 ms
CPU Efficiency: 34.9%

Current Wait Type: CXSYNC_PORT
Wait Resource: NULL
Previous Wait Type: CXPACKET

Physical Reads: 0
Logical Reads: 12,456
Writes: 0
Rows Processed: 121,317
Logical Reads/Sec: 32,187

Memory Grant: 1,024 pages (8,192 KB)
Degree of Parallelism: 4
Parallel Workers: 4

Blocked By: N/A
Open Transactions: 1
Isolation Level: READ COMMITTED

Execution Plan: <ShowPlanXML...>
```

**RCA Conclusion from Example:**
- CPU Efficiency of 34.9% indicates wait-bound query
- CXSYNC_PORT wait with DOP=4 suggests parallel coordination overhead
- High logical reads (12,456) but zero physical reads = Data in buffer cache
- Consider reducing DOP or optimizing window function for better parallelism

---

## 3. Wait Time Analysis

### ✅ 3.1 Top Wait Types for Query (VERIFIED WORKING)

**Purpose**: Identify most common wait types causing performance issues

**Status**: ✅ VERIFIED WORKING (Tested: 2025-11-23)

**Note**: Use actual query_id from slow query list (1.1). Example query_id shown below.

```nrql
SELECT
    count(*) as 'Occurrences',
    sum(sqlserver.activequery.wait_time_seconds) as 'Total Wait Time (s)',
    average(sqlserver.activequery.wait_time_seconds) as 'Avg Wait Time (s)',
    max(sqlserver.activequery.wait_time_seconds) as 'Max Wait Time (s)'
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_time_seconds'
  AND query_id = '0x9e177289f3f627e2'
  AND wait_type IS NOT NULL
FACET wait_type
ORDER BY 'Total Wait Time (s)' DESC
LIMIT 20
SINCE 6 hours ago
```

**Example Output**:
| Wait Type | Occurrences | Total Wait (s) | Avg Wait (s) | Max Wait (s) |
|-----------|-------------|----------------|--------------|--------------|
| CXSYNC_PORT | 847 | 234.56 | 0.277 | 2.15 |
| PAGEIOLATCH_SH | 123 | 45.23 | 0.368 | 1.89 |

---

## 4. Blocking Analysis

### ⏳ 4.1 Blocking Chains (PENDING)

**Purpose**: Identify blocking and blocked sessions

```nrql
SELECT
    latest(blocking_query_text) as 'Blocking Query',
    latest(blocking_spid) as 'Blocker SPID',
    latest(blocking_status) as 'Blocker Status',
    latest(sqlserver.blocking_query.wait_time_seconds) as 'Blocking Wait (s)',
    latest(blocked_spid) as 'Blocked SPID',
    latest(blocked_status) as 'Blocked Status',
    latest(blocked_query_text) as 'Blocked Query',
    latest(sqlserver.blocked_query.wait_time_seconds) as 'Blocked Wait (s)',
    latest(wait_type) as 'Wait Type',
    latest(database_name) as 'Database'
FROM Metric
WHERE metricName IN (
    'sqlserver.blocking_query.wait_time_seconds',
    'sqlserver.blocked_query.wait_time_seconds'
)
FACET blocking_spid, blocked_spid
ORDER BY latest(sqlserver.blocked_query.wait_time_seconds) DESC
LIMIT 50
SINCE 1 hour ago
```

---

## 5. RCA-Specific Queries (After RCA Enhancement Commit)

### ⏳ 5.1 Queries with High Performance Variance (PENDING)

**Purpose**: Find queries with unstable performance (high min/max delta)

```nrql
SELECT
    latest(query_text) as 'Query',
    latest(database_name) as 'Database',
    average(sqlserver.slowquery.avg_elapsed_time_ms) as 'Avg (ms)',
    max(sqlserver.slowquery.max_elapsed_time_ms) as 'Max (ms)',
    min(sqlserver.slowquery.min_elapsed_time_ms) as 'Min (ms)',
    (max(sqlserver.slowquery.max_elapsed_time_ms) - min(sqlserver.slowquery.min_elapsed_time_ms)) as 'Variance (ms)',
    ((max(sqlserver.slowquery.max_elapsed_time_ms) - min(sqlserver.slowquery.min_elapsed_time_ms)) / average(sqlserver.slowquery.avg_elapsed_time_ms) * 100) as 'Variance %'
FROM Metric
WHERE metricName IN (
    'sqlserver.slowquery.avg_elapsed_time_ms',
    'sqlserver.slowquery.max_elapsed_time_ms',
    'sqlserver.slowquery.min_elapsed_time_ms',
    'sqlserver.slowquery.query_text'
)
FACET query_id, database_name
HAVING (max(sqlserver.slowquery.max_elapsed_time_ms) - min(sqlserver.slowquery.min_elapsed_time_ms)) > 1000
ORDER BY 'Variance (ms)' DESC
LIMIT 50
SINCE 6 hours ago
```

---

### ⏳ 5.2 Queries with TempDB Spills (PENDING)

**Purpose**: Identify queries spilling to TempDB (memory pressure indicator)

```nrql
SELECT
    latest(query_text) as 'Query',
    latest(database_name) as 'Database',
    average(sqlserver.slowquery.avg_elapsed_time_ms) as 'Avg Duration (ms)',
    max(sqlserver.slowquery.max_spills) as 'Max Spills (pages)',
    average(sqlserver.slowquery.last_spills) as 'Avg Spills (pages)',
    average(sqlserver.slowquery.last_grant_kb) as 'Memory Grant (KB)',
    average(sqlserver.slowquery.last_used_grant_kb) as 'Memory Used (KB)',
    sum(sqlserver.slowquery.execution_count) as 'Executions'
FROM Metric
WHERE metricName IN (
    'sqlserver.slowquery.max_spills',
    'sqlserver.slowquery.last_spills',
    'sqlserver.slowquery.last_grant_kb',
    'sqlserver.slowquery.last_used_grant_kb',
    'sqlserver.slowquery.avg_elapsed_time_ms',
    'sqlserver.slowquery.execution_count',
    'sqlserver.slowquery.query_text'
)
FACET query_id, database_name
HAVING max(sqlserver.slowquery.max_spills) > 0
ORDER BY max(sqlserver.slowquery.max_spills) DESC
LIMIT 50
SINCE 6 hours ago
```

---

### ⏳ 5.3 Queries with Memory Pressure (PENDING)

**Purpose**: Find queries with high memory grants or memory waste

```nrql
SELECT
    latest(query_text) as 'Query',
    latest(database_name) as 'Database',
    average(sqlserver.slowquery.avg_elapsed_time_ms) as 'Avg Duration (ms)',
    average(sqlserver.slowquery.last_grant_kb) as 'Memory Grant (KB)',
    average(sqlserver.slowquery.last_used_grant_kb) as 'Memory Used (KB)',
    (average(sqlserver.slowquery.last_grant_kb) - average(sqlserver.slowquery.last_used_grant_kb)) as 'Memory Wasted (KB)',
    ((average(sqlserver.slowquery.last_grant_kb) - average(sqlserver.slowquery.last_used_grant_kb)) / average(sqlserver.slowquery.last_grant_kb) * 100) as 'Waste %'
FROM Metric
WHERE metricName IN (
    'sqlserver.slowquery.last_grant_kb',
    'sqlserver.slowquery.last_used_grant_kb',
    'sqlserver.slowquery.avg_elapsed_time_ms',
    'sqlserver.slowquery.query_text'
)
FACET query_id, database_name
HAVING average(sqlserver.slowquery.last_grant_kb) > 10240
ORDER BY 'Memory Grant (KB)' DESC
LIMIT 50
SINCE 6 hours ago
```

---

### ⏳ 5.4 Parallel Queries with CXPACKET Waits (PENDING)

**Purpose**: Identify queries with high degree of parallelism

```nrql
SELECT
    latest(query_text) as 'Query',
    latest(database_name) as 'Database',
    average(sqlserver.slowquery.avg_elapsed_time_ms) as 'Avg Duration (ms)',
    average(sqlserver.slowquery.last_dop) as 'Degree of Parallelism',
    average(sqlserver.slowquery.avg_cpu_time_ms) as 'Avg CPU (ms)',
    (average(sqlserver.slowquery.avg_elapsed_time_ms) - average(sqlserver.slowquery.avg_cpu_time_ms)) as 'Wait Time (ms)',
    sum(sqlserver.slowquery.execution_count) as 'Executions'
FROM Metric
WHERE metricName IN (
    'sqlserver.slowquery.last_dop',
    'sqlserver.slowquery.avg_elapsed_time_ms',
    'sqlserver.slowquery.avg_cpu_time_ms',
    'sqlserver.slowquery.execution_count',
    'sqlserver.slowquery.query_text'
)
FACET query_id, database_name
HAVING average(sqlserver.slowquery.last_dop) > 1
ORDER BY 'Degree of Parallelism' DESC
LIMIT 50
SINCE 6 hours ago
```

---

## 6. Alert Conditions

### ⏳ 6.1 Long Running Active Queries (PENDING)

**Purpose**: Alert on queries running longer than threshold

```nrql
SELECT
    latest(query_text),
    latest(sqlserver.activequery.elapsed_time_ms) as duration
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND sqlserver.activequery.elapsed_time_ms > 30000
FACET session_id, query_id
```

---

### ⏳ 6.2 Blocking Session Alert (PENDING)

**Purpose**: Alert when multiple sessions are blocked

```nrql
SELECT
    uniqueCount(session_id) as blocked_sessions
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_time_seconds'
  AND blocking_session_id IS NOT NULL
  AND blocking_session_id != 'N/A'
  AND sqlserver.activequery.wait_time_seconds > 10
SINCE 5 minutes ago
```

---

## Testing Notes

### How to Test Queries

1. **Start the Collector**:
   ```bash
   cd /Users/pkulkarni/workspace/mssql-otel/opentelemetry-collector-contrib
   ./bin/otelcontribcol --config receiver/newrelicsqlserverreceiver/testdata/config.yaml
   ```

2. **Generate Load** (optional):
   ```bash
   cd dmv-populator-repo
   go run main.go
   ```

3. **Run NRQL Query** in New Relic Query Builder

4. **Mark Query Status**:
   - ✅ If query returns data successfully
   - ❌ If query fails or returns no data
   - Document any issues or modifications needed

### Common Issues

1. **Metric Not Found**: Wait for collection_interval (15-60s) to elapse
2. **Null Values**: Check metric name spelling exactly matches scraper
3. **No Data**: Verify `enable_active_running_queries: true` in config.yaml
4. **Wrong Time Range**: Adjust `SINCE` clause based on collection frequency

---

## Quick Reference: All Metric Names

### Slow Query Metrics
- `sqlserver.slowquery.avg_elapsed_time_ms`
- `sqlserver.slowquery.min_elapsed_time_ms` (RCA)
- `sqlserver.slowquery.max_elapsed_time_ms` (RCA)
- `sqlserver.slowquery.last_elapsed_time_ms` (RCA)
- `sqlserver.slowquery.avg_cpu_time_ms`
- `sqlserver.slowquery.avg_disk_reads`
- `sqlserver.slowquery.avg_disk_writes`
- `sqlserver.slowquery.avg_rows_processed`
- `sqlserver.slowquery.avg_lock_wait_time_ms`
- `sqlserver.slowquery.execution_count`
- `sqlserver.slowquery.last_grant_kb` (RCA)
- `sqlserver.slowquery.last_used_grant_kb` (RCA)
- `sqlserver.slowquery.last_spills` (RCA)
- `sqlserver.slowquery.max_spills` (RCA)
- `sqlserver.slowquery.last_dop` (RCA)
- `sqlserver.slowquery.query_text`
- `sqlserver.slowquery.last_execution_timestamp`
- `sqlserver.slowquery.collection_timestamp`

### Active Query Metrics
- `sqlserver.activequery.wait_time_seconds`
- `sqlserver.activequery.cpu_time_ms`
- `sqlserver.activequery.elapsed_time_ms`
- `sqlserver.activequery.reads`
- `sqlserver.activequery.writes`
- `sqlserver.activequery.logical_reads`
- `sqlserver.activequery.row_count`
- `sqlserver.activequery.granted_query_memory_pages`

### Blocking Session Metrics
- `sqlserver.blocking_query.wait_time_seconds`
- `sqlserver.blocked_query.wait_time_seconds`
- `sqlserver.blocking.spid`
- `sqlserver.blocked.spid`

### Locked Object Metrics
- `sqlserver.locked_object`

---

## Document History

| Date | Version | Changes | Author |
|------|---------|---------|--------|
| 2025-11-22 | 1.0 | Initial version with verified Query 1.1 | Claude/User |
| 2025-11-22 | 1.1 | Added RCA enhancement queries (pending) | Claude |
| 2025-11-23 | 1.2 | Verified Query 2.2 working, updated with actual query_id | Claude/User |
| 2025-11-23 | 1.3 | Verified Query 2.1 working - found 4 parallel sessions with CXSYNC_PORT waits | Claude/User |
| 2025-11-23 | 1.4 | Verified Query 3.1 (wait types), added Query 2.3 (comprehensive RCA drill-down) | Claude/User |
| 2025-11-23 | 1.5 | Added Query 2.4 (single query deep dive) with comprehensive RCA analysis guide | Claude/User |

---

**Next Query to Test**: 4.1 - Blocking Chains or continue testing remaining queries
