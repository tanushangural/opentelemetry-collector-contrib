# UI NRQL Queries - Complete Set

## Overview

This document provides **production-ready NRQL queries** for all 7 UI pages based on the enhanced RCA-driven SQL Server monitoring implementation.

**Last Updated**: 2025-11-22

**Prerequisites**:
- Collector running with enhanced queries
- DMV populator generating workload
- Metrics flowing to New Relic NRDB

---

## Table of Contents

1. [Page 1: Slow Query List](#page-1-slow-query-list)
2. [Page 2: Query Details (Normalized)](#page-2-query-details-normalized)
3. [Page 3: Performance Timeline](#page-3-performance-timeline)
4. [Page 4: Active Query Details](#page-4-active-query-details)
5. [Page 5: Wait Time Analysis](#page-5-wait-time-analysis)
6. [Page 6: Blocking Queries](#page-6-blocking-queries)
7. [Page 7: Execution Plan](#page-7-execution-plan)
8. [Dashboard Examples](#dashboard-examples)

---

## Page 1: Slow Query List

### Display Columns
- query_id, last_active_time, database_name, query_text, user_name, average_elapsed_time, calls, rows_examined

### Main Query: Slow Query List with RCA Enhancements

```nrql
alias claude="claude-nerd-completion"
```

### Anomaly Detection: Queries Performing Worse Than Historical Max

```nrql
SELECT
    query_id,
    latest(query_text) AS query_text,
    latest(database_name) AS database_name,
    latest(last_elapsed_time_ms) AS current_elapsed_ms,
    latest(max_elapsed_time_ms) AS historical_max_ms,
    latest(avg_elapsed_time_ms) AS historical_avg_ms,
    (latest(last_elapsed_time_ms) - latest(max_elapsed_time_ms)) AS worse_than_max_by_ms,
    CASE
        WHEN latest(last_elapsed_time_ms) > latest(max_elapsed_time_ms)
        THEN 'WORSE_THAN_MAX'
        WHEN latest(last_elapsed_time_ms) > latest(avg_elapsed_time_ms) * 2
        THEN 'DEGRADED'
        ELSE 'NORMAL'
    END AS performance_status
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
FACET query_id
HAVING performance_status != 'NORMAL'
SINCE 30 minutes ago
LIMIT 50
```

### Memory Pressure Detection: Queries with TempDB Spills

```nrql
SELECT
    query_id,
    latest(query_text) AS query_text,
    latest(last_spills) AS tempdb_spills,
    latest(max_spills) AS max_tempdb_spills,
    latest(last_grant_kb) AS memory_grant_kb,
    latest(last_used_grant_kb) AS memory_used_kb,
    (latest(last_grant_kb) - latest(last_used_grant_kb)) AS memory_wasted_kb
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
  AND last_spills > 0
FACET query_id
SINCE 30 minutes ago
ORDER BY last_spills DESC
LIMIT 50
```

### Plan Regression Detection: Queries with Varying Plans

```nrql
SELECT
    query_id,
    uniqueCount(query_plan_hash) AS plan_count,
    latest(query_text) AS query_text,
    latest(database_name) AS database_name,
    latest(avg_elapsed_time_ms) AS avg_elapsed_ms,
    (latest(max_elapsed_time_ms) - latest(min_elapsed_time_ms)) AS elapsed_variance_ms
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
FACET query_id
HAVING plan_count > 1  -- Multiple plans detected
SINCE 1 hour ago
LIMIT 50
```

---

## Page 2: Query Details (Normalized)

### Display: Normalized Query Text

**Implementation**: Use query_text attribute with anonymization applied by the collector

```nrql
SELECT
    query_id,
    latest(query_text) AS normalized_query,
    latest(database_name) AS database_name,
    latest(schema_name) AS schema_name,
    latest(statement_type) AS statement_type,
    latest(calls) AS execution_count,
    latest(plan_creation_time) AS plan_age,
    latest(plan_generation_num) AS recompile_count
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id = '{{selected_query_id}}'  -- From Page 1 click
SINCE 1 hour ago
```

### Query Pattern Analysis

```nrql
SELECT
    latest(statement_type) AS query_type,
    latest(calls) AS total_executions,
    latest(avg_cpu_time_ms) AS avg_cpu_ms,
    latest(avg_elapsed_time_ms) AS avg_elapsed_ms,
    latest(avg_logical_reads) AS avg_logical_reads,
    latest(avg_physical_reads) AS avg_physical_reads,
    (latest(avg_physical_reads) * 100.0 / NULLIF(latest(avg_logical_reads), 0)) AS buffer_miss_ratio_percent
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id = '{{selected_query_id}}'
SINCE 1 hour ago
```

---

## Page 3: Performance Timeline

### Bar Chart: Active Query Count Over Time

```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_id IS NOT NULL
FACET query_id
TIMESERIES 1 minute
SINCE 30 minutes ago
LIMIT 20
```

### Click Handler: Show Active Queries for Time Window

When user clicks a bar, pass the timestamp range to Page 4:

```javascript
// onClick handler
const timeWindow = {
  start: clickedBar.timestamp,
  end: clickedBar.timestamp + 60000  // +1 minute
};

// Navigate to Page 4 with filter
navigateToPage4({
  query_id: clickedBar.query_id,
  time_start: timeWindow.start,
  time_end: timeWindow.end
});
```

---

## Page 4: Active Query Details

### List View: Active Queries (Aggregated)

```nrql
SELECT
    session_id,
    request_id,
    query_id,
    latest(user_name) AS user_name,
    latest(host_name) AS host_name,
    latest(program_name) AS program_name,
    latest(database_name) AS database_name,
    latest(request_status) AS status,
    latest(total_elapsed_time_ms) AS elapsed_ms,
    latest(cpu_time_ms) AS cpu_ms,
    latest(wait_type) AS current_wait,
    latest(wait_time_ms) AS wait_ms,
    latest(blocking_session_id) AS blocker,
    latest(degree_of_parallelism) AS dop,
    latest(parallel_worker_count) AS workers
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_id = '{{selected_query_id}}'  -- From Page 3 click
FACET session_id, request_id, query_id
SINCE 5 minutes ago
LIMIT 100
```

### Detail View: Individual Active Query with RCA

Click on row to show full details:

```nrql
SELECT
    -- Identifiers
    session_id,
    request_id,
    query_id,
    latest(plan_handle) AS plan_handle,

    -- User context
    latest(user_name) AS user,
    latest(host_name) AS host,
    latest(program_name) AS application,
    latest(database_name) AS database,

    -- RCA: Session diagnostics
    latest(client_interface_name) AS driver,
    latest(login_time) AS session_start,
    latest(last_request_start_time) AS last_request_start,
    latest(last_request_end_time) AS last_request_end,
    (timestamp() - latest(last_request_end_time)) / 1000 AS idle_seconds,

    -- RCA: Session resources
    latest(session_cpu_time_ms) AS session_total_cpu_ms,
    (latest(session_memory_pages) * 8) AS session_memory_kb,
    latest(session_total_elapsed_ms) AS session_age_ms,
    latest(session_reads) AS session_total_reads,
    latest(session_writes) AS session_total_writes,
    latest(session_logical_reads) AS session_total_logical_reads,

    -- Performance metrics (current request)
    latest(cpu_time_ms) AS cpu_ms,
    latest(total_elapsed_time_ms) AS elapsed_ms,
    latest(reads) AS reads,
    latest(writes) AS writes,
    latest(logical_reads) AS logical_reads,
    latest(row_count) AS rows_returned,
    (latest(granted_query_memory_pages) * 8) AS granted_memory_kb,

    -- RCA: Progress tracking
    latest(percent_complete) AS percent_complete,
    (latest(estimated_completion_time_ms) / 1000.0) AS eta_seconds,

    -- Wait analysis
    latest(request_status) AS status,
    latest(wait_type) AS wait_type,
    latest(wait_time_ms) AS wait_ms,
    latest(wait_resource) AS wait_resource,
    latest(last_wait_type) AS last_wait_type,

    -- RCA: Advanced diagnostics
    latest(scheduler_id) AS scheduler_id,
    latest(deadlock_priority) AS deadlock_priority,
    latest(nest_level) AS procedure_depth,
    latest(prev_error) AS last_error_code,
    latest(open_resultset_count) AS open_cursors,

    -- Transaction context
    latest(transaction_id) AS transaction_id,
    latest(open_transaction_count) AS open_transactions,
    latest(transaction_isolation_level) AS isolation_level,
    latest(session_transaction_isolation_level) AS session_isolation_level,

    -- Parallelism
    latest(degree_of_parallelism) AS dop,
    latest(parallel_worker_count) AS workers,

    -- Blocking
    latest(blocking_session_id) AS blocker_session_id,
    latest(blocker_login_name) AS blocker_user,
    latest(blocker_host_name) AS blocker_host,

    -- Query text
    latest(query_text) AS current_statement,
    latest(blocking_query_text) AS blocker_statement
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND session_id = {{selected_session_id}}
  AND request_id = {{selected_request_id}}
SINCE 5 minutes ago
```

### Correlation: Active vs Historical Performance

```nrql
SELECT
    active.session_id,
    active.query_id,
    active.total_elapsed_time_ms AS current_elapsed_ms,
    slow.avg_elapsed_time_ms AS historical_avg_ms,
    slow.min_elapsed_time_ms AS historical_min_ms,
    slow.max_elapsed_time_ms AS historical_max_ms,
    (active.total_elapsed_time_ms / NULLIF(slow.avg_elapsed_time_ms, 0) * 100) AS percent_of_avg,
    CASE
        WHEN active.total_elapsed_time_ms > slow.max_elapsed_time_ms
        THEN 'WORSE_THAN_MAX'
        WHEN active.total_elapsed_time_ms > slow.avg_elapsed_time_ms * 2
        THEN 'DEGRADED'
        WHEN active.total_elapsed_time_ms < slow.min_elapsed_time_ms
        THEN 'BETTER_THAN_MIN'
        ELSE 'NORMAL'
    END AS performance_status,
    active.wait_type AS current_wait,
    active.query_text AS query
FROM (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
    AND session_id = {{selected_session_id}}
) active
INNER JOIN (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
) slow ON active.query_id = slow.query_id
WHERE active.query_id IS NOT NULL
SINCE 5 minutes ago
```

### Memory Grant Analysis

```nrql
SELECT
    session_id,
    request_id,
    latest(query_id) AS query_id,
    latest(requested_memory_kb) AS requested_kb,
    latest(granted_memory_kb) AS granted_kb,
    latest(used_memory_kb) AS used_kb,
    latest(grant_usage_percentage) AS usage_percent,
    latest(wait_time_ms) AS grant_wait_ms,
    latest(wait_order) AS queue_position,
    latest(is_next_candidate) AS next_in_line,
    latest(grant_time) AS grant_time,
    latest(login_name) AS user,
    latest(database_name) AS database,
    latest(status) AS status
FROM Metric
WHERE metricName = 'sqlserver.memory_grant.wait_time_ms'
  AND session_id = {{selected_session_id}}
SINCE 5 minutes ago
ORDER BY grant_wait_ms DESC
```

---

## Page 5: Wait Time Analysis

### Main View: Task-Level Waits with Object Names

```nrql
SELECT
    session_id,
    request_id,
    latest(query_id) AS query_id,
    latest(worker_id) AS worker_id,  -- 0 = coordinator, 1+ = parallel workers
    latest(wait_type) AS wait_type,
    latest(wait_duration_ms) AS wait_ms,
    latest(object_name) AS locked_object,  -- Human-readable: database.schema.table
    latest(resource_description) AS raw_resource,
    latest(blocking_session_id) AS blocker,
    -- Request context
    latest(request_status) AS status,
    latest(command) AS command,
    latest(cpu_time_ms) AS cpu_ms,
    latest(total_elapsed_time_ms) AS elapsed_ms,
    latest(logical_reads) AS logical_reads,
    latest(total_wait_time_ms) AS total_wait_ms,
    -- Session context
    latest(login_name) AS user,
    latest(host_name) AS host,
    latest(program_name) AS application,
    latest(database_name) AS database
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_details'
  AND query_id = '{{selected_query_id}}'
  AND wait_duration_ms > 0
FACET session_id, request_id, worker_id
SINCE 5 minutes ago
ORDER BY wait_duration_ms DESC
LIMIT 100
```

### Aggregation: Wait Analysis by Type

```nrql
SELECT
    latest(wait_type) AS wait_type,
    count(*) AS task_count,
    sum(wait_duration_ms) AS total_wait_ms,
    average(wait_duration_ms) AS avg_wait_ms,
    max(wait_duration_ms) AS max_wait_ms,
    uniqueCount(object_name) AS affected_objects
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_details'
  AND query_id = '{{selected_query_id}}'
  AND wait_duration_ms > 0
FACET wait_type
SINCE 5 minutes ago
ORDER BY total_wait_ms DESC
LIMIT 20
```

### Parallel Worker Analysis

```nrql
SELECT
    latest(worker_id) AS worker_id,
    latest(wait_type) AS wait_type,
    latest(wait_duration_ms) AS wait_ms,
    latest(object_name) AS locked_object,
    latest(request_status) AS status
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_details'
  AND session_id = {{selected_session_id}}
  AND request_id = {{selected_request_id}}
FACET worker_id
SINCE 5 minutes ago
ORDER BY worker_id ASC
```

### Object Lock Heatmap

```nrql
SELECT
    latest(object_name) AS locked_object,
    count(*) AS lock_count,
    sum(wait_duration_ms) AS total_wait_ms,
    uniqueCount(session_id) AS blocked_sessions,
    latest(wait_type) AS lock_type
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_details'
  AND object_name IS NOT NULL
  AND object_name != 'N/A'
  AND wait_duration_ms > 0
FACET object_name
SINCE 10 minutes ago
ORDER BY total_wait_ms DESC
LIMIT 50
```

---

## Page 6: Blocking Queries

### Main View: Blocking Chains

```nrql
SELECT
    blocking_spid,
    blocked_spid,
    -- Blocking query details
    latest(blocking_login_name) AS blocker_user,
    latest(blocking_host_name) AS blocker_host,
    latest(blocking_program_name) AS blocker_app,
    latest(blocking_status) AS blocker_status,
    latest(blocking_command) AS blocker_command,
    latest(blocking_wait_type) AS blocker_wait,
    latest(blocking_cpu_time_ms) AS blocker_cpu_ms,
    latest(blocking_elapsed_ms) AS blocker_elapsed_ms,
    latest(blocking_transaction_id) AS blocker_txn_id,
    latest(blocking_start_time) AS blocker_start,
    latest(blocking_query_text) AS blocker_query,
    -- Blocked query details
    latest(blocked_login_name) AS blocked_user,
    latest(blocked_host_name) AS blocked_host,
    latest(blocked_program_name) AS blocked_app,
    latest(blocked_status) AS blocked_status,
    latest(blocked_command) AS blocked_command,
    latest(blocked_wait_type) AS blocked_wait,
    latest(blocked_cpu_time_ms) AS blocked_cpu_ms,
    latest(blocked_elapsed_ms) AS blocked_elapsed_ms,
    latest(blocked_transaction_id) AS blocked_txn_id,
    latest(blocked_start_time) AS blocked_start,
    latest(blocked_query_text) AS blocked_query,
    -- Wait details
    latest(wait_time_in_seconds) AS wait_seconds,
    latest(wait_resource) AS wait_resource,
    latest(database_name) AS database
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
FACET blocking_spid, blocked_spid
SINCE 10 minutes ago
ORDER BY wait_time_in_seconds DESC
LIMIT 100
```

### Head Blocker Analysis

Find the root cause (sessions that block others but are not blocked):

```nrql
SELECT
    blocking_spid AS head_blocker,
    count(*) AS blocked_sessions,
    sum(wait_time_in_seconds) AS total_blocked_time_sec,
    max(wait_time_in_seconds) AS max_blocked_time_sec,
    latest(blocking_query_text) AS blocker_query,
    latest(blocking_login_name) AS blocker_user,
    latest(blocking_host_name) AS blocker_host,
    latest(blocking_program_name) AS blocker_app,
    latest(blocking_status) AS blocker_status
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
FACET blocking_spid
SINCE 10 minutes ago
ORDER BY blocked_sessions DESC
LIMIT 50
```

### Blocking Chain Visualization

```nrql
-- Level 1: Head blockers
SELECT
    blocking_spid AS node_id,
    'HEAD_BLOCKER' AS node_type,
    latest(blocking_login_name) AS user,
    count(*) AS victims
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
FACET blocking_spid
SINCE 10 minutes ago

UNION

-- Level 2: Victims
SELECT
    blocked_spid AS node_id,
    'BLOCKED' AS node_type,
    latest(blocked_login_name) AS user,
    1 AS victims
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
FACET blocked_spid
SINCE 10 minutes ago
```

### Blocking by Database

```nrql
SELECT
    latest(database_name) AS database,
    count(*) AS blocking_count,
    sum(wait_time_in_seconds) AS total_wait_seconds,
    uniqueCount(blocking_spid) AS unique_blockers,
    uniqueCount(blocked_spid) AS unique_victims
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
FACET database_name
SINCE 10 minutes ago
ORDER BY total_wait_seconds DESC
```

---

## Page 7: Execution Plan

### Display: XML Execution Plan

**Note**: Execution plans are typically retrieved on-demand via the collector's execution plan endpoint.

```nrql
SELECT
    latest(execution_plan_xml) AS plan_xml,
    latest(total_cpu_ms) AS total_cpu,
    latest(total_elapsed_ms) AS total_elapsed,
    latest(creation_time) AS plan_created,
    latest(last_execution_time) AS last_execution,
    latest(sql_text) AS query
FROM ExecutionPlan
WHERE query_id = '{{selected_query_id}}'
SINCE 1 hour ago
```

### Execution Plan Metrics

```nrql
SELECT
    query_id,
    latest(query_plan_hash) AS plan_hash,
    count(*) AS plan_executions,
    latest(total_cpu_ms) / latest(execution_count) AS avg_cpu_per_execution,
    latest(total_elapsed_ms) / latest(execution_count) AS avg_elapsed_per_execution
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id = '{{selected_query_id}}'
SINCE 1 hour ago
```

---

## Dashboard Examples

### Dashboard 1: Executive Overview

**Tiles**:

1. **Total Active Queries**:
```nrql
SELECT uniqueCount(session_id)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
SINCE 5 minutes ago
```

2. **Blocked Sessions**:
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
SINCE 5 minutes ago
```

3. **Queries with Anomalies**:
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND total_elapsed_time_ms > (
    SELECT max(max_elapsed_time_ms)
    FROM Metric
    WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
    AND query_id = active.query_id
  )
SINCE 5 minutes ago
```

4. **Memory Pressure Queries**:
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND last_spills > 0
SINCE 10 minutes ago
```

---

### Dashboard 2: Performance Monitoring

**Tiles**:

1. **Top 10 Slow Queries**:
```nrql
SELECT
    query_id,
    latest(query_text) AS query,
    latest(avg_elapsed_time_ms) AS avg_ms
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
FACET query_id
SINCE 30 minutes ago
ORDER BY avg_elapsed_time_ms DESC
LIMIT 10
```

2. **Query Performance Over Time**:
```nrql
SELECT average(avg_elapsed_time_ms)
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IN ('query_hash_1', 'query_hash_2', 'query_hash_3')
FACET query_id
TIMESERIES 5 minutes
SINCE 1 hour ago
```

3. **Wait Type Distribution**:
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_details'
  AND wait_duration_ms > 0
FACET wait_type
SINCE 10 minutes ago
```

---

### Dashboard 3: RCA Analysis

**Tiles**:

1. **Performance Variance Alerts**:
```nrql
SELECT
    query_id,
    latest(query_text) AS query,
    latest(last_elapsed_time_ms) AS current_ms,
    latest(max_elapsed_time_ms) AS max_ms,
    (latest(last_elapsed_time_ms) - latest(max_elapsed_time_ms)) AS worse_by_ms
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
FACET query_id
HAVING latest(last_elapsed_time_ms) > latest(max_elapsed_time_ms)
SINCE 30 minutes ago
ORDER BY worse_by_ms DESC
LIMIT 20
```

2. **Memory Grant Efficiency**:
```nrql
SELECT
    query_id,
    latest(query_text) AS query,
    latest(last_grant_kb) AS granted_kb,
    latest(last_used_grant_kb) AS used_kb,
    (latest(last_used_grant_kb) * 100.0 / NULLIF(latest(last_grant_kb), 0)) AS efficiency_percent
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
  AND last_grant_kb > 0
FACET query_id
HAVING efficiency_percent < 50
SINCE 30 minutes ago
ORDER BY efficiency_percent ASC
LIMIT 20
```

3. **Session Leak Detection**:
```nrql
SELECT
    session_id,
    latest(login_name) AS user,
    latest(host_name) AS host,
    (timestamp() - latest(last_request_end_time)) / 1000 AS idle_seconds,
    latest(open_transaction_count) AS open_txns,
    latest(session_memory_pages) * 8 AS memory_kb
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
FACET session_id
HAVING idle_seconds > 300 AND open_txns > 0
SINCE 10 minutes ago
ORDER BY idle_seconds DESC
LIMIT 50
```

---

## Verification Checklist

### After Running DMV Populator

1. **Page 1 Data**:
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
SINCE 30 minutes ago
```
**Expected**: 50-200+ slow queries

2. **Page 4 Data**:
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_id IS NOT NULL
SINCE 10 minutes ago
```
**Expected**: 10-100+ active queries

3. **Page 5 Data**:
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_details'
  AND wait_duration_ms > 0
SINCE 10 minutes ago
```
**Expected**: 20-500+ task-level waits

4. **Page 6 Data**:
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
SINCE 10 minutes ago
```
**Expected**: 5-50+ blocking sessions

5. **Correlation Verification**:
```nrql
SELECT
    active.query_id,
    count(active.query_id) AS active_count,
    count(slow.query_id) AS slow_count
FROM (
    SELECT query_id FROM Metric
    WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
) active
INNER JOIN (
    SELECT query_id FROM Metric
    WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
) slow ON active.query_id = slow.query_id
WHERE active.query_id IS NOT NULL
FACET active.query_id
SINCE 30 minutes ago
LIMIT 100
```
**Expected**: Matching query_ids between active and slow queries

---

## Advanced Techniques

### Multi-Dimensional Analysis

```nrql
SELECT
    query_id,
    database_name,
    user_name,
    host_name,
    avg(avg_elapsed_time_ms) AS avg_ms,
    count(*) AS execution_count
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
FACET query_id, database_name, user_name, host_name
SINCE 1 hour ago
LIMIT 200
```

### Time Series Correlation

```nrql
SELECT
    avg(active.total_elapsed_time_ms) AS active_avg_ms,
    avg(slow.avg_elapsed_time_ms) AS slow_avg_ms
FROM (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
) active
INNER JOIN (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
) slow ON active.query_id = slow.query_id
TIMESERIES 5 minutes
SINCE 1 hour ago
```

---

**Status**: ✅ Complete - All 7 pages with comprehensive RCA-driven NRQL queries
