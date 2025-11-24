# SQL Server DMV Architecture and UI Flow Mapping

## Executive Summary

This document provides a comprehensive reference for all SQL Server Dynamic Management Views (DMVs) used in the New Relic SQL Server Query Performance Monitoring feature. It maps DMV columns to UI pages, explains the data flow, and provides sample NRQL queries for building the complete RCA-driven user experience.

**Last Updated**: 2025-11-22

---

## Table of Contents

1. [Overview](#overview)
2. [DMV Architecture](#dmv-architecture)
3. [UI Flow Mapping](#ui-flow-mapping)
4. [DMVs Used](#dmvs-used)
5. [Page-by-Page Implementation](#page-by-page-implementation)
6. [NRQL Query Examples](#nrql-query-examples)
7. [RCA Enhancement Columns](#rca-enhancement-columns)
8. [Performance Considerations](#performance-considerations)

---

## Overview

### Data Collection Strategy

The receiver implements a **3-tier correlation architecture**:

```
┌─────────────────────────────────────────────────────────────────┐
│                     TIER 1: Historical Metrics                   │
│  sys.dm_exec_query_stats (Plan Cache - Last 5-10 minutes)       │
│  - Aggregated performance statistics                             │
│  - Min/Max/Avg/Last performance variance                         │
│  - Memory grants, TempDB spills, parallelism                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓ query_hash
┌─────────────────────────────────────────────────────────────────┐
│                     TIER 2: Active Queries                       │
│  sys.dm_exec_requests + sys.dm_exec_sessions                    │
│  - Currently executing queries (real-time)                       │
│  - Session context, resource usage, progress tracking            │
└─────────────────────────────────────────────────────────────────┘
                              ↓ session_id
┌─────────────────────────────────────────────────────────────────┐
│                     TIER 3: Wait Analysis                        │
│  sys.dm_os_waiting_tasks + sys.dm_tran_locks                    │
│  - Task-level waits (parallel workers)                           │
│  - Blocking chains with human-readable object names              │
└─────────────────────────────────────────────────────────────────┘
```

### Correlation Key: `query_hash`

- **Primary Key**: SQL Server's native `query_hash` (binary(8))
- **Joins**: Historical metrics ↔ Active queries
- **Availability**: Populated for parameterized queries, NULL for ad-hoc SQL
- **Purpose**: Enables comparison of current vs historical performance

---

## DMV Architecture

### Core DMVs (Always Used)

| DMV | Purpose | Refresh Rate | Scope |
|-----|---------|--------------|-------|
| `sys.dm_exec_query_stats` | Historical query performance from plan cache | On plan compile/recompile | Per query pattern |
| `sys.dm_exec_requests` | Currently executing queries | Real-time | Per active request |
| `sys.dm_exec_sessions` | Session information and context | Real-time | Per session |
| `sys.dm_exec_sql_text()` | SQL text extraction | On demand | Per sql_handle |

### Wait Analysis DMVs

| DMV | Purpose | Refresh Rate | Scope |
|-----|---------|--------------|-------|
| `sys.dm_os_waiting_tasks` | Task-level wait analysis | Real-time | Per waiting task |
| `sys.dm_tran_locks` | Lock information | Real-time | Per lock |
| `sys.partitions` | Object name resolution | Static | Per partition |

### Memory Analysis DMVs

| DMV | Purpose | Refresh Rate | Scope |
|-----|---------|--------------|-------|
| `sys.dm_exec_query_memory_grants` | Memory grant tracking | Real-time | Per active grant |
| `sys.dm_os_wait_stats` | Server-level wait statistics | Cumulative | Server-wide |

### Helper DMVs

| DMV | Purpose | Refresh Rate | Scope |
|-----|---------|--------------|-------|
| `sys.dm_exec_input_buffer()` | Input buffer for blocking queries | On demand | Per session |
| `sys.dm_exec_cached_plans` | Plan cache metadata | Real-time | Per cached plan |
| `sys.dm_exec_plan_attributes()` | Plan attributes (database_id, etc.) | On demand | Per plan_handle |
| `sys.dm_exec_query_plan()` | XML execution plans | On demand | Per plan_handle |
| `sys.databases` | Database names and metadata | Mostly static | Per database |

---

## UI Flow Mapping

### Page 1: Slow Query List (Grouped by query_id)

**Source**: `sys.dm_exec_query_stats`

**Columns Displayed**:
- `query_id` (query_hash)
- `last_active_time`
- `database_name`
- `query_text` (truncated to 4KB)
- `user_name` (derived from first execution context)
- `average_elapsed_time`
- `calls` (execution_count)
- `rows_examined` (avg total_rows)

**RCA Enhancements**:
- Performance variance: min/max/last CPU, elapsed time, I/O
- Parallelism metrics: total_dop, last_dop
- Memory grant metrics: total_grant_kb, last_grant_kb, total_used_grant_kb
- TempDB spills: total_spills, last_spills, max_spills

**Query**: `SlowQueryEnhanced` in `query_performance_monitoring_metrics_ENHANCED.go:10-190`

---

### Page 2: Query Details (Normalized Query)

**Source**: Same data from Page 1 with anonymization

**Displayed**:
- Normalized/anonymized query text (literals replaced with placeholders)
- Query pattern analysis

**Implementation**: Uses `helpers/query_anonymizer.go` to anonymize query text

---

### Page 3: Performance Timeline Bar Chart

**Source**: `sys.dm_exec_requests` aggregated by time window

**Aggregation**: Count of active queries by query_id over time

**Click Action**: Shows Page 4 (active query list for that time window)

---

### Page 4: Active Query List & Details

**Source**: `sys.dm_exec_requests` + `sys.dm_exec_sessions`

**Columns Displayed**:

**Identifiers**:
- `session_id`
- `request_id`
- `query_id` (query_hash)
- `plan_handle`

**User Context**:
- `user_name` (login_name)
- `host_name`
- `program_name`
- `database_name`

**RCA Session Details**:
- `client_interface_name` (driver: ODBC, JDBC, .NET)
- `login_time` (session start)
- `last_request_start_time` (idle detection)
- `last_request_end_time`

**RCA Session Resources**:
- `session_cpu_time_ms` (total session CPU)
- `session_memory_pages` (session memory, 8KB pages)
- `session_total_elapsed_ms` (session age)
- `session_reads`, `session_writes`, `session_logical_reads`

**Performance Metrics**:
- `cpu_time_ms`
- `total_elapsed_time_ms`
- `reads`, `writes`, `logical_reads`
- `row_count`
- `granted_query_memory_pages`

**RCA Progress Tracking**:
- `percent_complete` (for BACKUP, INDEX REBUILD, etc.)
- `estimated_completion_time_ms` (ETA)

**RCA Advanced Diagnostics**:
- `task_address` (join to sys.dm_os_waiting_tasks)
- `scheduler_id` (CPU scheduler, affinity issues)
- `deadlock_priority` (victim selection)
- `lock_timeout_ms`
- `nest_level` (stored procedure depth)
- `prev_error` (last error)
- `context_info` (application custom context)
- `open_resultset_count` (cursor leak detection)

**Wait Analysis**:
- `request_status` (running, suspended, runnable)
- `wait_type`
- `wait_time_ms`
- `wait_resource`

**Transaction Context**:
- `transaction_id`
- `open_transaction_count`
- `transaction_isolation_level`

**Parallelism**:
- `degree_of_parallelism` (DOP)
- `parallel_worker_count`

**Query Text**:
- `query_text` (current statement, truncated to 4KB)

**Blocking** (if applicable):
- `blocking_session_id`
- `blocker_login_name`, `blocker_host_name`, `blocker_program_name`
- `blocking_query_text`

**Query**: `ActiveRunningQueriesQueryEnhanced` in `query_performance_monitoring_metrics_ENHANCED.go:192-354`

---

### Page 5: Wait Time Analysis

**Source**: `sys.dm_os_waiting_tasks` + `sys.dm_tran_locks` + `sys.partitions`

**Purpose**: Shows **task-level waits** (each parallel worker's wait state)

**Columns Displayed**:

**Identifiers**:
- `session_id`
- `request_id`
- `query_id` (query_hash)
- `worker_id` (exec_context_id) - 0 = coordinator, 1+ = workers
- `collection_timestamp`

**Wait Details**:
- `wait_type` (e.g., PAGEIOLATCH_SH, LCK_M_X, ASYNC_NETWORK_IO)
- `wait_duration_ms` (current wait time)
- `resource_description` (raw resource string)

**Human-Readable Object Names**:
- `object_name` - Resolved from KEY/PAGE/RID/OBJECT resources
  - Format: `database.schema.table`
  - Example: `AdventureWorks.Sales.Orders`

**Resolution Logic**:
- **KEY locks**: Joins `sys.dm_tran_locks` (resource_type='KEY') → `sys.partitions` (hobt_id) → `OBJECT_NAME()`
- **PAGE locks**: Same as KEY locks
- **OBJECT locks**: Direct join to `sys.dm_tran_locks` (resource_type='OBJECT') → `OBJECT_NAME()`
- **RID locks**: Same as PAGE locks

**Blocking Context** (if waiting on lock):
- `blocking_session_id`
- `blocking_task_address`
- `blocking_exec_context_id`

**Request Context**:
- `request_status`
- `command`
- `cpu_time_ms`
- `total_elapsed_time_ms`
- `logical_reads`
- `total_wait_time_ms`
- `last_wait_type`

**Session Context**:
- `login_name`
- `host_name`
- `program_name`
- `database_name`

**Query**: `ActiveQueryWaitDetailsQuery` in `query_performance_monitoring_metrics_ENHANCED.go:356-456`

---

### Page 6: Blocking Queries

**Source**: `sys.dm_exec_requests` (self-join on blocking_session_id)

**Purpose**: Shows blocking chains (blocker → blocked relationships)

**Columns Displayed**:

**Identifiers**:
- `blocking_spid` (blocking session ID)
- `blocked_spid` (blocked session ID)
- `collection_timestamp`

**Blocking Query Details**:
- `blocking_login_name`
- `blocking_host_name`
- `blocking_program_name`
- `blocking_status` (e.g., sleeping, running)
- `blocking_command` (e.g., SELECT, UPDATE)
- `blocking_wait_type` (what blocker is waiting on, if anything)
- `blocking_cpu_time_ms`
- `blocking_elapsed_ms`
- `blocking_transaction_id`
- `blocking_start_time`
- `blocking_query_text` (what blocker is doing)

**Blocked Query Details**:
- `blocked_login_name`
- `blocked_host_name`
- `blocked_program_name`
- `blocked_status` (typically 'suspended')
- `blocked_command` (e.g., UPDATE, DELETE)
- `blocked_wait_type` (e.g., LCK_M_X, LCK_M_U)
- `blocked_cpu_time_ms`
- `blocked_elapsed_ms`
- `blocked_transaction_id`
- `blocked_start_time`
- `blocked_query_text` (what blocked query is trying to do)

**Wait Details**:
- `wait_time_in_seconds` (how long blocked)
- `wait_resource` (e.g., KEY: 5:1:123456)

**Database Context**:
- `database_name`

**Query**: `BlockingSessionsQueryEnhanced` in `query_performance_monitoring_metrics_ENHANCED.go:458-544`

---

### Page 7: Execution Plan (Tabular Form)

**Source**: `sys.dm_exec_query_plan()`

**Input**: `plan_handle` from Page 4

**Output**: XML execution plan parsed into JSON structure

**Fields**:
- Operator details (Scan, Seek, Join, etc.)
- Estimated vs Actual rows
- Cost percentages
- Warnings (implicit conversions, missing indexes)

**Query**: `QueryExecutionPlan` in original `query_performance_monitoring_metrics.go`

---

## DMVs Used

### 1. sys.dm_exec_query_stats

**Purpose**: Historical query performance metrics from plan cache

**Columns Used** (67 total documented, 60+ captured):

| Column | Type | Description | UI Page |
|--------|------|-------------|---------|
| `query_hash` | binary(8) | Correlation key (query_id) | 1, 3, 4, 5 |
| `query_plan_hash` | binary(8) | Plan regression detection | 1 |
| `sql_handle` | varbinary(64) | SQL text identifier | 1 |
| `plan_handle` | varbinary(64) | Execution plan identifier | 1, 7 |
| `statement_start_offset` | int | Query text start position | 1 |
| `statement_end_offset` | int | Query text end position | 1 |
| `last_execution_time` | datetime | Last active time | 1 |
| `execution_count` | bigint | Calls | 1 |
| `total_worker_time` | bigint | Total CPU time (microseconds) | 1 |
| `last_worker_time` | bigint | Last CPU time | 1 (RCA) |
| `min_worker_time` | bigint | Min CPU time | 1 (RCA) |
| `max_worker_time` | bigint | Max CPU time | 1 (RCA) |
| `total_elapsed_time` | bigint | Total elapsed time | 1 |
| `last_elapsed_time` | bigint | Last elapsed time | 1 (RCA) |
| `min_elapsed_time` | bigint | Min elapsed time | 1 (RCA) |
| `max_elapsed_time` | bigint | Max elapsed time | 1 (RCA) |
| `total_logical_reads` | bigint | Total logical reads | 1 |
| `last_logical_reads` | bigint | Last logical reads | 1 (RCA) |
| `min_logical_reads` | bigint | Min logical reads | 1 (RCA) |
| `max_logical_reads` | bigint | Max logical reads | 1 (RCA) |
| `total_physical_reads` | bigint | Total physical reads | 1 |
| `total_logical_writes` | bigint | Total logical writes | 1 |
| `total_rows` | bigint | Total rows returned | 1 |
| `last_rows` | bigint | Last rows returned | 1 (RCA) |
| `min_rows` | bigint | Min rows returned | 1 (RCA) |
| `max_rows` | bigint | Max rows returned | 1 (RCA) |
| `total_dop` | bigint | Total DOP (parallelism) | 1 (RCA) |
| `last_dop` | bigint | Last DOP | 1 (RCA) |
| `min_dop` | bigint | Min DOP | 1 (RCA) |
| `max_dop` | bigint | Max DOP | 1 (RCA) |
| `total_grant_kb` | bigint | Total memory grant | 1 (RCA) |
| `last_grant_kb` | bigint | Last memory grant | 1 (RCA) |
| `min_grant_kb` | bigint | Min memory grant | 1 (RCA) |
| `max_grant_kb` | bigint | Max memory grant | 1 (RCA) |
| `total_used_grant_kb` | bigint | Total used memory | 1 (RCA) |
| `last_used_grant_kb` | bigint | Last used memory | 1 (RCA) |
| `min_used_grant_kb` | bigint | Min used memory | 1 (RCA) |
| `max_used_grant_kb` | bigint | Max used memory | 1 (RCA) |
| `total_ideal_grant_kb` | bigint | Total ideal memory | 1 (RCA) |
| `last_ideal_grant_kb` | bigint | Last ideal memory | 1 (RCA) |
| `total_spills` | bigint | Total TempDB spills | 1 (RCA) |
| `last_spills` | bigint | Last TempDB spills | 1 (RCA) |
| `min_spills` | bigint | Min TempDB spills | 1 (RCA) |
| `max_spills` | bigint | Max TempDB spills | 1 (RCA) |
| `creation_time` | datetime | Plan creation time | 1 |
| `plan_generation_num` | bigint | Plan recompile generation | 1 |

**Joins**:
- `sys.dm_exec_sql_text(sql_handle)` → Query text extraction
- `sys.dm_exec_cached_plans` (plan_handle) → Plan metadata
- `sys.dm_exec_plan_attributes(plan_handle)` → database_id

**Filters**:
- `last_execution_time >= DATEADD(SECOND, -@IntervalSeconds, GETUTCDATE())` (default 300s = 5 minutes)
- `execution_count > 0`
- Excludes system databases and internal queries

---

### 2. sys.dm_exec_requests

**Purpose**: Currently executing queries (real-time snapshot)

**Columns Used** (67 total documented, 40+ captured):

| Column | Type | Description | UI Page |
|--------|------|-------------|---------|
| `session_id` | smallint | Session ID | 4, 5, 6 |
| `request_id` | int | Request ID within session | 4, 5 |
| `query_hash` | binary(8) | Correlation key (query_id) | 4, 5 |
| `plan_handle` | varbinary(64) | Execution plan identifier | 4, 7 |
| `sql_handle` | varbinary(64) | SQL text identifier | 4 |
| `statement_start_offset` | int | Query text start position | 4 |
| `statement_end_offset` | int | Query text end position | 4 |
| `database_id` | smallint | Database ID | 4 |
| `start_time` | datetime | Query start time | 4 |
| `status` | nvarchar(30) | Request status (running, suspended, runnable) | 4 |
| `command` | nvarchar(32) | Command type (SELECT, INSERT, UPDATE) | 4 |
| `cpu_time` | int | CPU time in milliseconds | 4 |
| `total_elapsed_time` | int | Total elapsed time | 4 |
| `reads` | bigint | Physical disk reads | 4 |
| `writes` | bigint | Write operations | 4 |
| `logical_reads` | bigint | Buffer cache reads | 4 |
| `row_count` | bigint | Rows returned | 4 |
| `granted_query_memory` | int | Memory granted (8KB pages) | 4 |
| `wait_type` | nvarchar(60) | Current wait type | 4, 5 |
| `wait_time` | int | Wait time in milliseconds | 4, 5 |
| `wait_resource` | nvarchar(256) | Resource waiting on | 4, 5, 6 |
| `last_wait_type` | nvarchar(60) | Last wait type | 4 |
| `blocking_session_id` | smallint | Blocking session ID | 4, 6 |
| `percent_complete` | real | % complete (for long operations) | 4 (RCA) |
| `estimated_completion_time` | bigint | ETA in milliseconds | 4 (RCA) |
| `task_address` | varbinary(8) | Task memory address (join to dm_os_waiting_tasks) | 4 (RCA) |
| `scheduler_id` | int | CPU scheduler ID | 4 (RCA) |
| `deadlock_priority` | int | Deadlock victim selection | 4 (RCA) |
| `lock_timeout` | int | Lock timeout in milliseconds | 4 (RCA) |
| `nest_level` | int | Stored procedure nesting depth | 4 (RCA) |
| `prev_error` | int | Last error code | 4 (RCA) |
| `context_info` | varbinary(128) | Application custom context | 4 (RCA) |
| `open_resultset_count` | int | Open result sets (cursor leak detection) | 4 (RCA) |
| `transaction_id` | bigint | Transaction ID | 4, 6 |
| `open_transaction_count` | int | Open transactions | 4 |
| `transaction_isolation_level` | smallint | Isolation level | 4 |
| `dop` | int | Degree of parallelism | 4 |
| `parallel_worker_count` | int | Parallel workers | 4 |

**Joins**:
- `sys.dm_exec_sessions` (session_id) → Session context
- `sys.dm_exec_sql_text(sql_handle)` → Query text
- Self-join on `blocking_session_id` → Blocking query details

**Filters**:
- `session_id > 50` (exclude system sessions)
- `database_id > 4` (exclude system databases)
- `wait_type IS NOT NULL` (only queries with waits)

---

### 3. sys.dm_exec_sessions

**Purpose**: Session information and context

**Columns Used** (51 total documented, 30+ captured):

| Column | Type | Description | UI Page |
|--------|------|-------------|---------|
| `session_id` | smallint | Session ID | 4, 5, 6 |
| `login_name` | nvarchar(128) | SQL Server login name (user_name) | 4, 5, 6 |
| `host_name` | nvarchar(128) | Client workstation name | 4, 5, 6 |
| `program_name` | nvarchar(128) | Client program name | 4, 5, 6 |
| `client_interface_name` | nvarchar(32) | Driver (ODBC, JDBC, .NET) | 4 (RCA) |
| `login_time` | datetime | Session start time | 4 (RCA) |
| `last_request_start_time` | datetime | Last activity time (idle detection) | 4 (RCA) |
| `last_request_end_time` | datetime | Last request completion | 4 (RCA) |
| `status` | nvarchar(30) | Session status (running, sleeping, dormant) | 4 |
| `cpu_time` | int | Total session CPU time | 4 (RCA) |
| `memory_usage` | int | Session memory (8KB pages) | 4 (RCA) |
| `total_elapsed_time` | int | Session age | 4 (RCA) |
| `reads` | bigint | Session total reads | 4 (RCA) |
| `writes` | bigint | Session total writes | 4 (RCA) |
| `logical_reads` | bigint | Session logical reads | 4 (RCA) |
| `transaction_isolation_level` | smallint | Session isolation level | 4 (RCA) |
| `lock_timeout` | int | Session lock timeout | 4 (RCA) |
| `deadlock_priority` | int | Session deadlock priority | 4 (RCA) |
| `open_transaction_count` | int | Session open transactions | 4 (RCA) |

**Joins**:
- `sys.dm_exec_requests` (session_id) → Active query details

**Filters**:
- Joined with requests, inherits filters

---

### 4. sys.dm_os_waiting_tasks

**Purpose**: Task-level wait analysis (parallel workers)

**Columns Used**:

| Column | Type | Description | UI Page |
|--------|------|-------------|---------|
| `session_id` | smallint | Session ID | 5 |
| `exec_context_id` | int | Worker ID (0 = coordinator, 1+ = workers) | 5 |
| `waiting_task_address` | varbinary(8) | Task memory address | 5 |
| `wait_type` | nvarchar(60) | Wait type | 5 |
| `wait_duration_ms` | bigint | Current wait time | 5 |
| `resource_description` | nvarchar(3072) | Resource description | 5 |
| `blocking_session_id` | smallint | Blocking session ID | 5 |
| `blocking_task_address` | varbinary(8) | Blocking task address | 5 |
| `blocking_exec_context_id` | int | Blocking worker ID | 5 |

**Joins**:
- `sys.dm_exec_requests` (session_id) → Request context
- `sys.dm_exec_sessions` (session_id) → Session context
- `sys.dm_tran_locks` (session_id) → Lock and object resolution

**Filters**:
- `session_id > 50` (exclude system sessions)
- `wait_duration_ms > 0` (only active waits)

---

### 5. sys.dm_tran_locks

**Purpose**: Lock information and object name resolution

**Columns Used**:

| Column | Type | Description | UI Page |
|--------|------|-------------|---------|
| `request_session_id` | int | Session holding lock | 5 |
| `resource_type` | nvarchar(60) | Lock type (KEY, PAGE, OBJECT) | 5 |
| `resource_database_id` | int | Database ID | 5 |
| `resource_associated_entity_id` | bigint | HOBT ID or object ID | 5 |

**Purpose in Wait Analysis**:
- Resolves `KEY`, `PAGE`, `RID`, `OBJECT` resources to table names
- Provides `database.schema.table` format for human readability

**Join Pattern**:
```sql
FROM sys.dm_tran_locks l
LEFT JOIN sys.partitions p ON l.resource_associated_entity_id = p.hobt_id
WHERE l.request_session_id = @session_id
  AND l.resource_type = 'KEY'  -- or PAGE, OBJECT
```

---

### 6. sys.partitions

**Purpose**: Object name resolution from HOBT ID

**Columns Used**:

| Column | Type | Description | UI Page |
|--------|------|-------------|---------|
| `hobt_id` | bigint | Heap or B-tree ID | 5 |
| `object_id` | int | Object ID | 5 |

**Purpose**:
- Converts HOBT ID (from locks) to object_id
- Used with `OBJECT_NAME()` and `OBJECT_SCHEMA_NAME()` to get table names

---

### 7. sys.dm_exec_query_memory_grants

**Purpose**: Memory grant tracking (queries waiting for or using memory)

**Columns Used**:

| Column | Type | Description | Usage |
|--------|------|-------------|-------|
| `session_id` | smallint | Session ID | Identifier |
| `request_id` | int | Request ID | Identifier |
| `requested_memory_kb` | bigint | Requested memory | Memory pressure detection |
| `granted_memory_kb` | bigint | Granted memory | Memory pressure detection |
| `required_memory_kb` | bigint | Required memory | Memory pressure detection |
| `used_memory_kb` | bigint | Used memory | Grant efficiency |
| `max_used_memory_kb` | bigint | Max used memory | Grant efficiency |
| `query_cost` | float | Optimizer cost | Grant calculation |
| `wait_time_ms` | bigint | Wait time for grant | Memory contention |
| `wait_order` | int | Position in grant queue | Memory contention |
| `is_next_candidate` | bit | Next in line for grant | Memory contention |
| `grant_time` | datetime | When grant was given | Timeline |

**Calculated**:
- `grant_usage_percentage` = (used_memory_kb / granted_memory_kb * 100)

**Query**: `QueryMemoryGrantsQuery` in `query_performance_monitoring_metrics_ENHANCED.go:581-631`

---

### 8. sys.dm_os_wait_stats

**Purpose**: Server-level wait statistics (cumulative since startup)

**Columns Used**:

| Column | Type | Description | Usage |
|--------|------|-------------|-------|
| `wait_type` | nvarchar(60) | Wait type | Wait category |
| `waiting_tasks_count` | bigint | Number of waits | Frequency |
| `wait_time_ms` | bigint | Total wait time | Total impact |
| `max_wait_time_ms` | bigint | Maximum wait time | Worst case |
| `signal_wait_time_ms` | bigint | Signal wait time | CPU wait |

**Calculated**:
- `resource_wait_time_ms` = wait_time_ms - signal_wait_time_ms
- `percentage_of_total` = (wait_time_ms / SUM(wait_time_ms) * 100)

**Filters**:
- Excludes benign wait types (SLEEP_TASK, WAITFOR, etc.)
- Only waits with `wait_time_ms > 0`

**Query**: `ServerWaitStatsQuery` in `query_performance_monitoring_metrics_ENHANCED.go:546-575`

---

### 9. Helper DMVs and Functions

#### sys.dm_exec_sql_text(sql_handle)

**Purpose**: Retrieve SQL text from sql_handle

**Returns**:
- `text` (nvarchar(max)): Full SQL text

**Usage**: Extract statement text using `statement_start_offset` and `statement_end_offset`

#### sys.dm_exec_input_buffer(session_id, request_id)

**Purpose**: Get input buffer for blocking sessions

**Returns**:
- `event_info` (nvarchar(max)): Input buffer text

**Usage**: Fallback for blocking query text when `sql_handle` is NULL

#### sys.dm_exec_query_plan(plan_handle)

**Purpose**: Retrieve XML execution plan

**Returns**:
- `query_plan` (xml): Execution plan XML

**Usage**: Page 7 (execution plan visualization)

#### sys.dm_exec_cached_plans

**Purpose**: Plan cache metadata

**Usage**: Join with query_stats to verify plan exists

#### sys.dm_exec_plan_attributes(plan_handle)

**Purpose**: Extract plan attributes (database_id, etc.)

**Usage**: Determine database context for query

#### sys.databases

**Purpose**: Database name resolution

**Usage**: `DB_NAME(database_id)` to get database names

---

## Page-by-Page Implementation

### Implementation Status

| Page | Query Name | Status | File Location |
|------|-----------|--------|---------------|
| 1 | `SlowQueryEnhanced` | ✅ Ready | query_performance_monitoring_metrics_ENHANCED.go:10 |
| 2 | (Anonymization) | ✅ Ready | helpers/query_anonymizer.go |
| 3 | (Aggregation) | ⚠️ NRQL | Client-side aggregation |
| 4 | `ActiveRunningQueriesQueryEnhanced` | ✅ Ready | query_performance_monitoring_metrics_ENHANCED.go:192 |
| 5 | `ActiveQueryWaitDetailsQuery` | ✅ Ready | query_performance_monitoring_metrics_ENHANCED.go:356 |
| 6 | `BlockingSessionsQueryEnhanced` | ✅ Ready | query_performance_monitoring_metrics_ENHANCED.go:458 |
| 7 | `QueryExecutionPlan` | ✅ Exists | query_performance_monitoring_metrics.go |

---

## NRQL Query Examples

### Page 1: Slow Query List

```nrql
-- Slow queries with RCA enhancements
SELECT
    query_id,
    last_active_time,
    database_name,
    query_text,
    average_elapsed_time,
    calls,
    rows_examined,
    -- RCA: Performance variance
    min_elapsed_time_ms,
    max_elapsed_time_ms,
    last_elapsed_time_ms,
    (max_elapsed_time_ms - min_elapsed_time_ms) AS elapsed_time_variance,
    -- RCA: Memory grants
    last_grant_kb,
    last_used_grant_kb,
    (last_used_grant_kb * 100.0 / NULLIF(last_grant_kb, 0)) AS grant_efficiency,
    -- RCA: TempDB spills (indicator of memory pressure)
    last_spills,
    max_spills,
    -- RCA: Parallelism
    last_dop
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
  AND query_id IS NOT NULL
  AND average_elapsed_time > 100  -- Only queries > 100ms
FACET query_id, query_text
SINCE 10 minutes ago
LIMIT 100
```

### Page 3: Performance Timeline Bar Chart

```nrql
-- Active query count over time
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_id IS NOT NULL
FACET query_id
TIMESERIES 1 minute
SINCE 30 minutes ago
```

### Page 4: Active Query Details

```nrql
-- Active queries with full RCA context
SELECT
    session_id,
    request_id,
    query_id,
    user_name,
    host_name,
    program_name,
    database_name,
    -- RCA: Session diagnostics
    client_interface_name,
    DATEDIFF(NOW(), login_time) AS session_age_seconds,
    DATEDIFF(NOW(), last_request_start_time) AS idle_seconds,
    -- RCA: Session resources
    session_cpu_time_ms,
    session_memory_pages * 8 AS session_memory_kb,
    session_reads,
    session_writes,
    -- Performance
    cpu_time_ms,
    total_elapsed_time_ms,
    reads,
    logical_reads,
    row_count,
    granted_query_memory_pages * 8 AS granted_memory_kb,
    -- RCA: Progress tracking
    percent_complete,
    estimated_completion_time_ms / 1000.0 AS eta_seconds,
    -- Wait analysis
    request_status,
    wait_type,
    wait_time_ms,
    wait_resource,
    -- RCA: Advanced diagnostics
    scheduler_id,
    deadlock_priority,
    nest_level,
    prev_error,
    open_resultset_count,
    -- Transaction context
    transaction_id,
    open_transaction_count,
    transaction_isolation_level,
    -- Parallelism
    degree_of_parallelism,
    parallel_worker_count,
    -- Blocking
    blocking_session_id,
    blocker_login_name,
    -- Query text
    query_text
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_id = '0x9661971254EA9C69'  -- Click from Page 3
SINCE 5 minutes ago
LIMIT 100
```

### Page 4: Correlation with Historical Performance

```nrql
-- Compare active query with historical averages
SELECT
    active.session_id,
    active.query_id,
    active.total_elapsed_time_ms AS current_elapsed_ms,
    slow.avg_elapsed_time_ms AS historical_avg_ms,
    slow.min_elapsed_time_ms AS historical_min_ms,
    slow.max_elapsed_time_ms AS historical_max_ms,
    (active.total_elapsed_time_ms / NULLIF(slow.avg_elapsed_time_ms, 0) * 100) AS percent_of_avg,
    -- Determine if anomaly
    CASE
        WHEN active.total_elapsed_time_ms > slow.max_elapsed_time_ms THEN 'WORSE_THAN_MAX'
        WHEN active.total_elapsed_time_ms > slow.avg_elapsed_time_ms * 2 THEN 'DEGRADED'
        WHEN active.total_elapsed_time_ms < slow.min_elapsed_time_ms THEN 'BETTER_THAN_MIN'
        ELSE 'NORMAL'
    END AS performance_status,
    active.wait_type,
    active.query_text
FROM (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
) active
INNER JOIN (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
) slow ON active.query_id = slow.query_id
WHERE active.query_id IS NOT NULL
SINCE 5 minutes ago
LIMIT 100
```

### Page 5: Wait Time Analysis

```nrql
-- Task-level waits with object names
SELECT
    session_id,
    request_id,
    query_id,
    worker_id,  -- 0 = coordinator, 1+ = parallel workers
    wait_type,
    wait_duration_ms,
    object_name,  -- Human-readable table name
    blocking_session_id,
    -- Request context
    request_status,
    command,
    cpu_time_ms,
    total_elapsed_time_ms,
    logical_reads,
    total_wait_time_ms,
    -- Session context
    login_name,
    host_name,
    program_name,
    database_name,
    collection_timestamp
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_details'
  AND query_id = '0x9661971254EA9C69'
  AND wait_duration_ms > 0
ORDER BY wait_duration_ms DESC
SINCE 5 minutes ago
LIMIT 100
```

### Page 5: Wait Aggregation by Type

```nrql
-- Aggregate waits by type for a query
SELECT
    wait_type,
    count(*) AS task_count,
    sum(wait_duration_ms) AS total_wait_ms,
    avg(wait_duration_ms) AS avg_wait_ms,
    max(wait_duration_ms) AS max_wait_ms,
    latest(object_name) AS affected_objects
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_details'
  AND query_id = '0x9661971254EA9C69'
GROUP BY wait_type
ORDER BY total_wait_ms DESC
SINCE 5 minutes ago
```

### Page 6: Blocking Queries

```nrql
-- Blocking chains
SELECT
    blocking_spid,
    blocked_spid,
    -- Blocking query
    blocking_login_name,
    blocking_host_name,
    blocking_program_name,
    blocking_status,
    blocking_command,
    blocking_wait_type,
    blocking_cpu_time_ms,
    blocking_elapsed_ms,
    blocking_transaction_id,
    blocking_start_time,
    blocking_query_text,
    -- Blocked query
    blocked_login_name,
    blocked_host_name,
    blocked_program_name,
    blocked_status,
    blocked_command,
    blocked_wait_type,
    blocked_cpu_time_ms,
    blocked_elapsed_ms,
    blocked_transaction_id,
    blocked_start_time,
    blocked_query_text,
    -- Wait details
    wait_time_in_seconds,
    wait_resource,
    database_name,
    collection_timestamp
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
ORDER BY wait_time_in_seconds DESC
SINCE 10 minutes ago
LIMIT 100
```

### Page 6: Blocking Chain Visualization

```nrql
-- Find head blocker (blocking but not blocked)
SELECT
    blocking_spid AS head_blocker,
    count(*) AS blocked_sessions,
    sum(wait_time_in_seconds) AS total_blocked_time,
    max(wait_time_in_seconds) AS max_blocked_time,
    latest(blocking_query_text) AS blocker_query,
    latest(blocking_login_name) AS blocker_user
FROM Metric
WHERE metricName = 'sqlserver.blocking.wait_time_seconds'
GROUP BY blocking_spid
ORDER BY blocked_sessions DESC
SINCE 10 minutes ago
```

### Memory Grant Analysis (Bonus)

```nrql
-- Queries with memory grant issues
SELECT
    session_id,
    request_id,
    query_id,
    requested_memory_kb,
    granted_memory_kb,
    used_memory_kb,
    grant_usage_percentage,
    wait_time_ms,
    wait_order,
    is_next_candidate,
    grant_time,
    -- Session context
    login_name,
    host_name,
    database_name,
    -- Request context
    status,
    command,
    cpu_time_ms,
    total_elapsed_time_ms
FROM Metric
WHERE metricName = 'sqlserver.memory_grant.wait_time_ms'
  AND (wait_time_ms > 0 OR grant_usage_percentage < 50)  -- Waiting or inefficient
ORDER BY wait_time_ms DESC, grant_usage_percentage ASC
SINCE 5 minutes ago
LIMIT 100
```

### Server-Level Wait Stats (Bonus)

```nrql
-- Top wait types for the server
SELECT
    wait_type,
    waiting_tasks_count,
    wait_time_ms,
    resource_wait_time_ms,
    signal_wait_time_ms,
    percentage_of_total,
    collection_timestamp
FROM Metric
WHERE metricName = 'sqlserver.server.wait_stats'
ORDER BY wait_time_ms DESC
SINCE 10 minutes ago
LIMIT 20
```

---

## RCA Enhancement Columns

### Performance Variance Analysis

**Purpose**: Detect query regression and performance instability

| Metric | Columns | Insight |
|--------|---------|---------|
| CPU Time Variance | min_cpu_time_ms, max_cpu_time_ms, last_cpu_time_ms, avg_cpu_time_ms | Large variance = plan regression or parameter sniffing |
| Elapsed Time Variance | min_elapsed_time_ms, max_elapsed_time_ms, last_elapsed_time_ms, avg_elapsed_time_ms | Large variance = locking contention or I/O issues |
| I/O Variance | min_logical_reads, max_logical_reads, last_logical_reads | Large variance = index usage changes |
| Rows Variance | min_rows, max_rows, last_rows | Large variance = data skew or parameter sniffing |

**Anomaly Detection**:
```
IF last_elapsed_time_ms > max_elapsed_time_ms THEN
    Status = "WORSE_THAN_HISTORICAL_MAX"
ELSE IF last_elapsed_time_ms > avg_elapsed_time_ms * 2 THEN
    Status = "DEGRADED_PERFORMANCE"
ELSE IF last_elapsed_time_ms < min_elapsed_time_ms THEN
    Status = "BETTER_THAN_HISTORICAL_MIN"
ELSE
    Status = "NORMAL"
END IF
```

---

### Memory Grant Analysis

**Purpose**: Detect memory pressure and grant inefficiency

| Metric | Columns | Insight |
|--------|---------|---------|
| Grant Accuracy | requested_memory_kb, granted_memory_kb | granted < requested = memory pressure |
| Grant Efficiency | granted_memory_kb, used_memory_kb | used << granted = over-estimation |
| Grant Variance | min_grant_kb, max_grant_kb, last_grant_kb | Large variance = parameter sniffing |

**Memory Pressure Indicators**:
- `wait_time_ms > 0` on memory grants = Memory contention
- `grant_usage_percentage < 50%` = Over-estimation (wasted memory)
- `grant_usage_percentage > 95%` AND `last_spills > 0` = Under-estimation

---

### TempDB Spill Detection

**Purpose**: Detect memory pressure causing spills to disk

| Metric | Columns | Insight |
|--------|---------|---------|
| Spill Frequency | total_spills, last_spills | Frequent spills = insufficient memory grants |
| Spill Magnitude | min_spills, max_spills | Large spills = I/O bottleneck |

**Root Cause**:
- `last_spills > 0` AND `last_grant_kb > 0` = Under-estimated memory grant
- `last_spills > 0` AND `wait_time_ms > 0` = Memory contention prevented adequate grant

---

### Parallelism Analysis

**Purpose**: Detect parallelism issues

| Metric | Columns | Insight |
|--------|---------|---------|
| DOP Variance | min_dop, max_dop, last_dop | Inconsistent parallelism = resource contention |
| Worker Count | parallel_worker_count | Actual vs expected workers |

**Parallelism Issues**:
- `last_dop = 1` but `max_dop > 1` = MAXDOP hint or resource pressure
- `parallel_worker_count < last_dop` = Worker starvation
- `last_dop > max_dop` = Plan recompile with different parallelism

---

### Session Diagnostics

**Purpose**: Detect session-level issues

| Metric | Columns | Insight |
|--------|---------|---------|
| Session Age | login_time, session_total_elapsed_ms | Long-lived sessions |
| Idle Detection | last_request_start_time, last_request_end_time | Idle sessions holding locks |
| Resource Leaks | open_resultset_count, open_transaction_count | Cursor leaks, uncommitted transactions |
| Error History | prev_error | Last error code |

**Common Patterns**:
- `(NOW() - last_request_end_time) > 300s` AND `open_transaction_count > 0` = Idle session with open transaction
- `open_resultset_count > 10` = Cursor leak
- `session_cpu_time_ms >> request_cpu_time_ms` = Long-lived session with multiple queries

---

### Progress Tracking

**Purpose**: Estimate completion time for long operations

| Metric | Columns | Insight |
|--------|---------|---------|
| ETA | percent_complete, estimated_completion_time_ms | Only for BACKUP, INDEX REBUILD, etc. |

**Supported Operations**:
- BACKUP DATABASE/LOG
- RESTORE DATABASE/LOG
- DBCC CHECKDB/CHECKFILEGROUP/CHECKTABLE
- ALTER INDEX REORGANIZE
- DBCC SHRINKDATABASE/SHRINKFILE
- ROLLBACK

---

## Performance Considerations

### Query Execution Cost

| Query | DMVs Used | Approx Cost | Frequency Recommendation |
|-------|-----------|-------------|--------------------------|
| SlowQueryEnhanced | query_stats, sql_text, cached_plans, plan_attributes | Medium | Every 60s |
| ActiveRunningQueriesQueryEnhanced | requests, sessions, sql_text (blocking) | Low-Medium | Every 30-60s |
| ActiveQueryWaitDetailsQuery | waiting_tasks, requests, sessions, tran_locks, partitions | Medium-High | Every 60s |
| BlockingSessionsQueryEnhanced | requests (self-join), sessions, sql_text, input_buffer | Medium | Every 30-60s |
| QueryMemoryGrantsQuery | query_memory_grants, requests, sessions | Low | Every 60s |
| ServerWaitStatsQuery | os_wait_stats | Low | Every 300s (5 min) |

### Optimization Strategies

1. **Limit TOP N**: All queries use `TOP (@Limit)` to prevent excessive results
2. **Filter Early**: System sessions/databases excluded early in WHERE clause
3. **Indexed DMVs**: Most DMVs have internal indexes on session_id, query_hash
4. **CROSS APPLY**: Used for sql_text extraction (efficient)
5. **LEFT JOIN**: Used for optional data (blocking queries, object resolution)
6. **CASE Resolution**: Object name resolution only for matching resource types

### Resource Impact

**High-Traffic Scenarios** (1000+ active queries):
- Consider increasing `collection_interval` to 60-90s
- Reduce `@Limit` from 100 to 50
- Disable `ActiveQueryWaitDetailsQuery` (most expensive)

**Low-Traffic Scenarios** (<100 active queries):
- Can reduce `collection_interval` to 30s
- Increase `@Limit` to 200
- Enable all queries

---

## Data Flow Diagram

```
┌───────────────────────────────────────────────────────────────────────┐
│                           OTEL COLLECTOR                               │
│                                                                        │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │   Scraper: QueryPerformanceMonitoringScraper                  │  │
│  │                                                                │  │
│  │   1. Execute SlowQueryEnhanced (every 60s)                    │  │
│  │      → sys.dm_exec_query_stats (historical)                   │  │
│  │      → Emit: sqlserver.slowquery.* metrics                    │  │
│  │         Attributes: query_id, database_name, query_text       │  │
│  │                                                                │  │
│  │   2. Execute ActiveRunningQueriesQueryEnhanced (every 60s)    │  │
│  │      → sys.dm_exec_requests + sessions (real-time)            │  │
│  │      → Emit: sqlserver.activequery.* metrics                  │  │
│  │         Attributes: session_id, query_id, user_name, etc.     │  │
│  │                                                                │  │
│  │   3. Execute ActiveQueryWaitDetailsQuery (every 60s)          │  │
│  │      → sys.dm_os_waiting_tasks + locks + partitions           │  │
│  │      → Emit: sqlserver.activequery.wait_details metrics       │  │
│  │         Attributes: worker_id, wait_type, object_name         │  │
│  │                                                                │  │
│  │   4. Execute BlockingSessionsQueryEnhanced (every 60s)        │  │
│  │      → sys.dm_exec_requests (self-join)                       │  │
│  │      → Emit: sqlserver.blocking.* metrics                     │  │
│  │         Attributes: blocking_spid, blocked_spid               │  │
│  │                                                                │  │
│  │   5. Execute QueryMemoryGrantsQuery (every 60s)               │  │
│  │      → sys.dm_exec_query_memory_grants                        │  │
│  │      → Emit: sqlserver.memory_grant.* metrics                 │  │
│  │                                                                │  │
│  │   6. Execute ServerWaitStatsQuery (every 300s)                │  │
│  │      → sys.dm_os_wait_stats                                   │  │
│  │      → Emit: sqlserver.server.wait_stats metrics              │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                ↓                                       │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │   Helper: Query Anonymizer                                     │  │
│  │   - Replaces literals with placeholders                        │  │
│  │   - Applied to query_text attribute                            │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                ↓                                       │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │   OTLP Exporter                                                │  │
│  │   - Batch metrics                                              │  │
│  │   - Export to New Relic                                        │  │
│  └────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────┘
                                ↓
┌───────────────────────────────────────────────────────────────────────┐
│                          NEW RELIC NRDB                                │
│                                                                        │
│   Metrics:                                                             │
│   - sqlserver.slowquery.avg_elapsed_time_ms (query_id, query_text)    │
│   - sqlserver.activequery.elapsed_time_ms (session_id, query_id)      │
│   - sqlserver.activequery.wait_details (worker_id, wait_type)         │
│   - sqlserver.blocking.wait_time_seconds (blocking_spid, blocked_spid)│
│   - sqlserver.memory_grant.wait_time_ms (session_id)                  │
│   - sqlserver.server.wait_stats (wait_type)                           │
│                                                                        │
│   Correlation:                                                         │
│   - JOIN activequery ↔ slowquery ON query_id                          │
│   - JOIN activequery ↔ wait_details ON session_id, request_id         │
│   - JOIN blocking ↔ activequery ON blocked_spid = session_id          │
└───────────────────────────────────────────────────────────────────────┘
                                ↓
┌───────────────────────────────────────────────────────────────────────┐
│                          UI PAGES (NRQL)                               │
│                                                                        │
│   Page 1: Slow Query List                                             │
│     SELECT FROM slowquery.* WHERE query_id IS NOT NULL                │
│                                                                        │
│   Page 2: Query Details                                               │
│     Use query_text with anonymization                                 │
│                                                                        │
│   Page 3: Timeline Bar Chart                                          │
│     SELECT count(*) FROM activequery.* TIMESERIES 1 minute            │
│                                                                        │
│   Page 4: Active Query Details                                        │
│     SELECT FROM activequery.* WHERE query_id = @clicked               │
│     LEFT JOIN slowquery.* ON query_id (correlation)                   │
│                                                                        │
│   Page 5: Wait Analysis                                               │
│     SELECT FROM wait_details WHERE query_id = @clicked                │
│                                                                        │
│   Page 6: Blocking Queries                                            │
│     SELECT FROM blocking.* ORDER BY wait_time_seconds DESC            │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Next Steps

### Immediate Tasks

1. **Integration**: Replace sections in `query_performance_monitoring_metrics.go` with ENHANCED versions
2. **Models**: Update Go structs in `models/` to include new RCA fields
3. **Scrapers**: Update scrapers to emit new metrics
4. **Testing**: Validate all queries against SQL Server 2019+, Azure SQL DB, Azure SQL MI

### Future Enhancements

1. **Additional DMVs**:
   - `sys.dm_db_index_usage_stats` - Index usage patterns
   - `sys.dm_db_missing_index_details` - Missing index recommendations
   - `sys.dm_exec_procedure_stats` - Stored procedure performance

2. **Advanced RCA**:
   - Parameter sniffing detection (variance > 10x)
   - Plan regression detection (query_plan_hash changes)
   - Lock escalation tracking
   - Deadlock victim analysis

3. **Performance Optimizations**:
   - Incremental collection (only changed plans)
   - Client-side caching (reduce DMV queries)
   - Adaptive sampling (high-traffic scenarios)

---

## References

- [Microsoft Docs: sys.dm_exec_query_stats](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-stats-transact-sql)
- [Microsoft Docs: sys.dm_exec_requests](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-requests-transact-sql)
- [Microsoft Docs: sys.dm_exec_sessions](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-sessions-transact-sql)
- [Microsoft Docs: sys.dm_os_waiting_tasks](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-waiting-tasks-transact-sql)
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [CORRELATION_STRATEGY.md](./CORRELATION_STRATEGY.md)
- [IMPLEMENTATION_COMPLETE.md](./IMPLEMENTATION_COMPLETE.md)

---

**Document Version**: 1.0
**Created**: 2025-11-22
**Author**: Claude Code (Anthropic)
**Status**: ✅ Complete
