# RCA Column Analysis: Current vs. Missing

## Overview

This document analyzes which columns are currently captured vs. which columns are **MISSING but critical for Root Cause Analysis (RCA)**. The goal is to identify gaps in the current implementation and recommend enhancements.

---

## 1. sys.dm_exec_requests (Active Queries)

### ✅ Currently Captured (22 columns)

| Column | Purpose | RCA Value |
|--------|---------|-----------|
| session_id | Session identifier | HIGH - Join key |
| request_id | Request identifier | HIGH - Unique request ID |
| database_id | Database context | HIGH - Filter/group |
| start_time | When request started | HIGH - Duration calculation |
| status | Request status (running/suspended) | HIGH - State analysis |
| command | Command type (SELECT/INSERT) | MEDIUM - Query classification |
| wait_type | Current wait type | **CRITICAL** - Bottleneck identification |
| wait_time | Wait duration (ms) | **CRITICAL** - Performance impact |
| wait_resource | Resource being waited on | **CRITICAL** - Contention analysis |
| last_wait_type | Previous wait type | MEDIUM - Wait pattern |
| blocking_session_id | Blocker session ID | **CRITICAL** - Blocking chain |
| cpu_time | CPU consumed (ms) | HIGH - Resource usage |
| total_elapsed_time | Total execution time | HIGH - Performance |
| reads | Physical disk reads | HIGH - I/O analysis |
| writes | Disk writes | HIGH - I/O analysis |
| logical_reads | Buffer cache reads | HIGH - Memory pressure |
| row_count | Rows returned | MEDIUM - Result size |
| granted_query_memory | Memory pages granted | HIGH - Memory analysis |
| transaction_id | Transaction ID | HIGH - Transaction tracking |
| open_transaction_count | Open transactions | HIGH - Long transaction detection |
| transaction_isolation_level | Isolation level | HIGH - Concurrency issues |
| dop | Degree of parallelism | HIGH - Parallelism analysis |
| parallel_worker_count | Parallel workers | HIGH - Parallelism analysis |
| query_hash | Query hash (correlation) | **CRITICAL** - Query correlation |
| plan_handle | Execution plan handle | HIGH - Plan retrieval |
| sql_handle | SQL batch handle | HIGH - Text retrieval |
| statement_start_offset | Statement offset | HIGH - Text extraction |
| statement_end_offset | Statement end offset | HIGH - Text extraction |

### ❌ MISSING Columns - HIGH Priority for RCA

| Column | Data Type | RCA Use Case | Priority |
|--------|-----------|--------------|----------|
| **percent_complete** | real | **ETA for long-running operations** (BACKUP, RESTORE, DBCC, INDEX REBUILD). Shows % done. | **CRITICAL** |
| **estimated_completion_time** | bigint | **ETA in milliseconds** for long operations. Enables progress tracking. | **CRITICAL** |
| **scheduler_id** | int | **CPU affinity issues**. Identify which scheduler is handling request. | HIGH |
| **task_address** | varbinary(8) | **Join to sys.dm_os_waiting_tasks** for detailed wait analysis. | **CRITICAL** |
| **deadlock_priority** | int | **Deadlock victim selection**. Who gets killed in deadlock. | HIGH |
| **lock_timeout** | int | **Lock timeout setting** (ms). Explains timeout errors. | HIGH |
| **nest_level** | int | **Stored proc nesting**. Depth of procedure calls. | MEDIUM |
| **prev_error** | int | **Last error occurred**. Error context for troubleshooting. | HIGH |
| **connection_id** | uniqueidentifier | **Connection tracking**. Unique connection ID. | MEDIUM |
| **context_info** | varbinary(128) | **Application context**. Custom tracking info set by app. | LOW |
| **user_id** | int | **User who submitted request**. Security/audit trail. | MEDIUM |
| **open_resultset_count** | int | **Result sets still open**. Detects cursor/result set leaks. | MEDIUM |
| **is_resumable** | bit | **Resumable index operations** (SQL 2017+). | LOW |
| **page_resource** | binary(8) | **Page resource being waited on** (SQL 2019+). | MEDIUM |

**Recommendation:** Add these 14 columns to ActiveRunningQueriesQuery.

**Impact:**
- ✅ **ETA tracking** for long-running operations (BACKUP, INDEX REBUILD)
- ✅ **Detailed wait analysis** via task_address join
- ✅ **Deadlock analysis** with deadlock_priority
- ✅ **Error context** with prev_error
- ✅ **CPU affinity issues** with scheduler_id

---

## 2. sys.dm_exec_sessions (Session Context)

### ✅ Currently Captured (5 columns)

| Column | Purpose | RCA Value |
|--------|---------|-----------|
| session_id | Session identifier | HIGH - Join key |
| login_name | Login name | HIGH - User identification |
| host_name | Client hostname | HIGH - Client identification |
| program_name | Application name | HIGH - Application tracking |
| status | Session status | MEDIUM - Session state |

### ❌ MISSING Columns - HIGH Priority for RCA

| Column | Data Type | RCA Use Case | Priority |
|--------|-----------|--------------|----------|
| **login_time** | datetime | **Session age**. How long session has been connected. | HIGH |
| **last_request_start_time** | datetime | **Last activity time**. Detect idle sessions. | **CRITICAL** |
| **last_request_end_time** | datetime | **Request completion time**. Gap analysis. | HIGH |
| **cpu_time** | int | **Total CPU for session** (ms). Session-level resource usage. | **CRITICAL** |
| **memory_usage** | int | **Pages of memory used** (8KB pages). Memory leak detection. | **CRITICAL** |
| **total_elapsed_time** | int | **Time since session established** (ms). | HIGH |
| **total_scheduled_time** | int | **Total execution time** (ms). | HIGH |
| **reads** | bigint | **Session-level physical reads**. I/O impact. | HIGH |
| **writes** | bigint | **Session-level writes**. I/O impact. | HIGH |
| **logical_reads** | bigint | **Session-level logical reads**. Buffer usage. | HIGH |
| **transaction_isolation_level** | smallint | **Session isolation level**. Concurrency setting. | **CRITICAL** |
| **lock_timeout** | int | **Session lock timeout** (ms). | HIGH |
| **deadlock_priority** | int | **Session deadlock priority**. | HIGH |
| **row_count** | bigint | **Total rows returned**. Session activity. | MEDIUM |
| **open_transaction_count** | int | **Open transactions**. Long transaction detection. | **CRITICAL** |
| **client_version** | int | **TDS protocol version**. Driver issues. | LOW |
| **client_interface_name** | nvarchar(32) | **Driver/library name** (ODBC, JDBC, .NET). | HIGH |
| **nt_domain** | nvarchar(128) | **Windows domain**. Windows auth context. | LOW |
| **nt_user_name** | nvarchar(128) | **Windows user**. Windows auth context. | LOW |
| **original_login_name** | nvarchar(128) | **Original login** (before EXECUTE AS). Security audit. | MEDIUM |

**Recommendation:** Add these 20 columns to ActiveRunningQueriesQuery (via JOIN).

**Impact:**
- ✅ **Idle session detection** with last_request_start_time
- ✅ **Session-level resource usage** (CPU, memory, I/O)
- ✅ **Memory leak detection** with memory_usage
- ✅ **Driver identification** with client_interface_name
- ✅ **Long transaction detection** with open_transaction_count

---

## 3. sys.dm_exec_query_stats (Historical Query Performance)

### ✅ Currently Captured (12 columns)

| Column | Purpose | RCA Value |
|--------|---------|-----------|
| query_hash | Query identifier | **CRITICAL** - Correlation key |
| plan_handle | Plan identifier | HIGH - Plan retrieval |
| sql_handle | SQL batch handle | HIGH - Text retrieval |
| execution_count | Number of executions | HIGH - Frequency analysis |
| last_execution_time | Last execution | HIGH - Recency |
| total_worker_time | Total CPU (µs) | HIGH - Resource usage |
| total_elapsed_time | Total elapsed (µs) | HIGH - Performance |
| total_logical_reads | Total logical reads | HIGH - I/O |
| total_logical_writes | Total logical writes | HIGH - I/O |
| total_physical_reads | Total physical reads | HIGH - I/O |
| total_rows | Total rows returned | MEDIUM - Result size |
| avg_* (calculated) | Averages | HIGH - Performance baseline |

### ❌ MISSING Columns - HIGH Priority for RCA

| Column | Data Type | RCA Use Case | Priority |
|--------|-----------|--------------|----------|
| **query_plan_hash** | binary(8) | **Identify similar execution plans**. Plan regression analysis. | **CRITICAL** |
| **min_worker_time** | bigint | **Best CPU time** (µs). Variance analysis. | **CRITICAL** |
| **max_worker_time** | bigint | **Worst CPU time** (µs). Variance analysis. | **CRITICAL** |
| **min_elapsed_time** | bigint | **Best elapsed time** (µs). Performance variance. | **CRITICAL** |
| **max_elapsed_time** | bigint | **Worst elapsed time** (µs). Performance regression. | **CRITICAL** |
| **min_logical_reads** | bigint | **Best I/O**. Variance analysis. | HIGH |
| **max_logical_reads** | bigint | **Worst I/O**. I/O regression. | HIGH |
| **min_rows** | bigint | **Minimum rows returned**. Cardinality variance. | MEDIUM |
| **max_rows** | bigint | **Maximum rows returned**. Cardinality explosion. | HIGH |
| **last_rows** | bigint | **Rows in last execution**. Recent behavior. | MEDIUM |
| **creation_time** | datetime | **When plan compiled**. Plan age. | HIGH |
| **plan_generation_num** | bigint | **Plan version**. Recompile tracking. | MEDIUM |
| **total_dop** | bigint | **Total degree of parallelism**. (SQL 2016+) | **CRITICAL** |
| **last_dop** | bigint | **Last DOP used**. (SQL 2016+) | HIGH |
| **min_dop** | bigint | **Minimum DOP**. (SQL 2016+) | MEDIUM |
| **max_dop** | bigint | **Maximum DOP**. (SQL 2016+) | MEDIUM |
| **total_grant_kb** | bigint | **Total memory grant** (KB). (SQL 2016+) | **CRITICAL** |
| **last_grant_kb** | bigint | **Last memory grant** (KB). (SQL 2016+) | **CRITICAL** |
| **min_grant_kb** | bigint | **Minimum grant** (KB). (SQL 2016+) | HIGH |
| **max_grant_kb** | bigint | **Maximum grant** (KB). (SQL 2016+) | HIGH |
| **total_used_grant_kb** | bigint | **Total memory used** (KB). (SQL 2016+) | **CRITICAL** |
| **last_used_grant_kb** | bigint | **Last memory used** (KB). (SQL 2016+) | **CRITICAL** |
| **min_used_grant_kb** | bigint | **Min memory used** (KB). (SQL 2016+) | MEDIUM |
| **max_used_grant_kb** | bigint | **Max memory used** (KB). (SQL 2016+) | MEDIUM |
| **total_ideal_grant_kb** | bigint | **Total ideal grant** (KB). (SQL 2016+) | HIGH |
| **last_ideal_grant_kb** | bigint | **Last ideal grant** (KB). (SQL 2016+) | HIGH |
| **total_reserved_threads** | bigint | **Total threads reserved**. (SQL 2016+) | MEDIUM |
| **last_reserved_threads** | bigint | **Last threads reserved**. (SQL 2016+) | MEDIUM |
| **total_used_threads** | bigint | **Total threads used**. (SQL 2016+) | MEDIUM |
| **last_used_threads** | bigint | **Last threads used**. (SQL 2016+) | MEDIUM |
| **total_spills** | bigint | **Total TempDB spills** (pages). (SQL 2016 SP2+) | **CRITICAL** |
| **last_spills** | bigint | **Last spill count** (pages). (SQL 2016 SP2+) | **CRITICAL** |
| **min_spills** | bigint | **Min spills**. (SQL 2016 SP2+) | MEDIUM |
| **max_spills** | bigint | **Max spills**. (SQL 2016 SP2+) | **CRITICAL** |

**Recommendation:** Add these 33 columns to SlowQuery.

**Impact:**
- ✅ **Performance variance analysis** (min/max elapsed, CPU, I/O)
- ✅ **Plan regression detection** with query_plan_hash
- ✅ **Memory grant analysis** (requested vs used)
- ✅ **TempDB spill detection** with total_spills
- ✅ **Parallelism analysis** with DOP metrics
- ✅ **Memory grant accuracy** (ideal vs granted vs used)

---

## 4. sys.dm_tran_locks (Lock Information)

### ✅ Currently Captured (9 columns)

| Column | Purpose | RCA Value |
|--------|---------|-----------|
| request_session_id | Session holding lock | HIGH - Join key |
| resource_type | Resource type (KEY/PAGE/OBJECT) | **CRITICAL** - Lock granularity |
| resource_database_id | Database ID | HIGH - Scope |
| resource_description | Resource details | HIGH - Specific resource |
| resource_associated_entity_id | Object/HoBt ID | HIGH - Object identification |
| request_mode | Lock mode (S/X/IS/IX) | **CRITICAL** - Lock type |
| request_status | Lock status (GRANTED/WAIT) | **CRITICAL** - Contention |
| request_type | Request type (LOCK) | LOW - Always LOCK |
| lock_owner_address | Internal address | MEDIUM - Join key |

### ❌ MISSING Columns - HIGH Priority for RCA

| Column | Data Type | RCA Use Case | Priority |
|--------|-----------|--------------|----------|
| **request_owner_type** | nvarchar(60) | **Who owns lock** (TRANSACTION/CURSOR/SESSION). | **CRITICAL** |
| **request_owner_id** | bigint | **Transaction ID owning lock**. Transaction analysis. | **CRITICAL** |
| **request_owner_guid** | uniqueidentifier | **Distributed transaction GUID**. DTC tracking. | MEDIUM |
| **request_reference_count** | smallint | **Lock request count**. Hotspot detection. | HIGH |
| **request_lifetime** | int | **Lock lifetime**. Long-held lock detection. | HIGH |
| **request_exec_context_id** | int | **Execution context**. Parallel query tracking. | MEDIUM |
| **request_request_id** | int | **Batch ID**. Request correlation. | MEDIUM |
| **resource_lock_partition** | int | **Lock partition ID**. Partitioned lock resources. | LOW |
| **resource_subtype** | nvarchar(60) | **Resource subtype**. Detailed resource info. | MEDIUM |

**Recommendation:** Add these 9 columns to LockedObjectsBySessionQuery.

**Impact:**
- ✅ **Lock owner identification** with request_owner_type
- ✅ **Transaction tracking** with request_owner_id
- ✅ **Hotspot detection** with request_reference_count
- ✅ **Long-held lock detection** with request_lifetime

---

## 5. NEW DMVs to Add for RCA

### 🆕 sys.dm_os_waiting_tasks - **CRITICAL for RCA!**

**Why:** Provides detailed wait analysis at the **task level** (not just request level). Essential for understanding:
- **Which specific resource** is causing waits
- **Blocking chains** (who's blocking whom)
- **Wait duration** at task level
- **Parallel query waits** (each task can wait on different resources)

**Key Columns:**

| Column | Data Type | RCA Use Case | Priority |
|--------|-----------|--------------|----------|
| **waiting_task_address** | varbinary(8) | **Join to dm_exec_requests.task_address**. | **CRITICAL** |
| **session_id** | int | **Session waiting**. | **CRITICAL** |
| **exec_context_id** | int | **Execution context** (parallel worker ID). | **CRITICAL** |
| **wait_duration_ms** | bigint | **How long waiting** (ms). | **CRITICAL** |
| **wait_type** | nvarchar(60) | **Type of wait**. | **CRITICAL** |
| **resource_address** | varbinary(8) | **Resource being waited on**. | **CRITICAL** |
| **blocking_task_address** | varbinary(8) | **Task that's blocking**. | **CRITICAL** |
| **blocking_session_id** | smallint | **Session that's blocking**. | **CRITICAL** |
| **resource_description** | nvarchar(3072) | **What resource** (KEY, PAGE, RID). | **CRITICAL** |

**Use Case:**
```sql
-- Find what each parallel worker is waiting on
SELECT
    r.session_id,
    r.query_hash,
    wt.exec_context_id AS worker_id,
    wt.wait_type,
    wt.wait_duration_ms,
    wt.blocking_session_id,
    wt.resource_description
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_os_waiting_tasks wt
WHERE wt.session_id = r.session_id
```

**Recommendation:** Create new query `ActiveQueryWaitDetailsQuery` that joins `dm_exec_requests` with `dm_os_waiting_tasks`.

---

### 🆕 sys.dm_exec_query_memory_grants - Memory Grant Analysis

**Why:** Diagnose memory pressure, memory grant waits, and memory grant accuracy.

**Key Columns:**

| Column | Data Type | RCA Use Case | Priority |
|--------|-----------|--------------|----------|
| **session_id** | smallint | **Session ID**. | **CRITICAL** |
| **request_id** | int | **Request ID**. | **CRITICAL** |
| **requested_memory_kb** | bigint | **Memory requested** (KB). | **CRITICAL** |
| **granted_memory_kb** | bigint | **Memory granted** (KB). | **CRITICAL** |
| **required_memory_kb** | bigint | **Minimum memory needed** (KB). | **CRITICAL** |
| **used_memory_kb** | bigint | **Actually used** (KB). | **CRITICAL** |
| **max_used_memory_kb** | bigint | **Peak usage** (KB). | **CRITICAL** |
| **query_cost** | float | **Optimizer estimated cost**. | HIGH |
| **timeout_sec** | int | **Grant timeout**. | HIGH |
| **resource_semaphore_id** | smallint | **Semaphore type** (0=small, 1=large). | MEDIUM |
| **queue_id** | smallint | **Which queue** (0=default). | MEDIUM |
| **wait_order** | int | **Position in grant queue**. | **CRITICAL** |
| **is_next_candidate** | bit | **Next to receive grant**. | HIGH |
| **wait_time_ms** | int | **Time waiting for grant** (ms). | **CRITICAL** |
| **grant_time** | datetime | **When grant was given**. | MEDIUM |

**Use Case:**
```sql
-- Find queries waiting for memory grants
SELECT
    session_id,
    requested_memory_kb,
    wait_time_ms,
    wait_order,
    is_next_candidate
FROM sys.dm_exec_query_memory_grants
WHERE grant_time IS NULL  -- Not yet granted
ORDER BY wait_order
```

**Recommendation:** Create new query `ActiveQueryMemoryGrantsQuery`.

---

### 🆕 sys.dm_os_wait_stats - Server-Level Wait Statistics

**Why:** Aggregate wait statistics for the entire server. Identify top wait types.

**Key Columns:**

| Column | Data Type | RCA Use Case | Priority |
|--------|-----------|--------------|----------|
| **wait_type** | nvarchar(60) | **Type of wait**. | **CRITICAL** |
| **waiting_tasks_count** | bigint | **Number of waits**. | **CRITICAL** |
| **wait_time_ms** | bigint | **Total wait time** (ms). | **CRITICAL** |
| **max_wait_time_ms** | bigint | **Maximum single wait** (ms). | HIGH |
| **signal_wait_time_ms** | bigint | **Signal wait time** (CPU runnable but waiting). | **CRITICAL** |

**Calculated Metric:**
- `resource_wait_time_ms = wait_time_ms - signal_wait_time_ms` (actual resource wait)

**Recommendation:** Create new query `ServerWaitStatsQuery`.

---

## 6. Priority Summary

### 🔴 **CRITICAL - Implement Immediately**

1. **Add sys.dm_os_waiting_tasks** - Detailed wait analysis (NEW DMV)
2. **Add sys.dm_exec_query_memory_grants** - Memory grant analysis (NEW DMV)
3. **sys.dm_exec_requests missing columns:**
   - `percent_complete` - ETA for long operations
   - `estimated_completion_time` - ETA in milliseconds
   - `task_address` - Join to waiting_tasks
4. **sys.dm_exec_sessions missing columns:**
   - `last_request_start_time` - Idle session detection
   - `cpu_time` - Session CPU usage
   - `memory_usage` - Memory leak detection
   - `transaction_isolation_level` - Concurrency issues
   - `open_transaction_count` - Long transactions
5. **sys.dm_exec_query_stats missing columns:**
   - `min_worker_time, max_worker_time` - CPU variance
   - `min_elapsed_time, max_elapsed_time` - Performance variance
   - `query_plan_hash` - Plan regression
   - `total_spills, last_spills, max_spills` - TempDB spills (SQL 2016 SP2+)
   - Memory grant columns (SQL 2016+): `total_grant_kb, last_grant_kb, total_used_grant_kb, last_used_grant_kb`
   - DOP columns (SQL 2016+): `total_dop, last_dop`
6. **sys.dm_tran_locks missing columns:**
   - `request_owner_type, request_owner_id` - Lock owner identification

### 🟡 **HIGH - Implement Next**

1. **Add sys.dm_os_wait_stats** - Server-level wait analysis (NEW DMV)
2. Additional variance columns from dm_exec_query_stats
3. Additional session-level I/O columns (reads, writes, logical_reads)
4. Lock hotspot detection columns (request_reference_count, request_lifetime)

### 🟢 **MEDIUM - Nice to Have**

1. Nested procedure tracking (nest_level)
2. Client driver identification (client_interface_name)
3. Distributed transaction tracking
4. Context info and custom tracking

---

## 7. Implementation Roadmap

### Phase 1: Critical Wait Analysis (Week 1-2)
- [ ] Add sys.dm_os_waiting_tasks integration
- [ ] Add task_address to dm_exec_requests
- [ ] Add percent_complete and estimated_completion_time
- [ ] Update ActiveRunningQueriesQuery

### Phase 2: Session Context & Memory (Week 3-4)
- [ ] Add sys.dm_exec_query_memory_grants integration
- [ ] Add session-level resource columns (CPU, memory, I/O)
- [ ] Add last_request_start_time for idle detection
- [ ] Update ActiveRunningQueriesQuery

### Phase 3: Historical Performance Variance (Week 5-6)
- [ ] Add min/max columns to dm_exec_query_stats
- [ ] Add TempDB spill tracking
- [ ] Add memory grant columns (SQL 2016+)
- [ ] Add DOP columns (SQL 2016+)
- [ ] Update SlowQuery

### Phase 4: Lock Analysis & Server Wait Stats (Week 7-8)
- [ ] Add request_owner_type and request_owner_id to locks
- [ ] Add sys.dm_os_wait_stats integration
- [ ] Update LockedObjectsBySessionQuery

### Phase 5: Testing & Validation
- [ ] Test with production_load_generator.sql
- [ ] Verify all metrics in NRDB
- [ ] Performance testing
- [ ] Documentation updates

---

## 8. Expected Benefits

### Before (Current State)
- ✅ Basic active query monitoring
- ✅ Basic slow query monitoring
- ✅ Basic blocking detection
- ❌ No detailed wait analysis
- ❌ No memory grant visibility
- ❌ No performance variance tracking
- ❌ No TempDB spill detection
- ❌ No parallel query task-level waits

### After (Enhanced State)
- ✅ **Task-level wait analysis** (why each parallel worker is waiting)
- ✅ **Memory grant tracking** (requested vs granted vs used)
- ✅ **Performance variance** (min/max/avg for CPU, elapsed, I/O)
- ✅ **TempDB spill detection** (memory pressure indicator)
- ✅ **ETA for long operations** (BACKUP, INDEX REBUILD progress)
- ✅ **Idle session detection** (last_request_start_time)
- ✅ **Memory leak detection** (session memory_usage)
- ✅ **Lock owner identification** (transaction vs cursor vs session)
- ✅ **Server-level wait analysis** (top wait types)

### RCA Scenarios Enabled

1. **"Why is my query slow?"**
   - Before: See it's waiting, but not what it's waiting on
   - After: See exact resource (page/key/lock) causing wait

2. **"Is this query always this slow?"**
   - Before: Only see current execution
   - After: Compare current vs min/avg/max historical performance

3. **"Is memory pressure causing issues?"**
   - Before: No visibility
   - After: See memory grants (requested vs granted vs used), TempDB spills, wait queues

4. **"Which queries are spilling to TempDB?"**
   - Before: No visibility
   - After: Track total_spills, last_spills, max_spills per query

5. **"Why are parallel queries slow?"**
   - Before: Only see request-level waits
   - After: See each parallel worker's wait type and resource

6. **"How much longer will this BACKUP take?"**
   - Before: No ETA
   - After: See percent_complete and estimated_completion_time

7. **"Is this session leaking memory?"**
   - Before: No visibility
   - After: Track session-level memory_usage over time

8. **"Who owns this lock?"**
   - Before: Only see session_id
   - After: See request_owner_type (TRANSACTION vs CURSOR vs SESSION) and transaction_id

---

## 9. Next Steps

1. **Review this analysis** with the team
2. **Prioritize** which phases to implement
3. **Update queries** to include missing columns
4. **Update models** to capture new columns
5. **Test** with production_load_generator.sql
6. **Deploy** and validate in NRDB

---

## 10. References

- [sys.dm_exec_requests Documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-requests-transact-sql)
- [sys.dm_os_waiting_tasks Documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-waiting-tasks-transact-sql)
- [sys.dm_exec_query_memory_grants Documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-memory-grants-transact-sql)
- [SQL Server Wait Statistics](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql)
