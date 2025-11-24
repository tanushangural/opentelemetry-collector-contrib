# AdventureWorks2022 Demo Scripts - User Guide

## Overview

These scripts generate comprehensive, realistic SQL Server workload for demonstrating OpenTelemetry monitoring with New Relic. They cover all RCA scenarios including slow queries, blocking sessions, various wait types, and complex execution plans.

## Prerequisites

- **Database**: AdventureWorks2022 ([Download here](https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure))
- **SQL Server**: 2016 or later
- **OpenTelemetry Collector**: Running with newrelicsqlserver receiver configured
- **Permissions**: db_datareader, db_datawriter on AdventureWorks2022

## Files Included

| File | Purpose | Use Case |
|------|---------|----------|
| `adventureworks_comprehensive_demo.sql` | Complete demo script with all scenarios | Full presentation/demo |
| `blocking_quick_start.sql` | Fast blocking generation (3 windows) | Quick blocking demo (5 min) |
| `DEMO_README.md` | This file | Instructions |

---

## Quick Start: Generate Blocking Sessions (5 Minutes)

### Step 1: Open SQL Server Management Studio (SSMS)

### Step 2: Run Blocker Session
1. Open **NEW Query Window → Window 1**
2. Copy `blocking_quick_start.sql` **WINDOW 1** section
3. Execute
4. **LEAVE THIS WINDOW OPEN** (it will run for 5 minutes)

### Step 3: Run Blocked Session #1
1. Open **NEW Query Window → Window 2** (immediately after Step 2)
2. Copy `blocking_quick_start.sql` **WINDOW 2** section
3. Execute
4. You'll see "Trying to UPDATE..." messages - this session is BLOCKED

### Step 4: Run Blocked Session #2 (Optional)
1. Open **NEW Query Window → Window 3**
2. Copy `blocking_quick_start.sql` **WINDOW 3** section
3. Execute
4. This creates SHARED lock contention

### Step 5: Wait 60-90 Seconds
The collector scrapes every 60 seconds. Wait for the next scrape cycle.

### Step 6: Query New Relic

```nrql
SELECT
    latest(blocking_spid) AS 'Blocker SPID',
    latest(blocked_spid) AS 'Blocked SPID',
    latest(wait_type) AS 'Wait Type',
    latest(wait_time_seconds) AS 'Wait Time (s)',
    latest(blocker_status) AS 'Blocker Status',
    latest(blocker_open_transaction_count) AS 'Open Txns',
    latest(blocker_program_name) AS 'Blocker App',
    latest(blocker_login_name) AS 'Blocker User',
    latest(blocker_host_name) AS 'Blocker Host',
    latest(wait_resource) AS 'Wait Resource',
    latest(database_name) AS 'Database'
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
FACET blocking_spid, blocked_spid
SINCE 10 minutes ago
LIMIT 50
```

**Expected Results:**
- 2-3 blocking sessions visible
- `wait_type`: LCK_M_X (Exclusive Lock)
- `blocker_status`: running
- `blocker_open_transaction_count`: 1
- `wait_resource`: Shows locked page/key (e.g., "PAGE: 5:1:680")
- `database_name`: AdventureWorks2022

---

## Full Demo: Comprehensive Scenarios (30+ Minutes)

For a complete presentation covering all monitoring scenarios:

### Step 1: Run Main Script
```sql
-- Execute in SSMS
:r adventureworks_comprehensive_demo.sql
```

This script runs automatically for 30+ minutes and generates:
- **CPU-intensive queries** (complex aggregations)
- **I/O-intensive queries** (table scans, PAGEIOLATCH waits)
- **Memory-intensive queries** (hash aggregations, spills)
- **Parallel queries** (CXPACKET, CXSYNC_PORT waits)
- **Transaction log waits** (WRITELOG)
- **CPU pressure** (SOS_SCHEDULER_YIELD)
- **Complex execution plans** (Nested Loops, Hash Match, Merge Join)

### Step 2: Manual Blocking Scenarios
The script will **print instructions** for manual blocking scenarios. Follow the printed instructions to open additional query windows for:
- Exclusive lock blocking (LCK_M_X)
- Shared lock blocking (LCK_M_S)
- Update lock blocking (LCK_M_U)
- Page lock blocking
- **FORGOTTEN TRANSACTION** (critical RCA scenario!)
- SERIALIZABLE isolation level blocking

### Step 3: Monitor in New Relic
Use the NRQL queries from the main conversation to analyze:
- Slow queries by type
- Active running queries with waits
- Blocking sessions by application
- Forgotten transactions (sleeping blockers)
- Lock contention patterns
- Performance impact measurements

---

## Blocking Scenario Details

### 1. Exclusive Lock Blocking (LCK_M_X)
**Scenario:** Two UPDATEs on same row

**BLOCKER:**
```sql
BEGIN TRANSACTION;
UPDATE Production.Product SET ListPrice = ListPrice * 1.01 WHERE ProductID = 680;
-- Don't commit for 2 minutes
WAITFOR DELAY '00:02:00';
ROLLBACK;
```

**BLOCKED:**
```sql
UPDATE Production.Product SET StandardCost = StandardCost * 1.02 WHERE ProductID = 680;
-- Will wait for blocker's exclusive lock
```

**In New Relic:**
- `wait_type`: LCK_M_X
- `blocker_status`: running
- `wait_resource`: KEY or PAGE reference

---

### 2. Shared Lock Blocking (LCK_M_S)
**Scenario:** SERIALIZABLE SELECT blocks UPDATE

**BLOCKER:**
```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION;
SELECT * FROM Sales.Customer WHERE CustomerID = 11000;
WAITFOR DELAY '00:02:00';
ROLLBACK;
```

**BLOCKED:**
```sql
UPDATE Sales.Customer SET AccountNumber = AccountNumber WHERE CustomerID = 11000;
-- Waits for shared lock to release
```

**In New Relic:**
- `wait_type`: LCK_M_S
- `blocker_isolation_level`: 4 (SERIALIZABLE)
- Root cause: Overly restrictive isolation level

---

### 3. FORGOTTEN TRANSACTION (Critical!)
**Scenario:** Application crashes with open transaction

**BLOCKER:**
```sql
BEGIN TRANSACTION;
UPDATE Production.ProductInventory SET Quantity = Quantity - 1 WHERE ProductID = 1;
-- Simulate crash - DO NOT COMMIT
-- Leave session IDLE
```

**BLOCKED:**
```sql
SELECT * FROM Production.ProductInventory WHERE ProductID = 1;
-- Blocked by forgotten transaction
```

**In New Relic (RCA Gold!):**
- `blocker_status`: **sleeping** (KEY INDICATOR!)
- `blocker_open_transaction_count`: 1 (KEY INDICATOR!)
- `wait_time_seconds`: > 60 (keeps growing)
- **Root Cause:** Application didn't close transaction properly

**RCA Query:**
```nrql
SELECT
    count(*) AS 'Forgotten Transactions',
    latest(blocker_program_name) AS 'Problematic App',
    latest(blocker_login_name) AS 'User',
    sum(wait_time_seconds) AS 'Total Impact (s)'
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
  AND blocker_status = 'sleeping'
  AND blocker_open_transaction_count > 0
FACET blocker_program_name, blocker_host_name
SINCE 1 hour ago
```

---

## Wait Type Scenarios

### CXSYNC_PORT / CXPACKET (Parallel Query Waits)
**Generated by:** Section 1.4 and 4.2 in comprehensive script
**Indicates:** Parallel query execution with thread coordination
**NRQL Query:**
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_time_seconds'
  AND wait_type IN ('CXSYNC_PORT', 'CXPACKET')
FACET degree_of_parallelism
SINCE 30 minutes ago
```

### PAGEIOLATCH_SH/EX (Disk I/O Waits)
**Generated by:** Run after `DBCC DROPCLEANBUFFERS`
**Indicates:** Physical disk reads
**NRQL Query:**
```nrql
SELECT
    average(sqlserver.activequery.reads) AS 'Avg Physical Reads'
FROM Metric
WHERE wait_type LIKE 'PAGEIOLATCH%'
SINCE 30 minutes ago
```

### WRITELOG (Transaction Log Waits)
**Generated by:** Section 3.2 (INSERT loop)
**Indicates:** Transaction log write bottleneck
**Recommendation:** Check log file placement, consider log file size

### ASYNC_NETWORK_IO (Network/Client Waits)
**Generated by:** Large result set with slow client consumption
**Indicates:** Client application not reading results fast enough
**Recommendation:** Check application code, network latency

### RESOURCE_SEMAPHORE (Memory Grant Waits)
**Generated by:** Section 3.4 (large aggregations)
**Indicates:** Waiting for query memory grant
**Recommendation:** Increase SQL Server max memory, optimize queries

---

## Execution Plan Scenarios

The comprehensive script generates queries with different join types:

### Nested Loops Join
**Best for:** Small result sets with indexed lookups
**Generated by:** Section 4.1
**Look for in Plan:** Nested Loops operator

### Hash Match Join
**Best for:** Large table joins without suitable indexes
**Generated by:** Section 4.2
**Look for in Plan:** Hash Match operator, potential memory spills

### Merge Join
**Best for:** Large pre-sorted inputs
**Generated by:** Section 4.3
**Look for in Plan:** Merge Join operator

**Check for Issues:**
- Warnings (missing index, implicit conversion)
- Expensive operators (> 50% cost)
- Table scans on large tables
- Memory spills (TempDB usage)

---

## Troubleshooting

### "No chart data available" in New Relic

**Possible causes:**
1. Collector not running
2. No blocking occurred yet
3. Wrong time window

**Diagnostic queries:**
```nrql
-- Check if ANY SQL Server metrics exist
SELECT count(*)
FROM Metric
WHERE metricName LIKE 'sqlserver%'
SINCE 1 hour ago

-- Check for slow queries (should always have data if collector works)
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
SINCE 1 hour ago

-- Check for active queries
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
SINCE 1 hour ago
```

### Blocking metrics not appearing

**Check SQL Server directly:**
```sql
-- See if blocking exists right now
SELECT
    blocking_session_id,
    session_id,
    wait_type,
    wait_time,
    wait_resource
FROM sys.dm_exec_requests
WHERE blocking_session_id != 0;
```

**If no rows:** No blocking is occurring
**If rows exist:** Wait 60-90 seconds for next scrape cycle

### Collector logs
```bash
# Check if blocking scraper is running
grep -i "blocking" collector.log

# Should see:
# "Executing blocking session metrics collection"
# "Successfully scraped blocking session metrics"
```

---

## Cleanup

After demo, clean up test data:

```sql
USE AdventureWorks2022;

-- Remove demo inserts
DELETE FROM Sales.SalesOrderDetail
WHERE CarrierTrackingNumber LIKE 'DEMO-%';

-- Kill any remaining blocking sessions
-- (Replace XXX with actual SPIDs)
KILL XXX;

-- Rollback forgotten transactions
-- Find them:
SELECT session_id, open_transaction_count, status
FROM sys.dm_exec_sessions
WHERE open_transaction_count > 0 AND is_user_process = 1;

-- Kill them:
KILL <session_id>;
```

---

## New Relic Dashboard Widgets

Create these widgets for a comprehensive demo dashboard:

### 1. Active Blocking Sessions (Billboard)
```nrql
SELECT uniqueCount(blocking_spid)
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
SINCE 5 minutes ago
```

### 2. Blocking Timeline (Line Chart)
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
TIMESERIES 1 minute
SINCE 1 hour ago
```

### 3. Top Blocking Applications (Bar Chart)
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
FACET blocker_program_name
SINCE 1 hour ago
LIMIT 10
```

### 4. Forgotten Transactions Alert (Billboard - Red if > 0)
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
  AND blocker_status = 'sleeping'
  AND blocker_open_transaction_count > 0
SINCE 5 minutes ago
```

### 5. Lock Type Distribution (Pie Chart)
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
FACET CASES (
  WHERE wait_type = 'LCK_M_X' AS 'Exclusive Lock',
  WHERE wait_type = 'LCK_M_S' AS 'Shared Lock',
  WHERE wait_type = 'LCK_M_U' AS 'Update Lock'
)
SINCE 1 hour ago
```

### 6. Wait Type Distribution (Pie Chart)
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.activequery.wait_time_seconds'
  AND wait_type IS NOT NULL
FACET CASES (
  WHERE wait_type LIKE 'CXSYNC%' AS 'Parallel Coordination',
  WHERE wait_type LIKE 'PAGEIOLATCH%' AS 'Disk I/O',
  WHERE wait_type LIKE 'LCK_M_%' AS 'Lock Waits',
  WHERE wait_type = 'WRITELOG' AS 'Log Writes'
)
SINCE 30 minutes ago
```

### 7. Slow Query List (Table)
```nrql
SELECT
    latest(query_text) AS 'Query',
    average(sqlserver.slowquery.avg_elapsed_time_ms) AS 'Avg Duration (ms)',
    sum(sqlserver.slowquery.execution_count) AS 'Executions',
    latest(database_name) AS 'Database'
FROM Metric
WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
FACET query_id
SINCE 30 minutes ago
LIMIT 20
```

---

## Best Practices for Demo

1. **Start with Quick Start** (blocking_quick_start.sql) - 5 minutes
2. **Show immediate results** in New Relic after 60-90 seconds
3. **Highlight RCA features:**
   - WHO is blocking (blocker_program_name, blocker_login_name)
   - WHAT is locked (wait_resource)
   - WHY it's blocking (blocker_status = 'sleeping', blocker_open_transaction_count > 0)
4. **Run comprehensive script** for full feature showcase
5. **Create dashboard** with pre-built widgets
6. **Set up alerts** for forgotten transactions

---

## Common Demo Scenarios

### Scenario 1: "Find the Forgotten Transaction"
**Story:** Application crashed, transactions left open

**Query:**
```nrql
SELECT
    latest(blocking_spid) AS 'Culprit Session',
    latest(blocker_program_name) AS 'Application',
    latest(blocker_login_name) AS 'User',
    latest(blocker_host_name) AS 'Host',
    latest(blocker_open_transaction_count) AS 'Open Txns',
    sum(wait_time_seconds) AS 'Total Impact (s)',
    count(blocked_spid) AS 'Sessions Blocked'
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
  AND blocker_status = 'sleeping'
  AND blocker_open_transaction_count > 0
FACET blocking_spid
SINCE 1 hour ago
```

**Outcome:** Pinpoint exact session, user, host causing the issue

---

### Scenario 2: "Identify Problematic Application"
**Story:** Which app is causing most blocking?

**Query:**
```nrql
SELECT
    count(*) AS 'Block Events',
    sum(wait_time_seconds) AS 'Total Wait Time (s)',
    uniqueCount(blocking_spid) AS 'Unique Blockers'
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
FACET blocker_program_name, blocker_login_name
SINCE 24 hours ago
LIMIT 10
```

**Outcome:** Identify app with most blocking impact

---

### Scenario 3: "Isolation Level Issues"
**Story:** Why is this app causing so much blocking?

**Query:**
```nrql
SELECT count(*)
FROM Metric
WHERE metricName = 'sqlserver.blocking.spid'
  AND blocker_isolation_level = 4  -- SERIALIZABLE
FACET blocker_program_name, database_name
SINCE 24 hours ago
```

**Outcome:** App using overly restrictive isolation level (SERIALIZABLE)

---

## Support

For issues or questions:
- Check collector logs: `grep -i "blocking" collector.log`
- Verify config: `enable_query_monitoring: true`
- Test SQL Server connectivity
- Ensure AdventureWorks2022 is installed

---

## Summary

These scripts provide a **comprehensive, realistic demo** of SQL Server monitoring with OpenTelemetry and New Relic, covering:

✅ **Slow queries** (CPU, I/O, Memory intensive)
✅ **Blocking sessions** (all lock types)
✅ **Wait types** (I/O, Lock, CPU, Memory, Network, Log)
✅ **RCA scenarios** (forgotten transactions, isolation levels)
✅ **Execution plans** (Nested Loops, Hash Match, Merge Join)
✅ **Performance impact** (quantify business impact)

**Perfect for:**
- Customer demos
- POCs (Proof of Concept)
- Training sessions
- Testing monitoring pipelines
- RCA scenario validation
