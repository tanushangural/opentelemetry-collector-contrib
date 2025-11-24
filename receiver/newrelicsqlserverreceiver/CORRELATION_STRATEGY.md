# Query Correlation Strategy

## Overview

The New Relic SQL Server Receiver implements a comprehensive query correlation strategy that links **active running queries** with **historical slow query metrics**. This enables powerful Root Cause Analysis (RCA) by allowing you to:

1. Compare current query performance with historical averages
2. Identify queries that are performing worse than usual
3. Aggregate metrics across parallel query workers
4. Correlate wait events with query patterns

## Correlation Key: `query_id`

### What is it?

`query_id` is SQL Server's native `query_hash` - a binary(8) hash that uniquely identifies the structure of a query, regardless of literal parameter values.

### When is it Available?

| Query Type | query_hash Status | Correlatable? | Example |
|-----------|-------------------|---------------|---------|
| **Parameterized queries** | ✅ Populated | ✅ YES | `EXEC sp_executesql N'SELECT * FROM Orders WHERE ID = @ID', N'@ID INT', @ID = 123` |
| **Stored procedures** | ✅ Populated | ✅ YES | `EXEC sp_GetCustomerOrders @CustomerID = 123` |
| **Ad-hoc with literals** | ⚠️ Different per literal | ⚠️ Partial | `SELECT * FROM Orders WHERE ID = 123` (hash changes with each literal) |
| **OPTION(RECOMPILE)** | ❌ NULL | ❌ NO | `SELECT * FROM Orders WHERE ID = @ID OPTION(RECOMPILE)` |
| **Some dynamic SQL** | ❌ NULL | ❌ NO | `EXEC('SELECT * FROM ...')` (string concatenation) |

### Best Practices

✅ **DO:**
- Use parameterized queries (`sp_executesql` with parameters)
- Use stored procedures
- Filter on `WHERE query_id IS NOT NULL` in NRQL queries

❌ **DON'T:**
- Use ad-hoc SQL with hardcoded literals
- Use `OPTION(RECOMPILE)` unless absolutely necessary
- Expect correlation for string-concatenated dynamic SQL

## Architecture

### Data Flow

```
Active Running Queries               Slow Queries (Historical)
sys.dm_exec_requests      ----JOIN----      sys.dm_exec_query_stats
         |                    ON                    |
         |           query_id           |
         |                  (query_hash)            |
         v                                          v
ActiveQueryMetrics                         SlowQueryMetrics
(real-time snapshot)                      (aggregated history)
```

### Metrics Emitted

#### Active Query Metrics
- `sqlserver.activequery.elapsed_time_ms` - Current execution time
- `sqlserver.activequery.cpu_time_ms` - CPU time consumed
- `sqlserver.activequery.wait_time_seconds` - Time spent waiting
- `sqlserver.activequery.reads` - Physical disk reads
- `sqlserver.activequery.logical_reads` - Buffer cache reads
- `sqlserver.activequery.writes` - Write operations
- `sqlserver.activequery.row_count` - Rows returned
- `sqlserver.activequery.granted_query_memory_pages` - Memory granted

**Key Attributes:**
- `query_id` - Join key for correlation
- `session_id` - SQL Server session ID
- `request_id` - Request ID within session
- `wait_type` - Current wait type (e.g., ASYNC_NETWORK_IO)
- `blocking_session_id` - ID of blocking session (if blocked)
- `query_text` - Anonymized SQL text
- `database_name` - Database context
- `login_name` - User executing query

#### Slow Query Metrics
- `sqlserver.slowquery.execution_count` - Number of executions
- `sqlserver.slowquery.avg_elapsed_time_ms` - Average execution time
- `sqlserver.slowquery.max_elapsed_time_ms` - Maximum execution time
- `sqlserver.slowquery.min_elapsed_time_ms` - Minimum execution time
- `sqlserver.slowquery.total_elapsed_time_ms` - Total execution time
- `sqlserver.slowquery.avg_cpu_time_ms` - Average CPU time
- `sqlserver.slowquery.avg_reads` - Average physical reads
- `sqlserver.slowquery.avg_logical_reads` - Average logical reads

**Key Attributes:**
- `query_id` - Join key for correlation
- `query_id` - Same as query_id
- `query_text` - Anonymized SQL text
- `database_name` - Database context

## Correlation Scenarios

### Scenario 1: Perfect Correlation
**Active query with historical data**

```sql
-- Active query metrics
session_id: 71
query_id: 0x9661971254EA9C69
elapsed_time_ms: 42,000
cpu_time_ms: 1,620
wait_type: ASYNC_NETWORK_IO

-- Historical slow query metrics (JOIN on query_id)
query_id: 0x9661971254EA9C69
avg_elapsed_time_ms: 25,000
execution_count: 150
avg_cpu_time_ms: 1,200
```

**Analysis:**
- Current execution (42s) is **68% slower** than average (25s)
- CPU time is **35% higher** than average
- Likely issue: Client not fetching results (ASYNC_NETWORK_IO wait)

### Scenario 2: Parallel Worker Aggregation
**Multiple sessions executing same query**

```sql
-- Active queries (all same query_id)
session_id: 71, query_id: 0x9661..., cpu_time_ms: 1,620
session_id: 78, query_id: 0x9661..., cpu_time_ms: 1,480
session_id: 79, query_id: 0x9661..., cpu_time_ms: 890
session_id: 64, query_id: 0x9661..., cpu_time_ms: 980
session_id: 70, query_id: 0x9661..., cpu_time_ms: 570

-- Total resource usage for this query pattern
Total CPU: 5,540ms across 5 workers
Total Elapsed: 155,900ms (cumulative)
```

**Analysis:**
- Single query spawned 5 parallel workers
- Total CPU usage: 5.5 seconds
- Aggregate metrics show true resource consumption

### Scenario 3: New Query (No Historical Data)
**First-time execution**

```sql
-- Active query
query_id: 0xCCDD1234ABCD5678
elapsed_time_ms: 15,000

-- Historical query (NO MATCH - query hasn't completed yet)
query_id: 0xCCDD1234ABCD5678  → NOT FOUND
```

**Analysis:**
- Query is executing for the first time
- No historical baseline for comparison
- After completion, will appear in slow query metrics

### Scenario 4: Not Correlatable (NULL query_hash)
**Query with OPTION(RECOMPILE)**

```sql
-- Active query
query_id: NULL
elapsed_time_ms: 30,000
query_text: "SELECT ... OPTION(RECOMPILE)"

-- Historical query (CANNOT CORRELATE)
query_id: NULL  → Cannot join
```

**Analysis:**
- Query explicitly prevents plan caching
- Use `session_id` or `query_text` for troubleshooting
- No historical aggregation possible

## NRQL Query Examples

### Example 1: Find Active Queries with Historical Context
```sql
SELECT
    active.query_id,
    active.session_id,
    active.elapsed_ms AS current_elapsed,
    slow.avg_elapsed_ms AS historical_avg,
    active.wait_type,
    (active.elapsed_ms / slow.avg_elapsed_ms * 100) AS percent_of_avg
FROM (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
) active
LEFT JOIN (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
) slow
    ON active.query_id = slow.query_id
WHERE active.query_id IS NOT NULL
SINCE 5 minutes ago
```

### Example 2: Aggregate Parallel Workers
```sql
SELECT
    query_id,
    count(*) AS parallel_workers,
    sum(cpu_time_ms) AS total_cpu_ms,
    sum(logical_reads) AS total_logical_reads,
    max(elapsed_time_ms) AS max_elapsed_ms
FROM Metric
WHERE metricName = 'sqlserver.activequery.cpu_time_ms'
  AND query_id IS NOT NULL
GROUP BY query_id
SINCE 5 minutes ago
```

### Example 3: Find Queries Performing Worse Than Historical Average
```sql
SELECT
    active.query_id,
    active.session_id,
    active.elapsed_ms,
    slow.avg_elapsed_ms,
    ((active.elapsed_ms - slow.avg_elapsed_ms) / slow.avg_elapsed_ms * 100) AS percent_degradation
FROM (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
) active
INNER JOIN (
    SELECT * FROM Metric
    WHERE metricName = 'sqlserver.slowquery.avg_elapsed_time_ms'
) slow
    ON active.query_id = slow.query_id
WHERE active.elapsed_ms > slow.avg_elapsed_ms * 1.5  -- 50% slower than average
SINCE 10 minutes ago
```

### Example 4: Correlation Coverage Analysis
```sql
SELECT
    CASE
        WHEN query_id IS NOT NULL THEN 'Correlatable'
        ELSE 'Not Correlatable'
    END AS correlation_status,
    count(*) AS query_count,
    (count(*) * 100.0 / (SELECT count(*) FROM Metric WHERE metricName = 'sqlserver.activequery.elapsed_time_ms')) AS percentage
FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
GROUP BY CASE
    WHEN query_id IS NOT NULL THEN 'Correlatable'
    ELSE 'Not Correlatable'
END
SINCE 10 minutes ago
```

## Testing

### Test Scripts

1. **`test_correlation_comprehensive.sql`**
   - Tests all edge cases and correlation scenarios
   - Covers 15 different query patterns
   - Includes verification queries
   - **Run once** to test all scenarios

2. **`production_load_generator.sql`**
   - Simulates production workload
   - 10 different query types (OLTP, analytics, ETL, etc.)
   - Runs continuously (1000 iterations)
   - **Run in multiple sessions** for concurrent load
   - Each session creates a unique worker ID

### How to Test

#### Step 1: Run Comprehensive Test
```bash
sqlcmd -S your-server -U sa -P password -i test_correlation_comprehensive.sql
```

#### Step 2: Start Production Load (3-5 sessions)
```bash
# Terminal 1
sqlcmd -S your-server -U sa -P password -i production_load_generator.sql

# Terminal 2
sqlcmd -S your-server -U sa -P password -i production_load_generator.sql

# Terminal 3
sqlcmd -S your-server -U sa -P password -i production_load_generator.sql
```

#### Step 3: Start Collector
```bash
./bin/otelcontribcol_darwin_arm64 --config=receiver/newrelicsqlserverreceiver/testdata/config.yaml
```

#### Step 4: Verify in NRDB (after 2-3 minutes)
```sql
-- Check active queries
SELECT count(*) FROM Metric
WHERE metricName LIKE 'sqlserver.activequery.%'
SINCE 5 minutes ago

-- Check correlation coverage
SELECT count(*) FROM Metric
WHERE metricName = 'sqlserver.activequery.elapsed_time_ms'
  AND query_id IS NOT NULL
SINCE 5 minutes ago

-- Check slow queries
SELECT count(*) FROM Metric
WHERE metricName LIKE 'sqlserver.slowquery.%'
SINCE 5 minutes ago
```

## Edge Cases Handled

### ✅ Handled Correctly

1. **Parallel Workers** - All workers share same `query_id`
2. **Query Completion** - Query moves from active → slow query metrics
3. **Fast Queries** - Captured in slow query metrics even if missed in active
4. **Long-Running Queries** - Tracked across multiple collection cycles
5. **Parameterized Queries** - Consistent `query_id` across parameter values
6. **Stored Procedures** - Each statement has its own `query_id`
7. **Wait States** - Tracked regardless of wait type (ASYNC_NETWORK_IO, etc.)
8. **Blocking** - Blocker and blocked queries both have `query_id`

### ⚠️ Limitations

1. **Ad-hoc Queries with Literals** - Different `query_id` per literal set
2. **OPTION(RECOMPILE)** - `query_id` is NULL (not correlatable)
3. **Plan Cache Eviction** - Historical data lost if plan evicted
4. **First Execution** - No historical data until query completes once

## Configuration

### Recommended Settings

```yaml
# Collection frequency
collection_interval: 60s  # How often to scrape

# Slow query settings
query_monitoring_fetch_interval: 300  # Look back 5 minutes
query_monitoring_response_time_threshold: 0  # Capture all queries (0ms = no minimum)
query_monitoring_count_threshold: 20  # Minimum 20 executions to be considered "slow"
query_monitoring_text_truncate_limit: 4094  # 4KB limit for metrics

# Active query settings
enable_active_running_queries: true  # Enable active query monitoring
```

### Production Tuning

For high-volume production environments:

```yaml
collection_interval: 30s  # More frequent scraping
query_monitoring_response_time_threshold: 100  # Only queries > 100ms
query_monitoring_count_threshold: 50  # Higher threshold
```

## Troubleshooting

### Issue: query_id is NULL for all queries

**Possible Causes:**
1. All queries use `OPTION(RECOMPILE)`
2. Query Store disabled
3. Plan cache cleared recently

**Solution:**
- Check query patterns (use parameterized queries)
- Verify Query Store is enabled (not required but helpful)
- Check for frequent `DBCC FREEPROCCACHE` execution

### Issue: Active queries not showing in NRDB

**Possible Causes:**
1. Queries executing too fast (< collection_interval)
2. Collector not running
3. Queries filtered by `is_user_process = 1`

**Solution:**
- Use long-running test queries
- Verify collector logs show "Successfully scraped active query metrics"
- Check SQL Server DMV directly: `SELECT * FROM sys.dm_exec_requests`

### Issue: No slow query metrics

**Possible Causes:**
1. `query_monitoring_fetch_interval` too short
2. `query_monitoring_response_time_threshold` too high
3. Queries haven't completed yet

**Solution:**
- Increase `query_monitoring_fetch_interval` to 300+ seconds
- Lower `query_monitoring_response_time_threshold` to 0
- Wait for queries to complete and enter plan cache

## References

- [Microsoft: sys.dm_exec_requests](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-requests-transact-sql)
- [Microsoft: sys.dm_exec_query_stats](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-stats-transact-sql)
- [Microsoft: Query Hash and Query Plan Hash](https://learn.microsoft.com/en-us/sql/relational-databases/performance/monitoring-performance-by-using-the-query-store)
