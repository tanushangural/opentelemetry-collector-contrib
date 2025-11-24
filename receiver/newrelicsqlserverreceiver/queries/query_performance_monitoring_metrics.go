// // Copyright The OpenTelemetry Authors
// // SPDX-License-Identifier: Apache-2.0

// // Package queries provides SQL query definitions for performance monitoring.
// // This file contains all SQL queries related to SQL Server query performance monitoring.
// package queries

// // // Top N Slow Queries by Total Elapsed Time
// // var SlowQueriesByTotalTime = `
// // SELECT TOP (@topN)
// //     qs.query_hash,
// //     qs.query_plan_hash,
// //     qs.total_elapsed_time,
// //     qs.avg_elapsed_time,
// //     qs.execution_count,
// //     qs.total_worker_time AS total_cpu_time,
// //     qs.avg_worker_time AS avg_cpu_time,
// //     qs.total_logical_reads,
// //     qs.avg_logical_reads,
// //     qs.total_physical_reads,
// //     qs.avg_physical_reads,
// //     qs.total_logical_writes,
// //     qs.avg_logical_writes,
// //     qs.creation_time,
// //     qs.last_execution_time,
// //     SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
// //         ((CASE qs.statement_end_offset
// //             WHEN -1 THEN DATALENGTH(st.text)
// //             ELSE qs.statement_end_offset
// //         END - qs.statement_start_offset)/2) + 1) AS query_text
// // FROM sys.dm_exec_query_stats qs
// // CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
// // ORDER BY qs.total_elapsed_time DESC`

// // // Top N Slow Queries by Average Elapsed Time
// // var SlowQueriesByAvgTime = `
// // SELECT TOP (@topN)
// //     qs.query_hash,
// //     qs.query_plan_hash,
// //     qs.total_elapsed_time,
// //     qs.avg_elapsed_time,
// //     qs.execution_count,
// //     qs.total_worker_time AS total_cpu_time,
// //     qs.avg_worker_time AS avg_cpu_time,
// //     qs.total_logical_reads,
// //     qs.avg_logical_reads,
// //     qs.total_physical_reads,
// //     qs.avg_physical_reads,
// //     qs.total_logical_writes,
// //     qs.avg_logical_writes,
// //     qs.creation_time,
// //     qs.last_execution_time,
// //     SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
// //         ((CASE qs.statement_end_offset
// //             WHEN -1 THEN DATALENGTH(st.text)
// //             ELSE qs.statement_end_offset
// //         END - qs.statement_start_offset)/2) + 1) AS query_text
// // FROM sys.dm_exec_query_stats qs
// // CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
// // WHERE qs.execution_count > 5
// // ORDER BY qs.avg_elapsed_time DESC`

// // // Wait Statistics Query with Categorization
// // var WaitStatistics = `
// // SELECT
// //     wait_type,
// //     CASE
// //         WHEN wait_type LIKE 'PAGEIOLATCH%' OR wait_type LIKE 'WRITELOG%' OR wait_type LIKE 'IO_COMPLETION%' THEN 'I/O'
// //         WHEN wait_type LIKE 'LCK_%' OR wait_type LIKE 'LOCK_%' THEN 'Lock'
// //         WHEN wait_type LIKE 'SOS_SCHEDULER_YIELD%' OR wait_type LIKE 'THREADPOOL%' THEN 'CPU'
// //         WHEN wait_type LIKE 'NETWORK_%' OR wait_type LIKE 'ASYNC_NETWORK_%' THEN 'Network'
// //         WHEN wait_type LIKE 'RESOURCE_SEMAPHORE%' OR wait_type LIKE 'CMEMTHREAD%' THEN 'Memory'
// //         ELSE 'Other'
// //     END AS wait_category,
// //     waiting_tasks_count,
// //     wait_time_ms,
// //     max_wait_time_ms,
// //     signal_wait_time_ms,
// //     wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms,
// //     CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS DECIMAL(5,2)) AS percentage_total
// // FROM sys.dm_os_wait_stats
// // WHERE wait_type NOT IN (
// //     'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
// //     'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
// //     'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
// //     'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT',
// //     'DISPATCHER_QUEUE_SEMAPHORE', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT'
// // )
// // AND wait_time_ms > 0
// // ORDER BY wait_time_ms DESC`

// // // Active Blocking Sessions Query
// // var BlockingSessions = `
// // SELECT
// //     blocked.session_id AS blocked_session_id,
// //     blocked.blocking_session_id,
// //     blocked.wait_type,
// //     blocked.wait_resource,
// //     blocked.wait_time,
// //     blocked_session.login_name AS blocked_login_name,
// //     blocking_session.login_name AS blocking_login_name,
// //     blocked_session.host_name AS blocked_host_name,
// //     blocking_session.host_name AS blocking_host_name,
// //     blocked_session.program_name AS blocked_program_name,
// //     blocking_session.program_name AS blocking_program_name,
// //     blocked.command AS blocked_command,
// //     blocking.command AS blocking_command,
// //     blocked.status AS blocked_status,
// //     blocking.status AS blocking_status,
// //     CASE WHEN deadlock.session_id IS NOT NULL THEN 1 ELSE 0 END AS is_deadlock
// // FROM sys.dm_exec_requests blocked
// // LEFT JOIN sys.dm_exec_requests blocking ON blocked.blocking_session_id = blocking.session_id
// // LEFT JOIN sys.dm_exec_sessions blocked_session ON blocked.session_id = blocked_session.session_id
// // LEFT JOIN sys.dm_exec_sessions blocking_session ON blocking.session_id = blocking_session.session_id
// // LEFT JOIN sys.dm_exec_requests deadlock ON deadlock.blocking_session_id = blocked.session_id
// //     AND blocked.blocking_session_id = deadlock.session_id
// // WHERE blocked.blocking_session_id <> 0
// // ORDER BY blocked.wait_time DESC`

// // // Execution Plan Cache Statistics
// // var ExecutionPlanCache = `
// // SELECT
// //     cp.objtype AS plan_type,
// //     cp.cacheobjtype AS cache_object_type,
// //     cp.usecounts AS use_counts,
// //     cp.size_in_bytes,
// //     DATEDIFF(minute, cp.plan_generation_num, GETDATE()) AS plan_age_minutes,
// //     qs.creation_time,
// //     qs.last_execution_time,
// //     CASE
// //         WHEN cp.objtype = 'Adhoc' AND cp.usecounts = 1 THEN 1
// //         ELSE 0
// //     END AS is_single_use_plan
// // FROM sys.dm_exec_cached_plans cp
// // LEFT JOIN sys.dm_exec_query_stats qs ON cp.plan_handle = qs.plan_handle
// // WHERE cp.cacheobjtype = 'Compiled Plan'
// // ORDER BY cp.size_in_bytes DESC`

// // // Query to get execution plans for slow queries
// // var SlowQueryExecutionPlans = `
// // SELECT
// //     qs.query_hash,
// //     qs.query_plan_hash,
// //     qs.total_elapsed_time,
// //     qs.execution_count,
// //     qp.query_plan,
// //     SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
// //         ((CASE qs.statement_end_offset
// //             WHEN -1 THEN DATALENGTH(st.text)
// //             ELSE qs.statement_end_offset
// //         END - qs.statement_start_offset)/2) + 1) AS query_text
// // FROM sys.dm_exec_query_stats qs
// // CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
// // CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
// // WHERE qs.total_elapsed_time > @threshold_ms * 1000
// // ORDER BY qs.total_elapsed_time DESC`

// // // Currently executing queries with wait information
// // var CurrentlyExecutingQueries = `
// // SELECT
// //     r.session_id,
// //     r.request_id,
// //     r.start_time,
// //     r.status,
// //     r.command,
// //     r.wait_type,
// //     r.wait_time,
// //     r.last_wait_type,
// //     r.wait_resource,
// //     r.cpu_time,
// //     r.total_elapsed_time,
// //     r.reads,
// //     r.writes,
// //     r.logical_reads,
// //     s.login_name,
// //     s.host_name,
// //     s.program_name,
// //     SUBSTRING(st.text, (r.statement_start_offset/2)+1,
// //         ((CASE r.statement_end_offset
// //             WHEN -1 THEN DATALENGTH(st.text)
// //             ELSE r.statement_end_offset
// //         END - r.statement_start_offset)/2) + 1) AS current_query
// // FROM sys.dm_exec_requests r
// // LEFT JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
// // CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
// // WHERE r.session_id <> @@SPID
// // ORDER BY r.total_elapsed_time DESC`

// const (
//     SlowQuery = `DECLARE @IntervalSeconds INT = %d; 		-- Define the interval in seconds
// 				DECLARE @TopN INT = %d; 				-- Number of top queries to retrieve
// 				DECLARE @ElapsedTimeThreshold INT = %d; -- Elapsed time threshold in milliseconds
// 				DECLARE @TextTruncateLimit INT = %d; 	-- Truncate limit for query_text

// 				WITH RecentQueryIds AS (
// 					SELECT
// 						qs.query_hash as query_id
// 					FROM
// 						sys.dm_exec_query_stats qs
// 					WHERE
// 						qs.execution_count > 0
// 						AND qs.last_execution_time >= DATEADD(SECOND, -@IntervalSeconds, GETUTCDATE())
// 						AND qs.sql_handle IS NOT NULL
// 				),
// 				QueryStats AS (
// 					SELECT
// 						qs.plan_handle,
// 						qs.sql_handle,
// 						LEFT(SUBSTRING(
// 							qt.text,
// 							(qs.statement_start_offset / 2) + 1,
// 							(
// 								CASE
// 									qs.statement_end_offset
// 									WHEN -1 THEN DATALENGTH(qt.text)
// 									ELSE qs.statement_end_offset
// 								END - qs.statement_start_offset
// 							) / 2 + 1
// 						), @TextTruncateLimit) AS query_text,
// 						qs.query_hash AS query_id,
// 						qs.last_execution_time,
// 						qs.execution_count,
// 						(qs.total_worker_time / qs.execution_count) / 1000.0 AS avg_cpu_time_ms,
// 						(qs.total_elapsed_time / qs.execution_count) / 1000.0 AS avg_elapsed_time_ms,
// 						(qs.total_logical_reads / qs.execution_count) AS avg_disk_reads,
// 						(qs.total_logical_writes / qs.execution_count) AS avg_disk_writes,
// 						CASE
// 							WHEN UPPER(
// 								LTRIM(
// 									SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6)
// 								)
// 							) LIKE 'SELECT' THEN 'SELECT'
// 							WHEN UPPER(
// 								LTRIM(
// 									SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6)
// 								)
// 							) LIKE 'INSERT' THEN 'INSERT'
// 							WHEN UPPER(
// 								LTRIM(
// 									SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6)
// 								)
// 							) LIKE 'UPDATE' THEN 'UPDATE'
// 							WHEN UPPER(
// 								LTRIM(
// 									SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6)
// 								)
// 							) LIKE 'DELETE' THEN 'DELETE'
// 							ELSE 'OTHER'
// 						END AS statement_type,
// 						CONVERT(INT, pa.value) AS database_id,
// 						qt.objectid
// 					FROM
// 						sys.dm_exec_query_stats qs
// 						CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
// 						JOIN sys.dm_exec_cached_plans cp ON qs.plan_handle = cp.plan_handle
// 						CROSS APPLY sys.dm_exec_plan_attributes(cp.plan_handle) AS pa
// 					WHERE
// 						qs.query_hash IN (SELECT DISTINCT(query_id) FROM RecentQueryIds)
// 						AND qs.execution_count > 0
// 						AND pa.attribute = 'dbid'
// 						AND DB_NAME(CONVERT(INT, pa.value)) NOT IN ('master', 'model', 'msdb', 'tempdb')
// 						AND qt.text NOT LIKE '%%sys.%%'
// 						AND qt.text NOT LIKE '%%INFORMATION_SCHEMA%%'
// 						AND qt.text NOT LIKE '%%schema_name()%%'
// 						AND qt.text IS NOT NULL
// 						AND LTRIM(RTRIM(qt.text)) <> ''
// 						AND EXISTS (
// 							SELECT 1
// 							FROM sys.databases d
// 							WHERE d.database_id = CONVERT(INT, pa.value) AND d.is_query_store_on = 1
// 						)
// 				)
// 				SELECT
// 					TOP (@TopN) qs.query_id,
// 					MIN(qs.query_text) AS query_text,
// 					DB_NAME(MIN(qs.database_id)) AS database_name,
// 					COALESCE(
// 						OBJECT_SCHEMA_NAME(MIN(qs.objectid), MIN(qs.database_id)),
// 						'N/A'
// 					) AS schema_name,
// 					FORMAT(
// 						MAX(qs.last_execution_time) AT TIME ZONE 'UTC',
// 						'yyyy-MM-ddTHH:mm:ssZ'
// 					) AS last_execution_timestamp,
// 					SUM(qs.execution_count) AS execution_count,
// 					AVG(qs.avg_cpu_time_ms) AS avg_cpu_time_ms,
// 					AVG(qs.avg_elapsed_time_ms) AS avg_elapsed_time_ms,
// 					AVG(qs.avg_disk_reads) AS avg_disk_reads,
// 					AVG(qs.avg_disk_writes) AS avg_disk_writes,
// 					 MAX(qs.statement_type) AS statement_type,
// 					FORMAT(
// 						SYSDATETIMEOFFSET() AT TIME ZONE 'UTC',
// 						'yyyy-MM-ddTHH:mm:ssZ'
// 					) AS collection_timestamp
// 				FROM
// 					QueryStats qs
// 				GROUP BY
// 					qs.query_id
// 				HAVING
// 					AVG(qs.avg_elapsed_time_ms) > @ElapsedTimeThreshold
// 				ORDER BY
// 					avg_elapsed_time_ms DESC;
//     `
// )

package queries

const SlowQuery = `DECLARE @IntervalSeconds INT = %d; 		-- Define the interval in seconds
DECLARE @TopN INT = %d; 				-- Number of top queries to retrieve
DECLARE @ElapsedTimeThreshold INT = %d;  -- Elapsed time threshold in milliseconds
DECLARE @TextTruncateLimit INT = %d; 	-- Truncate limit for query_text
				
WITH StatementDetails AS (
	SELECT
		qs.plan_handle,
		qs.sql_handle,
		-- Extract query text using Microsoft's official offset logic (no +1 on length)
		-- Reference: https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-stats-transact-sql
		LEFT(SUBSTRING(
			qt.text,
			(qs.statement_start_offset / 2) + 1,
			(
				CASE
					qs.statement_end_offset
					WHEN -1 THEN DATALENGTH(qt.text)
					ELSE qs.statement_end_offset
				END - qs.statement_start_offset
			) / 2
		), @TextTruncateLimit) AS query_text,
		-- query_id: SQL Server's query_hash - used for correlating with active query metrics
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
		-- RCA Enhancement Fields: Performance variance
		qs.min_elapsed_time / 1000.0 AS min_elapsed_time_ms,
		qs.max_elapsed_time / 1000.0 AS max_elapsed_time_ms,
		qs.last_elapsed_time / 1000.0 AS last_elapsed_time_ms,
		-- RCA Enhancement Fields: Memory grants
		qs.last_grant_kb,
		qs.last_used_grant_kb,
		-- RCA Enhancement Fields: TempDB spills
		qs.last_spills,
		qs.max_spills,
		-- RCA Enhancement Fields: Parallelism
		qs.last_dop,
		-- Lock wait time approximation: elapsed_time - (cpu_time + io_time)
		-- NOTE: This is an approximation as dm_exec_query_stats doesn't track lock waits separately
		-- For precise lock wait time, Query Store wait_category = 4 (Lock waits) should be used
		CASE
			WHEN (qs.total_elapsed_time / qs.execution_count) > (qs.total_worker_time / qs.execution_count)
			THEN ((qs.total_elapsed_time - qs.total_worker_time) / qs.execution_count) / 1000.0
			ELSE 0.0
		END AS avg_lock_wait_time_ms,
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
		AND qt.text IS NOT NULL
		AND LTRIM(RTRIM(qt.text)) <> ''
		AND DB_NAME(CONVERT(INT, pa.value)) NOT IN ('master', 'model', 'msdb', 'tempdb')
		AND qt.text NOT LIKE '%%sys.%%'
		AND qt.text NOT LIKE '%%INFORMATION_SCHEMA%%'
		AND qt.text NOT LIKE '%%schema_name()%%'
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
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(s.last_execution_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS last_execution_timestamp,
    s.execution_count,
    s.avg_cpu_time_ms,
    s.avg_elapsed_time_ms,
    s.avg_disk_reads,
    s.avg_disk_writes,
    s.avg_rows_processed,
    s.avg_lock_wait_time_ms,
    s.statement_type,
    -- RCA Enhancement Fields
    s.min_elapsed_time_ms,
    s.max_elapsed_time_ms,
    s.last_elapsed_time_ms,
    s.last_grant_kb,
    s.last_used_grant_kb,
    s.last_spills,
    s.max_spills,
    s.last_dop,
    CONVERT(VARCHAR(25), SWITCHOFFSET(SYSDATETIMEOFFSET(), '+00:00'), 127) + 'Z' AS collection_timestamp
FROM
    StatementDetails s
WHERE
	    s.avg_elapsed_time_ms > @ElapsedTimeThreshold
ORDER BY
    s.last_execution_time DESC;`

const BlockingSessionsQuery = `
DECLARE @Limit INT = %d; -- Define the limit for the number of rows returned
DECLARE @TextTruncateLimit INT = %d; -- Define the truncate limit for the query text
WITH blocking_info AS (
    SELECT
        -- Existing: Basic blocking context
        req.blocking_session_id AS blocking_spid,
        req.session_id AS blocked_spid,
        req.wait_type AS wait_type,
        req.wait_time / 1000.0 AS wait_time_in_seconds,
        req.start_time AS blocked_start_time,
        sess.status AS blocked_status,
        req.command AS blocked_command_type,
        req.database_id AS database_id,
        req.sql_handle AS blocked_sql_handle,

        -- RCA Enhancement: Lock resource details (WHAT is being locked)
        req.wait_resource AS wait_resource,

        -- RCA Enhancement: Blocked query performance impact
        req.total_elapsed_time AS blocked_total_elapsed_ms,
        req.cpu_time AS blocked_cpu_time_ms,
        req.logical_reads AS blocked_logical_reads,

        -- RCA Enhancement: Transaction context
        req.transaction_isolation_level AS blocked_isolation_level,
        req.open_transaction_count AS blocked_open_transaction_count,

        -- Blocker query details
        blocking_req.sql_handle AS blocking_sql_handle,

        -- RCA Enhancement: Blocker activity context (WHAT is blocker doing)
        blocking_req.start_time AS blocker_start_time,
        blocking_req.command AS blocker_command_type,
        blocking_req.status AS blocker_req_status,  -- From requests (NULL if sleeping)
        blocking_req.transaction_isolation_level AS blocker_isolation_level,
        blocking_req.open_transaction_count AS blocker_req_open_transaction_count  -- From requests (NULL if sleeping)
    FROM
        sys.dm_exec_requests AS req
    LEFT JOIN sys.dm_exec_requests AS blocking_req ON blocking_req.session_id = req.blocking_session_id
    LEFT JOIN sys.dm_exec_sessions AS sess ON sess.session_id = req.session_id
    WHERE
        req.blocking_session_id != 0
)
SELECT TOP (@Limit)
    -- Existing: Basic blocking context
    blocking_info.blocking_spid,
    blocking_sessions.status AS blocking_status,
    blocking_info.blocked_spid,
    blocking_info.blocked_status,
    blocking_info.wait_type,
    blocking_info.wait_time_in_seconds,
    blocking_info.blocked_command_type AS command_type,
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(blocking_info.blocked_start_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS blocked_query_start_time,
    DB_NAME(blocking_info.database_id) AS database_name,

    -- RCA Enhancement: Blocker session identity (WHO is causing the block)
    blocking_sessions.login_name AS blocker_login_name,
    blocking_sessions.host_name AS blocker_host_name,
    blocking_sessions.program_name AS blocker_program_name,

    -- RCA Enhancement: Lock resource details (WHAT is being locked)
    ISNULL(blocking_info.wait_resource, 'N/A') AS wait_resource,

    -- RCA Enhancement: Blocker activity context (WHAT is blocker doing)
    ISNULL(blocking_info.blocker_command_type, 'N/A') AS blocker_command_type,
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(blocking_info.blocker_start_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS blocker_start_time,
    -- Use session status as fallback if blocker is not in dm_exec_requests (i.e., sleeping)
    COALESCE(blocking_info.blocker_req_status, blocking_sessions.status, 'N/A') AS blocker_status,
    -- Use session open_transaction_count as fallback if blocker is not in dm_exec_requests
    COALESCE(blocking_info.blocker_req_open_transaction_count, blocking_sessions.open_transaction_count, 0) AS blocker_open_transaction_count,

    -- RCA Enhancement: Transaction behavior (WHY is it blocking)
    blocking_info.blocked_isolation_level,
    ISNULL(blocking_info.blocker_isolation_level, 0) AS blocker_isolation_level,
    blocking_info.blocked_open_transaction_count,

    -- RCA Enhancement: Blocked query performance impact
    blocking_info.blocked_total_elapsed_ms,
    blocking_info.blocked_cpu_time_ms,
    blocking_info.blocked_logical_reads,

    -- Existing: Query texts
    CASE
        WHEN blocking_sql.text IS NULL THEN LEFT(input_buffer.event_info, @TextTruncateLimit)
        ELSE LEFT(blocking_sql.text, @TextTruncateLimit)
    END AS blocking_query_text,
    LEFT(blocked_sql.text, @TextTruncateLimit) AS blocked_query_text
FROM
    blocking_info
JOIN sys.dm_exec_sessions AS blocking_sessions ON blocking_sessions.session_id = blocking_info.blocking_spid
JOIN sys.dm_exec_sessions AS blocked_sessions ON blocked_sessions.session_id = blocking_info.blocked_spid
OUTER APPLY sys.dm_exec_sql_text(blocking_info.blocking_sql_handle) AS blocking_sql
OUTER APPLY sys.dm_exec_sql_text(blocking_info.blocked_sql_handle) AS blocked_sql
OUTER APPLY sys.dm_exec_input_buffer(blocking_info.blocking_spid, NULL) AS input_buffer
ORDER BY
    blocking_info.blocked_start_time;`

// WaitQuery - Real-time wait statistics from dm_exec_requests (NO Query Store)
// This query captures currently waiting queries directly from sys.dm_exec_requests
// providing real-time wait analysis without Query Store performance overhead
const WaitQuery = `DECLARE @TopN INT = %d; 				-- Number of results to retrieve
				DECLARE @TextTruncateLimit INT = %d; 	-- Truncate limit for query_text

SELECT TOP (@TopN)
    r.query_hash AS query_id,
    DB_NAME(r.database_id) AS database_name,
    LEFT(SUBSTRING(st.text, (r.statement_start_offset / 2) + 1,
        ((CASE r.statement_end_offset
            WHEN -1 THEN DATALENGTH(st.text)
            ELSE r.statement_end_offset
        END - r.statement_start_offset) / 2) + 1
    ), @TextTruncateLimit) AS query_text,
    -- Categorize wait types
    CASE
        WHEN r.wait_type LIKE 'PAGEIOLATCH%%' OR r.wait_type LIKE 'WRITELOG%%' OR r.wait_type LIKE 'IO_COMPLETION%%' THEN 'I/O'
        WHEN r.wait_type LIKE 'LCK_%%' OR r.wait_type LIKE 'LOCK_%%' THEN 'Lock'
        WHEN r.wait_type LIKE 'SOS_SCHEDULER_YIELD%%' OR r.wait_type LIKE 'THREADPOOL%%' THEN 'CPU'
        WHEN r.wait_type LIKE 'NETWORK_%%' OR r.wait_type LIKE 'ASYNC_NETWORK_%%' THEN 'Network'
        WHEN r.wait_type LIKE 'RESOURCE_SEMAPHORE%%' OR r.wait_type LIKE 'CMEMTHREAD%%' THEN 'Memory'
        WHEN r.wait_type LIKE 'CXPACKET%%' OR r.wait_type LIKE 'CXCONSUMER%%' THEN 'Parallelism'
        ELSE 'Other'
    END AS wait_category,
    r.wait_time AS total_wait_time_ms,
    r.wait_time AS avg_wait_time_ms,
    1 AS wait_event_count,
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(r.start_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS last_execution_time,
    CONVERT(VARCHAR(25), SWITCHOFFSET(SYSDATETIMEOFFSET(), '+00:00'), 127) + 'Z' AS collection_timestamp
FROM sys.dm_exec_requests r
INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) st
WHERE r.session_id > 50
    AND r.database_id > 4
    AND r.wait_type IS NOT NULL
    AND r.wait_time > 0
    AND st.text NOT LIKE '%%sys.%%'
    AND st.text NOT LIKE '%%INFORMATION_SCHEMA%%'
ORDER BY r.wait_time DESC;`

const QueryExecutionPlan = `
SELECT
    qs.query_hash AS query_id,
    qs.plan_handle,
    qs.query_plan_hash AS query_plan_id,
    CAST(qp.query_plan AS NVARCHAR(MAX)) AS execution_plan_xml,
    qs.total_worker_time / 1000.0 AS total_cpu_ms,
    qs.total_elapsed_time / 1000.0 AS total_elapsed_ms,
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(qs.creation_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS creation_time,
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(qs.last_execution_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS last_execution_time,
    st.text AS sql_text
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
WHERE qs.query_hash IN (%s)
    AND qp.query_plan IS NOT NULL;`

// ActiveQueryExecutionPlanQuery fetches the execution plan for an active query using its plan_handle
const ActiveQueryExecutionPlanQuery = `
SELECT
    CAST(qp.query_plan AS NVARCHAR(MAX)) AS execution_plan_xml
FROM sys.dm_exec_query_plan(%s) AS qp
WHERE qp.query_plan IS NOT NULL;`

// ActiveRunningQueriesQuery retrieves currently executing queries with wait and blocking details
// This query captures real-time execution state including wait types, blocking chains, and query text
//
// RCA Enhancement: Includes query_hash for correlation with slow queries, with fallback to text hash
// when query_hash is NULL (queries not yet cached in dm_exec_query_stats)
const ActiveRunningQueriesQuery = `
DECLARE @Limit INT = %d; -- Set the maximum number of rows to return
DECLARE @TextTruncateLimit INT = %d; -- Set the maximum length for query text

SELECT TOP (@Limit)
    -- A. CURRENT SESSION DETAILS
    r_wait.session_id AS current_session_id,
    r_wait.request_id AS request_id,
    DB_NAME(r_wait.database_id) AS database_name,
    s_wait.login_name AS login_name,
    s_wait.host_name AS host_name,
    s_wait.program_name AS program_name,
    r_wait.command AS request_command,
    r_wait.status AS request_status,

    -- B. CORRELATION KEY (Critical for RCA)
    -- query_id: SQL Server's query_hash - used for correlating active queries with slow query metrics
    -- NULL indicates query is not correlatable (ad-hoc SQL with different literals, OPTION(RECOMPILE), etc.)
    -- Only parameterized queries and stored procedures get consistent query_hash values
    r_wait.query_hash AS query_id,

    -- C. WAIT DETAILS
    r_wait.wait_type AS wait_type,
    r_wait.wait_time / 1000.0 AS wait_time_s,
    r_wait.wait_resource AS wait_resource,
    r_wait.last_wait_type AS last_wait_type,

    -- C2. DECODED WAIT RESOURCE (Human-readable names)
    -- Simplified to avoid SUBSTRING errors - detailed parsing done in Go helpers
    CASE
        -- KEY Lock: Show raw wait_resource (parsing done in Go helper)
        WHEN r_wait.wait_resource LIKE 'KEY:%%' THEN
            'KEY Lock: ' + r_wait.wait_resource

        -- PAGE Lock: Show raw wait_resource
        WHEN r_wait.wait_resource LIKE 'PAGE:%%' THEN
            'PAGE Lock: ' + r_wait.wait_resource

        -- RID Lock: Show raw wait_resource
        WHEN r_wait.wait_resource LIKE 'RID:%%' THEN
            'RID Lock: ' + r_wait.wait_resource

        -- OBJECT Lock: Show raw wait_resource
        WHEN r_wait.wait_resource LIKE 'OBJECT:%%' THEN
            'OBJECT Lock: ' + r_wait.wait_resource

        -- DATABASE Lock: Show raw wait_resource
        WHEN r_wait.wait_resource LIKE 'DATABASE:%%' THEN
            'DATABASE Lock: ' + r_wait.wait_resource

        -- Not applicable for non-lock waits
        WHEN r_wait.wait_resource = '' OR r_wait.wait_resource IS NULL THEN 'N/A'

        ELSE r_wait.wait_resource
    END AS wait_resource_decoded,

    -- D. PERFORMANCE/EXECUTION METRICS
    r_wait.cpu_time AS cpu_time_ms,
    r_wait.total_elapsed_time AS total_elapsed_time_ms,
    r_wait.reads AS reads,
    r_wait.writes AS writes,
    r_wait.logical_reads AS logical_reads,
    r_wait.row_count AS row_count,
    r_wait.granted_query_memory AS granted_query_memory_pages,
    FORMAT(
        r_wait.start_time AT TIME ZONE 'UTC',
        'yyyy-MM-ddTHH:mm:ssZ'
    ) AS request_start_time,
    CONVERT(VARCHAR(25), SWITCHOFFSET(SYSDATETIMEOFFSET(), '+00:00'), 127) + 'Z' AS collection_timestamp,

    -- E. TRANSACTION CONTEXT (RCA for long-running transactions)
    r_wait.transaction_id AS transaction_id,
    r_wait.open_transaction_count AS open_transaction_count,
    r_wait.transaction_isolation_level AS transaction_isolation_level,

    -- F. PARALLEL EXECUTION DETAILS (RCA for CXPACKET waits)
    r_wait.dop AS degree_of_parallelism,
    r_wait.parallel_worker_count AS parallel_worker_count,

    -- G. SESSION CONTEXT
    s_wait.status AS session_status,
    s_wait.client_interface_name AS client_interface_name,

    -- H. PLAN HANDLE (for execution plan retrieval)
    r_wait.plan_handle AS plan_handle,

    -- I. BLOCKING DETAILS
    CASE
        WHEN r_wait.blocking_session_id = 0 THEN 'N/A'
        ELSE CAST(r_wait.blocking_session_id AS NVARCHAR(10))
    END AS blocking_session_id,

    ISNULL(s_blocker.login_name, 'N/A') AS blocker_login_name,
    ISNULL(s_blocker.host_name, 'N/A') AS blocker_host_name,
    ISNULL(s_blocker.program_name, 'N/A') AS blocker_program_name,

    -- J. QUERY TEXT - Current Session
    -- Using full text to avoid SUBSTRING errors
    LEFT(st_wait.text, @TextTruncateLimit) AS query_statement_text,

    -- K. QUERY TEXT - Blocking Session
    -- Simplified to avoid SUBSTRING errors
    CASE
        WHEN r_wait.blocking_session_id = 0 THEN 'N/A'
        WHEN r_blocker.command IS NULL AND ib_blocker.event_info IS NOT NULL THEN LEFT(ib_blocker.event_info, @TextTruncateLimit)
        WHEN st_blocker.text IS NOT NULL THEN LEFT(st_blocker.text, @TextTruncateLimit)
        ELSE 'N/A'
    END AS blocking_query_statement_text

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
    AND r_wait.wait_type IS NOT NULL
    AND r_wait.query_hash IS NOT NULL  -- Filter out queries without query_hash (PREEMPTIVE waits, system queries)
ORDER BY
    r_wait.wait_time DESC;`

// LockedObjectsBySessionQuery retrieves detailed information about objects locked by a specific session
// This query resolves lock resources to actual table/object names for better troubleshooting
// Returns: locked object names, lock types, lock granularity (table/page/row level)
const LockedObjectsBySessionQuery = `
DECLARE @SessionID INT = %d;

SELECT
    l.request_session_id AS session_id,
    DB_NAME(l.resource_database_id) AS database_name,
    OBJECT_SCHEMA_NAME(p.object_id, l.resource_database_id) AS schema_name,
    OBJECT_NAME(p.object_id, l.resource_database_id) AS locked_object_name,
    l.resource_type,
    CASE l.resource_type
        WHEN 'OBJECT' THEN 'Table Lock'
        WHEN 'PAGE' THEN 'Page Lock'
        WHEN 'KEY' THEN 'Row Lock'
        WHEN 'RID' THEN 'Row Lock'
        WHEN 'DATABASE' THEN 'Database Lock'
        WHEN 'FILE' THEN 'File Lock'
        WHEN 'HOBT' THEN 'Heap/B-Tree Lock'
        WHEN 'ALLOCATION_UNIT' THEN 'Allocation Unit Lock'
        ELSE l.resource_type
    END AS lock_granularity,
    l.request_mode AS lock_mode,
    l.request_status AS lock_status,
    l.request_type AS lock_request_type,
    l.resource_description,
    CONVERT(VARCHAR(25), SWITCHOFFSET(SYSDATETIMEOFFSET(), '+00:00'), 127) + 'Z' AS collection_timestamp
FROM sys.dm_tran_locks l
LEFT JOIN sys.partitions p
    ON l.resource_associated_entity_id = p.hobt_id
    AND l.resource_type IN ('PAGE', 'KEY', 'RID', 'HOBT')
WHERE l.request_session_id = @SessionID
    AND l.resource_database_id > 0
ORDER BY l.resource_database_id, locked_object_name, l.resource_type;`
