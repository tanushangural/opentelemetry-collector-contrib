We want to work on scrapping the OTel metrics for sql server database where below are the details
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
