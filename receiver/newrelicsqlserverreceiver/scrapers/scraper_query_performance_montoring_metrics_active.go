// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// ScrapeActiveRunningQueriesMetrics collects currently executing queries with wait and blocking details
// This scraper captures real-time query execution state from sys.dm_exec_requests
// If a plan_handle is available, it fetches, parses, and emits execution plan as OTLP logs
func (s *QueryPerformanceScraper) ScrapeActiveRunningQueriesMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, logs plog.Logs, limit, textTruncateLimit int) error {
	query := fmt.Sprintf(queries.ActiveRunningQueriesQuery, limit, textTruncateLimit)

	s.logger.Debug("Executing active running queries metrics collection",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.Int("limit", limit),
		zap.Int("text_truncate_limit", textTruncateLimit))

	var results []models.ActiveRunningQuery
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute active running queries metrics query: %w", err)
	}

	s.logger.Debug("Active running queries metrics fetched", zap.Int("result_count", len(results)))

	// Log each query result from SQL Server
	for i, result := range results {
		s.logger.Info("=== SCRAPED ACTIVE QUERY FROM DB ===",
			zap.Int("index", i),
			zap.Any("session_id", result.CurrentSessionID),
			zap.Any("request_id", result.RequestID),
			zap.Any("database_name", result.DatabaseName),
			zap.Any("request_status", result.RequestStatus),
			zap.Any("wait_type", result.WaitType),
			zap.Any("wait_time_s", result.WaitTimeS),
			zap.Any("last_wait_type", result.LastWaitType),
			zap.Any("cpu_time_ms", result.CPUTimeMs),
			zap.Any("elapsed_time_ms", result.TotalElapsedTimeMs),
			zap.Any("reads", result.Reads),
			zap.Any("logical_reads", result.LogicalReads),
			zap.Any("writes", result.Writes),
			zap.Any("row_count", result.RowCount),
			zap.Any("granted_query_memory_pages", result.GrantedQueryMemoryPages),
			zap.Any("blocking_session_id", result.BlockingSessionID),
			zap.String("query_text_preview", func() string {
				if result.QueryStatementText != nil && len(*result.QueryStatementText) > 100 {
					return (*result.QueryStatementText)[:100] + "..."
				} else if result.QueryStatementText != nil {
					return *result.QueryStatementText
				}
				return "N/A"
			}()))

		// Fetch and parse execution plan for active queries with plan_handle
		var executionPlanXML string

		// Only fetch execution plan if plan_handle is available (active executing queries)
		if result.PlanHandle != nil && !result.PlanHandle.IsEmpty() {
			planXML, err := s.fetchExecutionPlanForActiveQuery(ctx, result)
			if err != nil {
				s.logger.Warn("Failed to fetch execution plan for active query",
					zap.Error(err),
					zap.Any("session_id", result.CurrentSessionID))
			} else if planXML != "" {
				executionPlanXML = planXML
				s.logger.Debug("Successfully fetched execution plan XML for active query",
					zap.Any("session_id", result.CurrentSessionID),
					zap.Int("xml_length", len(executionPlanXML)))

				// Parse XML and emit as OTLP logs
				queryID := ""
				if result.QueryID != nil {
					queryID = result.QueryID.String()
				}
				planHandle := result.PlanHandle.String()

				planAnalysis, err := models.ParseExecutionPlanXML(planXML, queryID, planHandle)
				if err != nil {
					s.logger.Warn("Failed to parse execution plan XML for active query",
						zap.Error(err),
						zap.Any("session_id", result.CurrentSessionID),
						zap.String("query_id", queryID))
				} else if planAnalysis != nil {
					// Set additional metadata
					planAnalysis.CollectionTime = time.Now().UTC().Format(time.RFC3339)
					if result.QueryStatementText != nil {
						planAnalysis.SQLText = helpers.AnonymizeQueryText(*result.QueryStatementText)
					}

					// Emit execution plan operators as OTLP logs
					s.emitActiveQueryExecutionPlanLogs(planAnalysis, result, logs)

					s.logger.Info("Successfully parsed and emitted execution plan logs for active query",
						zap.Any("session_id", result.CurrentSessionID),
						zap.String("query_id", queryID),
						zap.Int("operator_count", len(planAnalysis.Nodes)))
				}
			}
		}

		// Process active query metrics with execution plan
		if err := s.processActiveRunningQueryMetricsWithPlan(result, scopeMetrics, i, executionPlanXML); err != nil {
			s.logger.Error("Failed to process active running query metric", zap.Error(err), zap.Int("index", i))
		}
	}

	return nil
}

// processActiveRunningQueryMetricsWithPlan processes and emits metrics for a single active running query
// with optional execution plan XML
func (s *QueryPerformanceScraper) processActiveRunningQueryMetricsWithPlan(result models.ActiveRunningQuery, scopeMetrics pmetric.ScopeMetrics, index int, executionPlanXML string) error {
	if result.CurrentSessionID == nil {
		s.logger.Debug("Skipping active running query with nil session ID", zap.Int("index", index))
		return nil
	}

	// Create metrics for each active running query
	// We'll emit each query as a separate data point with all its attributes

	// Metric 1: Active query wait time
	if result.WaitTimeS != nil && *result.WaitTimeS > 0 {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.wait_time_seconds",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Float64("value", *result.WaitTimeS),
			zap.Any("wait_type", result.WaitType),
			zap.Any("database_name", result.DatabaseName))

		waitTimeMetric := scopeMetrics.Metrics().AppendEmpty()
		waitTimeMetric.SetName("sqlserver.activequery.wait_time_seconds")
		waitTimeMetric.SetDescription("Wait time for currently executing query")
		waitTimeMetric.SetUnit("s")
		gauge := waitTimeMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(*result.WaitTimeS)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Warn("❌ SKIPPED wait_time metric (wait_time_s <= 0 or nil)",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Any("wait_time_s", result.WaitTimeS),
			zap.Any("wait_type", result.WaitType))
	}

	// Metric 2: Active query CPU time
	if result.CPUTimeMs != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.cpu_time_ms",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.CPUTimeMs))

		cpuTimeMetric := scopeMetrics.Metrics().AppendEmpty()
		cpuTimeMetric.SetName("sqlserver.activequery.cpu_time_ms")
		cpuTimeMetric.SetDescription("CPU time for currently executing query")
		cpuTimeMetric.SetUnit("ms")
		gauge := cpuTimeMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.CPUTimeMs)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED cpu_time metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 3: Active query elapsed time
	if result.TotalElapsedTimeMs != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.elapsed_time_ms",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.TotalElapsedTimeMs))

		elapsedTimeMetric := scopeMetrics.Metrics().AppendEmpty()
		elapsedTimeMetric.SetName("sqlserver.activequery.elapsed_time_ms")
		elapsedTimeMetric.SetDescription("Total elapsed time for currently executing query")
		elapsedTimeMetric.SetUnit("ms")
		gauge := elapsedTimeMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.TotalElapsedTimeMs)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED elapsed_time metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 4: Active query reads (physical reads from disk)
	if result.Reads != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.reads",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.Reads))

		readsMetric := scopeMetrics.Metrics().AppendEmpty()
		readsMetric.SetName("sqlserver.activequery.reads")
		readsMetric.SetDescription("Number of physical reads from disk for currently executing query")
		readsMetric.SetUnit("{reads}")
		gauge := readsMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.Reads)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED reads metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 5: Active query writes
	if result.Writes != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.writes",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.Writes))

		writesMetric := scopeMetrics.Metrics().AppendEmpty()
		writesMetric.SetName("sqlserver.activequery.writes")
		writesMetric.SetDescription("Number of writes for currently executing query")
		writesMetric.SetUnit("{writes}")
		gauge := writesMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.Writes)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED writes metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 6: Active query logical reads (reads from buffer cache/memory)
	if result.LogicalReads != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.logical_reads",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.LogicalReads))

		logicalReadsMetric := scopeMetrics.Metrics().AppendEmpty()
		logicalReadsMetric.SetName("sqlserver.activequery.logical_reads")
		logicalReadsMetric.SetDescription("Number of logical reads from buffer cache for currently executing query")
		logicalReadsMetric.SetUnit("{reads}")
		gauge := logicalReadsMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.LogicalReads)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED logical_reads metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 7: Active query row count
	if result.RowCount != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.row_count",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.RowCount))

		rowCountMetric := scopeMetrics.Metrics().AppendEmpty()
		rowCountMetric.SetName("sqlserver.activequery.row_count")
		rowCountMetric.SetDescription("Number of rows returned by currently executing query")
		rowCountMetric.SetUnit("{rows}")
		gauge := rowCountMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.RowCount)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED row_count metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	// Metric 8: Active query granted memory
	if result.GrantedQueryMemoryPages != nil {
		s.logger.Info("✅ EMITTING METRIC: sqlserver.activequery.granted_query_memory_pages",
			zap.Any("session_id", result.CurrentSessionID),
			zap.Int64("value", *result.GrantedQueryMemoryPages))

		grantedMemoryMetric := scopeMetrics.Metrics().AppendEmpty()
		grantedMemoryMetric.SetName("sqlserver.activequery.granted_query_memory_pages")
		grantedMemoryMetric.SetDescription("Number of memory pages granted to currently executing query")
		grantedMemoryMetric.SetUnit("{pages}")
		gauge := grantedMemoryMetric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(*result.GrantedQueryMemoryPages)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

		// Add attributes including execution plan
		s.addActiveQueryAttributes(dp.Attributes(), result, executionPlanXML)
	} else {
		s.logger.Debug("SKIPPED granted_query_memory_pages metric (nil)",
			zap.Any("session_id", result.CurrentSessionID))
	}

	s.logger.Info("=== FINISHED PROCESSING ACTIVE QUERY ===",
		zap.Any("session_id", result.CurrentSessionID),
		zap.Int("total_metrics_in_scope", scopeMetrics.Metrics().Len()))

	return nil
}

// addActiveQueryAttributes adds all attributes from an active running query to a data point
func (s *QueryPerformanceScraper) addActiveQueryAttributes(attrs pcommon.Map, result models.ActiveRunningQuery, executionPlanXML string) {
	// Add execution plan XML if available
	if executionPlanXML != "" {
		attrs.PutStr("execution_plan_xml", executionPlanXML)
	}

	// Session details
	if result.CurrentSessionID != nil {
		attrs.PutInt("session_id", *result.CurrentSessionID)
	}
	if result.RequestID != nil {
		attrs.PutInt("request_id", *result.RequestID)
	}
	if result.DatabaseName != nil {
		attrs.PutStr("database_name", *result.DatabaseName)
	}
	if result.LoginName != nil {
		attrs.PutStr("login_name", *result.LoginName)
	}
	if result.HostName != nil {
		attrs.PutStr("host_name", *result.HostName)
	}
	if result.ProgramName != nil {
		attrs.PutStr("program_name", *result.ProgramName)
	}
	if result.RequestCommand != nil {
		attrs.PutStr("request_command", *result.RequestCommand)
	}
	if result.RequestStatus != nil {
		attrs.PutStr("request_status", *result.RequestStatus)
	}
	if result.SessionStatus != nil {
		attrs.PutStr("session_status", *result.SessionStatus)
	}
	if result.ClientInterfaceName != nil {
		attrs.PutStr("client_interface_name", *result.ClientInterfaceName)
	}

	// Wait details (raw and decoded)
	if result.WaitType != nil {
		waitType := *result.WaitType
		attrs.PutStr("wait_type", waitType)
		attrs.PutStr("wait_type_description", helpers.DecodeWaitType(waitType))
		attrs.PutStr("wait_type_category", helpers.GetWaitTypeCategory(waitType))
	}
	if result.WaitResource != nil {
		waitResource := *result.WaitResource
		attrs.PutStr("wait_resource", waitResource)
		// Add SQL Server's decoded wait resource (with database/table/index names)
		if result.WaitResourceDecoded != nil {
			attrs.PutStr("wait_resource_decoded", *result.WaitResourceDecoded)
		}
		// Add Go helper's parsed resource type and description (as backup/supplement)
		resourceType, resourceDesc := helpers.DecodeWaitResource(waitResource)
		attrs.PutStr("wait_resource_type", resourceType)
		attrs.PutStr("wait_resource_description", resourceDesc)
	}
	if result.LastWaitType != nil {
		lastWaitType := *result.LastWaitType
		attrs.PutStr("last_wait_type", lastWaitType)
		attrs.PutStr("last_wait_type_description", helpers.DecodeWaitType(lastWaitType))
	}

	// Timestamps
	if result.RequestStartTime != nil {
		attrs.PutStr("request_start_time", *result.RequestStartTime)
	}
	if result.CollectionTimestamp != nil {
		attrs.PutStr("collection_timestamp", *result.CollectionTimestamp)
	}

	// Transaction context (Phase 1 RCA)
	if result.TransactionID != nil {
		attrs.PutInt("transaction_id", *result.TransactionID)
	}
	if result.OpenTransactionCount != nil {
		attrs.PutInt("open_transaction_count", *result.OpenTransactionCount)
	}
	if result.TransactionIsolationLevel != nil {
		attrs.PutInt("transaction_isolation_level", *result.TransactionIsolationLevel)
	}

	// Parallel execution details (Phase 1 RCA)
	if result.DegreeOfParallelism != nil {
		attrs.PutInt("degree_of_parallelism", *result.DegreeOfParallelism)
	}
	if result.ParallelWorkerCount != nil {
		attrs.PutInt("parallel_worker_count", *result.ParallelWorkerCount)
	}

	// Blocking details
	if result.BlockingSessionID != nil {
		attrs.PutStr("blocking_session_id", *result.BlockingSessionID)
	}
	if result.BlockerLoginName != nil {
		attrs.PutStr("blocker_login_name", *result.BlockerLoginName)
	}
	if result.BlockerHostName != nil {
		attrs.PutStr("blocker_host_name", *result.BlockerHostName)
	}
	if result.BlockerProgramName != nil {
		attrs.PutStr("blocker_program_name", *result.BlockerProgramName)
	}

	// Query text (anonymized) and query ID for correlation
	if result.QueryStatementText != nil {
		anonymizedQuery := helpers.AnonymizeQueryText(*result.QueryStatementText)
		attrs.PutStr("query_text", anonymizedQuery)
	}

	// Phase 1 RCA: Correlation keys
	// Add SQL Server's native query_id if available (from dm_exec_query_stats)
	// This enables direct correlation with slow queries
	if result.QueryID != nil && !result.QueryID.IsEmpty() {
		attrs.PutStr("query_id", result.QueryID.String())
	}

	// ALSO compute and add query_signature (client-side hash) for backwards compatibility
	// This provides a secondary correlation method for queries not yet cached
	if result.QueryStatementText != nil {
		querySignature := helpers.ComputeQueryHash(*result.QueryStatementText)
		if querySignature != "" {
			attrs.PutStr("query_signature", querySignature)
		}
	}
	if result.BlockingQueryStatementText != nil && *result.BlockingQueryStatementText != "N/A" {
		anonymizedBlockingQuery := helpers.AnonymizeQueryText(*result.BlockingQueryStatementText)
		attrs.PutStr("blocking_query_text", anonymizedBlockingQuery)

		// Also compute hash for blocking query for additional correlation
		blockingQueryHash := helpers.ComputeQueryHash(*result.BlockingQueryStatementText)
		if blockingQueryHash != "" {
			attrs.PutStr("blocking_query_hash", blockingQueryHash)
		}
	}
}

// ScrapeLockedObjectsMetrics collects detailed information about objects locked by a specific session
// This provides table/object names for locked resources, enabling better lock contention troubleshooting
func (s *QueryPerformanceScraper) ScrapeLockedObjectsMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, sessionID int) error {
	query := fmt.Sprintf(queries.LockedObjectsBySessionQuery, sessionID)

	s.logger.Debug("Executing locked objects metrics collection",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.Int("session_id", sessionID))

	var results []models.LockedObject
	if err := s.connection.Query(ctx, &results, query); err != nil {
		return fmt.Errorf("failed to execute locked objects metrics query: %w", err)
	}

	s.logger.Debug("Locked objects metrics fetched", zap.Int("result_count", len(results)))

	for i, result := range results {
		if err := s.processLockedObjectMetrics(result, scopeMetrics, i); err != nil {
			s.logger.Error("Failed to process locked object metric", zap.Error(err), zap.Int("index", i))
		}
	}

	return nil
}

// processLockedObjectMetrics processes and emits metrics for a single locked object
func (s *QueryPerformanceScraper) processLockedObjectMetrics(result models.LockedObject, scopeMetrics pmetric.ScopeMetrics, index int) error {
	if result.SessionID == nil {
		s.logger.Debug("Skipping locked object with nil session ID", zap.Int("index", index))
		return nil
	}

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Create a metric for locked object tracking
	// Value = 1 to indicate presence of lock, attributes contain all details
	metric := scopeMetrics.Metrics().AppendEmpty()
	metric.SetName("sqlserver.locked_object")
	metric.SetDescription("Database object locked by a session")
	metric.SetUnit("1")

	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetIntValue(1) // Presence indicator
	dp.SetTimestamp(timestamp)
	dp.SetStartTimestamp(s.startTime)

	// Add all locked object attributes
	attrs := dp.Attributes()

	if result.SessionID != nil {
		attrs.PutInt("session_id", *result.SessionID)
	}
	if result.DatabaseName != nil {
		attrs.PutStr("database_name", *result.DatabaseName)
	}
	if result.SchemaName != nil && *result.SchemaName != "" {
		attrs.PutStr("schema_name", *result.SchemaName)
	}
	if result.LockedObjectName != nil && *result.LockedObjectName != "" {
		attrs.PutStr("locked_object_name", *result.LockedObjectName)
	} else {
		// If object name is NULL, this might be a database or file lock
		attrs.PutStr("locked_object_name", "N/A")
	}
	if result.ResourceType != nil {
		attrs.PutStr("resource_type", *result.ResourceType)
	}
	if result.LockGranularity != nil {
		attrs.PutStr("lock_granularity", *result.LockGranularity)
	}
	if result.LockMode != nil {
		attrs.PutStr("lock_mode", *result.LockMode)
	}
	if result.LockStatus != nil {
		attrs.PutStr("lock_status", *result.LockStatus)
	}
	if result.LockRequestType != nil {
		attrs.PutStr("lock_request_type", *result.LockRequestType)
	}
	if result.ResourceDescription != nil && *result.ResourceDescription != "" {
		attrs.PutStr("resource_description", *result.ResourceDescription)
	}
	if result.CollectionTimestamp != nil {
		attrs.PutStr("collection_timestamp", *result.CollectionTimestamp)
	}

	s.logger.Debug("Processed locked object metric",
		zap.Any("session_id", result.SessionID),
		zap.Any("locked_object_name", result.LockedObjectName),
		zap.Any("lock_granularity", result.LockGranularity),
		zap.Any("lock_mode", result.LockMode))

	return nil
}

// fetchExecutionPlanForActiveQuery fetches the execution plan XML for an active query using plan_handle only
// This ensures we get the execution plan for the CURRENTLY RUNNING query, not from historical stats
// Returns the execution plan XML string or empty string if not found
func (s *QueryPerformanceScraper) fetchExecutionPlanForActiveQuery(ctx context.Context, activeQuery models.ActiveRunningQuery) (string, error) {
	var executionPlanXML string

	// Fetch using plan_handle (for currently executing queries only)
	if activeQuery.PlanHandle == nil || activeQuery.PlanHandle.IsEmpty() {
		s.logger.Debug("No plan_handle available for active query, skipping execution plan fetch",
			zap.Any("session_id", activeQuery.CurrentSessionID),
			zap.Any("query_id", activeQuery.QueryID))
		return "", nil
	}

	planHandleHex := activeQuery.PlanHandle.String()
	query := fmt.Sprintf(queries.ActiveQueryExecutionPlanQuery, planHandleHex)

	s.logger.Debug("Fetching execution plan for active query using plan_handle",
		zap.String("plan_handle", planHandleHex),
		zap.Any("session_id", activeQuery.CurrentSessionID),
		zap.Any("query_id", activeQuery.QueryID))

	// Execute the query to fetch the execution plan XML
	var results []struct {
		ExecutionPlanXML *string `db:"execution_plan_xml"`
	}

	if err := s.connection.Query(ctx, &results, query); err != nil {
		return "", fmt.Errorf("failed to fetch execution plan using plan_handle: %w", err)
	}

	if len(results) == 0 || results[0].ExecutionPlanXML == nil {
		s.logger.Debug("No execution plan found for plan_handle",
			zap.String("plan_handle", planHandleHex),
			zap.Any("session_id", activeQuery.CurrentSessionID))
		return "", nil
	}

	executionPlanXML = *results[0].ExecutionPlanXML
	s.logger.Debug("Successfully fetched execution plan XML using plan_handle",
		zap.String("plan_handle", planHandleHex),
		zap.Any("session_id", activeQuery.CurrentSessionID),
		zap.Int("xml_length", len(executionPlanXML)))

	// Return the execution plan XML
	return executionPlanXML, nil
}

// emitActiveQueryExecutionPlanLogs emits execution plan operators as OTLP log records for active queries
// Each operator becomes a separate log event with proper correlation (query_id, session_id, request_id)
func (s *QueryPerformanceScraper) emitActiveQueryExecutionPlanLogs(planAnalysis *models.ExecutionPlanAnalysis, activeQuery models.ActiveRunningQuery, logs plog.Logs) {
	if planAnalysis == nil || len(planAnalysis.Nodes) == 0 {
		return
	}

	// Create resource logs
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// Add resource attributes for correlation
	resourceAttrs := resourceLogs.Resource().Attributes()
	if activeQuery.DatabaseName != nil {
		resourceAttrs.PutStr("db.name", *activeQuery.DatabaseName)
	}
	if activeQuery.LoginName != nil {
		resourceAttrs.PutStr("db.user", *activeQuery.LoginName)
	}

	// Create scope logs
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scopeLogs.Scope().SetName("newrelicsqlserverreceiver")
	scopeLogs.Scope().SetVersion("1.0.0")

	timestamp := pcommon.NewTimestampFromTime(time.Now())

	// Emit a log record for each execution plan operator
	for i := range planAnalysis.Nodes {
		node := &planAnalysis.Nodes[i]
		s.createActiveQueryExecutionPlanNodeLog(node, activeQuery, scopeLogs, timestamp)
	}

	s.logger.Debug("Emitted execution plan logs for active query",
		zap.String("query_id", planAnalysis.QueryID),
		zap.Int("operator_count", len(planAnalysis.Nodes)),
		zap.Any("session_id", activeQuery.CurrentSessionID))
}

// createActiveQueryExecutionPlanNodeLog creates a single OTLP log record for an execution plan operator from active query
func (s *QueryPerformanceScraper) createActiveQueryExecutionPlanNodeLog(node *models.ExecutionPlanNode, activeQuery models.ActiveRunningQuery, scopeLogs plog.ScopeLogs, timestamp pcommon.Timestamp) {
	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetTimestamp(timestamp)
	logRecord.SetObservedTimestamp(timestamp)
	logRecord.SetSeverityNumber(plog.SeverityNumberInfo)
	logRecord.SetSeverityText("INFO")

	// Set event name for proper log ingestion in New Relic
	logRecord.Body().SetStr(fmt.Sprintf("Execution Plan Node: %s (NodeID=%d, Parent=%d, Type=%s)",
		node.PhysicalOp, node.NodeID, node.ParentNodeID, node.InputType))

	// Set all attributes for the execution plan operator
	attrs := logRecord.Attributes()

	// Set event name attribute for NRQL queries
	attrs.PutStr("event.name", "sqlserver.execution_plan_operator")

	// Correlation keys
	attrs.PutStr("query_id", node.QueryID)
	attrs.PutStr("plan_handle", node.PlanHandle)
	if activeQuery.CurrentSessionID != nil {
		attrs.PutInt("session_id", *activeQuery.CurrentSessionID)
	}
	if activeQuery.RequestID != nil {
		attrs.PutInt("request_id", *activeQuery.RequestID)
	}
	if activeQuery.DatabaseName != nil {
		attrs.PutStr("database_name", *activeQuery.DatabaseName)
	}

	// Node structure
	attrs.PutInt("node_id", int64(node.NodeID))
	attrs.PutInt("parent_node_id", int64(node.ParentNodeID))
	attrs.PutStr("input_type", node.InputType)

	// Operator information
	attrs.PutStr("physical_op", node.PhysicalOp)
	attrs.PutStr("logical_op", node.LogicalOp)
	attrs.PutStr("sql_text", node.SQLText)

	// Object information (for Index Scan/Seek operators)
	if node.SchemaName != "" {
		attrs.PutStr("schema_name", node.SchemaName)
	}
	if node.TableName != "" {
		attrs.PutStr("table_name", node.TableName)
	}
	if node.IndexName != "" {
		attrs.PutStr("index_name", node.IndexName)
	}
	if node.ReferencedColumns != "" {
		attrs.PutStr("referenced_columns", node.ReferencedColumns)
	}

	// Cost estimates
	attrs.PutDouble("estimate_rows", node.EstimateRows)
	attrs.PutDouble("estimate_io", node.EstimateIO)
	attrs.PutDouble("estimate_cpu", node.EstimateCPU)
	attrs.PutDouble("avg_row_size", node.AvgRowSize)
	attrs.PutDouble("total_subtree_cost", node.TotalSubtreeCost)
	attrs.PutDouble("estimated_operator_cost", node.EstimatedOperatorCost)

	// Execution details
	attrs.PutStr("estimated_execution_mode", node.EstimatedExecutionMode)
	attrs.PutInt("granted_memory_kb", node.GrantedMemoryKb)
	attrs.PutBool("spill_occurred", node.SpillOccurred)
	attrs.PutBool("no_join_predicate", node.NoJoinPredicate)

	// Performance metrics
	attrs.PutDouble("total_worker_time", node.TotalWorkerTime)
	attrs.PutDouble("total_elapsed_time", node.TotalElapsedTime)
	attrs.PutInt("total_logical_reads", node.TotalLogicalReads)
	attrs.PutInt("total_logical_writes", node.TotalLogicalWrites)
	attrs.PutInt("execution_count", node.ExecutionCount)
	attrs.PutDouble("avg_elapsed_time_ms", node.AvgElapsedTimeMs)

	// Timestamps (all in RFC3339 format: "2025-11-24T13:44:01Z")
	// Note: collection_timestamp is only included in active query metrics for wait time analysis
	if node.LastExecutionTime != "" {
		attrs.PutStr("last_execution_time", node.LastExecutionTime)
	}
	if activeQuery.RequestStartTime != nil {
		attrs.PutStr("request_start_time", *activeQuery.RequestStartTime)
	}

	// Active query context (with decoded wait information)
	if activeQuery.WaitType != nil {
		waitType := *activeQuery.WaitType
		attrs.PutStr("wait_type", waitType)
		attrs.PutStr("wait_type_description", helpers.DecodeWaitType(waitType))
		attrs.PutStr("wait_type_category", helpers.GetWaitTypeCategory(waitType))
	}
	if activeQuery.WaitResource != nil {
		waitResource := *activeQuery.WaitResource
		attrs.PutStr("wait_resource", waitResource)
		// Add SQL Server's decoded wait resource (with database/table/index names)
		if activeQuery.WaitResourceDecoded != nil {
			attrs.PutStr("wait_resource_decoded", *activeQuery.WaitResourceDecoded)
		}
		// Add Go helper's parsed resource type and description (as backup/supplement)
		resourceType, resourceDesc := helpers.DecodeWaitResource(waitResource)
		attrs.PutStr("wait_resource_type", resourceType)
		attrs.PutStr("wait_resource_description", resourceDesc)
	}
	if activeQuery.RequestStatus != nil {
		attrs.PutStr("request_status", *activeQuery.RequestStatus)
	}
	if activeQuery.LoginName != nil {
		attrs.PutStr("login_name", *activeQuery.LoginName)
	}
	if activeQuery.HostName != nil {
		attrs.PutStr("host_name", *activeQuery.HostName)
	}
	if activeQuery.ProgramName != nil {
		attrs.PutStr("program_name", *activeQuery.ProgramName)
	}
	if activeQuery.RequestCommand != nil {
		attrs.PutStr("request_command", *activeQuery.RequestCommand)
	}
	
	// Add blocking information for execution plan correlation
	if activeQuery.BlockingSessionID != nil {
		attrs.PutStr("blocking_session_id", *activeQuery.BlockingSessionID)
	}
	if activeQuery.BlockerLoginName != nil {
		attrs.PutStr("blocker_login_name", *activeQuery.BlockerLoginName)
	}
	if activeQuery.BlockerHostName != nil {
		attrs.PutStr("blocker_host_name", *activeQuery.BlockerHostName)
	}
	if activeQuery.BlockerProgramName != nil {
		attrs.PutStr("blocker_program_name", *activeQuery.BlockerProgramName)
	}
	
	// Add query performance metrics from active query context
	if activeQuery.CPUTimeMs != nil {
		attrs.PutInt("query_cpu_time_ms", *activeQuery.CPUTimeMs)
	}
	if activeQuery.TotalElapsedTimeMs != nil {
		attrs.PutInt("query_total_elapsed_time_ms", *activeQuery.TotalElapsedTimeMs)
	}
	if activeQuery.LogicalReads != nil {
		attrs.PutInt("query_logical_reads", *activeQuery.LogicalReads)
	}
	if activeQuery.Reads != nil {
		attrs.PutInt("query_reads", *activeQuery.Reads)
	}
	if activeQuery.Writes != nil {
		attrs.PutInt("query_writes", *activeQuery.Writes)
	}
	if activeQuery.RowCount != nil {
		attrs.PutInt("query_row_count", *activeQuery.RowCount)
	}
	if activeQuery.GrantedQueryMemoryPages != nil {
		attrs.PutInt("query_granted_memory_pages", *activeQuery.GrantedQueryMemoryPages)
	}
	
	// Add transaction and isolation level information
	if activeQuery.TransactionID != nil {
		attrs.PutInt("transaction_id", *activeQuery.TransactionID)
	}
	if activeQuery.OpenTransactionCount != nil {
		attrs.PutInt("open_transaction_count", *activeQuery.OpenTransactionCount)
	}
	if activeQuery.TransactionIsolationLevel != nil {
		attrs.PutInt("transaction_isolation_level", *activeQuery.TransactionIsolationLevel)
	}
	
	// Add parallel query information
	if activeQuery.ParallelWorkerCount != nil {
		attrs.PutInt("parallel_worker_count", *activeQuery.ParallelWorkerCount)
	}
	if activeQuery.DegreeOfParallelism != nil {
		attrs.PutInt("degree_of_parallelism", *activeQuery.DegreeOfParallelism)
	}
	
	// Add session context
	if activeQuery.SessionStatus != nil {
		attrs.PutStr("session_status", *activeQuery.SessionStatus)
	}
	if activeQuery.ClientInterfaceName != nil {
		attrs.PutStr("client_interface_name", *activeQuery.ClientInterfaceName)
	}
	
	// Add wait information
	if activeQuery.LastWaitType != nil {
		attrs.PutStr("last_wait_type", *activeQuery.LastWaitType)
	}
	if activeQuery.WaitTimeS != nil {
		attrs.PutDouble("wait_time_seconds", *activeQuery.WaitTimeS)
	}
	
	// Add query texts
	if activeQuery.QueryStatementText != nil {
		attrs.PutStr("query_statement_text", helpers.AnonymizeQueryText(*activeQuery.QueryStatementText))
	}
	if activeQuery.BlockingQueryStatementText != nil {
		attrs.PutStr("blocking_query_statement_text", helpers.AnonymizeQueryText(*activeQuery.BlockingQueryStatementText))
	}
	
	// Add collection timestamp
	if activeQuery.CollectionTimestamp != nil {
		attrs.PutStr("collection_timestamp", *activeQuery.CollectionTimestamp)
	}
}
