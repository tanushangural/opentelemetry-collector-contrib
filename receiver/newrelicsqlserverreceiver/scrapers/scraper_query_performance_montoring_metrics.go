// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package scrapers provides the performance monitoring scraper for SQL Server.
// This file implements comprehensive query performance monitoring including
// slow query analysis, wait time statistics, blocking session detection, and execution plan analysis.
//
// Performance Monitoring Features:
//
// 1. Top N Slow Queries:
//   - Query dm_exec_query_stats for execution statistics
//   - Rank by total_elapsed_time, avg_elapsed_time, execution_count
//   - Include query text from dm_exec_sql_text()
//   - Track CPU time, logical reads, physical reads, writes
//   - Monitor plan generation time and recompiles
//
// 2. Blocking Session Detection:
//   - Query dm_exec_requests for blocked sessions
//   - Identify blocking session hierarchy (head blockers)
//   - Track blocked session duration and wait types
//   - Include session details (login, program, host)
//   - Monitor lock resource information
//
// 3. Execution Plan Analysis:
//   - Query dm_exec_cached_plans for plan cache statistics
//   - Track plan reuse ratio and cache hit rates
//   - Monitor plan cache memory consumption
//   - Identify ad-hoc vs prepared statement ratios
//   - Include parameterized vs non-parameterized queries
//
// Scraper Structure:
//
//	type PerformanceScraper struct {
//	    config   *Config
//	    mb       *metadata.MetricsBuilder
//	    queries  *queries.PerformanceQueries
//	    logger   *zap.Logger
//	}
//
// Metrics Generated:
// - sqlserver.slowquery.avg_cpu_time_ms
// - sqlserver.slowquery.avg_disk_reads
// - sqlserver.slowquery.avg_disk_writes
// - sqlserver.slowquery.avg_elapsed_time_ms
// - sqlserver.slowquery.execution_count
// - sqlserver.slowquery.query_text
// - sqlserver.slowquery.query_id
// - sqlserver.blocking_query.wait_time_seconds
// - sqlserver.blocked_query.wait_time_seconds
// - sqlserver.wait_analysis.query_text
// - sqlserver.wait_analysis.total_wait_time_ms
// - sqlserver.wait_analysis.avg_wait_time_ms
// - sqlserver.wait_analysis.wait_event_count
// - sqlserver.wait_analysis.last_execution_time
//
// Engine-Specific Considerations:
// - Azure SQL Database: Limited access to some DMVs, use Azure-specific alternatives
// - Azure SQL Managed Instance: Most performance DMVs available
// - Standard SQL Server: Full access to all performance monitoring DMVs
package scrapers

import (
    "context"
    "fmt"
    "time"

    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/pmetric"
    "go.uber.org/zap"

    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/helpers"
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)



// QueryPerformanceScraper handles SQL Server query performance monitoring metrics collection
type QueryPerformanceScraper struct {
    connection    SQLConnectionInterface
    logger        *zap.Logger
    startTime     pcommon.Timestamp
    engineEdition int
}

// NewQueryPerformanceScraper creates a new query performance scraper
func NewQueryPerformanceScraper(conn SQLConnectionInterface, logger *zap.Logger, engineEdition int) *QueryPerformanceScraper {
    return &QueryPerformanceScraper{
        connection:    conn,
        logger:        logger,
        startTime:     pcommon.NewTimestampFromTime(time.Now()),
        engineEdition: engineEdition,
    }
}

// ScrapeSlowQueryMetrics collects slow query performance monitoring metrics
func (s *QueryPerformanceScraper) ScrapeSlowQueryMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit int) error {
    query := fmt.Sprintf(queries.SlowQuery, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit)

    s.logger.Debug("Executing slow query metrics collection",
        zap.String("query", queries.TruncateQuery(query, 100)),
        zap.Int("interval_seconds", intervalSeconds),
        zap.Int("top_n", topN),
        zap.Int("elapsed_time_threshold", elapsedTimeThreshold))

    var results []models.SlowQuery
    if err := s.connection.Query(ctx, &results, query); err != nil {
        return fmt.Errorf("failed to execute slow query metrics query: %w", err)
    }

    s.logger.Debug("Slow query metrics fetched", zap.Int("result_count", len(results)))

    for i, result := range results {
        if err := s.processSlowQueryMetrics(result, scopeMetrics, i); err != nil {
            s.logger.Error("Failed to process slow query metric", zap.Error(err), zap.Int("index", i))
        }
    }

    return nil
}

// ScrapeBlockingSessionMetrics collects blocking session metrics
func (s *QueryPerformanceScraper) ScrapeBlockingSessionMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, limit int, textTruncateLimit int) error {
    query := fmt.Sprintf(queries.BlockingSessionsQuery, limit, textTruncateLimit)
    
    s.logger.Debug("Executing blocking session metrics collection",
        zap.String("query", queries.TruncateQuery(query, 100)),
        zap.Int("limit", limit),
        zap.Int("text_truncate_limit", textTruncateLimit))

    var results []models.BlockingSession
    if err := s.connection.Query(ctx, &results, query); err != nil {
        return fmt.Errorf("failed to execute blocking session metrics query: %w", err)
    }

    s.logger.Debug("Blocking session metrics fetched", zap.Int("result_count", len(results)))

    for i, result := range results {
        if err := s.processBlockingSessionMetrics(result, scopeMetrics, i); err != nil {
            s.logger.Error("Failed to process blocking session metric", zap.Error(err), zap.Int("index", i))
        }
    }

    return nil
}

// ScrapeWaitTimeAnalysisMetrics collects wait time analysis metrics
func (s *QueryPerformanceScraper) ScrapeWaitTimeAnalysisMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, topN int, maxQueryTextSize int) error {
    query := fmt.Sprintf(queries.WaitQuery, topN, maxQueryTextSize)

    s.logger.Debug("Executing wait time analysis metrics collection",
        zap.String("query", queries.TruncateQuery(query, 100)),
        zap.Int("top_n", topN),
        zap.Int("max_query_text_size", maxQueryTextSize))

    var results []models.WaitTimeAnalysis
    if err := s.connection.Query(ctx, &results, query); err != nil {
        return fmt.Errorf("failed to execute wait time analysis metrics query: %w", err)
    }

    s.logger.Debug("Wait time analysis metrics fetched", zap.Int("result_count", len(results)))

    for i, result := range results {
        if err := s.processWaitTimeAnalysisMetrics(result, scopeMetrics, i); err != nil {
            s.logger.Error("Failed to process wait time analysis metric", zap.Error(err), zap.Int("index", i))
        }
    }

    return nil
}

// ScrapeQueryExecutionPlanMetrics collects query execution plan metrics with cardinality safety
func (s *QueryPerformanceScraper) ScrapeQueryExecutionPlanMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit int) error {
    s.logger.Debug("Starting query execution plan metrics collection",
        zap.Int("interval_seconds", intervalSeconds),
        zap.Int("top_n", topN),
        zap.Int("elapsed_time_threshold", elapsedTimeThreshold))

    // Step 1: Get slow queries to extract QueryIDs
    slowQueries, err := s.getSlowQueryResults(ctx, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit)
    if err != nil {
        return fmt.Errorf("failed to get slow queries for execution plan analysis: %w", err)
    }

    if len(slowQueries) == 0 {
        s.logger.Debug("No slow queries found for execution plan analysis")
        return nil
    }

    // Step 2: Extract QueryIDs and PlanHandles from slow queries
    queryIDs := s.extractQueryIDsFromSlowQueries(slowQueries)
    planHandles := s.extractPlanHandlesFromSlowQueries(slowQueries)
    
    if len(planHandles) == 0 {
        s.logger.Debug("No valid PlanHandles found for execution plan analysis")
        return nil
    }

    // Step 3: Format PlanHandles and QueryIDs for SQL query
    planHandlesString := s.formatPlanHandlesForSQL(planHandles)
    queryIDsString := s.formatQueryIDsForSQL(queryIDs)

    // Step 4: Execute execution plan query with extracted PlanHandles and QueryIDs
    formattedQuery := fmt.Sprintf(queries.QueryExecutionPlan, topN, elapsedTimeThreshold, planHandlesString, queryIDsString, intervalSeconds, textTruncateLimit)

    s.logger.Debug("Executing query execution plan metrics collection",
        zap.String("query", queries.TruncateQuery(formattedQuery, 200)),
        zap.Int("plan_handle_count", len(planHandles)),
        zap.Int("query_id_count", len(queryIDs)))

    var results []models.QueryExecutionPlan
    if err := s.connection.Query(ctx, &results, formattedQuery); err != nil {
        return fmt.Errorf("failed to execute query execution plan metrics query: %w", err)
    }

    s.logger.Debug("Query execution plan metrics fetched", 
        zap.Int("result_count", len(results)),
        zap.Int("source_slow_queries", len(slowQueries)))

    // Step 5: Process the results with cardinality safety
    for i, result := range results {
        if err := s.processQueryExecutionPlanMetrics(result, scopeMetrics, i); err != nil {
            s.logger.Error("Failed to process query execution plan metric", zap.Error(err), zap.Int("index", i))
        }
    }

    return nil
}

// processSlowQueryMetrics processes slow query metrics and creates separate OpenTelemetry metrics for each measurement
// CARDINALITY-SAFE: Implements controlled attribute strategy to prevent metric explosion
func (s *QueryPerformanceScraper) processSlowQueryMetrics(result models.SlowQuery, scopeMetrics pmetric.ScopeMetrics, index int) error {
    timestamp := pcommon.NewTimestampFromTime(time.Now())
    
    // CARDINALITY-SAFE: Create limited common attributes (QueryID only as primary key)
    // Avoids high-cardinality attributes like full query text, timestamps, and multiple identifiers
    createSafeAttributes := func() pcommon.Map {
        attrs := pcommon.NewMap()
        if result.QueryID != nil {
            attrs.PutStr("QueryID", result.QueryID.String())
        }
        if result.DatabaseName != nil {
            attrs.PutStr("DatabaseName", *result.DatabaseName)
        }
        if result.StatementType != nil {
            attrs.PutStr("statement_type", *result.StatementType) 
        }
        // NOTE: NOT including CollectionTimestamp, PlanHandle, QueryText as attributes to prevent cardinality explosion
        // These can be logged separately for debugging/drill-down analysis
        return attrs
    }
    
    // Create detailed attributes for logging/debugging (not used in metrics)
    logAttributes := func() []zap.Field {
        var fields []zap.Field
        if result.QueryID != nil {
            fields = append(fields, zap.String("query_id", result.QueryID.String()))
        }
        if result.PlanHandle != nil {
            fields = append(fields, zap.String("plan_handle", result.PlanHandle.String()))
        }
        if result.DatabaseName != nil {
            fields = append(fields, zap.String("database_name", *result.DatabaseName))
        }
        if result.QueryText != nil {
            // Anonymize and truncate query text for logging
            anonymizedSQL := helpers.SafeAnonymizeQueryText(result.QueryText)
            if len(anonymizedSQL) > 100 {
                anonymizedSQL = anonymizedSQL[:100] + "..."
            }
            fields = append(fields, zap.String("query_text_preview", anonymizedSQL))
        }
        if result.CollectionTimestamp != nil {
            fields = append(fields, zap.String("collection_timestamp", *result.CollectionTimestamp))
        }
        if result.LastExecutionTimestamp != nil {
            fields = append(fields, zap.String("last_execution_timestamp", *result.LastExecutionTimestamp))
        }
        return fields
    }

    // Create avg_cpu_time_ms metric - CARDINALITY SAFE
    if result.AvgCPUTimeMS != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.avg_cpu_time_ms")
        metric.SetDescription("Average CPU time in milliseconds for slow query")
        metric.SetUnit("ms")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.AvgCPUTimeMS)
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create avg_disk_reads metric - CARDINALITY SAFE
    if result.AvgDiskReads != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.avg_disk_reads")
        metric.SetDescription("Average disk reads for slow query")
        metric.SetUnit("1")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.AvgDiskReads)
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create avg_disk_writes metric - CARDINALITY SAFE
    if result.AvgDiskWrites != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.avg_disk_writes")
        metric.SetDescription("Average disk writes for slow query")
        metric.SetUnit("1")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.AvgDiskWrites)
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create avg_elapsed_time_ms metric - CARDINALITY SAFE
    if result.AvgElapsedTimeMS != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.avg_elapsed_time_ms")
        metric.SetDescription("Average elapsed time in milliseconds for slow query")
        metric.SetUnit("ms")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.AvgElapsedTimeMS)
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create execution_count metric - CARDINALITY SAFE
    if result.ExecutionCount != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.execution_count")
        metric.SetDescription("Execution count for slow query")
        metric.SetUnit("1")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetIntValue(*result.ExecutionCount)
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create query_id metric - CARDINALITY SAFE (QueryID as attribute)
    if result.QueryID != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.query_id")
        metric.SetDescription("Query ID for slow query identification")
        metric.SetUnit("1")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetIntValue(1) // Dummy value since this is primarily for the query ID attribute
        
        attrs := createSafeAttributes()
        attrs.PutStr("query_id", result.QueryID.String())
        attrs.CopyTo(dataPoint.Attributes())
    }

    // Create plan_handle metric - CARDINALITY SAFE (QueryID + PlanHandle as attribute)
    if result.PlanHandle != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.plan_handle")
        metric.SetDescription("Plan handle for slow query execution plan")
        metric.SetUnit("1")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetIntValue(1) // Dummy value since this is primarily for the plan handle attribute
        
        // Include PlanHandle as additional attribute (controlled cardinality)
        attrs := createSafeAttributes()
        attrs.PutStr("plan_handle", result.PlanHandle.String())
        attrs.CopyTo(dataPoint.Attributes())
    }

    // Create query_text metric with cardinality control
    if result.QueryText != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.query_text")
        metric.SetDescription("Query text for slow query")
        metric.SetUnit("1")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetIntValue(1) // Dummy value since this is primarily for the string attribute
        
        attrs := createSafeAttributes()
        // Safely anonymize query text with size limits to control cardinality
        anonymizedText := helpers.SafeAnonymizeQueryText(result.QueryText)
        // Truncate to prevent attribute size issues
        if len(anonymizedText) > 1000 {
            anonymizedText = anonymizedText[:1000] + "...[truncated]"
        }
        attrs.PutStr("query_text", anonymizedText)
        attrs.CopyTo(dataPoint.Attributes())
    }

    // Create collection_timestamp metric - CARDINALITY SAFE (timestamp as Unix epoch seconds)
    if result.CollectionTimestamp != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.collection_timestamp")
        metric.SetDescription("Collection timestamp for slow query (Unix epoch seconds)")
        metric.SetUnit("s")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        
        // Parse the timestamp string and convert to Unix epoch seconds (cardinality-safe)
        if parsedTime, err := time.Parse(time.RFC3339, *result.CollectionTimestamp); err == nil {
            dataPoint.SetIntValue(parsedTime.Unix())
        } else if parsedTime, err := time.Parse("2006-01-02T15:04:05", *result.CollectionTimestamp); err == nil {
            dataPoint.SetIntValue(parsedTime.Unix())
        } else if parsedTime, err := time.Parse("2006-01-02 15:04:05", *result.CollectionTimestamp); err == nil {
            dataPoint.SetIntValue(parsedTime.Unix())
        } else {
            // Fallback: use current time if parsing fails
            dataPoint.SetIntValue(time.Now().Unix())
        }
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create last_execution_timestamp metric - CARDINALITY SAFE (timestamp as Unix epoch seconds)
    if result.LastExecutionTimestamp != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.slowquery.last_execution_timestamp")
        metric.SetDescription("Last execution timestamp for slow query (Unix epoch seconds)")
        metric.SetUnit("s")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        
        // Parse the timestamp string and convert to Unix epoch seconds (cardinality-safe)
        if parsedTime, err := time.Parse(time.RFC3339, *result.LastExecutionTimestamp); err == nil {
            dataPoint.SetIntValue(parsedTime.Unix())
        } else if parsedTime, err := time.Parse("2006-01-02T15:04:05", *result.LastExecutionTimestamp); err == nil {
            dataPoint.SetIntValue(parsedTime.Unix())
        } else if parsedTime, err := time.Parse("2006-01-02 15:04:05", *result.LastExecutionTimestamp); err == nil {
            dataPoint.SetIntValue(parsedTime.Unix())
        } else {
            // Fallback: use current time if parsing fails
            dataPoint.SetIntValue(time.Now().Unix())
        }
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Use dedicated logging function with cardinality-safe approach
    s.logger.Debug("Processed slow query metrics with cardinality safety", logAttributes()...)

    return nil
}

// processBlockingSessionMetrics processes blocking session metrics and creates separate OpenTelemetry metrics for BlockingSPID and BlockedSPID
func (s *QueryPerformanceScraper) processBlockingSessionMetrics(result models.BlockingSession, scopeMetrics pmetric.ScopeMetrics, index int) error {
    timestamp := pcommon.NewTimestampFromTime(time.Now())
    
    // Create BlockingSPID metric as wait time
    if result.WaitTimeInSeconds != nil && result.BlockingSPID != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.blocking_query.wait_time_seconds")
        metric.SetDescription("Wait time in seconds for blocking query")
        metric.SetUnit("s")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.WaitTimeInSeconds)
        
        // Set attributes including blocking SPID as an attribute
        attrs := dataPoint.Attributes()
        attrs.PutInt("BlockingSPID", int64(*result.BlockingSPID))
        if result.BlockingStatus != nil {
            attrs.PutStr("BlockingStatus", *result.BlockingStatus)
        }
        if result.WaitType != nil {
            attrs.PutStr("WaitType", *result.WaitType)
        }
        if result.DatabaseName != nil {
            attrs.PutStr("DatabaseName", *result.DatabaseName)
        }
        if result.CommandType != nil {
            attrs.PutStr("CommandType", *result.CommandType)
        }
        if result.BlockingQueryText != nil && *result.BlockingQueryText != "" {
            attrs.PutStr("BlockingQueryText", helpers.AnonymizeQueryText(*result.BlockingQueryText))
        }
    }

    // Create BlockedSPID metric as wait time
    if result.WaitTimeInSeconds != nil && result.BlockedSPID != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.blocked_query.wait_time_seconds")
        metric.SetDescription("Wait time in seconds for blocked query")
        metric.SetUnit("s")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.WaitTimeInSeconds)
        
        // Set attributes including blocked SPID as an attribute
        attrs := dataPoint.Attributes()
        attrs.PutInt("BlockedSPID", int64(*result.BlockedSPID))
        if result.BlockedStatus != nil {
            attrs.PutStr("BlockedStatus", *result.BlockedStatus)
        }
        if result.WaitType != nil {
            attrs.PutStr("WaitType", *result.WaitType)
        }
        if result.DatabaseName != nil {
            attrs.PutStr("DatabaseName", *result.DatabaseName)
        }
        if result.CommandType != nil {
            attrs.PutStr("CommandType", *result.CommandType)
        }
        if result.BlockedQueryText != nil {
            attrs.PutStr("BlockedQueryText", helpers.AnonymizeQueryText(*result.BlockedQueryText))
        }
        if result.BlockedQueryStartTime != nil {
            attrs.PutStr("BlockedQueryStartTime", *result.BlockedQueryStartTime)
        }
    }

    s.logger.Debug("Processed blocking session metrics as separate metrics",
        zap.Any("blocking_spid", result.BlockingSPID),
        zap.Any("blocked_spid", result.BlockedSPID),
        zap.Any("wait_type", result.WaitType),
        zap.Any("wait_time_seconds", result.WaitTimeInSeconds))

    return nil
}

// processWaitTimeAnalysisMetrics processes wait time analysis metrics and creates separate OpenTelemetry metrics for each measurement
func (s *QueryPerformanceScraper) processWaitTimeAnalysisMetrics(result models.WaitTimeAnalysis, scopeMetrics pmetric.ScopeMetrics, index int) error {
    timestamp := pcommon.NewTimestampFromTime(time.Now())
    
    // Helper function to create common attributes for all metrics
    createCommonAttributes := func() pcommon.Map {
        attrs := pcommon.NewMap()
        if result.DatabaseName != nil {
            attrs.PutStr("DatabaseName", *result.DatabaseName)
        }
        if result.QueryID != nil {
            attrs.PutStr("QueryID", result.QueryID.String())
        }
        if result.WaitCategory != nil {
            attrs.PutStr("WaitCategory", *result.WaitCategory)
        }
        if result.CollectionTimestamp != nil {
            attrs.PutStr("CollectionTimestamp", *result.CollectionTimestamp)
        }
        return attrs
    }

    // Create query_text metric
    if result.QueryText != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.wait_analysis.query_text")
        metric.SetDescription("Query text for wait time analysis")
        metric.SetUnit("{dimensionless}")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetIntValue(1) // Dummy numeric value required for OpenTelemetry metrics
        
        // Add query text as an attribute and also include it in common attributes
        attrs := createCommonAttributes()
        attrs.PutStr("query_text", helpers.AnonymizeQueryText(*result.QueryText))
        attrs.CopyTo(dataPoint.Attributes())
    }

    // Create total_wait_time_ms metric
    if result.TotalWaitTimeMs != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.wait_analysis.total_wait_time_ms")
        metric.SetDescription("Total wait time in milliseconds for wait analysis")
        metric.SetUnit("ms")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.TotalWaitTimeMs)
        createCommonAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create avg_wait_time_ms metric
    if result.AvgWaitTimeMs != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.wait_analysis.avg_wait_time_ms")
        metric.SetDescription("Average wait time in milliseconds for wait analysis")
        metric.SetUnit("ms")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.AvgWaitTimeMs)
        createCommonAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create wait_event_count metric
    if result.WaitEventCount != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.wait_analysis.wait_event_count")
        metric.SetDescription("Wait event count for wait analysis")
        metric.SetUnit("{count}")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetIntValue(*result.WaitEventCount)
        createCommonAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create last_execution_time metric
    if result.LastExecutionTime != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.wait_analysis.last_execution_time")
        metric.SetDescription("Last execution time for wait analysis")
        metric.SetUnit("{dimensionless}")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetIntValue(1) // Dummy value since this is primarily for the string attribute
        
        // Only use common attributes as per specification
        createCommonAttributes().CopyTo(dataPoint.Attributes())
    }

    s.logger.Debug("Processed wait time analysis metrics as separate metrics",
        zap.Any("query_id", result.QueryID),
        zap.Any("database_name", result.DatabaseName),
        zap.Any("wait_category", result.WaitCategory),
        zap.Any("total_wait_time_ms", result.TotalWaitTimeMs),
        zap.Any("avg_wait_time_ms", result.AvgWaitTimeMs),
        zap.Any("wait_event_count", result.WaitEventCount))

    return nil
}

// processQueryExecutionPlanMetrics processes query execution plan metrics with cardinality safety
func (s *QueryPerformanceScraper) processQueryExecutionPlanMetrics(result models.QueryExecutionPlan, scopeMetrics pmetric.ScopeMetrics, index int) error {
    timestamp := pcommon.NewTimestampFromTime(time.Now())
    
    // CARDINALITY-SAFE: Create limited common attributes
    // Only include QueryID as the primary identifier, avoid full SQL text and multiple hashes
    createSafeAttributes := func() pcommon.Map {
        attrs := pcommon.NewMap()
        if result.QueryID != nil {
            attrs.PutStr("QueryID", result.QueryID.String())
        }
        // NOTE: Not including PlanHandle, QueryPlanID, and SQLText as attributes to prevent cardinality explosion
        // These can be logged or stored separately for drill-down analysis
        return attrs
    }
    
    // Create detailed attributes for logging/debugging (not used in metrics)
    logAttributes := func() []zap.Field {
        var fields []zap.Field
        if result.QueryID != nil {
            fields = append(fields, zap.String("query_id", result.QueryID.String()))
        }
        if result.PlanHandle != nil {
            fields = append(fields, zap.String("plan_handle", result.PlanHandle.String()))
        }
        if result.QueryPlanID != nil {
            fields = append(fields, zap.String("query_plan_id", result.QueryPlanID.String()))
        }
        if result.SQLText != nil {
            // Anonymize and truncate SQL text for logging
            anonymizedSQL := helpers.AnonymizeQueryText(*result.SQLText)
            if len(anonymizedSQL) > 100 {
                anonymizedSQL = anonymizedSQL[:100] + "..."
            }
            fields = append(fields, zap.String("sql_text_preview", anonymizedSQL))
        }
        return fields
    }

    // Create TotalCPUMs metric - CARDINALITY SAFE
    if result.TotalCPUMs != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.individual_query.total_cpu_ms")
        metric.SetDescription("Total CPU time in milliseconds for individual query analysis")
        metric.SetUnit("ms")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.TotalCPUMs)
        
        // Only use safe attributes (QueryID only)
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create TotalElapsedMs metric - CARDINALITY SAFE  
    if result.TotalElapsedMs != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.individual_query.total_elapsed_ms")
        metric.SetDescription("Total elapsed time in milliseconds for individual query analysis")
        metric.SetUnit("ms")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetDoubleValue(*result.TotalElapsedMs)
        
        // Only use safe attributes (QueryID only)
        createSafeAttributes().CopyTo(dataPoint.Attributes())
    }

    // Create execution plan info metric with anonymized XML content
    if result.ExecutionPlanXML != nil {
        metric := scopeMetrics.Metrics().AppendEmpty()
        metric.SetName("sqlserver.query_execution_plan.execution_plan_xml")
        metric.SetDescription("Execution plan XML content for detailed query analysis")
        metric.SetUnit("1")
        
        gauge := metric.SetEmptyGauge()
        dataPoint := gauge.DataPoints().AppendEmpty()
        dataPoint.SetTimestamp(timestamp)
        dataPoint.SetStartTimestamp(s.startTime)
        dataPoint.SetIntValue(1) // 1 = plan available, 0 = no plan
        
        // Include XML content with StatementText anonymized
        attrs := createSafeAttributes()
        xmlContent := helpers.AnonymizeExecutionPlanXML(*result.ExecutionPlanXML)
        // Limit XML size to prevent attribute size issues
        if len(xmlContent) > 8192 {
            xmlContent = xmlContent[:8192] + "...[truncated]"
        }
        attrs.PutStr("execution_plan_xml", xmlContent)
        attrs.CopyTo(dataPoint.Attributes())
    }

    // Log detailed information for debugging/analysis (not in metrics)
    s.logger.Debug("Processed query execution plan metrics with cardinality safety",
        logAttributes()...)

    return nil
}

// getSlowQueryResults fetches slow query results to extract QueryIDs for execution plan analysis
func (s *QueryPerformanceScraper) getSlowQueryResults(ctx context.Context, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit int) ([]models.SlowQuery, error) {
    // Format the slow query with parameters
    formattedQuery := fmt.Sprintf(queries.SlowQuery, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit)

    s.logger.Debug("Executing slow query to extract QueryIDs for execution plan analysis",
        zap.String("query", queries.TruncateQuery(formattedQuery, 100)),
        zap.Int("interval_seconds", intervalSeconds),
        zap.Int("top_n", topN),
        zap.Int("elapsed_time_threshold", elapsedTimeThreshold))

    var results []models.SlowQuery
    if err := s.connection.Query(ctx, &results, formattedQuery); err != nil {
        return nil, fmt.Errorf("failed to execute slow query for QueryID extraction: %w", err)
    }

    s.logger.Debug("Successfully fetched slow queries for QueryID extraction", 
        zap.Int("result_count", len(results)))

    return results, nil
}

// extractQueryIDsFromSlowQueries extracts unique QueryIDs from slow query results
func (s *QueryPerformanceScraper) extractQueryIDsFromSlowQueries(slowQueries []models.SlowQuery) []string {
    queryIDMap := make(map[string]bool)
    var queryIDs []string

    for _, slowQuery := range slowQueries {
        if slowQuery.QueryID != nil && !slowQuery.QueryID.IsEmpty() {
            queryIDStr := slowQuery.QueryID.String()
            if !queryIDMap[queryIDStr] {
                queryIDMap[queryIDStr] = true
                queryIDs = append(queryIDs, queryIDStr)
            }
        }
    }

    s.logger.Debug("Extracted unique QueryIDs from slow queries",
        zap.Int("total_slow_queries", len(slowQueries)),
        zap.Int("unique_query_ids", len(queryIDs)))

    return queryIDs
}

// formatQueryIDsForSQL converts QueryID slice to comma-separated string for SQL IN clause
// Follows nri-mssql pattern for QueryID formatting
func (s *QueryPerformanceScraper) formatQueryIDsForSQL(queryIDs []string) string {
    if len(queryIDs) == 0 {
        return "0x0" // Return placeholder if no QueryIDs
    }

    // Join QueryIDs with commas for SQL STRING_SPLIT
    // QueryIDs are already in hex format (0x...), so we can use them directly
    queryIDsString := ""
    for i, queryID := range queryIDs {
        if i > 0 {
            queryIDsString += ","
        }
        queryIDsString += queryID
    }

    s.logger.Debug("Formatted QueryIDs for SQL query",
        zap.Int("query_id_count", len(queryIDs)),
        zap.String("formatted_query_ids", queries.TruncateQuery(queryIDsString, 100)))

    return queryIDsString
}

// extractPlanHandlesFromSlowQueries extracts unique PlanHandles from slow query results
func (s *QueryPerformanceScraper) extractPlanHandlesFromSlowQueries(slowQueries []models.SlowQuery) []string {
    planHandleMap := make(map[string]bool)
    var planHandles []string

    for _, slowQuery := range slowQueries {
        if slowQuery.PlanHandle != nil && !slowQuery.PlanHandle.IsEmpty() {
            planHandleStr := slowQuery.PlanHandle.String()
            if !planHandleMap[planHandleStr] {
                planHandleMap[planHandleStr] = true
                planHandles = append(planHandles, planHandleStr)
            }
        }
    }

    s.logger.Debug("Extracted unique PlanHandles from slow queries",
        zap.Int("total_slow_queries", len(slowQueries)),
        zap.Int("unique_plan_handles", len(planHandles)))

    return planHandles
}

// formatPlanHandlesForSQL converts PlanHandle slice to comma-separated string for SQL IN clause
func (s *QueryPerformanceScraper) formatPlanHandlesForSQL(planHandles []string) string {
    if len(planHandles) == 0 {
        return "0x0" // Return placeholder if no PlanHandles
    }

    // Join PlanHandles with commas for SQL STRING_SPLIT
    // PlanHandles are already in hex format (0x...), so we can use them directly
    planHandlesString := ""
    for i, planHandle := range planHandles {
        if i > 0 {
            planHandlesString += ","
        }
        planHandlesString += planHandle
    }

    s.logger.Debug("Formatted PlanHandles for SQL query",
        zap.Int("plan_handle_count", len(planHandles)),
        zap.String("formatted_plan_handles", queries.TruncateQuery(planHandlesString, 100)))

    return planHandlesString
}



