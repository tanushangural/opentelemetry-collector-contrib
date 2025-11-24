// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// LockScraper handles SQL Server lock metrics collection
type LockScraper struct {
	connection    SQLConnectionInterface
	logger        *zap.Logger
	startTime     pcommon.Timestamp
	engineEdition int
}

// NewLockScraper creates a new lock scraper
func NewLockScraper(conn SQLConnectionInterface, logger *zap.Logger, engineEdition int) *LockScraper {
	return &LockScraper{
		connection:    conn,
		logger:        logger,
		startTime:     pcommon.NewTimestampFromTime(time.Now()),
		engineEdition: engineEdition,
	}
}

// getQueryForMetric retrieves the appropriate query for a metric based on engine edition with Default fallback
func (s *LockScraper) getQueryForMetric(metricName string) (string, bool) {
	query, found := queries.GetQueryForMetric(queries.LockQueries, metricName, s.engineEdition)
	if found {
		s.logger.Debug("Using query for metric",
			zap.String("metric_name", metricName),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
	}
	return query, found
}

// ScrapeLockResourceMetrics collects lock resource metrics using engine-specific queries
func (s *LockScraper) ScrapeLockResourceMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics) error {
	s.logger.Debug("Scraping SQL Server lock resource metrics")

	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.LockQueries, "sqlserver.lock.resource", s.engineEdition) {
		s.logger.Debug("Lock resource metrics not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.lock.resource")
	if !found {
		return fmt.Errorf("no lock resource query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing lock resource query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.LockResourceSummary
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute lock resource query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute lock resource query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Debug("No lock resource results returned - no active locks")
		return nil
	}

	s.logger.Debug("Processing lock resource metrics results",
		zap.Int("result_count", len(results)))

	// Process each database's lock resource metrics
	for _, result := range results {
		if result.DatabaseName == nil {
			s.logger.Warn("Database name is null in lock resource results")
			continue
		}

		if err := s.processLockResourceMetrics(result, scopeMetrics); err != nil {
			s.logger.Error("Failed to process lock resource metrics",
				zap.Error(err),
				zap.String("database_name", *result.DatabaseName))
			continue
		}

		s.logger.Debug("Successfully processed lock resource metrics",
			zap.String("database_name", *result.DatabaseName),
			zap.Int64("total_active_locks", getInt64Value(result.TotalActiveLocks)))
	}

	s.logger.Debug("Successfully scraped lock resource metrics",
		zap.Int("database_count", len(results)))

	return nil
}

// ScrapeLockModeMetrics collects lock mode metrics using engine-specific queries
func (s *LockScraper) ScrapeLockModeMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics) error {
	s.logger.Debug("Scraping SQL Server lock mode metrics")

	// Check if this metric is compatible with the current engine edition
	if !queries.IsMetricCompatible(queries.LockQueries, "sqlserver.lock.mode", s.engineEdition) {
		s.logger.Debug("Lock mode metrics not supported for this engine edition",
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)),
			zap.Int("engine_edition", s.engineEdition))
		return nil // Return nil to indicate successful skip, not an error
	}

	// Get the appropriate query for this engine edition
	query, found := s.getQueryForMetric("sqlserver.lock.mode")
	if !found {
		return fmt.Errorf("no lock mode query available for engine edition %d", s.engineEdition)
	}

	s.logger.Debug("Executing lock mode query",
		zap.String("query", queries.TruncateQuery(query, 100)),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	var results []models.LockModeSummary
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute lock mode query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(query, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return fmt.Errorf("failed to execute lock mode query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Debug("No lock mode results returned - no active locks")
		return nil
	}

	s.logger.Debug("Processing lock mode metrics results",
		zap.Int("result_count", len(results)))

	// Process each database's lock mode metrics
	for _, result := range results {
		if result.DatabaseName == nil {
			s.logger.Warn("Database name is null in lock mode results")
			continue
		}

		if err := s.processLockModeMetrics(result, scopeMetrics); err != nil {
			s.logger.Error("Failed to process lock mode metrics",
				zap.Error(err),
				zap.String("database_name", *result.DatabaseName))
			continue
		}

		s.logger.Debug("Successfully processed lock mode metrics",
			zap.String("database_name", *result.DatabaseName),
			zap.Int64("total_active_locks", getInt64Value(result.TotalActiveLocks)))
	}

	s.logger.Debug("Successfully scraped lock mode metrics",
		zap.Int("database_count", len(results)))

	return nil
}

// processLockResourceMetrics processes lock resource metrics and creates OpenTelemetry metrics
func (s *LockScraper) processLockResourceMetrics(result models.LockResourceSummary, scopeMetrics pmetric.ScopeMetrics) error {
	// Use reflection to process the struct fields with metric tags
	resultValue := reflect.ValueOf(result)
	resultType := reflect.TypeOf(result)

	for i := 0; i < resultValue.NumField(); i++ {
		field := resultValue.Field(i)
		fieldType := resultType.Field(i)

		// Skip nil values and non-metric fields
		if field.Kind() == reflect.Ptr && field.IsNil() {
			continue
		}

		// Get metric metadata from struct tags
		metricName := fieldType.Tag.Get("metric_name")
		sourceType := fieldType.Tag.Get("source_type")

		if metricName == "" || sourceType == "attribute" {
			continue
		}

		// Create the metric
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName(metricName)
		metric.SetUnit("1")
		metric.SetDescription(fmt.Sprintf("SQL Server lock resource metric: %s", metricName))

		// Create gauge metric
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dataPoint.SetStartTimestamp(s.startTime)

		// Add database name as attribute
		if result.DatabaseName != nil {
			dataPoint.Attributes().PutStr("database_name", *result.DatabaseName)
		}

		// Handle pointer fields and set the value
		fieldValue := field
		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				dataPoint.SetIntValue(0)
				continue
			}
			fieldValue = field.Elem()
		}

		// Set the value based on field type
		switch fieldValue.Kind() {
		case reflect.Int64:
			dataPoint.SetIntValue(fieldValue.Int())
		default:
			s.logger.Warn("Unsupported field type for lock resource metric",
				zap.String("metric_name", metricName),
				zap.String("field_type", fieldValue.Kind().String()))
			continue
		}
	}

	return nil
}

// processLockModeMetrics processes lock mode metrics and creates OpenTelemetry metrics
func (s *LockScraper) processLockModeMetrics(result models.LockModeSummary, scopeMetrics pmetric.ScopeMetrics) error {
	// Use reflection to process the struct fields with metric tags
	resultValue := reflect.ValueOf(result)
	resultType := reflect.TypeOf(result)

	for i := 0; i < resultValue.NumField(); i++ {
		field := resultValue.Field(i)
		fieldType := resultType.Field(i)

		// Skip nil values and non-metric fields
		if field.Kind() == reflect.Ptr && field.IsNil() {
			continue
		}

		// Get metric metadata from struct tags
		metricName := fieldType.Tag.Get("metric_name")
		sourceType := fieldType.Tag.Get("source_type")

		if metricName == "" || sourceType == "attribute" {
			continue
		}

		// Create the metric
		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName(metricName)
		metric.SetUnit("1")
		metric.SetDescription(fmt.Sprintf("SQL Server lock mode metric: %s", metricName))

		// Create gauge metric
		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dataPoint.SetStartTimestamp(s.startTime)

		// Add database name as attribute
		if result.DatabaseName != nil {
			dataPoint.Attributes().PutStr("database_name", *result.DatabaseName)
		}

		// Handle pointer fields and set the value
		fieldValue := field
		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				dataPoint.SetIntValue(0)
				continue
			}
			fieldValue = field.Elem()
		}

		// Set the value based on field type
		switch fieldValue.Kind() {
		case reflect.Int64:
			dataPoint.SetIntValue(fieldValue.Int())
		default:
			s.logger.Warn("Unsupported field type for lock mode metric",
				zap.String("metric_name", metricName),
				zap.String("field_type", fieldValue.Kind().String()))
			continue
		}
	}

	return nil
}

// getInt64Value safely extracts int64 value from pointer, returns 0 if nil
func getInt64Value(ptr *int64) int64 {
	if ptr == nil {
		return 0
	}
	return *ptr
}
