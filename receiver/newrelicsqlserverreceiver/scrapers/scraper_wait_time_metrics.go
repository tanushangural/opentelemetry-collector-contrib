// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// WaitTimeScraper handles SQL Server wait time metrics collection
type WaitTimeScraper struct {
	connection    SQLConnectionInterface
	logger        *zap.Logger
	startTime     pcommon.Timestamp
	engineEdition int
}

// NewWaitTimeScraper creates a new wait time scraper instance
func NewWaitTimeScraper(connection SQLConnectionInterface, logger *zap.Logger, engineEdition int) *WaitTimeScraper {
	return &WaitTimeScraper{
		connection:    connection,
		logger:        logger,
		startTime:     pcommon.NewTimestampFromTime(time.Now()),
		engineEdition: engineEdition,
	}
}

// getQueryForMetric retrieves the appropriate query for a metric based on engine edition with Default fallback
func (s *WaitTimeScraper) getQueryForMetric(metricName string) (string, bool) {
	query, found := queries.GetQueryForMetric(queries.WaitTimeQueries, metricName, s.engineEdition)
	return query, found
}

// ScrapeWaitTimeMetrics collects wait time statistics from SQL Server
func (s *WaitTimeScraper) ScrapeWaitTimeMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics) error {
	query, found := s.getQueryForMetric("sqlserver.wait_stats.wait_time_metrics")
	if !found {
		return fmt.Errorf("no wait time metrics query available for engine edition %d", s.engineEdition)
	}

	var results []models.WaitTimeMetricsModel
	if err := s.connection.Query(ctx, &results, query); err != nil {
		s.logger.Error("Failed to execute wait time metrics query", zap.Error(err))
		return fmt.Errorf("failed to execute wait time metrics query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from wait time metrics query")
		return fmt.Errorf("no results returned from wait time metrics query")
	}

	waitTimeMetric := scopeMetrics.Metrics().AppendEmpty()
	waitTimeMetric.SetName("sqlserver.wait_stats.wait_time_ms")
	waitTimeMetric.SetDescription("Total wait time in milliseconds")
	waitTimeMetric.SetUnit("ms")
	waitTimeSum := waitTimeMetric.SetEmptySum()
	waitTimeSum.SetIsMonotonic(true)
	waitTimeSum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	waitingTasksMetric := scopeMetrics.Metrics().AppendEmpty()
	waitingTasksMetric.SetName("sqlserver.wait_stats.waiting_tasks_count")
	waitingTasksMetric.SetDescription("Number of tasks currently waiting")
	waitingTasksMetric.SetUnit("1")
	waitingTasksSum := waitingTasksMetric.SetEmptySum()
	waitingTasksSum.SetIsMonotonic(true)
	waitingTasksSum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, result := range results {
		if result.WaitType == nil {
			continue
		}

		if result.WaitTimeMs != nil {
			dp := waitTimeSum.DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetStartTimestamp(s.startTime)
			dp.SetDoubleValue(float64(*result.WaitTimeMs))

			attrs := dp.Attributes()
			attrs.PutStr("wait_type", *result.WaitType)
			attrs.PutStr("metric.type", "rate")
		}

		if result.WaitingTasksCount != nil {
			dp := waitingTasksSum.DataPoints().AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetStartTimestamp(s.startTime)
			dp.SetDoubleValue(float64(*result.WaitingTasksCount))

			attrs := dp.Attributes()
			attrs.PutStr("wait_type", *result.WaitType)
			attrs.PutStr("metric.type", "rate")
		}
	}

	return nil
}
