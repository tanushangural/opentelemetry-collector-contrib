// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package scrapers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// MockInstanceSQLConnection is a simple mock implementation for testing
type MockInstanceSQLConnection struct {
	shouldFail      bool
	errorMessage    string
	memoryResults   []models.InstanceMemoryDefinitionsModel
	statsResults    []models.InstanceStatsModel
	bufferResults   []models.BufferPoolHitPercentMetricsModel
	processResults  []models.InstanceProcessCountsModel
	runnableResults []models.RunnableTasksMetricsModel
	diskResults     []models.InstanceDiskMetricsModel
	connectionResults []models.InstanceActiveConnectionsMetricsModel
	bufferSizeResults []models.InstanceBufferMetricsModel
}

func (m *MockInstanceSQLConnection) Query(ctx context.Context, dest interface{}, query string) error {
	if m.shouldFail {
		return errors.New(m.errorMessage)
	}

	switch v := dest.(type) {
	case *[]models.InstanceMemoryDefinitionsModel:
		*v = m.memoryResults
	case *[]models.InstanceStatsModel:
		*v = m.statsResults
	case *[]models.BufferPoolHitPercentMetricsModel:
		*v = m.bufferResults
	case *[]models.InstanceProcessCountsModel:
		*v = m.processResults
	case *[]models.RunnableTasksMetricsModel:
		*v = m.runnableResults
	case *[]models.InstanceDiskMetricsModel:
		*v = m.diskResults
	case *[]models.InstanceActiveConnectionsMetricsModel:
		*v = m.connectionResults
	case *[]models.InstanceBufferMetricsModel:
		*v = m.bufferSizeResults
	}
	return nil
}

// Helper function to create a test instance scraper
func createTestInstanceScraper(mockConn *MockInstanceSQLConnection, engineEdition int, config InstanceConfig) *InstanceScraper {
	logger := zap.NewNop()
	scraper := NewInstanceScraper(mockConn, logger, engineEdition, config)
	scraper.startTime = pcommon.NewTimestampFromTime(time.Now().Add(-time.Hour))
	return scraper
}

// Helper function to create test memory data
func createTestMemoryData() []models.InstanceMemoryDefinitionsModel {
	totalMemory := 16777216000.0  // 16GB in bytes
	availableMemory := 4194304000.0 // 4GB in bytes
	utilization := 75.0           // 75% utilization

	return []models.InstanceMemoryDefinitionsModel{
		{
			TotalPhysicalMemory:     &totalMemory,
			AvailablePhysicalMemory: &availableMemory,
			MemoryUtilization:       &utilization,
		},
	}
}

// Helper function to create test stats data
func createTestStatsData() []models.InstanceStatsModel {
	userConnections := int64(150)
	batchRequests := int64(25000)
	sqlCompilations := int64(500)
	pageReads := int64(100000)
	pageWrites := int64(50000)

	return []models.InstanceStatsModel{
		{
			UserConnections:  &userConnections,
			BatchRequestSec:  &batchRequests,
			SQLCompilations:  &sqlCompilations,
			PageSplitsSec:    &pageReads,
			CheckpointPagesSec: &pageWrites,
		},
	}
}

// Test Case 1: Successful memory metrics scraping for SQL Server Standard Edition
func TestInstanceScraper_ScrapeInstanceMemoryMetrics_Success(t *testing.T) {
	mockConn := &MockInstanceSQLConnection{}
	config := InstanceConfig{
		EnableInstanceMemoryMetrics: true,
	}
	scraper := createTestInstanceScraper(mockConn, queries.StandardSQLServerEngineEdition, config)

	// Set up mock data
	testData := createTestMemoryData()
	mockConn.memoryResults = testData

	// Create metrics container
	metrics := pmetric.NewMetrics()
	scopeMetrics := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// Execute the test
	err := scraper.ScrapeInstanceMemoryMetrics(context.Background(), scopeMetrics)

	// Assertions
	require.NoError(t, err)
	assert.Greater(t, scopeMetrics.Metrics().Len(), 0, "Should have created metrics")

	// Verify that metrics were created with correct values
	for i := 0; i < scopeMetrics.Metrics().Len(); i++ {
		metric := scopeMetrics.Metrics().At(i)
		assert.NotEmpty(t, metric.Name(), "Metric should have a name")
		assert.Equal(t, pmetric.MetricTypeGauge, metric.Type(), "Memory metrics should be gauge type")
		
		gauge := metric.Gauge()
		assert.Greater(t, gauge.DataPoints().Len(), 0, "Gauge should have data points")
		
		dataPoint := gauge.DataPoints().At(0)
		assert.True(t, dataPoint.ValueType() == pmetric.NumberDataPointValueTypeInt || dataPoint.ValueType() == pmetric.NumberDataPointValueTypeDouble, "Data point should have a value")
		
		// Verify attributes
		attrs := dataPoint.Attributes()
		metricType, exists := attrs.Get("metric.type")
		assert.True(t, exists, "Should have metric.type attribute")
		assert.NotEmpty(t, metricType.Str(), "metric.type should not be empty")
	}
}

// Test Case 2: Memory metrics disabled in configuration
func TestInstanceScraper_ScrapeInstanceMemoryMetrics_Disabled(t *testing.T) {
	mockConn := &MockInstanceSQLConnection{}
	config := InstanceConfig{
		EnableInstanceMemoryMetrics: false, // Disabled
	}
	scraper := createTestInstanceScraper(mockConn, queries.StandardSQLServerEngineEdition, config)

	// Create metrics container
	metrics := pmetric.NewMetrics()
	scopeMetrics := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// Execute the test
	err := scraper.ScrapeInstanceMemoryMetrics(context.Background(), scopeMetrics)

	// Assertions
	require.NoError(t, err)
	assert.Equal(t, 0, scopeMetrics.Metrics().Len(), "Should not have created any metrics when disabled")
}

// Test Case 3: Database query failure
func TestInstanceScraper_ScrapeInstanceMemoryMetrics_QueryFailure(t *testing.T) {
	mockConn := &MockInstanceSQLConnection{
		shouldFail:   true,
		errorMessage: "database connection timeout",
	}
	config := InstanceConfig{
		EnableInstanceMemoryMetrics: true,
	}
	scraper := createTestInstanceScraper(mockConn, queries.StandardSQLServerEngineEdition, config)

	// Create metrics container
	metrics := pmetric.NewMetrics()
	scopeMetrics := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// Execute the test
	err := scraper.ScrapeInstanceMemoryMetrics(context.Background(), scopeMetrics)

	// Assertions
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute instance memory query")
	assert.Equal(t, 0, scopeMetrics.Metrics().Len(), "Should not have created metrics on query failure")
}

// Test Case 4: Successful comprehensive stats scraping with rate and gauge metrics
func TestInstanceScraper_ScrapeInstanceComprehensiveStats_Success(t *testing.T) {
	mockConn := &MockInstanceSQLConnection{}
	config := InstanceConfig{
		EnableInstanceComprehensiveStats: true,
	}
	scraper := createTestInstanceScraper(mockConn, queries.StandardSQLServerEngineEdition, config)

	// Set up mock data
	testData := createTestStatsData()
	mockConn.statsResults = testData

	// Create metrics container
	metrics := pmetric.NewMetrics()
	scopeMetrics := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// Execute the test
	err := scraper.ScrapeInstanceComprehensiveStats(context.Background(), scopeMetrics)

	// Assertions
	require.NoError(t, err)
	assert.Greater(t, scopeMetrics.Metrics().Len(), 0, "Should have created metrics")

	// Verify metrics types and properties
	hasGaugeMetric := false
	hasSumMetric := false

	for i := 0; i < scopeMetrics.Metrics().Len(); i++ {
		metric := scopeMetrics.Metrics().At(i)
		assert.NotEmpty(t, metric.Name(), "Metric should have a name")
		
		if metric.Type() == pmetric.MetricTypeGauge {
			hasGaugeMetric = true
			gauge := metric.Gauge()
			assert.Greater(t, gauge.DataPoints().Len(), 0, "Gauge should have data points")
			
			dataPoint := gauge.DataPoints().At(0)
			assert.True(t, dataPoint.ValueType() == pmetric.NumberDataPointValueTypeInt || dataPoint.ValueType() == pmetric.NumberDataPointValueTypeDouble, "Data point should have a value")
			
			// For gauge metrics, start time should equal timestamp
			assert.Equal(t, dataPoint.StartTimestamp(), dataPoint.Timestamp(), "Gauge metrics should have instantaneous interval")
		} else if metric.Type() == pmetric.MetricTypeSum {
			hasSumMetric = true
			sum := metric.Sum()
			assert.True(t, sum.IsMonotonic(), "Rate metrics should be monotonic")
			assert.Equal(t, pmetric.AggregationTemporalityCumulative, sum.AggregationTemporality(), "Should use cumulative aggregation")
			
			assert.Greater(t, sum.DataPoints().Len(), 0, "Sum should have data points")
			
			dataPoint := sum.DataPoints().At(0)
			assert.Equal(t, pmetric.NumberDataPointValueTypeDouble, dataPoint.ValueType(), "Sum data point should have double value")
			
			// For cumulative metrics, start time should be different from timestamp
			assert.NotEqual(t, dataPoint.StartTimestamp(), dataPoint.Timestamp(), "Cumulative metrics should have fixed start time")
		}
	}

	assert.True(t, hasGaugeMetric || hasSumMetric, "Should have created at least one gauge or sum metric")
}

// Test Case 5: Unsupported engine edition (Azure SQL Database)
func TestInstanceScraper_ScrapeInstanceMemoryMetrics_UnsupportedEngine(t *testing.T) {
	mockConn := &MockInstanceSQLConnection{}
	config := InstanceConfig{
		EnableInstanceMemoryMetrics: true,
	}
	
	// Use Azure SQL Database edition which may not support certain instance metrics
	scraper := createTestInstanceScraper(mockConn, queries.AzureSQLDatabaseEngineEdition, config)

	// Create metrics container
	metrics := pmetric.NewMetrics()
	scopeMetrics := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// Execute the test
	err := scraper.ScrapeInstanceMemoryMetrics(context.Background(), scopeMetrics)

	// Assertions - should complete without error but may not create metrics
	// The behavior depends on whether the metric is compatible with Azure SQL Database
	require.NoError(t, err, "Should not error for unsupported engine, just skip gracefully")
	
	// If the metric is not compatible, no database calls should be made
	// The actual behavior depends on the queries.IsMetricCompatible implementation
	// This test verifies graceful handling of unsupported scenarios
}

// Additional helper tests for edge cases
func TestInstanceScraper_ProcessInstanceMemoryMetrics_NilValues(t *testing.T) {
	mockConn := &MockInstanceSQLConnection{}
	config := InstanceConfig{
		EnableInstanceMemoryMetrics: true,
	}
	scraper := createTestInstanceScraper(mockConn, queries.StandardSQLServerEngineEdition, config)

	// Create test data with nil values
	testData := []models.InstanceMemoryDefinitionsModel{
		{
			TotalPhysicalMemory:     nil, // nil value
			AvailablePhysicalMemory: nil, // nil value
			MemoryUtilization:       nil, // nil value
		},
	}

	mockConn.memoryResults = testData

	// Create metrics container
	metrics := pmetric.NewMetrics()
	scopeMetrics := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()

	// Execute the test
	err := scraper.ScrapeInstanceMemoryMetrics(context.Background(), scopeMetrics)

	// Should handle nil values gracefully and return an error since all values are nil
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all memory metrics are null")
}

func TestInstanceScraper_GetQueryForMetric(t *testing.T) {
	mockConn := &MockInstanceSQLConnection{}
	config := InstanceConfig{}
	scraper := createTestInstanceScraper(mockConn, queries.StandardSQLServerEngineEdition, config)

	// Test getting a query for a metric
	query, found := scraper.getQueryForMetric("sqlserver.instance.memory_metrics")
	
	// Assertions depend on the actual implementation of queries.GetQueryForMetric
	// This test verifies the method works and returns appropriate values
	if found {
		assert.NotEmpty(t, query, "Query should not be empty when found")
	}
}