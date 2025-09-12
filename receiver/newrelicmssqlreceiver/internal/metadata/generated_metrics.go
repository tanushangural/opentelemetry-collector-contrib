// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
)

// MetricsBuilderConfig is the configuration for the metrics builder.
type MetricsBuilderConfig struct {
	Metrics MetricsConfig `mapstructure:",squash"`
}

// MetricsConfig provides config for newrelicmssql metrics.
type MetricsConfig struct {
	MssqlInstanceBufferCacheHitRatio      MetricConfig `mapstructure:"mssql.instance.buffer_cache_hit_ratio"`
	MssqlInstancePageLifeExpectancy       MetricConfig `mapstructure:"mssql.instance.page_life_expectancy"`
	MssqlInstanceBatchRequestsPerSec      MetricConfig `mapstructure:"mssql.instance.batch_requests_per_sec"`
	MssqlInstanceSQLCompilationsPerSec    MetricConfig `mapstructure:"mssql.instance.sql_compilations_per_sec"`
	MssqlInstanceSQLRecompilationsPerSec  MetricConfig `mapstructure:"mssql.instance.sql_recompilations_per_sec"`
	MssqlInstanceProcessesBlocked         MetricConfig `mapstructure:"mssql.instance.processes_blocked"`
	MssqlInstanceUserConnections          MetricConfig `mapstructure:"mssql.instance.user_connections"`
	MssqlInstanceMemoryUtilizationPercentage MetricConfig `mapstructure:"mssql.instance.memory_utilization_percentage"`
	MssqlInstanceCPUTimeRaw               MetricConfig `mapstructure:"mssql.instance.cpu_time_raw"`
	MssqlDatabaseDataFileSize             MetricConfig `mapstructure:"mssql.database.data_file_size"`
	MssqlDatabaseLogFileSize              MetricConfig `mapstructure:"mssql.database.log_file_size"`
	MssqlDatabaseLogFileUsedSize          MetricConfig `mapstructure:"mssql.database.log_file_used_size"`
	MssqlDatabaseLogFileUsedPercentage    MetricConfig `mapstructure:"mssql.database.log_file_used_percentage"`
	MssqlDatabaseTransactionsPerSec       MetricConfig `mapstructure:"mssql.database.transactions_per_sec"`
	MssqlDatabaseActiveConnections        MetricConfig `mapstructure:"mssql.database.active_connections"`
	MssqlDatabaseState                    MetricConfig `mapstructure:"mssql.database.state"`
	MssqlWaitStatsWaitingTasksCount       MetricConfig `mapstructure:"mssql.wait_stats.waiting_tasks_count"`
	MssqlWaitStatsWaitTimeMs              MetricConfig `mapstructure:"mssql.wait_stats.wait_time_ms"`
	MssqlWaitStatsMaxWaitTimeMs           MetricConfig `mapstructure:"mssql.wait_stats.max_wait_time_ms"`
	MssqlDeadlocksTotal                   MetricConfig `mapstructure:"mssql.deadlocks.total"`
	MssqlLockStatsTimeoutCount            MetricConfig `mapstructure:"mssql.lock_stats.timeout_count"`
	MssqlLockStatsWaitCount               MetricConfig `mapstructure:"mssql.lock_stats.wait_count"`
	MssqlLockStatsWaitTimeMs              MetricConfig `mapstructure:"mssql.lock_stats.wait_time_ms"`
	MssqlIndexStatsPageSplitsPerSec       MetricConfig `mapstructure:"mssql.index_stats.page_splits_per_sec"`
	MssqlIndexStatsPageLookupsPerSec      MetricConfig `mapstructure:"mssql.index_stats.page_lookups_per_sec"`
	MssqlIndexStatsPageReadsPerSec        MetricConfig `mapstructure:"mssql.index_stats.page_reads_per_sec"`
	MssqlIndexStatsPageWritesPerSec       MetricConfig `mapstructure:"mssql.index_stats.page_writes_per_sec"`
}

// MetricConfig provides configuration for a metric.
type MetricConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// DefaultMetricsBuilderConfig returns the default configuration for the metrics builder.
func DefaultMetricsBuilderConfig() MetricsBuilderConfig {
	return MetricsBuilderConfig{
		Metrics: MetricsConfig{
			MssqlInstanceBufferCacheHitRatio:      MetricConfig{Enabled: true},
			MssqlInstancePageLifeExpectancy:       MetricConfig{Enabled: true},
			MssqlInstanceBatchRequestsPerSec:      MetricConfig{Enabled: true},
			MssqlInstanceSQLCompilationsPerSec:    MetricConfig{Enabled: true},
			MssqlInstanceSQLRecompilationsPerSec:  MetricConfig{Enabled: true},
			MssqlInstanceProcessesBlocked:         MetricConfig{Enabled: true},
			MssqlInstanceUserConnections:          MetricConfig{Enabled: true},
			MssqlInstanceMemoryUtilizationPercentage: MetricConfig{Enabled: true},
			MssqlInstanceCPUTimeRaw:               MetricConfig{Enabled: true},
			MssqlDatabaseDataFileSize:             MetricConfig{Enabled: true},
			MssqlDatabaseLogFileSize:              MetricConfig{Enabled: true},
			MssqlDatabaseLogFileUsedSize:          MetricConfig{Enabled: true},
			MssqlDatabaseLogFileUsedPercentage:    MetricConfig{Enabled: true},
			MssqlDatabaseTransactionsPerSec:       MetricConfig{Enabled: true},
			MssqlDatabaseActiveConnections:        MetricConfig{Enabled: true},
			MssqlDatabaseState:                    MetricConfig{Enabled: true},
			MssqlWaitStatsWaitingTasksCount:       MetricConfig{Enabled: true},
			MssqlWaitStatsWaitTimeMs:              MetricConfig{Enabled: true},
			MssqlWaitStatsMaxWaitTimeMs:           MetricConfig{Enabled: true},
			MssqlDeadlocksTotal:                   MetricConfig{Enabled: true},
			MssqlLockStatsTimeoutCount:            MetricConfig{Enabled: true},
			MssqlLockStatsWaitCount:               MetricConfig{Enabled: true},
			MssqlLockStatsWaitTimeMs:              MetricConfig{Enabled: true},
			MssqlIndexStatsPageSplitsPerSec:       MetricConfig{Enabled: true},
			MssqlIndexStatsPageLookupsPerSec:      MetricConfig{Enabled: true},
			MssqlIndexStatsPageReadsPerSec:        MetricConfig{Enabled: true},
			MssqlIndexStatsPageWritesPerSec:       MetricConfig{Enabled: true},
		},
	}
}

// MetricsBuilder provides an interface for building metrics.
type MetricsBuilder struct {
	config   MetricsBuilderConfig
	settings receiver.Settings
	resource pcommon.Resource
	metrics  pmetric.Metrics
}

// NewMetricsBuilder creates a new metrics builder.
func NewMetricsBuilder(config MetricsBuilderConfig, settings receiver.Settings) *MetricsBuilder {
	return &MetricsBuilder{
		config:   config,
		settings: settings,
		metrics:  pmetric.NewMetrics(),
	}
}

// Emit returns the built metrics and resets the metrics builder to its initial state.
func (mb *MetricsBuilder) Emit() pmetric.Metrics {
	m := mb.metrics
	mb.metrics = pmetric.NewMetrics()
	return m
}

// Helper function to get or create a metric
func (mb *MetricsBuilder) getOrCreateMetric(name string, description string, unit string) pmetric.Metric {
	rm := mb.metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	return sm.Metrics().AppendEmpty()
}

// Instance metrics recording methods
func (mb *MetricsBuilder) RecordMssqlInstanceBufferCacheHitRatioDataPoint(ts pcommon.Timestamp, value float64, instanceName string) {
	if !mb.config.Metrics.MssqlInstanceBufferCacheHitRatio.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.instance.buffer_cache_hit_ratio", "Buffer cache hit ratio for the MSSQL instance", "%")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlInstancePageLifeExpectancyDataPoint(ts pcommon.Timestamp, value int64, instanceName string) {
	if !mb.config.Metrics.MssqlInstancePageLifeExpectancy.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.instance.page_life_expectancy", "Page life expectancy in seconds", "s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlInstanceBatchRequestsPerSecDataPoint(ts pcommon.Timestamp, value float64, instanceName string) {
	if !mb.config.Metrics.MssqlInstanceBatchRequestsPerSec.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.instance.batch_requests_per_sec", "Number of batch requests per second", "{requests}/s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlInstanceSQLCompilationsPerSecDataPoint(ts pcommon.Timestamp, value float64, instanceName string) {
	if !mb.config.Metrics.MssqlInstanceSQLCompilationsPerSec.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.instance.sql_compilations_per_sec", "Number of SQL compilations per second", "{compilations}/s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlInstanceSQLRecompilationsPerSecDataPoint(ts pcommon.Timestamp, value float64, instanceName string) {
	if !mb.config.Metrics.MssqlInstanceSQLRecompilationsPerSec.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.instance.sql_recompilations_per_sec", "Number of SQL recompilations per second", "{recompilations}/s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlInstanceProcessesBlockedDataPoint(ts pcommon.Timestamp, value int64, instanceName string) {
	if !mb.config.Metrics.MssqlInstanceProcessesBlocked.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.instance.processes_blocked", "Number of blocked processes", "{processes}")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlInstanceUserConnectionsDataPoint(ts pcommon.Timestamp, value int64, instanceName string) {
	if !mb.config.Metrics.MssqlInstanceUserConnections.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.instance.user_connections", "Number of user connections", "{connections}")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("instance_name", instanceName)
}

// Database metrics recording methods
func (mb *MetricsBuilder) RecordMssqlDatabaseDataFileSizeDataPoint(ts pcommon.Timestamp, value int64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlDatabaseDataFileSize.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.database.data_file_size", "Size of database data files", "By")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlDatabaseLogFileSizeDataPoint(ts pcommon.Timestamp, value int64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlDatabaseLogFileSize.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.database.log_file_size", "Size of database log files", "By")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlDatabaseLogFileUsedSizeDataPoint(ts pcommon.Timestamp, value int64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlDatabaseLogFileUsedSize.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.database.log_file_used_size", "Used size of database log files", "By")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlDatabaseLogFileUsedPercentageDataPoint(ts pcommon.Timestamp, value float64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlDatabaseLogFileUsedPercentage.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.database.log_file_used_percentage", "Used percentage of database log files", "%")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlDatabaseTransactionsPerSecDataPoint(ts pcommon.Timestamp, value float64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlDatabaseTransactionsPerSec.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.database.transactions_per_sec", "Number of transactions per second for the database", "{transactions}/s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlDatabaseActiveConnectionsDataPoint(ts pcommon.Timestamp, value int64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlDatabaseActiveConnections.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.database.active_connections", "Number of active connections to the database", "{connections}")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

// Wait stats recording methods
func (mb *MetricsBuilder) RecordMssqlWaitStatsWaitingTasksCountDataPoint(ts pcommon.Timestamp, value int64, waitType, category, instanceName string) {
	if !mb.config.Metrics.MssqlWaitStatsWaitingTasksCount.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.wait_stats.waiting_tasks_count", "Number of waiting tasks", "{tasks}")
	dp := m.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("wait_type", waitType)
	dp.Attributes().PutStr("category", category)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlWaitStatsWaitTimeMsDataPoint(ts pcommon.Timestamp, value int64, waitType, category, instanceName string) {
	if !mb.config.Metrics.MssqlWaitStatsWaitTimeMs.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.wait_stats.wait_time_ms", "Total wait time in milliseconds", "ms")
	dp := m.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("wait_type", waitType)
	dp.Attributes().PutStr("category", category)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlWaitStatsMaxWaitTimeMsDataPoint(ts pcommon.Timestamp, value int64, waitType, category, instanceName string) {
	if !mb.config.Metrics.MssqlWaitStatsMaxWaitTimeMs.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.wait_stats.max_wait_time_ms", "Maximum wait time in milliseconds", "ms")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("wait_type", waitType)
	dp.Attributes().PutStr("category", category)
	dp.Attributes().PutStr("instance_name", instanceName)
}

// Lock stats recording methods
func (mb *MetricsBuilder) RecordMssqlLockStatsTimeoutCountDataPoint(ts pcommon.Timestamp, value int64, lockType, lockResource, instanceName string) {
	if !mb.config.Metrics.MssqlLockStatsTimeoutCount.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.lock_stats.timeout_count", "Number of lock timeouts", "{timeouts}")
	dp := m.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("lock_type", lockType)
	dp.Attributes().PutStr("lock_resource", lockResource)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlLockStatsWaitCountDataPoint(ts pcommon.Timestamp, value int64, lockType, lockResource, instanceName string) {
	if !mb.config.Metrics.MssqlLockStatsWaitCount.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.lock_stats.wait_count", "Number of lock waits", "{waits}")
	dp := m.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("lock_type", lockType)
	dp.Attributes().PutStr("lock_resource", lockResource)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlLockStatsWaitTimeMsDataPoint(ts pcommon.Timestamp, value int64, lockType, lockResource, instanceName string) {
	if !mb.config.Metrics.MssqlLockStatsWaitTimeMs.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.lock_stats.wait_time_ms", "Total lock wait time in milliseconds", "ms")
	dp := m.SetEmptySum().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetIntValue(value)
	dp.Attributes().PutStr("lock_type", lockType)
	dp.Attributes().PutStr("lock_resource", lockResource)
	dp.Attributes().PutStr("instance_name", instanceName)
}

// Index stats recording methods
func (mb *MetricsBuilder) RecordMssqlIndexStatsPageSplitsPerSecDataPoint(ts pcommon.Timestamp, value float64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlIndexStatsPageSplitsPerSec.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.index_stats.page_splits_per_sec", "Number of page splits per second", "{splits}/s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlIndexStatsPageLookupsPerSecDataPoint(ts pcommon.Timestamp, value float64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlIndexStatsPageLookupsPerSec.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.index_stats.page_lookups_per_sec", "Number of page lookups per second", "{lookups}/s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlIndexStatsPageReadsPerSecDataPoint(ts pcommon.Timestamp, value float64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlIndexStatsPageReadsPerSec.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.index_stats.page_reads_per_sec", "Number of page reads per second", "{reads}/s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}

func (mb *MetricsBuilder) RecordMssqlIndexStatsPageWritesPerSecDataPoint(ts pcommon.Timestamp, value float64, databaseName, instanceName string) {
	if !mb.config.Metrics.MssqlIndexStatsPageWritesPerSec.Enabled {
		return
	}
	m := mb.getOrCreateMetric("mssql.index_stats.page_writes_per_sec", "Number of page writes per second", "{writes}/s")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(ts)
	dp.SetDoubleValue(value)
	dp.Attributes().PutStr("database_name", databaseName)
	dp.Attributes().PutStr("instance_name", instanceName)
}
