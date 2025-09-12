// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmssqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmssqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb" // Register the MSSQL driver
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmssqlreceiver/internal/metadata"
)

type mssqlScraper struct {
	logger   *zap.Logger
	cfg      *Config
	mb       *metadata.MetricsBuilder
	db       *sql.DB
}

func newMSSQLScraper(params receiver.Settings, cfg *Config) *mssqlScraper {
	return &mssqlScraper{
		logger: params.Logger,
		cfg:    cfg,
		mb:     metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, params),
	}
}

func (m *mssqlScraper) start(ctx context.Context, _ receiver.Host) error {
	connectionString := m.buildConnectionString()
	
	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	ctx, cancel := context.WithTimeout(ctx, time.Duration(m.cfg.Timeout)*time.Second)
	defer cancel()
	
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	m.db = db
	m.logger.Info("Successfully connected to MSSQL database")
	return nil
}

func (m *mssqlScraper) shutdown(_ context.Context) error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *mssqlScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	if m.db == nil {
		return pmetric.NewMetrics(), fmt.Errorf("database connection not established")
	}

	// Set a timeout for the entire scraping operation
	ctx, cancel := context.WithTimeout(ctx, time.Duration(m.cfg.Timeout)*time.Second)
	defer cancel()

	now := pcommon.NewTimestampFromTime(time.Now())

	// Collect instance metrics
	if err := m.collectInstanceMetrics(ctx, now); err != nil {
		m.logger.Error("Failed to collect instance metrics", zap.Error(err))
	}

	// Collect database metrics
	if err := m.collectDatabaseMetrics(ctx, now); err != nil {
		m.logger.Error("Failed to collect database metrics", zap.Error(err))
	}

	// Collect wait stats
	if err := m.collectWaitStats(ctx, now); err != nil {
		m.logger.Error("Failed to collect wait stats", zap.Error(err))
	}

	// Collect lock stats
	if err := m.collectLockStats(ctx, now); err != nil {
		m.logger.Error("Failed to collect lock stats", zap.Error(err))
	}

	// Collect index stats
	if err := m.collectIndexStats(ctx, now); err != nil {
		m.logger.Error("Failed to collect index stats", zap.Error(err))
	}

	return m.mb.Emit(), nil
}

func (m *mssqlScraper) buildConnectionString() string {
	var parts []string

	if m.cfg.ClientID != "" && m.cfg.TenantID != "" && string(m.cfg.ClientSecret) != "" {
		// Azure AD Service Principal authentication
		parts = append(parts, fmt.Sprintf("server=%s", m.cfg.Hostname))
		parts = append(parts, fmt.Sprintf("port=%d", m.cfg.Port))
		parts = append(parts, fmt.Sprintf("fedauth=ActiveDirectoryServicePrincipal"))
		parts = append(parts, fmt.Sprintf("user id=%s", m.cfg.ClientID))
		parts = append(parts, fmt.Sprintf("password=%s", string(m.cfg.ClientSecret)))
		parts = append(parts, fmt.Sprintf("TenantId=%s", m.cfg.TenantID))
	} else {
		// SQL Server authentication
		parts = append(parts, fmt.Sprintf("server=%s", m.cfg.Hostname))
		parts = append(parts, fmt.Sprintf("user id=%s", m.cfg.Username))
		parts = append(parts, fmt.Sprintf("password=%s", string(m.cfg.Password)))
		parts = append(parts, fmt.Sprintf("port=%d", m.cfg.Port))
	}

	if m.cfg.Instance != "" {
		parts = append(parts, fmt.Sprintf("instance=%s", m.cfg.Instance))
	}

	if m.cfg.EnableSSL {
		parts = append(parts, "encrypt=true")
		if m.cfg.TrustServerCertificate {
			parts = append(parts, "TrustServerCertificate=true")
		} else if m.cfg.CertificateLocation != "" {
			parts = append(parts, fmt.Sprintf("certificate=%s", m.cfg.CertificateLocation))
		}
	} else {
		parts = append(parts, "encrypt=false")
	}

	if m.cfg.Timeout > 0 {
		parts = append(parts, fmt.Sprintf("connection timeout=%d", m.cfg.Timeout))
	}

	if m.cfg.ExtraConnectionURLArgs != "" {
		parts = append(parts, m.cfg.ExtraConnectionURLArgs)
	}

	return strings.Join(parts, ";")
}

func (m *mssqlScraper) collectInstanceMetrics(ctx context.Context, ts pcommon.Timestamp) error {
	// Get instance name
	instanceName, err := m.getInstanceName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get instance name: %w", err)
	}

	// Buffer cache hit ratio
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlInstanceBufferCacheHitRatio.Enabled {
		if value, err := m.getBufferCacheHitRatio(ctx); err == nil {
			m.mb.RecordMssqlInstanceBufferCacheHitRatioDataPoint(ts, value, instanceName)
		}
	}

	// Page life expectancy
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlInstancePageLifeExpectancy.Enabled {
		if value, err := m.getPageLifeExpectancy(ctx); err == nil {
			m.mb.RecordMssqlInstancePageLifeExpectancyDataPoint(ts, value, instanceName)
		}
	}

	// Batch requests per second
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlInstanceBatchRequestsPerSec.Enabled {
		if value, err := m.getBatchRequestsPerSec(ctx); err == nil {
			m.mb.RecordMssqlInstanceBatchRequestsPerSecDataPoint(ts, value, instanceName)
		}
	}

	// SQL compilations per second
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlInstanceSQLCompilationsPerSec.Enabled {
		if value, err := m.getSQLCompilationsPerSec(ctx); err == nil {
			m.mb.RecordMssqlInstanceSQLCompilationsPerSecDataPoint(ts, value, instanceName)
		}
	}

	// SQL recompilations per second
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlInstanceSQLRecompilationsPerSec.Enabled {
		if value, err := m.getSQLRecompilationsPerSec(ctx); err == nil {
			m.mb.RecordMssqlInstanceSQLRecompilationsPerSecDataPoint(ts, value, instanceName)
		}
	}

	// Processes blocked
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlInstanceProcessesBlocked.Enabled {
		if value, err := m.getProcessesBlocked(ctx); err == nil {
			m.mb.RecordMssqlInstanceProcessesBlockedDataPoint(ts, value, instanceName)
		}
	}

	// User connections
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlInstanceUserConnections.Enabled {
		if value, err := m.getUserConnections(ctx); err == nil {
			m.mb.RecordMssqlInstanceUserConnectionsDataPoint(ts, value, instanceName)
		}
	}

	return nil
}

func (m *mssqlScraper) collectDatabaseMetrics(ctx context.Context, ts pcommon.Timestamp) error {
	databases, err := m.getDatabaseList(ctx)
	if err != nil {
		return fmt.Errorf("failed to get database list: %w", err)
	}

	instanceName, err := m.getInstanceName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get instance name: %w", err)
	}

	for _, dbName := range databases {
		// Skip system databases if not configured to collect them
		if m.isSystemDatabase(dbName) {
			continue
		}

		// Database file sizes
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlDatabaseDataFileSize.Enabled {
			if value, err := m.getDatabaseDataFileSize(ctx, dbName); err == nil {
				m.mb.RecordMssqlDatabaseDataFileSizeDataPoint(ts, value, dbName, instanceName)
			}
		}

		if m.cfg.MetricsBuilderConfig.Metrics.MssqlDatabaseLogFileSize.Enabled {
			if value, err := m.getDatabaseLogFileSize(ctx, dbName); err == nil {
				m.mb.RecordMssqlDatabaseLogFileSizeDataPoint(ts, value, dbName, instanceName)
			}
		}

		if m.cfg.MetricsBuilderConfig.Metrics.MssqlDatabaseLogFileUsedSize.Enabled {
			if value, err := m.getDatabaseLogFileUsedSize(ctx, dbName); err == nil {
				m.mb.RecordMssqlDatabaseLogFileUsedSizeDataPoint(ts, value, dbName, instanceName)
			}
		}

		if m.cfg.MetricsBuilderConfig.Metrics.MssqlDatabaseLogFileUsedPercentage.Enabled {
			if value, err := m.getDatabaseLogFileUsedPercentage(ctx, dbName); err == nil {
				m.mb.RecordMssqlDatabaseLogFileUsedPercentageDataPoint(ts, value, dbName, instanceName)
			}
		}

		// Database transactions per second
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlDatabaseTransactionsPerSec.Enabled {
			if value, err := m.getDatabaseTransactionsPerSec(ctx, dbName); err == nil {
				m.mb.RecordMssqlDatabaseTransactionsPerSecDataPoint(ts, value, dbName, instanceName)
			}
		}

		// Active connections
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlDatabaseActiveConnections.Enabled {
			if value, err := m.getDatabaseActiveConnections(ctx, dbName); err == nil {
				m.mb.RecordMssqlDatabaseActiveConnectionsDataPoint(ts, value, dbName, instanceName)
			}
		}
	}

	return nil
}

func (m *mssqlScraper) collectWaitStats(ctx context.Context, ts pcommon.Timestamp) error {
	instanceName, err := m.getInstanceName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get instance name: %w", err)
	}

	waitStats, err := m.getWaitStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get wait stats: %w", err)
	}

	for _, stat := range waitStats {
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlWaitStatsWaitingTasksCount.Enabled {
			m.mb.RecordMssqlWaitStatsWaitingTasksCountDataPoint(ts, stat.WaitingTasksCount, stat.WaitType, stat.Category, instanceName)
		}

		if m.cfg.MetricsBuilderConfig.Metrics.MssqlWaitStatsWaitTimeMs.Enabled {
			m.mb.RecordMssqlWaitStatsWaitTimeMsDataPoint(ts, stat.WaitTimeMs, stat.WaitType, stat.Category, instanceName)
		}

		if m.cfg.MetricsBuilderConfig.Metrics.MssqlWaitStatsMaxWaitTimeMs.Enabled {
			m.mb.RecordMssqlWaitStatsMaxWaitTimeMsDataPoint(ts, stat.MaxWaitTimeMs, stat.WaitType, stat.Category, instanceName)
		}
	}

	return nil
}

func (m *mssqlScraper) collectLockStats(ctx context.Context, ts pcommon.Timestamp) error {
	instanceName, err := m.getInstanceName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get instance name: %w", err)
	}

	lockStats, err := m.getLockStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get lock stats: %w", err)
	}

	for _, stat := range lockStats {
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlLockStatsTimeoutCount.Enabled {
			m.mb.RecordMssqlLockStatsTimeoutCountDataPoint(ts, stat.TimeoutCount, stat.LockType, stat.LockResource, instanceName)
		}

		if m.cfg.MetricsBuilderConfig.Metrics.MssqlLockStatsWaitCount.Enabled {
			m.mb.RecordMssqlLockStatsWaitCountDataPoint(ts, stat.WaitCount, stat.LockType, stat.LockResource, instanceName)
		}

		if m.cfg.MetricsBuilderConfig.Metrics.MssqlLockStatsWaitTimeMs.Enabled {
			m.mb.RecordMssqlLockStatsWaitTimeMsDataPoint(ts, stat.WaitTimeMs, stat.LockType, stat.LockResource, instanceName)
		}
	}

	return nil
}

func (m *mssqlScraper) collectIndexStats(ctx context.Context, ts pcommon.Timestamp) error {
	databases, err := m.getDatabaseList(ctx)
	if err != nil {
		return fmt.Errorf("failed to get database list: %w", err)
	}

	instanceName, err := m.getInstanceName(ctx)
	if err != nil {
		return fmt.Errorf("failed to get instance name: %w", err)
	}

	for _, dbName := range databases {
		if m.isSystemDatabase(dbName) {
			continue
		}

		// Page splits per second
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlIndexStatsPageSplitsPerSec.Enabled {
			if value, err := m.getIndexPageSplitsPerSec(ctx, dbName); err == nil {
				m.mb.RecordMssqlIndexStatsPageSplitsPerSecDataPoint(ts, value, dbName, instanceName)
			}
		}

		// Page lookups per second
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlIndexStatsPageLookupsPerSec.Enabled {
			if value, err := m.getIndexPageLookupsPerSec(ctx, dbName); err == nil {
				m.mb.RecordMssqlIndexStatsPageLookupsPerSecDataPoint(ts, value, dbName, instanceName)
			}
		}

		// Page reads per second
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlIndexStatsPageReadsPerSec.Enabled {
			if value, err := m.getIndexPageReadsPerSec(ctx, dbName); err == nil {
				m.mb.RecordMssqlIndexStatsPageReadsPerSecDataPoint(ts, value, dbName, instanceName)
			}
		}

		// Page writes per second
		if m.cfg.MetricsBuilderConfig.Metrics.MssqlIndexStatsPageWritesPerSec.Enabled {
			if value, err := m.getIndexPageWritesPerSec(ctx, dbName); err == nil {
				m.mb.RecordMssqlIndexStatsPageWritesPerSecDataPoint(ts, value, dbName, instanceName)
			}
		}
	}

	return nil
}

func (m *mssqlScraper) isSystemDatabase(dbName string) bool {
	systemDbs := []string{"master", "msdb", "model", "tempdb"}
	for _, sysDb := range systemDbs {
		if strings.EqualFold(dbName, sysDb) {
			return true
		}
	}
	return false
}
