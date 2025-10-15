// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/scrapers"
)

// sqlServerScraper handles SQL Server metrics collection following nri-mssql patterns
type sqlServerScraper struct {
	connection              *SQLConnection
	config                  *Config
	logger                  *zap.Logger
	startTime               pcommon.Timestamp
	settings                receiver.Settings
	instanceScraper         *scrapers.InstanceScraper
	queryPerformanceScraper *scrapers.QueryPerformanceScraper
	//slowQueryScraper  *scrapers.SlowQueryScraper
	databaseScraper               *scrapers.DatabaseScraper
	userConnectionScraper         *scrapers.UserConnectionScraper
	failoverClusterScraper        *scrapers.FailoverClusterScraper
	databasePrincipalsScraper     *scrapers.DatabasePrincipalsScraper
	databaseRoleMembershipScraper *scrapers.DatabaseRoleMembershipScraper
	waitTimeScraper               *scrapers.WaitTimeScraper // Add this line
	engineEdition                 int                       // SQL Server engine edition (0=Unknown, 5=Azure DB, 8=Azure MI)
}

// newSqlServerScraper creates a new SQL Server scraper with structured approach
func newSqlServerScraper(settings receiver.Settings, cfg *Config) *sqlServerScraper {
	return &sqlServerScraper{
		config:   cfg,
		logger:   settings.Logger,
		settings: settings,
	}
}

// start initializes the scraper and establishes database connection
func (s *sqlServerScraper) start(ctx context.Context, _ component.Host) error {
	s.logger.Info("Starting New Relic SQL Server receiver")

	connection, err := NewSQLConnection(ctx, s.config, s.logger)
	if err != nil {
		s.logger.Error("Failed to connect to SQL Server", zap.Error(err))
		return err
	}
	s.connection = connection
	s.startTime = pcommon.NewTimestampFromTime(time.Now())

	if err := s.connection.Ping(ctx); err != nil {
		s.logger.Error("Failed to ping SQL Server", zap.Error(err))
		return err
	}

	// Get EngineEdition (following nri-mssql pattern)
	s.engineEdition = 0 // Default to 0 (Unknown)
	s.engineEdition, err = s.detectEngineEdition(ctx)
	if err != nil {
		s.logger.Debug("Failed to get engine edition, using default", zap.Error(err))
		s.engineEdition = queries.StandardSQLServerEngineEdition
	} else {
		s.logger.Info("Detected SQL Server engine edition",
			zap.Int("engine_edition", s.engineEdition),
			zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))
	}

	// Initialize instance scraper with engine edition for engine-specific queries
	// Create instance scraper for instance-level metrics
	s.instanceScraper = scrapers.NewInstanceScraper(s.connection, s.logger, s.engineEdition)

	// Create database scraper for database-level metrics
	s.databaseScraper = scrapers.NewDatabaseScraper(s.connection, s.logger, s.engineEdition)

	// Create failover cluster scraper for Always On Availability Group metrics
	s.failoverClusterScraper = scrapers.NewFailoverClusterScraper(s.connection, s.logger, s.engineEdition)

	// Create database principals scraper for database security metrics
	s.databasePrincipalsScraper = scrapers.NewDatabasePrincipalsScraper(s.connection, s.logger, s.engineEdition)

	// Create database role membership scraper for database role and membership metrics
	s.databaseRoleMembershipScraper = scrapers.NewDatabaseRoleMembershipScraper(s.logger, s.connection, s.engineEdition)

	// Initialize query performance scraper for blocking sessions and performance monitoring
	s.queryPerformanceScraper = scrapers.NewQueryPerformanceScraper(s.connection, s.logger, s.engineEdition)
	//s.slowQueryScraper = scrapers.NewSlowQueryScraper(s.logger, s.connection)

	// Initialize user connection scraper for user connection and authentication metrics
	s.userConnectionScraper = scrapers.NewUserConnectionScraper(s.connection, s.logger, s.engineEdition)

	// Initialize wait time scraper for wait time metrics
	s.waitTimeScraper = scrapers.NewWaitTimeScraper(s.connection, s.logger, s.engineEdition)

	s.logger.Info("Successfully connected to SQL Server",
		zap.String("hostname", s.config.Hostname),
		zap.String("port", s.config.Port),
		zap.Int("engine_edition", s.engineEdition),
		zap.String("engine_type", queries.GetEngineTypeName(s.engineEdition)))

	return nil
}

// shutdown closes the database connection
func (s *sqlServerScraper) shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down New Relic SQL Server receiver")
	if s.connection != nil {
		s.connection.Close()
	}
	return nil
}

// detectEngineEdition detects the SQL Server engine edition following nri-mssql pattern
func (s *sqlServerScraper) detectEngineEdition(ctx context.Context) (int, error) {
	queryFunc := func(query string) (int, error) {
		var results []struct {
			EngineEdition int `db:"EngineEdition"`
		}

		err := s.connection.Query(ctx, &results, query)
		if err != nil {
			return 0, err
		}

		if len(results) == 0 {
			s.logger.Debug("EngineEdition query returned empty output.")
			return 0, nil
		}

		s.logger.Debug("Detected EngineEdition", zap.Int("engine_edition", results[0].EngineEdition))
		return results[0].EngineEdition, nil
	}

	return queries.DetectEngineEdition(queryFunc)
}

// scrape collects SQL Server instance metrics using structured approach
func (s *sqlServerScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	s.logger.Debug("Starting SQL Server metrics collection",
		zap.String("hostname", s.config.Hostname),
		zap.String("port", s.config.Port))

	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()

	// Set basic resource attributes
	attrs := resourceMetrics.Resource().Attributes()
	attrs.PutStr("server.address", s.config.Hostname)
	attrs.PutStr("server.port", s.config.Port)
	attrs.PutStr("db.system", "mssql")
	attrs.PutStr("service.name", "sql-server-monitoring")

	// Add instance name if configured
	if s.config.Instance != "" {
		attrs.PutStr("db.instance", s.config.Instance)
	}

	// Collect and add comprehensive system/host information as resource attributes
	if err := s.addSystemInformationAsResourceAttributes(ctx, attrs); err != nil {
		s.logger.Warn("Failed to collect system information, continuing with basic attributes",
			zap.Error(err))
		// Continue with scraping - system info is supplementary
	}

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName("newrelicsqlserverreceiver")

	// Track scraping errors but continue with partial results
	var scrapeErrors []error

	// Check if connection is still valid before scraping
	if s.connection != nil {
		if err := s.connection.Ping(ctx); err != nil {
			s.logger.Error("Connection health check failed before scraping", zap.Error(err))
			scrapeErrors = append(scrapeErrors, fmt.Errorf("connection health check failed: %w", err))
			// Continue with scraping attempt - connection might recover
		}
	} else {
		s.logger.Error("No database connection available for scraping")
		return metrics, fmt.Errorf("no database connection available")
	}

	// Scrape database-level buffer pool metrics (bufferpool.sizePerDatabaseInBytes)
	if s.config.IsBufferMetricsEnabled() {
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabaseBufferMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database buffer metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database buffer metrics")
		}
	}

	// Scrape database-level IO metrics (io.stallInMilliseconds)
	if s.config.IsIOMetricsEnabled() {
		s.logger.Debug("Starting database IO metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabaseIOMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database IO metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database IO metrics")
		}
	} else {
		s.logger.Debug("Database IO metrics disabled in configuration")
	}

	// Scrape database-level log growth metrics (log.transactionGrowth)
	if s.config.IsLogGrowthMetricsEnabled() {
		s.logger.Debug("Starting database log growth metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabaseLogGrowthMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database log growth metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database log growth metrics")
		}
	} else {
		s.logger.Debug("Database log growth metrics disabled in configuration")
	}

	// Scrape database-level page file metrics (pageFileAvailable)
	if s.config.IsPageFileMetricsEnabled() {
		s.logger.Debug("Starting database page file metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabasePageFileMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database page file metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database page file metrics")
		}
	} else {
		s.logger.Debug("Database page file metrics disabled in configuration")
	}

	// Scrape database-level page file total metrics (pageFileTotal)
	if s.config.IsPageFileTotalMetricsEnabled() {
		s.logger.Debug("Starting database page file total metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabasePageFileTotalMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database page file total metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database page file total metrics")
		}
	} else {
		s.logger.Debug("Database page file total metrics disabled in configuration")
	}

	// Scrape instance-level memory metrics (memoryTotal, memoryAvailable, memoryUtilization)
	if s.config.IsMemoryMetricsEnabled() || s.config.IsMemoryTotalMetricsEnabled() || s.config.IsMemoryAvailableMetricsEnabled() || s.config.IsMemoryUtilizationMetricsEnabled() {
		s.logger.Debug("Starting database memory metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabaseMemoryMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database memory metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database memory metrics")
		}
	} else {
		s.logger.Debug("Database memory metrics disabled in configuration")
	}

	// Scrape database size metrics (total size and data size in MB)
	if s.config.IsDatabaseSizeMetricsEnabled() {
		s.logger.Debug("Starting database size metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabaseSizeMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database size metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database size metrics")
		}
	} else {
		s.logger.Debug("Database size metrics disabled in configuration")
	}

	// Scrape database disk metrics (max disk size for Azure SQL Database)
	if s.config.IsDiskMetricsInBytesEnabled() {
		s.logger.Debug("Starting database disk metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabaseDiskMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database disk metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database disk metrics")
		}
	} else {
		s.logger.Debug("Database disk metrics disabled in configuration")
	}

	// Scrape database transaction log metrics (flushes, bytes flushed, flush waits, active transactions)
	if s.config.IsDatabaseTransactionLogMetricsEnabled() {
		s.logger.Debug("Starting database transaction log metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabaseTransactionLogMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database transaction log metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database transaction log metrics")
		}
	} else {
		s.logger.Debug("Database transaction log metrics disabled in configuration")
	}

	// Scrape database log space usage metrics (used log space in MB)
	if s.config.IsDatabaseLogSpaceUsageMetricsEnabled() {
		s.logger.Debug("Starting database log space usage metrics scraping")
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.databaseScraper.ScrapeDatabaseLogSpaceUsageMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database log space usage metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database log space usage metrics")
		}
	} else {
		s.logger.Debug("Database log space usage metrics disabled in configuration")
	}

	// Scrape blocking session metrics if query monitoring is enabled
	if s.config.EnableQueryMonitoring {
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.queryPerformanceScraper.ScrapeBlockingSessionMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Warn("Failed to scrape blocking session metrics - continuing with other metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			// Don't add to scrapeErrors - just warn and continue
		} else {
			s.logger.Debug("Successfully scraped blocking session metrics")
		}
	}

	// Scrape slow query metrics if query monitoring is enabled
	if s.config.EnableQueryMonitoring {
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		// Use config values for slow query parameters
		intervalSeconds := s.config.QueryMonitoringFetchInterval
		topN := s.config.QueryMonitoringCountThreshold
		elapsedTimeThreshold := s.config.QueryMonitoringResponseTimeThreshold
		textTruncateLimit := 4094 // Default text truncate limit from nri-mssql

		if err := s.queryPerformanceScraper.ScrapeSlowQueryMetrics(scrapeCtx, scopeMetrics, intervalSeconds, topN, elapsedTimeThreshold, textTruncateLimit); err != nil {
			s.logger.Warn("Failed to scrape slow query metrics - continuing with other metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout),
				zap.Int("interval_seconds", intervalSeconds),
				zap.Int("top_n", topN),
				zap.Int("elapsed_time_threshold", elapsedTimeThreshold),
				zap.Int("text_truncate_limit", textTruncateLimit))
			// Don't add to scrapeErrors - just warn and continue
		} else {
			s.logger.Debug("Successfully scraped slow query metrics",
				zap.Int("interval_seconds", intervalSeconds),
				zap.Int("top_n", topN),
				zap.Int("elapsed_time_threshold", elapsedTimeThreshold),
				zap.Int("text_truncate_limit", textTruncateLimit))
		}
	}

	if s.config.EnableQueryMonitoring {
		scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		// Use config values for wait analysis parameters
		topN := s.config.QueryMonitoringCountThreshold
		textTruncateLimit := 4094 // Default text truncate limit from nri-mssql

		if err := s.queryPerformanceScraper.ScrapeWaitTimeAnalysisMetrics(scrapeCtx, scopeMetrics, topN, textTruncateLimit); err != nil {
			s.logger.Warn("Failed to scrape wait time analysis metrics - continuing with other metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout),
				zap.Int("top_n", topN),
				zap.Int("text_truncate_limit", textTruncateLimit))
			// Don't add to scrapeErrors - just warn and continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped wait time analysis metrics",
				zap.Int("top_n", topN),
				zap.Int("text_truncate_limit", textTruncateLimit))
		}
	}

	s.logger.Debug("Starting instance buffer pool hit percent metrics scraping")
	scrapeCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.instanceScraper.ScrapeInstanceMemoryMetrics(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape instance memory metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped instance memory metrics")
	}

	// Scrape instance-level process counts metrics
	s.logger.Debug("Starting instance process counts metrics scraping")
	scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.instanceScraper.ScrapeInstanceProcessCounts(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape instance process counts metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped instance process counts metrics")
	}

	// Scrape instance-level runnable tasks metrics
	s.logger.Debug("Starting instance runnable tasks metrics scraping")
	scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.instanceScraper.ScrapeInstanceRunnableTasks(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape instance runnable tasks metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped instance runnable tasks metrics")
	}

	// Scrape instance-level active connections metrics
	s.logger.Debug("Starting instance active connections metrics scraping")
	scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.instanceScraper.ScrapeInstanceActiveConnections(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape instance active connections metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped instance active connections metrics")
	}

	// Scrape instance-level buffer pool hit percent metrics
	s.logger.Debug("Starting instance buffer pool hit percent metrics scraping")
	scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.instanceScraper.ScrapeInstanceBufferPoolHitPercent(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape instance buffer pool hit percent metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped instance buffer pool hit percent metrics")
	}

	// Scrape instance-level disk metrics
	s.logger.Debug("Starting instance disk metrics scraping")
	scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.instanceScraper.ScrapeInstanceDiskMetrics(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape instance disk metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped instance disk metrics")
	}

	// Scrape instance-level buffer pool size metrics
	s.logger.Debug("Starting instance buffer pool size metrics scraping")
	scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.instanceScraper.ScrapeInstanceBufferPoolSize(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape instance buffer pool size metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped instance buffer pool size metrics")
	}

	// Scrape instance-level comprehensive statistics
	s.logger.Debug("Starting instance comprehensive statistics scraping")
	scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.instanceScraper.ScrapeInstanceComprehensiveStats(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape instance comprehensive statistics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped instance comprehensive statistics")
	}

	// Scrape user connection metrics with granular toggles
	s.logger.Debug("Checking user connection metrics configuration",
		zap.Bool("enable_user_connection_metrics", s.config.IsUserConnectionMetricsEnabled()),
		zap.Bool("enable_user_connection_status_metrics", s.config.IsUserConnectionStatusMetricsEnabled()),
		zap.Bool("enable_user_connection_summary_metrics", s.config.IsUserConnectionSummaryMetricsEnabled()),
		zap.Bool("enable_user_connection_utilization_metrics", s.config.IsUserConnectionUtilizationMetricsEnabled()),
		zap.Bool("enable_user_connection_client_metrics", s.config.IsUserConnectionClientMetricsEnabled()),
		zap.Bool("enable_user_connection_client_summary", s.config.IsUserConnectionClientSummaryEnabled()),
		zap.Bool("enable_user_connection_stats_metrics", s.config.IsUserConnectionStatsMetricsEnabled()),
		zap.Bool("enable_login_logout_metrics", s.config.IsLoginLogoutMetricsEnabled()))

	// Scrape user connection status metrics if enabled
	if s.config.IsUserConnectionStatusMetricsEnabled() {
		s.logger.Debug("Starting user connection status metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeUserConnectionStatusMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape user connection status metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped user connection status metrics")
		}
	} else {
		s.logger.Debug("User connection status metrics disabled in configuration")
	}

	// Scrape user connection summary metrics if enabled
	if s.config.IsUserConnectionSummaryMetricsEnabled() {
		s.logger.Debug("Starting user connection summary metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeUserConnectionSummaryMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape user connection summary metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped user connection summary metrics")
		}
	} else {
		s.logger.Debug("User connection summary metrics disabled in configuration")
	}

	// Scrape user connection utilization metrics if enabled
	if s.config.IsUserConnectionUtilizationMetricsEnabled() {
		s.logger.Debug("Starting user connection utilization metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeUserConnectionUtilizationMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape user connection utilization metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped user connection utilization metrics")
		}
	} else {
		s.logger.Debug("User connection utilization metrics disabled in configuration")
	}

	// Scrape user connection by client metrics if enabled
	if s.config.IsUserConnectionClientMetricsEnabled() {
		s.logger.Debug("Starting user connection by client metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeUserConnectionByClientMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape user connection by client metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped user connection by client metrics")
		}
	} else {
		s.logger.Debug("User connection client metrics disabled in configuration")
	}

	// Scrape user connection client summary metrics if enabled
	if s.config.IsUserConnectionClientSummaryEnabled() {
		s.logger.Debug("Starting user connection client summary metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeUserConnectionClientSummaryMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape user connection client summary metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped user connection client summary metrics")
		}
	} else {
		s.logger.Debug("User connection client summary metrics disabled in configuration")
	}

	// Scrape user connection stats metrics if enabled
	if s.config.IsUserConnectionStatsMetricsEnabled() {
		s.logger.Debug("Starting user connection stats metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeUserConnectionStatsMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape user connection stats metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped user connection stats metrics")
		}
	} else {
		s.logger.Debug("User connection stats metrics disabled in configuration")
	}

	// Scrape authentication metrics with granular toggles

	// Scrape login/logout rate metrics if enabled
	if s.config.IsLoginLogoutRateMetricsEnabled() {
		s.logger.Debug("Starting login/logout rate metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeLoginLogoutMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape login/logout rate metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped login/logout rate metrics")
		}
	} else {
		s.logger.Debug("Login/logout rate metrics disabled in configuration")
	}

	// Scrape login/logout summary metrics if enabled
	if s.config.IsLoginLogoutSummaryMetricsEnabled() {
		s.logger.Debug("Starting login/logout summary metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeLoginLogoutSummaryMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape login/logout summary metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped login/logout summary metrics")
		}
	} else {
		s.logger.Debug("Login/logout summary metrics disabled in configuration")
	}

	// Scrape failed login metrics if enabled
	if s.config.IsFailedLoginMetricsEnabled() {
		s.logger.Debug("Starting failed login metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeFailedLoginMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape failed login metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped failed login metrics")
		}
	} else {
		s.logger.Debug("Failed login metrics disabled in configuration")
	}

	// Scrape failed login summary metrics if enabled
	if s.config.IsFailedLoginSummaryMetricsEnabled() {
		s.logger.Debug("Starting failed login summary metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()

		if err := s.userConnectionScraper.ScrapeFailedLoginSummaryMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape failed login summary metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped failed login summary metrics")
		}
	} else {
		s.logger.Debug("Failed login summary metrics disabled in configuration")
	}

	// Scrape failover cluster metrics if enabled (using granular toggles)

	// Scrape failover cluster replica metrics if enabled
	if s.config.IsFailoverClusterReplicaMetricsEnabled() {
		s.logger.Debug("Starting failover cluster replica metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.failoverClusterScraper.ScrapeFailoverClusterMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape failover cluster replica metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped failover cluster replica metrics")
		}
	} else {
		s.logger.Debug("Failover cluster replica metrics disabled in configuration")
	}

	// Scrape failover cluster replica state metrics if enabled
	if s.config.IsFailoverClusterReplicaStateMetricsEnabled() {
		s.logger.Debug("Starting failover cluster replica state metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.failoverClusterScraper.ScrapeFailoverClusterReplicaStateMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape failover cluster replica state metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped failover cluster replica state metrics")
		}
	} else {
		s.logger.Debug("Failover cluster replica state metrics disabled in configuration")
	}

	// Scrape failover cluster node metrics if enabled
	if s.config.IsFailoverClusterNodeMetricsEnabled() {
		s.logger.Debug("Starting failover cluster node metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.failoverClusterScraper.ScrapeFailoverClusterNodeMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape failover cluster node metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped failover cluster node metrics")
		}
	} else {
		s.logger.Debug("Failover cluster node metrics disabled in configuration")
	}

	// Scrape availability group health metrics if enabled
	if s.config.IsFailoverClusterAvailabilityGroupHealthMetricsEnabled() {
		s.logger.Debug("Starting availability group health metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.failoverClusterScraper.ScrapeFailoverClusterAvailabilityGroupHealthMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape availability group health metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped availability group health metrics")
		}
	} else {
		s.logger.Debug("Availability group health metrics disabled in configuration")
	}

	// Scrape availability group configuration metrics if enabled
	if s.config.IsFailoverClusterAvailabilityGroupMetricsEnabled() {
		s.logger.Debug("Starting availability group configuration metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.failoverClusterScraper.ScrapeFailoverClusterAvailabilityGroupMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape availability group configuration metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped availability group configuration metrics")
		}
	} else {
		s.logger.Debug("Availability group configuration metrics disabled in configuration")
	}

	// Scrape failover cluster performance counter metrics if enabled
	if s.config.IsFailoverClusterPerformanceCounterMetricsEnabled() {
		s.logger.Debug("Starting failover cluster performance counter metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.failoverClusterScraper.ScrapeFailoverClusterPerformanceCounterMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape failover cluster performance counter metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped failover cluster performance counter metrics")
		}
	} else {
		s.logger.Debug("Failover cluster performance counter metrics disabled in configuration")
	}

	// Scrape cluster properties metrics if enabled
	if s.config.IsFailoverClusterClusterPropertiesMetricsEnabled() {
		s.logger.Debug("Starting cluster properties metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.failoverClusterScraper.ScrapeFailoverClusterClusterPropertiesMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape cluster properties metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped cluster properties metrics")
		}
	} else {
		s.logger.Debug("Cluster properties metrics disabled in configuration")
	}

	// Scrape database principals metrics if enabled (using granular toggles)

	// Scrape database principals details metrics if enabled
	if s.config.IsDatabasePrincipalsDetailsMetricsEnabled() {
		s.logger.Debug("Starting database principals details metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.databasePrincipalsScraper.ScrapeDatabasePrincipalsMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database principals details metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database principals details metrics")
		}
	} else {
		s.logger.Debug("Database principals details metrics disabled in configuration")
	}

	// Scrape database principals summary metrics if enabled
	if s.config.IsDatabasePrincipalsSummaryMetricsEnabled() {
		s.logger.Debug("Starting database principals summary metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.databasePrincipalsScraper.ScrapeDatabasePrincipalsSummaryMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database principals summary metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database principals summary metrics")
		}
	} else {
		s.logger.Debug("Database principals summary metrics disabled in configuration")
	}

	// Scrape database principals activity metrics if enabled
	if s.config.IsDatabasePrincipalsActivityMetricsEnabled() {
		s.logger.Debug("Starting database principals activity metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.databasePrincipalsScraper.ScrapeDatabasePrincipalActivityMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database principals activity metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database principals activity metrics")
		}
	} else {
		s.logger.Debug("Database principals activity metrics disabled in configuration")
	}

	// Scrape database role membership metrics if enabled (using granular toggles)

	// Scrape database role membership details metrics if enabled
	if s.config.IsDatabaseRoleMembershipDetailsMetricsEnabled() {
		s.logger.Debug("Starting database role membership details metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.databaseRoleMembershipScraper.ScrapeDatabaseRoleMembershipMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database role membership details metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database role membership details metrics")
		}
	} else {
		s.logger.Debug("Database role membership details metrics disabled in configuration")
	}

	// Scrape database role membership summary metrics if enabled
	if s.config.IsDatabaseRoleMembershipSummaryMetricsEnabled() {
		s.logger.Debug("Starting database role membership summary metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.databaseRoleMembershipScraper.ScrapeDatabaseRoleMembershipSummaryMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database role membership summary metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database role membership summary metrics")
		}
	} else {
		s.logger.Debug("Database role membership summary metrics disabled in configuration")
	}

	// Scrape database role hierarchy metrics if enabled
	if s.config.IsDatabaseRoleHierarchyMetricsEnabled() {
		s.logger.Debug("Starting database role hierarchy metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.databaseRoleMembershipScraper.ScrapeDatabaseRoleHierarchyMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database role hierarchy metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database role hierarchy metrics")
		}
	} else {
		s.logger.Debug("Database role hierarchy metrics disabled in configuration")
	}

	// Scrape database role activity metrics if enabled
	if s.config.IsDatabaseRoleActivityMetricsEnabled() {
		s.logger.Debug("Starting database role activity metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.databaseRoleMembershipScraper.ScrapeDatabaseRoleActivityMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database role activity metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database role activity metrics")
		}
	} else {
		s.logger.Debug("Database role activity metrics disabled in configuration")
	}

	// Scrape database role permission matrix metrics if enabled
	if s.config.IsDatabaseRolePermissionMatrixMetricsEnabled() {
		s.logger.Debug("Starting database role permission matrix metrics scraping")
		scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
		if err := s.databaseRoleMembershipScraper.ScrapeDatabaseRolePermissionMatrixMetrics(scrapeCtx, scopeMetrics); err != nil {
			s.logger.Error("Failed to scrape database role permission matrix metrics",
				zap.Error(err),
				zap.Duration("timeout", s.config.Timeout))
			scrapeErrors = append(scrapeErrors, err)
			// Don't return here - continue with other metrics
		} else {
			s.logger.Debug("Successfully scraped database role permission matrix metrics")
		}
	} else {
		s.logger.Debug("Database role permission matrix metrics disabled in configuration")
	}

	// Scrape wait time metrics
	s.logger.Debug("Starting wait time metrics scraping")
	scrapeCtx, cancel = context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	if err := s.waitTimeScraper.ScrapeWaitTimeMetrics(scrapeCtx, scopeMetrics); err != nil {
		s.logger.Error("Failed to scrape wait time metrics",
			zap.Error(err),
			zap.Duration("timeout", s.config.Timeout))
		scrapeErrors = append(scrapeErrors, err)
		// Don't return here - continue with other metrics
	} else {
		s.logger.Debug("Successfully scraped wait time metrics")
	}

	// Log summary of scraping results
	if len(scrapeErrors) > 0 {
		s.logger.Warn("Completed scraping with errors",
			zap.Int("error_count", len(scrapeErrors)),
			zap.Int("metrics_collected", scopeMetrics.Metrics().Len()))

		// Return the first error but with partial metrics
		return metrics, scrapeErrors[0]
	}

	s.logger.Debug("Successfully completed SQL Server metrics collection",
		zap.Int("metrics_collected", scopeMetrics.Metrics().Len()))

	return metrics, nil
}

// addSystemInformationAsResourceAttributes collects system/host information and adds it as resource attributes
// This ensures that all metrics sent by the scraper include comprehensive host context
func (s *sqlServerScraper) addSystemInformationAsResourceAttributes(ctx context.Context, attrs pcommon.Map) error {
	// Collect system information using the main scraper
	systemInfo, err := s.CollectSystemInformation(ctx)
	if err != nil {
		return fmt.Errorf("failed to collect system information: %w", err)
	}

	// Add SQL Server instance information
	if systemInfo.ServerName != nil && *systemInfo.ServerName != "" {
		attrs.PutStr("sql.instance_name", *systemInfo.ServerName)
	}
	if systemInfo.ComputerName != nil && *systemInfo.ComputerName != "" {
		attrs.PutStr("host.name", *systemInfo.ComputerName)
	}
	if systemInfo.ServiceName != nil && *systemInfo.ServiceName != "" {
		attrs.PutStr("sql.service_name", *systemInfo.ServiceName)
	}

	// Add SQL Server edition and version information
	if systemInfo.Edition != nil && *systemInfo.Edition != "" {
		attrs.PutStr("sql.edition", *systemInfo.Edition)
	}
	if systemInfo.EngineEdition != nil {
		attrs.PutInt("sql.engine_edition", int64(*systemInfo.EngineEdition))
	}
	if systemInfo.ProductVersion != nil && *systemInfo.ProductVersion != "" {
		attrs.PutStr("sql.version", *systemInfo.ProductVersion)
	}
	if systemInfo.VersionDesc != nil && *systemInfo.VersionDesc != "" {
		attrs.PutStr("sql.version_description", *systemInfo.VersionDesc)
	}

	// Add hardware information
	if systemInfo.CPUCount != nil {
		attrs.PutInt("host.cpu.count", int64(*systemInfo.CPUCount))
	}
	if systemInfo.ServerMemoryKB != nil {
		attrs.PutInt("host.memory.total_kb", *systemInfo.ServerMemoryKB)
	}
	if systemInfo.AvailableMemoryKB != nil {
		attrs.PutInt("host.memory.available_kb", *systemInfo.AvailableMemoryKB)
	}

	// Add instance configuration
	if systemInfo.IsClustered != nil {
		attrs.PutBool("sql.is_clustered", *systemInfo.IsClustered)
	}
	if systemInfo.IsHadrEnabled != nil {
		attrs.PutBool("sql.is_hadr_enabled", *systemInfo.IsHadrEnabled)
	}
	if systemInfo.Uptime != nil {
		attrs.PutInt("sql.uptime_minutes", int64(*systemInfo.Uptime))
	}
	if systemInfo.ComputerUptime != nil {
		attrs.PutInt("host.uptime_seconds", int64(*systemInfo.ComputerUptime))
	}

	// Add network configuration
	if systemInfo.Port != nil && *systemInfo.Port != "" {
		attrs.PutStr("sql.port", *systemInfo.Port)
	}
	if systemInfo.PortType != nil && *systemInfo.PortType != "" {
		attrs.PutStr("sql.port_type", *systemInfo.PortType)
	}
	if systemInfo.ForceEncryption != nil {
		attrs.PutBool("sql.force_encryption", *systemInfo.ForceEncryption != 0)
	}

	s.logger.Debug("Successfully added system information as resource attributes",
		zap.String("host_name", getStringValueFromMap(systemInfo.ComputerName)),
		zap.String("sql_instance", getStringValueFromMap(systemInfo.ServerName)),
		zap.String("sql_edition", getStringValueFromMap(systemInfo.Edition)),
		zap.Int("cpu_count", getIntValueFromMap(systemInfo.CPUCount)),
		zap.Bool("is_clustered", getBoolValueFromMap(systemInfo.IsClustered)))

	return nil
}

// Helper functions to safely extract values from pointers for logging
func getStringValueFromMap(ptr *string) string {
	if ptr != nil {
		return *ptr
	}
	return ""
}

func getIntValueFromMap(ptr *int) int {
	if ptr != nil {
		return *ptr
	}
	return 0
}

func getInt64ValueFromMap(ptr *int64) int64 {
	if ptr != nil {
		return *ptr
	}
	return 0
}

func getBoolValueFromMap(ptr *bool) bool {
	if ptr != nil {
		return *ptr
	}
	return false
}

// CollectSystemInformation retrieves comprehensive system and host information
// This information should be included as resource attributes with all metrics
func (s *sqlServerScraper) CollectSystemInformation(ctx context.Context) (*models.SystemInformation, error) {
	s.logger.Debug("Collecting SQL Server system and host information")

	var results []models.SystemInformation
	if err := s.connection.Query(ctx, &results, queries.SystemInformationQuery); err != nil {
		s.logger.Error("Failed to execute system information query",
			zap.Error(err),
			zap.String("query", queries.TruncateQuery(queries.SystemInformationQuery, 100)),
			zap.Int("engine_edition", s.engineEdition))
		return nil, fmt.Errorf("failed to execute system information query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from system information query - SQL Server may not be ready")
		return nil, fmt.Errorf("no results returned from system information query")
	}

	if len(results) > 1 {
		s.logger.Warn("Multiple results returned from system information query",
			zap.Int("result_count", len(results)))
	}

	result := results[0]

	// Log collected system information for debugging
	s.logger.Info("Successfully collected system information",
		zap.String("server_name", getStringValueFromMap(result.ServerName)),
		zap.String("computer_name", getStringValueFromMap(result.ComputerName)),
		zap.String("edition", getStringValueFromMap(result.Edition)),
		zap.Int("engine_edition", getIntValueFromMap(result.EngineEdition)),
		zap.String("product_version", getStringValueFromMap(result.ProductVersion)),
		zap.Int("cpu_count", getIntValueFromMap(result.CPUCount)),
		zap.Int64("server_memory_kb", getInt64ValueFromMap(result.ServerMemoryKB)),
		zap.Bool("is_clustered", getBoolValueFromMap(result.IsClustered)),
		zap.Bool("is_hadr_enabled", getBoolValueFromMap(result.IsHadrEnabled)))

	return &result, nil
}
