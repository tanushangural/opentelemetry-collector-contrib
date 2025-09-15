// Enhanced scraper.go with per-query timeouts for Azure SQL Server
// This is an example of how to modify the scraper for better Azure support

func (m *mssqlScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	if m.db == nil {
		return pmetric.NewMetrics(), fmt.Errorf("database connection not established")
	}

	// Use individual timeouts for each collection instead of one global timeout
	// This prevents one slow query from timing out all subsequent queries
	now := pcommon.NewTimestampFromTime(time.Now())
	
	// Individual timeout for each metric collection (30% of total timeout)
	individualTimeout := time.Duration(m.cfg.Timeout) * time.Second / 3

	m.logger.Info("Starting scrape with configured timeout", 
		zap.Int("timeout", m.cfg.Timeout),
		zap.Duration("individual_timeout", individualTimeout))

	// Collect instance metrics with individual timeout
	instanceCtx, instanceCancel := context.WithTimeout(ctx, individualTimeout)
	if err := m.collectInstanceMetrics(instanceCtx, now); err != nil {
		m.logger.Error("Failed to collect instance metrics", zap.Error(err))
	}
	instanceCancel()

	// Collect database metrics with individual timeout
	dbCtx, dbCancel := context.WithTimeout(ctx, individualTimeout)
	if err := m.collectDatabaseMetrics(dbCtx, now); err != nil {
		m.logger.Error("Failed to collect database metrics", zap.Error(err))
	}
	dbCancel()

	// Only collect wait stats if enabled and with individual timeout
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlWaitStatsWaitingTasksCount.Enabled {
		waitCtx, waitCancel := context.WithTimeout(ctx, individualTimeout)
		if err := m.collectWaitStats(waitCtx, now); err != nil {
			m.logger.Error("Failed to collect wait stats", zap.Error(err))
		}
		waitCancel()
	}

	// Only collect lock stats if enabled and with individual timeout
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlLockStatsTimeoutCount.Enabled {
		lockCtx, lockCancel := context.WithTimeout(ctx, individualTimeout)
		if err := m.collectLockStats(lockCtx, now); err != nil {
			m.logger.Error("Failed to collect lock stats", zap.Error(err))
		}
		lockCancel()
	}

	// Only collect index stats if enabled and with individual timeout
	if m.cfg.MetricsBuilderConfig.Metrics.MssqlIndexStatsPageSplitsPerSec.Enabled {
		indexCtx, indexCancel := context.WithTimeout(ctx, individualTimeout)
		if err := m.collectIndexStats(indexCtx, now); err != nil {
			m.logger.Error("Failed to collect index stats", zap.Error(err))
		}
		indexCancel()
	}

	return m.mb.Emit(), nil
}

// Enhanced connection string with Azure-optimized parameters
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

	// Azure SQL-optimized connection parameters
	if m.cfg.Timeout > 0 {
		parts = append(parts, fmt.Sprintf("connection timeout=%d", m.cfg.Timeout))
		parts = append(parts, fmt.Sprintf("command timeout=%d", m.cfg.Timeout))
	}
	
	// Additional Azure optimizations
	parts = append(parts, "dial timeout=180")  // 3 minutes for initial connection
	parts = append(parts, "keepalive=30")      // Keep connection alive
	parts = append(parts, "packet size=32767") // Larger packet size for better throughput

	if m.cfg.ExtraConnectionURLArgs != "" {
		parts = append(parts, m.cfg.ExtraConnectionURLArgs)
	}

	return strings.Join(parts, ";")
}
