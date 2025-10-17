// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicsqlserverreceiver

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
)

// Config represents the receiver config settings within the collector's config.yaml
// Based on nri-mssql ArgumentList structure for compatibility
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`

	// Connection configuration
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Hostname string `mapstructure:"hostname"`
	Port     string `mapstructure:"port"`
	Instance string `mapstructure:"instance"`

	// Azure AD authentication
	ClientID     string `mapstructure:"client_id"`
	TenantID     string `mapstructure:"tenant_id"`
	ClientSecret string `mapstructure:"client_secret"`

	// SSL configuration
	EnableSSL              bool   `mapstructure:"enable_ssl"`
	TrustServerCertificate bool   `mapstructure:"trust_server_certificate"`
	CertificateLocation    string `mapstructure:"certificate_location"`

	// Performance and feature toggles
	EnableDatabaseSampleMetrics  bool `mapstructure:"enable_database_sample_metrics"`
	EnableFailoverClusterMetrics bool `mapstructure:"enable_failover_cluster_metrics"`

	// Granular failover cluster metrics configuration
	EnableFailoverClusterReplicaMetrics                 bool `mapstructure:"enable_failover_cluster_replica_metrics"`
	EnableFailoverClusterReplicaStateMetrics            bool `mapstructure:"enable_failover_cluster_replica_state_metrics"`
	EnableFailoverClusterNodeMetrics                    bool `mapstructure:"enable_failover_cluster_node_metrics"`
	EnableFailoverClusterAvailabilityGroupHealthMetrics bool `mapstructure:"enable_failover_cluster_availability_group_health_metrics"`
	EnableFailoverClusterAvailabilityGroupMetrics       bool `mapstructure:"enable_failover_cluster_availability_group_metrics"`
	EnableFailoverClusterPerformanceCounterMetrics      bool `mapstructure:"enable_failover_cluster_performance_counter_metrics"`
	EnableFailoverClusterRedoQueueMetrics               bool `mapstructure:"enable_failover_cluster_redo_queue_metrics"`

	// Database security metrics configuration
	EnableDatabasePrincipalsMetrics     bool `mapstructure:"enable_database_principals_metrics"`
	EnableDatabaseRoleMembershipMetrics bool `mapstructure:"enable_database_role_membership_metrics"`

	// Granular database principals metrics configuration
	EnableDatabasePrincipalsDetailsMetrics  bool `mapstructure:"enable_database_principals_details_metrics"`  // Database principals and users information
	EnableDatabasePrincipalsSummaryMetrics  bool `mapstructure:"enable_database_principals_summary_metrics"`  // Database principals summary statistics
	EnableDatabasePrincipalsActivityMetrics bool `mapstructure:"enable_database_principals_activity_metrics"` // Database principals activity and lifecycle

	// Granular database role membership metrics configuration
	EnableDatabaseRoleMembershipDetailsMetrics bool `mapstructure:"enable_database_role_membership_details_metrics"` // Database role membership relationships
	EnableDatabaseRoleMembershipSummaryMetrics bool `mapstructure:"enable_database_role_membership_summary_metrics"` // Database role membership summary statistics
	EnableDatabaseRoleHierarchyMetrics         bool `mapstructure:"enable_database_role_hierarchy_metrics"`          // Database role hierarchy and nesting information
	EnableDatabaseRoleActivityMetrics          bool `mapstructure:"enable_database_role_activity_metrics"`           // Database role activity and usage metrics
	EnableDatabaseRolePermissionMatrixMetrics  bool `mapstructure:"enable_database_role_permission_matrix_metrics"`  // Database role permission matrix analysis
	EnableBufferMetrics                        bool `mapstructure:"enable_buffer_metrics"`
	EnableDatabaseReserveMetrics               bool `mapstructure:"enable_database_reserve_metrics"`
	EnableDiskMetricsInBytes                   bool `mapstructure:"enable_disk_metrics_in_bytes"`
	EnableIOMetrics                            bool `mapstructure:"enable_io_metrics"`
	EnableLogGrowthMetrics                     bool `mapstructure:"enable_log_growth_metrics"`
	EnablePageFileMetrics                      bool `mapstructure:"enable_page_file_metrics"`
	EnablePageFileTotalMetrics                 bool `mapstructure:"enable_page_file_total_metrics"`
	EnableMemoryMetrics                        bool `mapstructure:"enable_memory_metrics"`
	EnableMemoryTotalMetrics                   bool `mapstructure:"enable_memory_total_metrics"`
	EnableMemoryAvailableMetrics               bool `mapstructure:"enable_memory_available_metrics"`
	EnableMemoryUtilizationMetrics             bool `mapstructure:"enable_memory_utilization_metrics"`

	// User Connection Metrics - Granular toggles for different metric categories
	EnableUserConnectionMetrics            bool `mapstructure:"enable_user_connection_metrics"`             // Master toggle for all user connection metrics
	EnableUserConnectionStatusMetrics      bool `mapstructure:"enable_user_connection_status_metrics"`      // UserConnectionStatusMetrics - individual connection status counts
	EnableUserConnectionSummaryMetrics     bool `mapstructure:"enable_user_connection_summary_metrics"`     // UserConnectionStatusSummary - aggregated connection counts by status
	EnableUserConnectionUtilizationMetrics bool `mapstructure:"enable_user_connection_utilization_metrics"` // UserConnectionUtilization - efficiency ratios and percentages
	EnableUserConnectionClientMetrics      bool `mapstructure:"enable_user_connection_client_metrics"`      // UserConnectionByClientMetrics - connections by host/program
	EnableUserConnectionClientSummary      bool `mapstructure:"enable_user_connection_client_summary"`      // UserConnectionClientSummary - aggregated client statistics
	EnableUserConnectionStatsMetrics       bool `mapstructure:"enable_user_connection_stats_metrics"`       // General statistical analysis metrics

	// Authentication Metrics - Granular toggles for authentication monitoring
	EnableLoginLogoutMetrics        bool `mapstructure:"enable_login_logout_metrics"`         // Master toggle for all authentication metrics
	EnableLoginLogoutRateMetrics    bool `mapstructure:"enable_login_logout_rate_metrics"`    // LoginLogoutMetrics - individual login/logout rates
	EnableLoginLogoutSummaryMetrics bool `mapstructure:"enable_login_logout_summary_metrics"` // LoginLogoutSummary - aggregated authentication activity
	EnableFailedLoginMetrics        bool `mapstructure:"enable_failed_login_metrics"`         // FailedLoginMetrics - individual failed login attempts
	EnableFailedLoginSummaryMetrics bool `mapstructure:"enable_failed_login_summary_metrics"` // FailedLoginSummary - aggregated failed login statistics

	// Concurrency and timeouts
	MaxConcurrentWorkers int           `mapstructure:"max_concurrent_workers"`
	Timeout              time.Duration `mapstructure:"timeout"`

	// Custom queries
	CustomMetricsQuery  string `mapstructure:"custom_metrics_query"`
	CustomMetricsConfig string `mapstructure:"custom_metrics_config"`

	// Additional connection parameters
	ExtraConnectionURLArgs string `mapstructure:"extra_connection_url_args"`

	// Query monitoring configuration
	EnableQueryMonitoring                bool `mapstructure:"enable_query_monitoring"`
	QueryMonitoringResponseTimeThreshold int  `mapstructure:"query_monitoring_response_time_threshold"`
	QueryMonitoringCountThreshold        int  `mapstructure:"query_monitoring_count_threshold"`
	QueryMonitoringFetchInterval         int  `mapstructure:"query_monitoring_fetch_interval"`
}

// DefaultConfig returns a Config struct with default values
func DefaultConfig() component.Config {
	cfg := &Config{
		ControllerConfig: scraperhelper.NewDefaultControllerConfig(),

		// Default connection settings
		Hostname: "127.0.0.1",
		Port:     "1433",

		// Default feature toggles (matching nri-mssql defaults)
		EnableDatabaseSampleMetrics:  false, // Master toggle - when true, enables all database metrics
		EnableFailoverClusterMetrics: false, // Failover cluster and Always On metrics

		// Default granular failover cluster metrics (disabled by default)
		EnableFailoverClusterReplicaMetrics:                 false, // Always On replica performance metrics
		EnableFailoverClusterReplicaStateMetrics:            false, // Database replica state and synchronization metrics
		EnableFailoverClusterNodeMetrics:                    false, // Windows Server Failover Cluster node metrics
		EnableFailoverClusterAvailabilityGroupHealthMetrics: false, // Availability Group health and role status
		EnableFailoverClusterAvailabilityGroupMetrics:       false, // Availability Group configuration metrics
		EnableFailoverClusterPerformanceCounterMetrics:      false, // Extended performance counters for Always On
		EnableFailoverClusterRedoQueueMetrics:               false, // Redo queue metrics (Azure SQL Managed Instance only)

		// Database security metrics defaults
		EnableDatabasePrincipalsMetrics:     false, // Database principals and users information
		EnableDatabaseRoleMembershipMetrics: false, // Database role membership and security hierarchy

		// Default granular database principals metrics (disabled by default)
		EnableDatabasePrincipalsDetailsMetrics:  false, // Database principals and users details
		EnableDatabasePrincipalsSummaryMetrics:  false, // Database principals summary statistics
		EnableDatabasePrincipalsActivityMetrics: false, // Database principals activity and lifecycle

		// Default granular database role membership metrics (disabled by default)
		EnableDatabaseRoleMembershipDetailsMetrics: false, // Database role membership relationships
		EnableDatabaseRoleMembershipSummaryMetrics: false, // Database role membership summary statistics
		EnableDatabaseRoleHierarchyMetrics:         false, // Database role hierarchy and nesting
		EnableDatabaseRoleActivityMetrics:          false, // Database role activity and usage
		EnableDatabaseRolePermissionMatrixMetrics:  false, // Database role permission matrix analysis
		EnableBufferMetrics:                        true,
		EnableDatabaseReserveMetrics:               true,
		EnableDiskMetricsInBytes:                   true,
		EnableIOMetrics:                            true,
		EnableLogGrowthMetrics:                     true,
		EnablePageFileMetrics:                      true,
		EnablePageFileTotalMetrics:                 true,
		EnableMemoryMetrics:                        true,
		EnableMemoryTotalMetrics:                   true,
		EnableMemoryAvailableMetrics:               true,
		EnableMemoryUtilizationMetrics:             true,

		// Default user connection metrics (all enabled by default for comprehensive monitoring)
		EnableUserConnectionMetrics:            true, // Master toggle
		EnableUserConnectionStatusMetrics:      true, // Individual status metrics
		EnableUserConnectionSummaryMetrics:     true, // Aggregated status summary
		EnableUserConnectionUtilizationMetrics: true, // Utilization ratios
		EnableUserConnectionClientMetrics:      true, // Client/host breakdown
		EnableUserConnectionClientSummary:      true, // Client summary stats
		EnableUserConnectionStatsMetrics:       true, // Statistical analysis

		// Default authentication metrics (all enabled by default for security monitoring)
		EnableLoginLogoutMetrics:        true, // Master toggle
		EnableLoginLogoutRateMetrics:    true, // Individual rates
		EnableLoginLogoutSummaryMetrics: true, // Aggregated auth activity
		EnableFailedLoginMetrics:        true, // Individual failed attempts
		EnableFailedLoginSummaryMetrics: true, // Failed login statistics

		// Default concurrency and timeout
		MaxConcurrentWorkers: 10,
		Timeout:              30 * time.Second,

		// Default SSL settings
		EnableSSL:              false,
		TrustServerCertificate: false,

		// Default query monitoring settings
		EnableQueryMonitoring:                true,
		QueryMonitoringResponseTimeThreshold: 1,
		QueryMonitoringCountThreshold:        20,
		QueryMonitoringFetchInterval:         15,
	}

	// Set default collection interval to 15 seconds
	cfg.ControllerConfig.CollectionInterval = 15 * time.Second

	return cfg
}

// Validate validates the configuration and sets defaults where needed
// Based on nri-mssql ArgumentList.Validate() method
func (cfg *Config) Validate() error {
	if cfg.Hostname == "" {
		return errors.New("hostname cannot be empty")
	}

	if cfg.Port != "" && cfg.Instance != "" {
		return errors.New("specify either port or instance but not both")
	} else if cfg.Port == "" && cfg.Instance == "" {
		// Default to port 1433 if neither is specified (matching nri-mssql behavior)
		cfg.Port = "1433"
	}

	if cfg.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	if cfg.MaxConcurrentWorkers <= 0 {
		return errors.New("max_concurrent_workers must be positive")
	}

	if cfg.EnableQueryMonitoring {
		if cfg.QueryMonitoringResponseTimeThreshold <= 0 {
			return errors.New("query_monitoring_response_time_threshold must be positive when query monitoring is enabled")
		}
		if cfg.QueryMonitoringCountThreshold <= 0 {
			return errors.New("query_monitoring_count_threshold must be positive when query monitoring is enabled")
		}
	}

	if cfg.EnableSSL && (!cfg.TrustServerCertificate && cfg.CertificateLocation == "") {
		return errors.New("must specify a certificate file when using SSL and not trusting server certificate")
	}

	if len(cfg.CustomMetricsConfig) > 0 {
		if len(cfg.CustomMetricsQuery) > 0 {
			return errors.New("cannot specify both custom_metrics_query and custom_metrics_config")
		}
		if _, err := os.Stat(cfg.CustomMetricsConfig); err != nil {
			return fmt.Errorf("custom_metrics_config file error: %w", err)
		}
	}

	return nil
}

// Unmarshal implements the confmap.Unmarshaler interface
func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	if err := conf.Unmarshal(cfg); err != nil {
		return err
	}
	return cfg.Validate()
}

// GetMaxConcurrentWorkers returns the configured max concurrent workers with fallback
// Based on nri-mssql ArgumentList.GetMaxConcurrentWorkers() method
func (cfg *Config) GetMaxConcurrentWorkers() int {
	if cfg.MaxConcurrentWorkers <= 0 {
		return 10 // DefaultMaxConcurrentWorkers from nri-mssql
	}
	return cfg.MaxConcurrentWorkers
}

// IsAzureADAuth checks if Azure AD Service Principal authentication is configured
func (cfg *Config) IsAzureADAuth() bool {
	return cfg.ClientID != "" && cfg.TenantID != "" && cfg.ClientSecret != ""
}

// CreateConnectionURL creates a connection string for SQL Server authentication
// Based on nri-mssql connection.CreateConnectionURL() method
func (cfg *Config) CreateConnectionURL(dbName string) string {
	connectionURL := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(cfg.Username, cfg.Password),
		Host:   cfg.Hostname,
	}

	// If port is present use port, if not use instance
	if cfg.Port != "" {
		connectionURL.Host = fmt.Sprintf("%s:%s", connectionURL.Host, cfg.Port)
	} else {
		connectionURL.Path = cfg.Instance
	}

	// Format query parameters
	query := url.Values{}
	query.Add("dial timeout", fmt.Sprintf("%.0f", cfg.Timeout.Seconds()))
	query.Add("connection timeout", fmt.Sprintf("%.0f", cfg.Timeout.Seconds()))

	if dbName != "" {
		query.Add("database", dbName)
	}

	if cfg.ExtraConnectionURLArgs != "" {
		extraArgsMap, err := url.ParseQuery(cfg.ExtraConnectionURLArgs)
		if err == nil {
			for k, v := range extraArgsMap {
				query.Add(k, v[0])
			}
		}
	}

	if cfg.EnableSSL {
		query.Add("encrypt", "true")
		query.Add("TrustServerCertificate", strconv.FormatBool(cfg.TrustServerCertificate))
		if !cfg.TrustServerCertificate && cfg.CertificateLocation != "" {
			query.Add("certificate", cfg.CertificateLocation)
		}
	}

	connectionURL.RawQuery = query.Encode()
	return connectionURL.String()
}

// CreateAzureADConnectionURL creates a connection string for Azure AD authentication
// Based on nri-mssql connection.CreateAzureADConnectionURL() method
func (cfg *Config) CreateAzureADConnectionURL(dbName string) string {
	connectionString := fmt.Sprintf(
		"server=%s;port=%s;fedauth=ActiveDirectoryServicePrincipal;applicationclientid=%s;clientsecret=%s;database=%s",
		cfg.Hostname,
		cfg.Port,
		cfg.ClientID,     // Client ID
		cfg.ClientSecret, // Client Secret
		dbName,           // Database
	)

	if cfg.ExtraConnectionURLArgs != "" {
		extraArgsMap, err := url.ParseQuery(cfg.ExtraConnectionURLArgs)
		if err == nil {
			for k, v := range extraArgsMap {
				connectionString += fmt.Sprintf(";%s=%s", k, v[0])
			}
		}
	}

	if cfg.EnableSSL {
		connectionString += ";encrypt=true"
		if cfg.TrustServerCertificate {
			connectionString += ";TrustServerCertificate=true"
		} else {
			connectionString += ";TrustServerCertificate=false"
			if cfg.CertificateLocation != "" {
				connectionString += fmt.Sprintf(";certificate=%s", cfg.CertificateLocation)
			}
		}
	}

	return connectionString
}

// Helper methods to check if metrics are enabled (either individually or via master toggle)

// IsBufferMetricsEnabled checks if buffer metrics should be collected
func (cfg *Config) IsBufferMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableBufferMetrics
}

// IsDatabaseReserveMetricsEnabled checks if database reserve metrics should be collected
func (cfg *Config) IsDatabaseReserveMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableDatabaseReserveMetrics
}

// IsDiskMetricsInBytesEnabled checks if disk metrics in bytes should be collected
func (cfg *Config) IsDiskMetricsInBytesEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableDiskMetricsInBytes
}

// IsIOMetricsEnabled checks if IO metrics should be collected
func (cfg *Config) IsIOMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableIOMetrics
}

// IsLogGrowthMetricsEnabled checks if log growth metrics should be collected
func (cfg *Config) IsLogGrowthMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableLogGrowthMetrics
}

// IsPageFileMetricsEnabled checks if page file metrics should be collected
func (cfg *Config) IsPageFileMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnablePageFileMetrics
}

// IsPageFileTotalMetricsEnabled checks if page file total metrics should be collected
func (cfg *Config) IsPageFileTotalMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnablePageFileTotalMetrics
}

// IsMemoryMetricsEnabled checks if memory metrics should be collected
func (cfg *Config) IsMemoryMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableMemoryMetrics
}

// IsMemoryTotalMetricsEnabled checks if memory total metrics should be collected
func (cfg *Config) IsMemoryTotalMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableMemoryTotalMetrics
}

// IsMemoryAvailableMetricsEnabled checks if memory available metrics should be collected
func (cfg *Config) IsMemoryAvailableMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableMemoryAvailableMetrics
}

// IsMemoryUtilizationMetricsEnabled checks if memory utilization metrics should be collected
func (cfg *Config) IsMemoryUtilizationMetricsEnabled() bool {
	return cfg.EnableDatabaseSampleMetrics || cfg.EnableMemoryUtilizationMetrics
}

// IsFailoverClusterMetricsEnabled checks if failover cluster metrics should be collected
func (cfg *Config) IsFailoverClusterMetricsEnabled() bool {
	return cfg.EnableFailoverClusterMetrics
}

// Granular failover cluster metrics helper methods

// IsFailoverClusterReplicaMetricsEnabled checks if failover cluster replica metrics should be collected
func (cfg *Config) IsFailoverClusterReplicaMetricsEnabled() bool {
	return cfg.EnableFailoverClusterMetrics || cfg.EnableFailoverClusterReplicaMetrics
}

// IsFailoverClusterReplicaStateMetricsEnabled checks if failover cluster replica state metrics should be collected
func (cfg *Config) IsFailoverClusterReplicaStateMetricsEnabled() bool {
	return cfg.EnableFailoverClusterMetrics || cfg.EnableFailoverClusterReplicaStateMetrics
}

// IsFailoverClusterNodeMetricsEnabled checks if failover cluster node metrics should be collected
func (cfg *Config) IsFailoverClusterNodeMetricsEnabled() bool {
	return cfg.EnableFailoverClusterMetrics || cfg.EnableFailoverClusterNodeMetrics
}

// IsFailoverClusterAvailabilityGroupHealthMetricsEnabled checks if availability group health metrics should be collected
func (cfg *Config) IsFailoverClusterAvailabilityGroupHealthMetricsEnabled() bool {
	return cfg.EnableFailoverClusterMetrics || cfg.EnableFailoverClusterAvailabilityGroupHealthMetrics
}

// IsFailoverClusterAvailabilityGroupMetricsEnabled checks if availability group configuration metrics should be collected
func (cfg *Config) IsFailoverClusterAvailabilityGroupMetricsEnabled() bool {
	return cfg.EnableFailoverClusterMetrics || cfg.EnableFailoverClusterAvailabilityGroupMetrics
}

// IsFailoverClusterPerformanceCounterMetricsEnabled checks if failover cluster performance counter metrics should be collected
func (cfg *Config) IsFailoverClusterPerformanceCounterMetricsEnabled() bool {
	return cfg.EnableFailoverClusterMetrics || cfg.EnableFailoverClusterPerformanceCounterMetrics
}

// IsFailoverClusterRedoQueueMetricsEnabled checks if failover cluster redo queue metrics should be collected
// This is only applicable to Azure SQL Managed Instance
func (cfg *Config) IsFailoverClusterRedoQueueMetricsEnabled() bool {
	return cfg.EnableFailoverClusterMetrics || cfg.EnableFailoverClusterRedoQueueMetrics
}

// IsDatabasePrincipalsMetricsEnabled checks if database principals metrics should be collected
func (cfg *Config) IsDatabasePrincipalsMetricsEnabled() bool {
	return cfg.EnableDatabasePrincipalsMetrics
}

// Granular database principals metrics helper methods

// IsDatabasePrincipalsDetailsMetricsEnabled checks if database principals details metrics should be collected
func (cfg *Config) IsDatabasePrincipalsDetailsMetricsEnabled() bool {
	return cfg.EnableDatabasePrincipalsMetrics || cfg.EnableDatabasePrincipalsDetailsMetrics
}

// IsDatabasePrincipalsSummaryMetricsEnabled checks if database principals summary metrics should be collected
func (cfg *Config) IsDatabasePrincipalsSummaryMetricsEnabled() bool {
	return cfg.EnableDatabasePrincipalsMetrics || cfg.EnableDatabasePrincipalsSummaryMetrics
}

// IsDatabasePrincipalsActivityMetricsEnabled checks if database principals activity metrics should be collected
func (cfg *Config) IsDatabasePrincipalsActivityMetricsEnabled() bool {
	return cfg.EnableDatabasePrincipalsMetrics || cfg.EnableDatabasePrincipalsActivityMetrics
}

// IsDatabaseRoleMembershipMetricsEnabled checks if database role membership metrics should be collected
func (cfg *Config) IsDatabaseRoleMembershipMetricsEnabled() bool {
	return cfg.EnableDatabaseRoleMembershipMetrics
}

// Granular database role membership metrics helper methods

// IsDatabaseRoleMembershipDetailsMetricsEnabled checks if database role membership details metrics should be collected
func (cfg *Config) IsDatabaseRoleMembershipDetailsMetricsEnabled() bool {
	return cfg.EnableDatabaseRoleMembershipMetrics || cfg.EnableDatabaseRoleMembershipDetailsMetrics
}

// IsDatabaseRoleMembershipSummaryMetricsEnabled checks if database role membership summary metrics should be collected
func (cfg *Config) IsDatabaseRoleMembershipSummaryMetricsEnabled() bool {
	return cfg.EnableDatabaseRoleMembershipMetrics || cfg.EnableDatabaseRoleMembershipSummaryMetrics
}

// IsDatabaseRoleHierarchyMetricsEnabled checks if database role hierarchy metrics should be collected
func (cfg *Config) IsDatabaseRoleHierarchyMetricsEnabled() bool {
	return cfg.EnableDatabaseRoleMembershipMetrics || cfg.EnableDatabaseRoleHierarchyMetrics
}

// IsDatabaseRoleActivityMetricsEnabled checks if database role activity metrics should be collected
func (cfg *Config) IsDatabaseRoleActivityMetricsEnabled() bool {
	return cfg.EnableDatabaseRoleMembershipMetrics || cfg.EnableDatabaseRoleActivityMetrics
}

// IsDatabaseRolePermissionMatrixMetricsEnabled checks if database role permission matrix metrics should be collected
func (cfg *Config) IsDatabaseRolePermissionMatrixMetricsEnabled() bool {
	return cfg.EnableDatabaseRoleMembershipMetrics || cfg.EnableDatabaseRolePermissionMatrixMetrics
}

// User Connection Metrics - Granular helper methods with master toggle support

// IsUserConnectionMetricsEnabled checks if any user connection metrics should be collected (master toggle)
func (cfg *Config) IsUserConnectionMetricsEnabled() bool {
	return cfg.EnableUserConnectionMetrics
}

// IsUserConnectionStatusMetricsEnabled checks if user connection status metrics should be collected
func (cfg *Config) IsUserConnectionStatusMetricsEnabled() bool {
	return cfg.EnableUserConnectionMetrics || cfg.EnableUserConnectionStatusMetrics
}

// IsUserConnectionSummaryMetricsEnabled checks if user connection summary metrics should be collected
func (cfg *Config) IsUserConnectionSummaryMetricsEnabled() bool {
	return cfg.EnableUserConnectionMetrics || cfg.EnableUserConnectionSummaryMetrics
}

// IsUserConnectionUtilizationMetricsEnabled checks if user connection utilization metrics should be collected
func (cfg *Config) IsUserConnectionUtilizationMetricsEnabled() bool {
	return cfg.EnableUserConnectionMetrics || cfg.EnableUserConnectionUtilizationMetrics
}

// IsUserConnectionClientMetricsEnabled checks if user connection client metrics should be collected
func (cfg *Config) IsUserConnectionClientMetricsEnabled() bool {
	return cfg.EnableUserConnectionMetrics || cfg.EnableUserConnectionClientMetrics
}

// IsUserConnectionClientSummaryEnabled checks if user connection client summary metrics should be collected
func (cfg *Config) IsUserConnectionClientSummaryEnabled() bool {
	return cfg.EnableUserConnectionMetrics || cfg.EnableUserConnectionClientSummary
}

// IsUserConnectionStatsMetricsEnabled checks if user connection stats metrics should be collected
func (cfg *Config) IsUserConnectionStatsMetricsEnabled() bool {
	return cfg.EnableUserConnectionMetrics || cfg.EnableUserConnectionStatsMetrics
}

// Authentication Metrics - Granular helper methods with master toggle support

// IsLoginLogoutMetricsEnabled checks if any login/logout metrics should be collected (master toggle)
func (cfg *Config) IsLoginLogoutMetricsEnabled() bool {
	return cfg.EnableLoginLogoutMetrics
}

// IsLoginLogoutRateMetricsEnabled checks if login/logout rate metrics should be collected
func (cfg *Config) IsLoginLogoutRateMetricsEnabled() bool {
	return cfg.EnableLoginLogoutMetrics || cfg.EnableLoginLogoutRateMetrics
}

// IsLoginLogoutSummaryMetricsEnabled checks if login/logout summary metrics should be collected
func (cfg *Config) IsLoginLogoutSummaryMetricsEnabled() bool {
	return cfg.EnableLoginLogoutMetrics || cfg.EnableLoginLogoutSummaryMetrics
}

// IsFailedLoginMetricsEnabled checks if failed login metrics should be collected
func (cfg *Config) IsFailedLoginMetricsEnabled() bool {
	return cfg.EnableLoginLogoutMetrics || cfg.EnableFailedLoginMetrics
}

// IsFailedLoginSummaryMetricsEnabled checks if failed login summary metrics should be collected
func (cfg *Config) IsFailedLoginSummaryMetricsEnabled() bool {
	return cfg.EnableLoginLogoutMetrics || cfg.EnableFailedLoginSummaryMetrics
}
