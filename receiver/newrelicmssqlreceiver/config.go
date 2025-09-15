// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmssqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmssqlreceiver"

import (
	"errors"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmssqlreceiver/internal/metadata"
)

// Config defines configuration for New Relic MSSQL receiver.
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	metadata.MetricsBuilderConfig  `mapstructure:",squash"`

	// Connection details
	Hostname                string              `mapstructure:"hostname"`
	Port                    int                 `mapstructure:"port"`
	Username                string              `mapstructure:"username"`
	Password                configopaque.String `mapstructure:"password"`
	Instance                string              `mapstructure:"instance"`

	// SSL Configuration
	EnableSSL              bool                   `mapstructure:"enable_ssl"`
	TrustServerCertificate bool                   `mapstructure:"trust_server_certificate"`
	CertificateLocation    string                 `mapstructure:"certificate_location"`
	TLS                    configtls.ClientConfig `mapstructure:"tls,omitempty"`

	// Connection settings
	Timeout                       int    `mapstructure:"timeout"`
	ExtraConnectionURLArgs        string `mapstructure:"extra_connection_url_args"`

	// Azure AD Authentication (optional)
	ClientID     string              `mapstructure:"client_id"`
	TenantID     string              `mapstructure:"tenant_id"`
	ClientSecret configopaque.String `mapstructure:"client_secret"`

	// Feature flags
	EnableBufferMetrics          bool `mapstructure:"enable_buffer_metrics"`
	EnableDatabaseReserveMetrics bool `mapstructure:"enable_database_reserve_metrics"`
	EnableDiskMetricsInBytes     bool `mapstructure:"enable_disk_metrics_in_bytes"`

	// Custom queries
	CustomMetricsConfig string `mapstructure:"custom_metrics_config"`
	CustomMetricsQuery  string `mapstructure:"custom_metrics_query"`

	// Query monitoring (not implemented as per requirements)
	// EnableQueryMonitoring bool `mapstructure:"enable_query_monitoring"`
}

func (cfg *Config) Validate() error {
	if cfg.Hostname == "" {
		return errors.New("hostname is required")
	}

	if cfg.Port <= 0 || cfg.Port > 65535 {
		return errors.New("port must be between 1 and 65535")
	}

	if cfg.Username == "" {
		return errors.New("username is required")
	}

	if string(cfg.Password) == "" {
		return errors.New("password is required")
	}

	if cfg.Timeout < 0 {
		return errors.New("timeout must be non-negative")
	}

	// Validate Azure AD auth settings if any are provided
	if cfg.ClientID != "" || cfg.TenantID != "" || string(cfg.ClientSecret) != "" {
		if cfg.ClientID == "" || cfg.TenantID == "" || string(cfg.ClientSecret) == "" {
			return errors.New("when using Azure AD authentication, client_id, tenant_id, and client_secret must all be provided")
		}
		// Clear username/password when using Azure AD
		cfg.Username = ""
		cfg.Password = ""
	}

	return nil
}

func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return nil
	}

	// Set default TLS settings
	if !componentParser.IsSet("tls") {
		cfg.TLS = configtls.ClientConfig{}
		cfg.TLS.Insecure = !cfg.EnableSSL
	}

	return componentParser.Unmarshal(cfg)
}
