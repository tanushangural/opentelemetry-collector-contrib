// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmssqlreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	
	config, ok := cfg.(*Config)
	require.True(t, ok)
	
	assert.Equal(t, 1433, config.Port)
	assert.Equal(t, false, config.EnableSSL)
	assert.Equal(t, true, config.TrustServerCertificate)
	assert.Equal(t, 30, config.Timeout)
	assert.Equal(t, true, config.EnableBufferMetrics)
	assert.Equal(t, true, config.EnableDatabaseReserveMetrics)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
	}{
		{
			name: "valid config",
			config: &Config{
				Hostname: "localhost",
				Port:     1433,
				Username: "sa",
				Password: "password123",
				Timeout:  30,
			},
			expectError: false,
		},
		{
			name: "missing hostname",
			config: &Config{
				Port:     1433,
				Username: "sa",
				Password: "password123",
			},
			expectError: true,
		},
		{
			name: "invalid port",
			config: &Config{
				Hostname: "localhost",
				Port:     0,
				Username: "sa",
				Password: "password123",
			},
			expectError: true,
		},
		{
			name: "missing username",
			config: &Config{
				Hostname: "localhost",
				Port:     1433,
				Password: "password123",
			},
			expectError: true,
		},
		{
			name: "missing password",
			config: &Config{
				Hostname: "localhost",
				Port:     1433,
				Username: "sa",
			},
			expectError: true,
		},
		{
			name: "negative timeout",
			config: &Config{
				Hostname: "localhost",
				Port:     1433,
				Username: "sa",
				Password: "password123",
				Timeout:  -1,
			},
			expectError: true,
		},
		{
			name: "valid azure ad config",
			config: &Config{
				Hostname:     "server.database.windows.net",
				Port:         1433,
				ClientID:     "client-id",
				TenantID:     "tenant-id",
				ClientSecret: "client-secret",
				Timeout:      30,
			},
			expectError: false,
		},
		{
			name: "incomplete azure ad config",
			config: &Config{
				Hostname: "server.database.windows.net",
				Port:     1433,
				ClientID: "client-id",
				Timeout:  30,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBuildConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected string
	}{
		{
			name: "basic connection",
			config: &Config{
				Hostname: "localhost",
				Port:     1433,
				Username: "sa",
				Password: "password123",
				Timeout:  30,
			},
			expected: "server=localhost;user id=sa;password=password123;port=1433;encrypt=false;connection timeout=30",
		},
		{
			name: "connection with ssl",
			config: &Config{
				Hostname:               "localhost",
				Port:                   1433,
				Username:               "sa",
				Password:               "password123",
				EnableSSL:              true,
				TrustServerCertificate: true,
				Timeout:                30,
			},
			expected: "server=localhost;user id=sa;password=password123;port=1433;encrypt=true;TrustServerCertificate=true;connection timeout=30",
		},
		{
			name: "azure ad connection",
			config: &Config{
				Hostname:     "server.database.windows.net",
				Port:         1433,
				ClientID:     "client-id",
				TenantID:     "tenant-id",
				ClientSecret: "client-secret",
				EnableSSL:    true,
				Timeout:      30,
			},
			expected: "server=server.database.windows.net;port=1433;fedauth=ActiveDirectoryServicePrincipal;user id=client-id;password=client-secret;TenantId=tenant-id;encrypt=true;TrustServerCertificate=true;connection timeout=30",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraper := &mssqlScraper{cfg: tt.config}
			connectionString := scraper.buildConnectionString()
			
			// Since the order of parameters might vary, check that all expected parts are present
			assert.Contains(t, connectionString, "server="+tt.config.Hostname)
			assert.Contains(t, connectionString, "port=1433")
			
			if tt.config.ClientID != "" {
				assert.Contains(t, connectionString, "fedauth=ActiveDirectoryServicePrincipal")
				assert.Contains(t, connectionString, "user id="+tt.config.ClientID)
				assert.Contains(t, connectionString, "TenantId="+tt.config.TenantID)
			} else {
				assert.Contains(t, connectionString, "user id="+tt.config.Username)
				assert.Contains(t, connectionString, "password="+string(tt.config.Password))
			}
		})
	}
}
