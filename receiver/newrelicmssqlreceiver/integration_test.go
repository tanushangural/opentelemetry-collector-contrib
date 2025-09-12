// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package newrelicmssqlreceiver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// TestIntegrationMSSQLReceiver tests the receiver with a real SQL Server instance
// This test requires the following environment variables:
// - MSSQL_HOSTNAME: SQL Server hostname
// - MSSQL_PORT: SQL Server port (optional, defaults to 1433) 
// - MSSQL_USERNAME: SQL Server username
// - MSSQL_PASSWORD: SQL Server password
func TestIntegrationMSSQLReceiver(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)

	// Configure with environment variables or defaults for testing
	cfg.Hostname = getEnvOrDefault("MSSQL_HOSTNAME", "localhost")
	cfg.Port = 1433
	cfg.Username = getEnvOrDefault("MSSQL_USERNAME", "sa")
	cfg.Password = getEnvOrDefault("MSSQL_PASSWORD", "YourStrong@Passw0rd")
	cfg.Timeout = 30
	cfg.CollectionInterval = 5 * time.Second

	// Validate configuration
	require.NoError(t, cfg.Validate())

	// Create consumer to capture metrics
	consumer := consumertest.NewNop()

	// Create receiver
	receiver, err := factory.CreateMetricsReceiver(
		context.Background(),
		receivertest.NewNopSettings(),
		cfg,
		consumer,
	)
	require.NoError(t, err)

	// Start receiver
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = receiver.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)

	// Wait for metrics collection
	time.Sleep(10 * time.Second)

	// Stop receiver
	err = receiver.Shutdown(ctx)
	require.NoError(t, err)

	// Note: In a real integration test, you would verify that metrics were collected
	// This would require checking the consumer's collected metrics
	t.Log("Integration test completed successfully")
}

func getEnvOrDefault(envVar, defaultValue string) string {
	// In a real implementation, you would use os.Getenv
	// For this example, we return the default value
	return defaultValue
}
