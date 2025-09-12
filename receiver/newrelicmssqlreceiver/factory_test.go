// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmssqlreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	
	assert.Equal(t, "newrelicmssql", string(factory.Type()))
	
	cfg := factory.CreateDefaultConfig()
	assert.NotNil(t, cfg)
	
	// Test that we can create a metrics receiver
	_, err := factory.CreateMetricsReceiver(
		receivertest.NewNopSettings(),
		cfg,
		consumertest.NewNop(),
	)
	assert.NoError(t, err)
}

func TestCreateMetricsReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	
	// Update config with required fields
	config := cfg.(*Config)
	config.Hostname = "localhost"
	config.Username = "sa"
	config.Password = "password123"
	
	receiver, err := factory.CreateMetricsReceiver(
		receivertest.NewNopSettings(),
		config,
		consumertest.NewNop(),
	)
	
	require.NoError(t, err)
	require.NotNil(t, receiver)
	
	// Test that we can start and stop the receiver (without actually connecting)
	err = receiver.Start(componenttest.NewNopHost())
	assert.Error(t, err) // Expected to fail since we don't have a real SQL Server
	
	err = receiver.Shutdown(componenttest.NewNopHost())
	assert.NoError(t, err)
}
