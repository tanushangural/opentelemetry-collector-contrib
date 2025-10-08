// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetorateprocessor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetorateprocessor/internal/metadata"
)

func TestType(t *testing.T) {
	factory := NewFactory()
	pType := factory.Type()
	assert.Equal(t, pType, metadata.Type)
}

func TestCreateDefaultConfig(t *testing.T) {
    cfg := createDefaultConfig()
    assert.NotNil(t, cfg, "failed to create default config")
    assert.NoError(t, componenttest.CheckConfigStruct(cfg))
    
    // Update expected config to match actual default values
    expected := &Config{
        StateTTL: time.Hour, // Default is 1 hour, not 0
        Include:  nil,
        Exclude:  nil,
    }
    assert.Equal(t, expected, cfg)
}

func TestCreateProcessors(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	// Test traces processor (should not be implemented)
	tp, tErr := factory.CreateTraces(
		t.Context(),
		processortest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop())
	// Not implemented error
	assert.Error(t, tErr)
	assert.Nil(t, tp)

	// Test logs processor (should not be implemented)
	lp, lErr := factory.CreateLogs(
		t.Context(),
		processortest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop())
	// Not implemented error
	assert.Error(t, lErr)
	assert.Nil(t, lp)

	// Test metrics processor (should be implemented)
	mp, mErr := factory.CreateMetrics(
		t.Context(),
		processortest.NewNopSettings(metadata.Type),
		cfg,
		consumertest.NewNop())

	assert.NotNil(t, mp)
	assert.NoError(t, mErr)
	assert.NoError(t, mp.Shutdown(t.Context()))
}
