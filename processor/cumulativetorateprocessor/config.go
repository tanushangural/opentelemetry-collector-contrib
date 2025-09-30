// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetorateprocessor

import (
    "time"

    "go.opentelemetry.io/collector/component"
)

// Config represents the receiver config settings within the collector's config.yaml
type Config struct {
    // StateTTL defines how long to keep metric state before evicting it
    // Default is 1 hour
    StateTTL time.Duration `mapstructure:"state_ttl"`
}

var _ component.Config = (*Config)(nil)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
    if cfg.StateTTL <= 0 {
        cfg.StateTTL = time.Hour // Default to 1 hour
    }
    return nil
}