// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetorateprocessor

import (
    "fmt"
    "time"

    "go.opentelemetry.io/collector/component"
)

// Config defines the configuration for the cumulative to rate processor
type Config struct {
    // StateTTL defines how long to keep state for a metric series
    StateTTL time.Duration `mapstructure:"state_ttl"`
    
    // Include specifies metric names to include for processing
    // If empty, all metrics are processed (unless Exclude is specified)
    Include []string `mapstructure:"include"`
    
    // Exclude specifies metric names to exclude from processing
    // Takes precedence over Include if both are specified
    Exclude []string `mapstructure:"exclude"`
}

var _ component.Config = (*Config)(nil)

// Validate checks if the processor configuration is valid
func (cfg *Config) Validate() error {
    if cfg.StateTTL <= 0 {
        return fmt.Errorf("state_ttl must be positive")
    }
    
    // Validate that include and exclude are not both specified with overlapping values
    if len(cfg.Include) > 0 && len(cfg.Exclude) > 0 {
        includeSet := make(map[string]bool)
        for _, metric := range cfg.Include {
            includeSet[metric] = true
        }
        
        for _, metric := range cfg.Exclude {
            if includeSet[metric] {
                return fmt.Errorf("metric '%s' cannot be both included and excluded", metric)
            }
        }
    }
    
    return nil
}