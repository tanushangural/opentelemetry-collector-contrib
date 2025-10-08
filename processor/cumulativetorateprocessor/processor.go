// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetorateprocessor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type metricSeriesState struct {
	lastValue     float64
	lastTimestamp time.Time
	lastSeen      time.Time // Add this field for TTL tracking
}

type cumulativeToRateProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	stopCh       chan struct{}
	wg           sync.WaitGroup

	// State management
	stateMutex  sync.RWMutex
	metricState map[string]*metricSeriesState

	// Filtering
	includeSet map[string]bool
	excludeSet map[string]bool
}

func newCumulativeToRateProcessor(config *Config, logger *zap.Logger, nextConsumer consumer.Metrics) (*cumulativeToRateProcessor, error) {
	processor := &cumulativeToRateProcessor{
		config:       config,
		logger:       logger,
		nextConsumer: nextConsumer,
		stopCh:       make(chan struct{}),
		metricState:  make(map[string]*metricSeriesState),
	}

	// Pre-build sets for efficient lookup
	if len(config.Include) > 0 {
		processor.includeSet = make(map[string]bool)
		for _, metric := range config.Include {
			processor.includeSet[metric] = true
		}
	}

	if len(config.Exclude) > 0 {
		processor.excludeSet = make(map[string]bool)
		for _, metric := range config.Exclude {
			processor.excludeSet[metric] = true
		}
	}

	return processor, nil
}

// Start implements processor.Metrics
func (ctrp *cumulativeToRateProcessor) Start(ctx context.Context, host component.Host) error {
	// Start the cleanup goroutine
	ctrp.wg.Add(1)
	go ctrp.cleanupExpiredStates()
	ctrp.logger.Info("Started cumulative to rate processor with TTL cleanup",
		zap.Duration("state_ttl", ctrp.config.StateTTL))
	return nil
}

// Shutdown implements processor.Metrics
func (ctrp *cumulativeToRateProcessor) Shutdown(ctx context.Context) error {
	close(ctrp.stopCh)
	ctrp.wg.Wait()
	ctrp.logger.Info("Shutdown cumulative to rate processor")
	return nil
}

// Capabilities implements processor.Metrics
func (ctrp *cumulativeToRateProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeMetrics implements processor.Metrics
func (ctrp *cumulativeToRateProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	processedMetrics, err := ctrp.processMetrics(ctx, md)
	if err != nil {
		return err
	}
	return ctrp.nextConsumer.ConsumeMetrics(ctx, processedMetrics)
}

// processMetrics implements the ProcessMetricsFunc type.
func (ctrp *cumulativeToRateProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	resourceMetrics := md.ResourceMetrics()

	for i := 0; i < resourceMetrics.Len(); i++ {
		resourceMetric := resourceMetrics.At(i)
		scopeMetrics := resourceMetric.ScopeMetrics()

		for j := 0; j < scopeMetrics.Len(); j++ {
			scopeMetric := scopeMetrics.At(j)
			metrics := scopeMetric.Metrics()

			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				metricName := metric.Name()

				// Check if this metric should be processed
				if !ctrp.shouldProcessMetric(metricName) {
					ctrp.logger.Debug("Skipping metric due to include/exclude rules",
						zap.String("metric_name", metricName))
					continue
				}

				// Process only Sum metrics
				switch metric.Type() {
				case pmetric.MetricTypeSum:
					sum := metric.Sum()
					// Only process if it's cumulative AND monotonic
					if sum.AggregationTemporality() == pmetric.AggregationTemporalityCumulative && sum.IsMonotonic() {
						ctrp.logger.Debug("Processing cumulative monotonic sum metric",
							zap.String("metric_name", metricName))
						ctrp.processCumulativeSum(metric)
					} else {
						ctrp.logger.Debug("Skipping sum metric - not cumulative or not monotonic",
							zap.String("metric_name", metricName),
							zap.String("aggregation_temporality", sum.AggregationTemporality().String()),
							zap.Bool("is_monotonic", sum.IsMonotonic()))
					}
				default:
					// Skip all other metric types (Gauge, Histogram, Summary, etc.)
					ctrp.logger.Debug("Skipping metric - only processing Sum metrics",
						zap.String("metric_name", metricName),
						zap.String("metric_type", metric.Type().String()))
				}
			}
		}
	}

	// Clean up old state entries
	ctrp.cleanupOldState()

	return md, nil
}

func (ctrp *cumulativeToRateProcessor) processCumulativeSum(metric pmetric.Metric) {
	sum := metric.Sum()
	dataPoints := sum.DataPoints()

	// Process data points and collect indices of points to remove
	var indicesToRemove []int
	for i := 0; i < dataPoints.Len(); i++ {
		dataPoint := dataPoints.At(i)
		if !ctrp.processDataPoint(metric.Name(), dataPoint) {
			indicesToRemove = append(indicesToRemove, i)
		}
	}

	// Remove data points in reverse order to maintain correct indices
	for i := len(indicesToRemove) - 1; i >= 0; i-- {
		dataPoints.RemoveIf(func(dp pmetric.NumberDataPoint) bool {
			// Compare by finding the data point at the specific index
			for j := 0; j < dataPoints.Len(); j++ {
				if dataPoints.At(j) == dp && j == indicesToRemove[i] {
					return true
				}
			}
			return false
		})
	}

	// Convert Sum to Gauge
	ctrp.convertSumToGauge(metric)
}

// convertSumToGauge converts a Sum metric to a Gauge metric
func (ctrp *cumulativeToRateProcessor) convertSumToGauge(metric pmetric.Metric) {
	sum := metric.Sum()
	sumDataPoints := sum.DataPoints()

	// Create new gauge
	gauge := metric.SetEmptyGauge()
	gaugeDataPoints := gauge.DataPoints()

	// Copy data points from sum to gauge
	for i := 0; i < sumDataPoints.Len(); i++ {
		sumDP := sumDataPoints.At(i)
		gaugeDP := gaugeDataPoints.AppendEmpty()

		// Copy all properties
		gaugeDP.SetTimestamp(sumDP.Timestamp())
		gaugeDP.SetStartTimestamp(sumDP.StartTimestamp())
		sumDP.Attributes().CopyTo(gaugeDP.Attributes())

		// Copy value based on type
		switch sumDP.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			gaugeDP.SetIntValue(sumDP.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			gaugeDP.SetDoubleValue(sumDP.DoubleValue())
		}

		// Copy exemplars if any
		sumExemplars := sumDP.Exemplars()
		gaugeExemplars := gaugeDP.Exemplars()
		for j := 0; j < sumExemplars.Len(); j++ {
			sumExemplar := sumExemplars.At(j)
			gaugeExemplar := gaugeExemplars.AppendEmpty()
			sumExemplar.CopyTo(gaugeExemplar)
		}
	}
}

func (ctrp *cumulativeToRateProcessor) processDataPoint(metricName string, dataPoint pmetric.NumberDataPoint) bool {
	// Create a unique key for this data point series
	seriesKey := ctrp.createSeriesKey(metricName, dataPoint.Attributes())

	ctrp.stateMutex.Lock()
	defer ctrp.stateMutex.Unlock()

	currentTime := dataPoint.Timestamp().AsTime()
	now := time.Now() // For TTL tracking
	var currentValue float64

	switch dataPoint.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		currentValue = float64(dataPoint.IntValue())
	case pmetric.NumberDataPointValueTypeDouble:
		currentValue = dataPoint.DoubleValue()
	}

	if state, exists := ctrp.metricState[seriesKey]; exists {
		// Calculate rate
		timeDiff := currentTime.Sub(state.lastTimestamp).Seconds()
		if timeDiff > 0 {
			rate := (currentValue - state.lastValue) / timeDiff

			// Skip if rate is negative or zero
			if rate <= 0 {
				ctrp.logger.Debug("Skipping data point due to non-positive rate",
					zap.String("metric_name", metricName),
					zap.String("series_key", seriesKey),
					zap.Float64("rate", rate))
				// Update state but don't include this data point
				ctrp.metricState[seriesKey] = &metricSeriesState{
					lastValue:     currentValue,
					lastTimestamp: currentTime,
					lastSeen:      now, // Update lastSeen for TTL
				}
				return false
			}

			// Update the data point with the rate value
			if dataPoint.ValueType() == pmetric.NumberDataPointValueTypeInt {
				dataPoint.SetIntValue(int64(rate))
			} else {
				dataPoint.SetDoubleValue(rate)
			}
		}
	} else {
		// First time seeing this series, skip it but store the state
		ctrp.logger.Debug("Skipping data point for first-time metric series",
			zap.String("metric_name", metricName),
			zap.String("series_key", seriesKey))

		// Store initial state for future calculations
		ctrp.metricState[seriesKey] = &metricSeriesState{
			lastValue:     currentValue,
			lastTimestamp: currentTime,
			lastSeen:      now, // Set lastSeen for TTL
		}
		return false
	}

	// Update state
	ctrp.metricState[seriesKey] = &metricSeriesState{
		lastValue:     currentValue,
		lastTimestamp: currentTime,
		lastSeen:      now, // Update lastSeen for TTL
	}

	return true
}

func (ctrp *cumulativeToRateProcessor) createSeriesKey(metricName string, attributes pcommon.Map) string {
	return metricName + "|" + fmt.Sprintf("%x", pdatautil.MapHash(attributes))

}

func (ctrp *cumulativeToRateProcessor) cleanupOldState() {
	ctrp.stateMutex.Lock()
	defer ctrp.stateMutex.Unlock()

	cutoffTime := time.Now().Add(-ctrp.config.StateTTL)
	expiredKeys := make([]string, 0)

	// Collect expired keys first (avoid modification during iteration)
	for key, state := range ctrp.metricState {
		if state.lastSeen.Before(cutoffTime) {
			expiredKeys = append(expiredKeys, key)
		}
	}

	// Delete expired entries
	for _, key := range expiredKeys {
		delete(ctrp.metricState, key)
	}

	// Log cleanup statistics
	if len(expiredKeys) > 0 {
		ctrp.logger.Debug("Cleaned up expired metric states",
			zap.Int("expired_count", len(expiredKeys)),
			zap.Int("remaining_count", len(ctrp.metricState)),
			zap.Duration("ttl", ctrp.config.StateTTL))
	}
}

func (ctrp *cumulativeToRateProcessor) shouldProcessMetric(metricName string) bool {
	// If exclude list is specified and metric is in it, don't process
	if len(ctrp.excludeSet) > 0 {
		if ctrp.excludeSet[metricName] {
			return false
		}
		// Also check for pattern matching (simple wildcard support)
		for excludePattern := range ctrp.excludeSet {
			if ctrp.matchesPattern(metricName, excludePattern) {
				return false
			}
		}
	}

	// If include list is specified, only process if metric is in it
	if len(ctrp.includeSet) > 0 {
		if ctrp.includeSet[metricName] {
			return true
		}
		// Also check for pattern matching (simple wildcard support)
		for includePattern := range ctrp.includeSet {
			if ctrp.matchesPattern(metricName, includePattern) {
				return true
			}
		}
		return false
	}

	// If no include/exclude specified, process all metrics
	return true
}

func (ctrp *cumulativeToRateProcessor) matchesPattern(metricName, pattern string) bool {
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(metricName, prefix)
	}
	return metricName == pattern
}

func (ctrp *cumulativeToRateProcessor) cleanupExpiredStates() {
	defer ctrp.wg.Done()

	// Run cleanup every 15 minutes or 1/4 of TTL, whichever is smaller
	cleanupInterval := ctrp.config.StateTTL / 4
	if cleanupInterval > 15*time.Minute {
		cleanupInterval = 15 * time.Minute
	}

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctrp.evictExpiredStates()
		case <-ctrp.stopCh:
			return
		}
	}
}

func (ctrp *cumulativeToRateProcessor) evictExpiredStates() {
	ctrp.stateMutex.Lock() // Use stateMutex instead of mutex
	defer ctrp.stateMutex.Unlock()

	now := time.Now()
	cutoffTime := now.Add(-ctrp.config.StateTTL)
	expiredKeys := make([]string, 0)

	// Use metricState instead of previousState
	for key, state := range ctrp.metricState {
		if state.lastSeen.Before(cutoffTime) {
			expiredKeys = append(expiredKeys, key)
		}
	}

	if len(expiredKeys) > 0 {
		for _, key := range expiredKeys {
			delete(ctrp.metricState, key)
		}
		ctrp.logger.Debug("Evicted expired metric states",
			zap.Int("evicted_count", len(expiredKeys)),
			zap.Int("remaining_count", len(ctrp.metricState)))
	}
}
