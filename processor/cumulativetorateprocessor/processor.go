// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetorateprocessor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type metricState struct {
	value     float64
	timestamp time.Time
	lastSeen  time.Time
}

type cumulativeToRateProcessor struct {
	config        *Config
	logger        *zap.Logger
	previousState map[string]*metricState
	mutex         sync.RWMutex
	nextConsumer  consumer.Metrics
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

func newCumulativeToRateProcessor(config *Config, logger *zap.Logger, nextConsumer consumer.Metrics) (*cumulativeToRateProcessor, error) {
	return &cumulativeToRateProcessor{
		config:        config,
		logger:        logger,
		previousState: make(map[string]*metricState),
		nextConsumer:  nextConsumer,
		stopCh:        make(chan struct{}),
	}, nil
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
	ctrp.mutex.Lock()
	defer ctrp.mutex.Unlock()

	resourceMetrics := md.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		scopeMetrics := rm.ScopeMetrics()

		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			metrics := sm.Metrics()

			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				ctrp.processMetric(metric)
			}
		}
	}

	return md, nil
}

func (ctrp *cumulativeToRateProcessor) processMetric(metric pmetric.Metric) {
	// Only process Sum metrics that are cumulative (rate source type)
	if metric.Type() != pmetric.MetricTypeSum {
		return
	}

	sum := metric.Sum()

	// Only process cumulative metrics (which represent rate source type)
	if sum.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		return
	}

	dataPoints := sum.DataPoints()

	// Process each data point to calculate rates
	for i := 0; i < dataPoints.Len(); i++ {
		dp := dataPoints.At(i)
		ctrp.processDataPoint(metric.Name(), dp)
	}

	// Convert Sum metric to Gauge metric with summary statistics
	ctrp.convertSumToGauge(metric)
}

func (ctrp *cumulativeToRateProcessor) convertSumToGauge(metric pmetric.Metric) {
	// Get the current sum data points
	sum := metric.Sum()
	sumDataPoints := sum.DataPoints()

	// Convert to gauge
	gauge := metric.SetEmptyGauge()
	gaugeDataPoints := gauge.DataPoints()

	// Copy data points from sum to gauge
	for i := 0; i < sumDataPoints.Len(); i++ {
		sumDP := sumDataPoints.At(i)

		// Skip data points marked as invalid
		if sumDP.Flags().NoRecordedValue() {
			continue
		}

		gaugeDP := gaugeDataPoints.AppendEmpty()

		// Copy basic properties
		gaugeDP.SetTimestamp(sumDP.Timestamp())
		gaugeDP.SetStartTimestamp(sumDP.StartTimestamp())

		// Get the calculated rate value
		rateValue := sumDP.DoubleValue()

		// Set gauge value (this will be the rate)
		gaugeDP.SetDoubleValue(rateValue)

		// Copy attributes and update metric.type
		sumDP.Attributes().CopyTo(gaugeDP.Attributes())
		attrs := gaugeDP.Attributes()

		// Convert source type from "rate" to "gauge" after processing
		if sourceType, exists := attrs.Get("metric.type"); exists && sourceType.AsString() == "rate" {
			attrs.PutStr("metric.type", "gauge")
		}
	}
}

func (ctrp *cumulativeToRateProcessor) processDataPoint(metricName string, dp pmetric.NumberDataPoint) {
	// Create unique key for this metric series
	key := ctrp.generateKey(metricName, dp.Attributes())

	currentValue := dp.DoubleValue()
	currentTime := dp.Timestamp().AsTime()
	now := time.Now()

	// Check if we have previous state for this metric
	if prevState, exists := ctrp.previousState[key]; exists {
		// Update last seen time
		prevState.lastSeen = now

		// Calculate rate: (Current Value - Previous Value) / (Current Time - Previous Time)
		timeDiff := currentTime.Sub(prevState.timestamp).Seconds()
		if timeDiff > 0 {
			rate := (currentValue - prevState.value) / timeDiff

			// Check if rate is negative (counter reset or anomaly)
			if rate < 0 {
				ctrp.logger.Warn("Negative rate detected, skipping metric (possible counter reset)",
					zap.String("metric", metricName),
					zap.Float64("current_value", currentValue),
					zap.Float64("previous_value", prevState.value),
					zap.Float64("rate", rate))
				dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
			} else {
				dp.SetDoubleValue(rate)
				ctrp.logger.Debug("Calculated rate",
					zap.String("metric", metricName),
					zap.Float64("current_value", currentValue),
					zap.Float64("previous_value", prevState.value),
					zap.Float64("time_diff_seconds", timeDiff),
					zap.Float64("rate", rate))
			}
		} else {
			// Time difference is 0 or negative, mark for removal
			ctrp.logger.Warn("Invalid time difference for rate calculation",
				zap.String("metric", metricName),
				zap.Float64("time_diff_seconds", timeDiff))
			dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
		}
	} else {
		// First time seeing this metric, mark for removal (don't send)
		ctrp.logger.Debug("First occurrence of metric, skipping current interval",
			zap.String("metric", metricName))
		dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
	}

	// Replace previous state with current state (previous state becomes current state)
	ctrp.previousState[key] = &metricState{
		value:     currentValue,
		timestamp: currentTime,
		lastSeen:  now,
	}
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
	ctrp.mutex.Lock()
	defer ctrp.mutex.Unlock()

	now := time.Now()
	expiredKeys := make([]string, 0)

	for key, state := range ctrp.previousState {
		if now.Sub(state.lastSeen) > ctrp.config.StateTTL {
			expiredKeys = append(expiredKeys, key)
		}
	}

	if len(expiredKeys) > 0 {
		for _, key := range expiredKeys {
			delete(ctrp.previousState, key)
		}
		ctrp.logger.Debug("Evicted expired metric states",
			zap.Int("evicted_count", len(expiredKeys)),
			zap.Int("remaining_count", len(ctrp.previousState)))
	}
}

func (ctrp *cumulativeToRateProcessor) generateKey(metricName string, attributes pcommon.Map) string {
	var keyParts []string
	keyParts = append(keyParts, metricName)

	// Sort attributes to ensure consistent key generation
	var attrPairs []string
	attributes.Range(func(k string, v pcommon.Value) bool {
		attrPairs = append(attrPairs, fmt.Sprintf("%s=%s", k, v.AsString()))
		return true
	})

	// Sort to ensure consistent ordering
	sort.Strings(attrPairs)
	keyParts = append(keyParts, attrPairs...)

	return strings.Join(keyParts, "|")
}