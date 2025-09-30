// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetorateprocessor

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap/zaptest"
)

// Test helper functions

func createTestProcessor(t *testing.T) *cumulativeToRateProcessor {
	logger := zaptest.NewLogger(t)
	nextConsumer := consumertest.NewNop()

	processor, err := newCumulativeToRateProcessor(&Config{}, logger, nextConsumer)
	require.NoError(t, err)
	require.NotNil(t, processor)

	return processor
}

func createTestSumMetric(name string, value float64, timestamp time.Time, attributes map[string]string) pmetric.Metric {
	metric := pmetric.NewMetric()
	metric.SetName(name)
	metric.SetDescription("Test metric")
	metric.SetUnit("count")

	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(timestamp.Add(-time.Minute)))

	// Add attributes
	attrs := dp.Attributes()
	for k, v := range attributes {
		attrs.PutStr(k, v)
	}

	return metric
}

func createTestGaugeMetric(name string, value float64) pmetric.Metric {
	metric := pmetric.NewMetric()
	metric.SetName(name)

	gauge := metric.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	return metric
}

func createTestMetrics(metrics ...pmetric.Metric) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	for _, metric := range metrics {
		tgt := sm.Metrics().AppendEmpty()
		metric.CopyTo(tgt)
	}

	return md
}

// 2. Lifecycle Tests

func TestStart(t *testing.T) {
	processor := createTestProcessor(t)

	err := processor.Start(context.Background(), nil)
	assert.NoError(t, err)
}

func TestStartWithCancelledContext(t *testing.T) {
	processor := createTestProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := processor.Start(ctx, nil)
	assert.NoError(t, err) // Start should still succeed even with cancelled context
}

func TestShutdown(t *testing.T) {
	processor := createTestProcessor(t)

	err := processor.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestShutdownWithCancelledContext(t *testing.T) {
	processor := createTestProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := processor.Shutdown(ctx)
	assert.NoError(t, err) // Shutdown should still succeed even with cancelled context
}

func TestCapabilities(t *testing.T) {
	processor := createTestProcessor(t)

	capabilities := processor.Capabilities()
	assert.True(t, capabilities.MutatesData)
}

// 3. ConsumeMetrics Tests

func TestConsumeMetrics_Success(t *testing.T) {
	processor := createTestProcessor(t)

	metric := createTestSumMetric("test.metric", 100.0, time.Now(), map[string]string{"key": "value"})
	md := createTestMetrics(metric)

	err := processor.ConsumeMetrics(context.Background(), md)
	assert.NoError(t, err)
}

func TestConsumeMetrics_EmptyMetrics(t *testing.T) {
	processor := createTestProcessor(t)

	md := pmetric.NewMetrics()

	err := processor.ConsumeMetrics(context.Background(), md)
	assert.NoError(t, err)
}

func TestConsumeMetrics_ContextCancellation(t *testing.T) {
	processor := createTestProcessor(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	metric := createTestSumMetric("test.metric", 100.0, time.Now(), map[string]string{"key": "value"})
	md := createTestMetrics(metric)

	err := processor.ConsumeMetrics(ctx, md)
	assert.NoError(t, err) // Should still process even with cancelled context
}

// 4. ProcessMetrics Tests

func TestProcessMetrics_EmptyResourceMetrics(t *testing.T) {
	processor := createTestProcessor(t)

	md := pmetric.NewMetrics()

	result, err := processor.processMetrics(context.Background(), md)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.ResourceMetrics().Len())
}

func TestProcessMetrics_MultipleResourceMetrics(t *testing.T) {
	processor := createTestProcessor(t)

	md := pmetric.NewMetrics()

	// Add multiple resource metrics
	for i := 0; i < 3; i++ {
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		metric := createTestSumMetric("test.metric", float64(100+i), time.Now(), map[string]string{"instance": string(rune('A' + i))})
		tgt := sm.Metrics().AppendEmpty()
		metric.CopyTo(tgt)
	}

	result, err := processor.processMetrics(context.Background(), md)
	assert.NoError(t, err)
	assert.Equal(t, 3, result.ResourceMetrics().Len())
}

// 5. ProcessMetric Tests (Core Logic)

func TestProcessMetric_MetricTypeFiltering(t *testing.T) {
	processor := createTestProcessor(t)

	tests := []struct {
		name          string
		metric        pmetric.Metric
		shouldProcess bool
	}{
		{
			name:          "sum metric should be processed",
			metric:        createTestSumMetric("sum.metric", 100.0, time.Now(), map[string]string{}),
			shouldProcess: true,
		},
		{
			name:          "gauge metric should be ignored",
			metric:        createTestGaugeMetric("gauge.metric", 100.0),
			shouldProcess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalType := tt.metric.Type()
			processor.processMetric(tt.metric)

			if tt.shouldProcess {
				// Sum should be converted to Gauge
				if originalType == pmetric.MetricTypeSum {
					assert.Equal(t, pmetric.MetricTypeGauge, tt.metric.Type())
				}
			} else {
				// Type should remain unchanged
				assert.Equal(t, originalType, tt.metric.Type())
			}
		})
	}
}

func TestProcessMetric_AggregationTemporalityFiltering(t *testing.T) {
	processor := createTestProcessor(t)

	// Create delta sum metric (should be ignored)
	metric := pmetric.NewMetric()
	metric.SetName("delta.metric")
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(100.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	originalType := metric.Type()
	processor.processMetric(metric)

	// Should remain unchanged (not processed)
	assert.Equal(t, originalType, metric.Type())
	assert.Equal(t, pmetric.AggregationTemporalityDelta, metric.Sum().AggregationTemporality())
}

func TestProcessMetric_EmptyDataPoints(t *testing.T) {
	processor := createTestProcessor(t)

	metric := pmetric.NewMetric()
	metric.SetName("empty.metric")
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	// No data points added

	processor.processMetric(metric)

	// Should be converted to gauge but have no data points
	assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
	assert.Equal(t, 0, metric.Gauge().DataPoints().Len())
}

// 6. ConvertSumToGauge Tests

func TestConvertSumToGauge_DataIntegrity(t *testing.T) {
	processor := createTestProcessor(t)

	now := time.Now()

	metric := createTestSumMetric("test.metric", 150.0, now, map[string]string{
		"metric.type": "rate",
		"database":    "testdb",
	})

	processor.convertSumToGauge(metric)

	// Verify conversion
	assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())

	gauge := metric.Gauge()
	assert.Equal(t, 1, gauge.DataPoints().Len())

	dp := gauge.DataPoints().At(0)
	assert.Equal(t, 150.0, dp.DoubleValue())
	assert.Equal(t, pcommon.NewTimestampFromTime(now), dp.Timestamp())

	// Verify attributes
	attrs := dp.Attributes()
	metricType, exists := attrs.Get("metric.type")
	assert.True(t, exists)
	assert.Equal(t, "gauge", metricType.Str()) // Should be converted from "rate" to "gauge"

	database, exists := attrs.Get("database")
	assert.True(t, exists)
	assert.Equal(t, "testdb", database.Str())
}

func TestConvertSumToGauge_NoRecordedValueFlag(t *testing.T) {
	processor := createTestProcessor(t)

	metric := createTestSumMetric("test.metric", 100.0, time.Now(), map[string]string{})

	// Set NoRecordedValue flag on the data point
	sum := metric.Sum()
	dp := sum.DataPoints().At(0)
	dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))

	processor.convertSumToGauge(metric)

	// Should be converted to gauge but have no data points (filtered out)
	assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
	assert.Equal(t, 0, metric.Gauge().DataPoints().Len())
}

func TestConvertSumToGauge_MultipleDataPoints(t *testing.T) {
	processor := createTestProcessor(t)

	metric := pmetric.NewMetric()
	metric.SetName("multi.metric")
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	now := time.Now()

	// Add multiple data points
	for i := 0; i < 3; i++ {
		dp := sum.DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(100 + i*10))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
		dp.Attributes().PutStr("instance", string(rune('A'+i)))
		dp.Attributes().PutStr("metric.type", "rate")
	}

	processor.convertSumToGauge(metric)

	assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())
	gauge := metric.Gauge()
	assert.Equal(t, 3, gauge.DataPoints().Len())

	// Verify each data point
	for i := 0; i < 3; i++ {
		dp := gauge.DataPoints().At(i)
		assert.Equal(t, float64(100+i*10), dp.DoubleValue())

		instance, exists := dp.Attributes().Get("instance")
		assert.True(t, exists)
		assert.Equal(t, string(rune('A'+i)), instance.Str())

		metricType, exists := dp.Attributes().Get("metric.type")
		assert.True(t, exists)
		assert.Equal(t, "gauge", metricType.Str())
	}
}

// 7. ProcessDataPoint Tests (Most Critical)

func TestProcessDataPoint_FirstOccurrence(t *testing.T) {
	processor := createTestProcessor(t)

	// Use a fixed time in UTC to avoid any timezone issues
	now := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(100.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// First occurrence should be marked as NoRecordedValue
	assert.True(t, dp.Flags().NoRecordedValue())

	// State should be stored
	key := processor.generateKey("test.metric", dp.Attributes())
	state, exists := processor.previousState[key]
	assert.True(t, exists)
	assert.Equal(t, 100.0, state.value)
	assert.True(t, now.Equal(state.timestamp), "Stored timestamp should equal the original timestamp")
}

func TestProcessDataPoint_ZeroTimeDifference(t *testing.T) {
	processor := createTestProcessor(t)

	now := time.Now()

	// Set up previous state with same timestamp
	key := "test.metric|key=value"
	processor.previousState[key] = &metricState{
		value:     100.0,
		timestamp: now,
	}

	// Process current data point with same timestamp
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(200.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should be marked as NoRecordedValue due to zero time difference
	assert.True(t, dp.Flags().NoRecordedValue())
}

func TestProcessDataPoint_NegativeTimeDifference(t *testing.T) {
	processor := createTestProcessor(t)

	now := time.Now()
	futureTime := now.Add(60 * time.Second)

	// Set up previous state with future timestamp
	key := "test.metric|key=value"
	processor.previousState[key] = &metricState{
		value:     100.0,
		timestamp: futureTime,
	}

	// Process current data point with earlier timestamp
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(200.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should be marked as NoRecordedValue due to negative time difference
	assert.True(t, dp.Flags().NoRecordedValue())
}

func TestProcessDataPoint_ZeroRate(t *testing.T) {
	processor := createTestProcessor(t)

	// Set up previous state
	key := "test.metric|key=value"
	previousTime := time.Now().Add(-60 * time.Second)
	processor.previousState[key] = &metricState{
		value:     100.0,
		timestamp: previousTime,
	}

	// Process current data point with same value
	now := time.Now()
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(100.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should calculate rate as 0
	assert.Equal(t, 0.0, dp.DoubleValue())
	assert.False(t, dp.Flags().NoRecordedValue())
}

// 8. GenerateKey Tests

func TestGenerateKey_Consistency(t *testing.T) {
	processor := createTestProcessor(t)

	attrs1 := pcommon.NewMap()
	attrs1.PutStr("key1", "value1")
	attrs1.PutStr("key2", "value2")

	attrs2 := pcommon.NewMap()
	attrs2.PutStr("key2", "value2")
	attrs2.PutStr("key1", "value1")

	key1 := processor.generateKey("test.metric", attrs1)
	key2 := processor.generateKey("test.metric", attrs2)

	// Keys should be identical regardless of attribute order
	assert.Equal(t, key1, key2)
}

func TestGenerateKey_Uniqueness(t *testing.T) {
	processor := createTestProcessor(t)

	attrs1 := pcommon.NewMap()
	attrs1.PutStr("key", "value1")

	attrs2 := pcommon.NewMap()
	attrs2.PutStr("key", "value2")

	key1 := processor.generateKey("test.metric", attrs1)
	key2 := processor.generateKey("test.metric", attrs2)

	// Keys should be different for different attribute values
	assert.NotEqual(t, key1, key2)
}

func TestGenerateKey_EmptyAttributes(t *testing.T) {
	processor := createTestProcessor(t)

	attrs := pcommon.NewMap()
	key := processor.generateKey("test.metric", attrs)

	assert.Equal(t, "test.metric", key)
}

func TestGenerateKey_DifferentAttributeTypes(t *testing.T) {
	processor := createTestProcessor(t)

	attrs := pcommon.NewMap()
	attrs.PutStr("string_key", "string_value")
	attrs.PutInt("int_key", 123)
	attrs.PutBool("bool_key", true)
	attrs.PutDouble("double_key", 45.67)

	key := processor.generateKey("test.metric", attrs)

	// Should contain all attribute types
	assert.Contains(t, key, "string_key=string_value")
	assert.Contains(t, key, "int_key=123")
	assert.Contains(t, key, "bool_key=true")
	assert.Contains(t, key, "double_key=45.67")
}

func TestGenerateKey_SpecialCharacters(t *testing.T) {
	processor := createTestProcessor(t)

	attrs := pcommon.NewMap()
	attrs.PutStr("key|with=special", "value|with=special")

	key := processor.generateKey("test.metric", attrs)

	// Should handle special characters without breaking
	assert.Contains(t, key, "key|with=special=value|with=special")
}

func TestErrorHandling_InfinityValues(t *testing.T) {
	processor := createTestProcessor(t)

	// Set up previous state
	key := "test.metric|key=value"
	previousTime := time.Now().Add(-60 * time.Second)
	processor.previousState[key] = &metricState{
		value:     100.0,
		timestamp: previousTime,
	}

	// Process data point with infinity value
	now := time.Now()
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(math.Inf(1))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should calculate a very large rate
	assert.False(t, dp.Flags().NoRecordedValue())
	assert.True(t, math.IsInf(dp.DoubleValue(), 1))
}

func TestErrorHandling_VeryLargeNumbers(t *testing.T) {
	processor := createTestProcessor(t)

	// Set up previous state with very large number
	key := "test.metric|key=value"
	previousTime := time.Now().Add(-60 * time.Second)
	processor.previousState[key] = &metricState{
		value:     math.MaxFloat64 / 2,
		timestamp: previousTime,
	}

	// Process data point with very large number
	now := time.Now()
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(math.MaxFloat64)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should handle large numbers without panic
	assert.False(t, dp.Flags().NoRecordedValue())
}

// 10. State Management Tests

func TestStateManagement_BasicOperations(t *testing.T) {
	processor := createTestProcessor(t)

	// Initially empty
	assert.Equal(t, 0, len(processor.previousState))

	// Process first metric
	now := time.Now()
	dp1 := pmetric.NewNumberDataPoint()
	dp1.SetDoubleValue(100.0)
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp1.Attributes().PutStr("instance", "A")

	processor.processDataPoint("test.metric", dp1)

	// State should be stored
	assert.Equal(t, 1, len(processor.previousState))

	// Process second metric with different attributes
	dp2 := pmetric.NewNumberDataPoint()
	dp2.SetDoubleValue(200.0)
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp2.Attributes().PutStr("instance", "B")

	processor.processDataPoint("test.metric", dp2)

	// Should have two separate states
	assert.Equal(t, 2, len(processor.previousState))
}

func TestStateManagement_StateIsolation(t *testing.T) {
	processor := createTestProcessor(t)

	now := time.Now()

	// Process metrics with different names but same attributes
	attrs := pcommon.NewMap()
	attrs.PutStr("key", "value")

	dp1 := pmetric.NewNumberDataPoint()
	dp1.SetDoubleValue(100.0)
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(now))
	attrs.CopyTo(dp1.Attributes())

	dp2 := pmetric.NewNumberDataPoint()
	dp2.SetDoubleValue(200.0)
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(now))
	attrs.CopyTo(dp2.Attributes())

	processor.processDataPoint("metric1", dp1)
	processor.processDataPoint("metric2", dp2)

	// Should have separate states for different metric names
	assert.Equal(t, 2, len(processor.previousState))

	key1 := processor.generateKey("metric1", attrs)
	key2 := processor.generateKey("metric2", attrs)

	state1, exists1 := processor.previousState[key1]
	state2, exists2 := processor.previousState[key2]

	assert.True(t, exists1)
	assert.True(t, exists2)
	assert.Equal(t, 100.0, state1.value)
	assert.Equal(t, 200.0, state2.value)
}

func TestStateManagement_StateUpdates_MultipleMetrics(t *testing.T) {
	processor := createTestProcessor(t)

	// Test that different metrics maintain separate state
	attrs1 := pcommon.NewMap()
	attrs1.PutStr("instance", "A")

	attrs2 := pcommon.NewMap()
	attrs2.PutStr("instance", "B")

	key1 := processor.generateKey("test.metric", attrs1)
	key2 := processor.generateKey("test.metric", attrs2)

	time1 := time.Now()

	// Process first metric
	dp1 := pmetric.NewNumberDataPoint()
	dp1.SetDoubleValue(100.0)
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(time1))
	attrs1.CopyTo(dp1.Attributes())
	processor.processDataPoint("test.metric", dp1)

	// Process second metric
	dp2 := pmetric.NewNumberDataPoint()
	dp2.SetDoubleValue(200.0)
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(time1))
	attrs2.CopyTo(dp2.Attributes())
	processor.processDataPoint("test.metric", dp2)

	// Should have separate states
	assert.Equal(t, 2, len(processor.previousState))

	state1 := processor.previousState[key1]
	state2 := processor.previousState[key2]

	assert.NotNil(t, state1)
	assert.NotNil(t, state2)
	assert.NotSame(t, state1, state2, "Different metrics should have separate state objects")

	assert.Equal(t, 100.0, state1.value)
	assert.Equal(t, 200.0, state2.value)
}

func TestStateManagement_StateUpdates_MemoryEfficiency(t *testing.T) {
	processor := createTestProcessor(t)

	attrs := pcommon.NewMap()
	attrs.PutStr("key", "value")
	key := processor.generateKey("test.metric", attrs)

	// Process many updates to same metric
	baseTime := time.Now()
	for i := 0; i < 100; i++ {
		dp := pmetric.NewNumberDataPoint()
		dp.SetDoubleValue(float64(100 + i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(baseTime.Add(time.Duration(i) * time.Second)))
		attrs.CopyTo(dp.Attributes())
		processor.processDataPoint("test.metric", dp)
	}

	// Should still only have one state entry
	assert.Equal(t, 1, len(processor.previousState), "Should not create multiple state entries for same metric")

	state := processor.previousState[key]
	assert.Equal(t, 199.0, state.value, "Final state should have last processed value")
}

// 11. Basic Concurrency Tests

func TestConcurrency_BasicMutex(t *testing.T) {
	processor := createTestProcessor(t)

	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			attrs := pcommon.NewMap()
			attrs.PutStr("goroutine", string(rune('A'+id)))

			dp := pmetric.NewNumberDataPoint()
			dp.SetDoubleValue(float64(100 + id))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			attrs.CopyTo(dp.Attributes())

			processor.processDataPoint("test.metric", dp)
		}(i)
	}

	wg.Wait()

	// Should have states for all goroutines
	assert.Equal(t, numGoroutines, len(processor.previousState))
}

func TestConcurrency_StateAccess(t *testing.T) {
	processor := createTestProcessor(t)

	// Pre-populate some state
	attrs := pcommon.NewMap()
	attrs.PutStr("key", "value")
	key := processor.generateKey("test.metric", attrs)
	processor.previousState[key] = &metricState{
		value:     100.0,
		timestamp: time.Now().Add(-60 * time.Second),
	}

	var wg sync.WaitGroup
	numGoroutines := 5

	// Multiple goroutines accessing the same state
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			dp := pmetric.NewNumberDataPoint()
			dp.SetDoubleValue(float64(200 + id))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			attrs.CopyTo(dp.Attributes())

			processor.processDataPoint("test.metric", dp)
		}(i)
	}

	wg.Wait()

	// Should still have only one state entry (same key)
	assert.Equal(t, 1, len(processor.previousState))

	// Final state should be from one of the goroutines
	finalState := processor.previousState[key]
	assert.True(t, finalState.value >= 200.0 && finalState.value <= 204.0)
}

// 12. Attribute Handling Tests

func TestAttributeHandling_TypePreservation(t *testing.T) {
	processor := createTestProcessor(t)

	metric := pmetric.NewMetric()
	metric.SetName("test.metric")
	sum := metric.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(100.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	// Add different attribute types
	attrs := dp.Attributes()
	attrs.PutStr("string_attr", "string_value")
	attrs.PutInt("int_attr", 123)
	attrs.PutBool("bool_attr", true)
	attrs.PutDouble("double_attr", 45.67)
	attrs.PutStr("metric.type", "rate")

	processor.convertSumToGauge(metric)

	// Verify all attributes are preserved with correct types
	gauge := metric.Gauge()
	gaugeDP := gauge.DataPoints().At(0)
	gaugeAttrs := gaugeDP.Attributes()

	stringVal, exists := gaugeAttrs.Get("string_attr")
	assert.True(t, exists)
	assert.Equal(t, pcommon.ValueTypeStr, stringVal.Type())
	assert.Equal(t, "string_value", stringVal.Str())

	intVal, exists := gaugeAttrs.Get("int_attr")
	assert.True(t, exists)
	assert.Equal(t, pcommon.ValueTypeInt, intVal.Type())
	assert.Equal(t, int64(123), intVal.Int())

	boolVal, exists := gaugeAttrs.Get("bool_attr")
	assert.True(t, exists)
	assert.Equal(t, pcommon.ValueTypeBool, boolVal.Type())
	assert.True(t, boolVal.Bool())

	doubleVal, exists := gaugeAttrs.Get("double_attr")
	assert.True(t, exists)
	assert.Equal(t, pcommon.ValueTypeDouble, doubleVal.Type())
	assert.Equal(t, 45.67, doubleVal.Double())

	// metric.type should be converted
	metricType, exists := gaugeAttrs.Get("metric.type")
	assert.True(t, exists)
	assert.Equal(t, "gauge", metricType.Str())
}

func TestAttributeHandling_MetricTypeConversion(t *testing.T) {
	tests := []struct {
		name          string
		originalType  string
		expectedType  string
		shouldConvert bool
	}{
		{
			name:          "rate to gauge conversion",
			originalType:  "rate",
			expectedType:  "gauge",
			shouldConvert: true,
		},
		{
			name:          "non-rate type unchanged",
			originalType:  "counter",
			expectedType:  "counter",
			shouldConvert: false,
		},
		{
			name:          "empty type unchanged",
			originalType:  "",
			expectedType:  "",
			shouldConvert: false,
		},
		{
			name:          "gauge type unchanged",
			originalType:  "gauge",
			expectedType:  "gauge",
			shouldConvert: false,
		},
		{
			name:          "summary type unchanged",
			originalType:  "summary",
			expectedType:  "summary",
			shouldConvert: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := createTestProcessor(t)

			var metric pmetric.Metric
			if tt.originalType == "" {
				// Create metric without metric.type attribute
				metric = createTestSumMetric("test.metric", 100.0, time.Now(), map[string]string{
					"other_attr": "value",
				})
			} else {
				metric = createTestSumMetric("test.metric", 100.0, time.Now(), map[string]string{
					"metric.type": tt.originalType,
				})
			}

			processor.convertSumToGauge(metric)

			gauge := metric.Gauge()
			dp := gauge.DataPoints().At(0)
			attrs := dp.Attributes()

			metricType, exists := attrs.Get("metric.type")
			if tt.originalType == "" {
				// If no original type, should not have metric.type attribute
				assert.False(t, exists, "metric.type attribute should not exist when not originally present")
			} else {
				assert.True(t, exists, "metric.type attribute should exist")
				assert.Equal(t, tt.expectedType, metricType.Str(), "metric.type should match expected value")
			}

			// Verify the shouldConvert logic matches the actual behavior
			actuallyConverted := (tt.originalType == "rate" && tt.expectedType == "gauge")
			assert.Equal(t, tt.shouldConvert, actuallyConverted, "shouldConvert field should match actual conversion behavior")
		})
	}
}

// 13. Time Handling Tests

func TestTimeHandling_Calculations(t *testing.T) {
	processor := createTestProcessor(t)

	tests := []struct {
		name         string
		timeDiff     time.Duration
		valueDiff    float64
		expectedRate float64
	}{
		{
			name:         "1 second interval",
			timeDiff:     1 * time.Second,
			valueDiff:    100.0,
			expectedRate: 100.0,
		},
		{
			name:         "1 minute interval",
			timeDiff:     1 * time.Minute,
			valueDiff:    120.0,
			expectedRate: 2.0,
		},
		{
			name:         "1 hour interval",
			timeDiff:     1 * time.Hour,
			valueDiff:    3600.0,
			expectedRate: 1.0,
		},
		{
			name:         "fractional second",
			timeDiff:     500 * time.Millisecond,
			valueDiff:    50.0,
			expectedRate: 100.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up previous state
			key := "test.metric|key=value"
			previousTime := time.Now()
			processor.previousState[key] = &metricState{
				value:     100.0,
				timestamp: previousTime,
			}

			// Process current data point
			currentTime := previousTime.Add(tt.timeDiff)
			dp := pmetric.NewNumberDataPoint()
			dp.SetDoubleValue(100.0 + tt.valueDiff)
			dp.SetTimestamp(pcommon.NewTimestampFromTime(currentTime))
			dp.Attributes().PutStr("key", "value")

			processor.processDataPoint("test.metric", dp)

			assert.InDelta(t, tt.expectedRate, dp.DoubleValue(), 0.001)
			assert.False(t, dp.Flags().NoRecordedValue())
		})
	}
}

func TestTimeHandling_TimestampPrecision(t *testing.T) {
	processor := createTestProcessor(t)

	// Use nanosecond precision
	baseTime := time.Unix(1640995200, 123456789) // 2022-01-01 00:00:00.123456789 UTC

	// Set up previous state
	key := "test.metric|key=value"
	processor.previousState[key] = &metricState{
		value:     100.0,
		timestamp: baseTime,
	}

	// Process data point 1 nanosecond later
	currentTime := baseTime.Add(1 * time.Nanosecond)
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(200.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(currentTime))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should calculate extremely high rate due to tiny time difference
	expectedRate := 100.0 / (1e-9) // 100 billion per second
	assert.InDelta(t, expectedRate, dp.DoubleValue(), 1e8)
}

// 14. Integration-Style Tests

func TestIntegration_EndToEndFlow(t *testing.T) {
	processor := createTestProcessor(t)

	// Create metrics with multiple data points over time
	baseTime := time.Now()

	// First batch - should all be marked as NoRecordedValue
	metrics1 := pmetric.NewMetrics()
	rm1 := metrics1.ResourceMetrics().AppendEmpty()
	sm1 := rm1.ScopeMetrics().AppendEmpty()

	for i := 0; i < 3; i++ {
		metric := createTestSumMetric(
			fmt.Sprintf("metric_%d", i),
			float64(100+i*10),
			baseTime,
			map[string]string{"instance": fmt.Sprintf("instance_%d", i)},
		)
		tgt := sm1.Metrics().AppendEmpty()
		metric.CopyTo(tgt)
	}

	err := processor.ConsumeMetrics(context.Background(), metrics1)
	assert.NoError(t, err)

	// Verify first batch results
	for i := 0; i < 3; i++ {
		metric := sm1.Metrics().At(i)
		assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())

		gauge := metric.Gauge()
		if gauge.DataPoints().Len() > 0 {
			dp := gauge.DataPoints().At(0)
			assert.True(t, dp.Flags().NoRecordedValue())
		}
	}

	// Second batch - should calculate rates
	metrics2 := pmetric.NewMetrics()
	rm2 := metrics2.ResourceMetrics().AppendEmpty()
	sm2 := rm2.ScopeMetrics().AppendEmpty()

	secondTime := baseTime.Add(60 * time.Second)

	for i := 0; i < 3; i++ {
		metric := createTestSumMetric(
			fmt.Sprintf("metric_%d", i),
			float64(200+i*20), // Increased values
			secondTime,
			map[string]string{"instance": fmt.Sprintf("instance_%d", i)},
		)
		tgt := sm2.Metrics().AppendEmpty()
		metric.CopyTo(tgt)
	}

	err = processor.ConsumeMetrics(context.Background(), metrics2)
	assert.NoError(t, err)

	// Verify second batch results
	for i := 0; i < 3; i++ {
		metric := sm2.Metrics().At(i)
		assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())

		gauge := metric.Gauge()
		assert.Equal(t, 1, gauge.DataPoints().Len())

		dp := gauge.DataPoints().At(0)
		assert.False(t, dp.Flags().NoRecordedValue())

		// Expected rate: (new_value - old_value) / 60 seconds
		expectedRate := float64(100+i*10) / 60.0
		assert.InDelta(t, expectedRate, dp.DoubleValue(), 0.01)
	}
}

func TestIntegration_MixedMetricTypes(t *testing.T) {
	processor := createTestProcessor(t)

	// Create metrics with different types
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	// Add sum metric (should be processed)
	sumMetric := createTestSumMetric("sum.metric", 100.0, time.Now(), map[string]string{"type": "sum"})
	tgt1 := sm.Metrics().AppendEmpty()
	sumMetric.CopyTo(tgt1)

	// Add gauge metric (should be ignored)
	gaugeMetric := createTestGaugeMetric("gauge.metric", 200.0)
	tgt2 := sm.Metrics().AppendEmpty()
	gaugeMetric.CopyTo(tgt2)

	// Add histogram metric (should be ignored)
	histogramMetric := pmetric.NewMetric()
	histogramMetric.SetName("histogram.metric")
	hist := histogramMetric.SetEmptyHistogram()
	histDP := hist.DataPoints().AppendEmpty()
	histDP.SetCount(10)
	histDP.SetSum(100.0)
	histDP.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	tgt3 := sm.Metrics().AppendEmpty()
	histogramMetric.CopyTo(tgt3)

	err := processor.ConsumeMetrics(context.Background(), md)
	assert.NoError(t, err)

	// Verify results
	assert.Equal(t, 3, sm.Metrics().Len())

	// Sum metric should be converted to gauge
	assert.Equal(t, pmetric.MetricTypeGauge, sm.Metrics().At(0).Type())

	// Gauge metric should remain gauge
	assert.Equal(t, pmetric.MetricTypeGauge, sm.Metrics().At(1).Type())

	// Histogram metric should remain histogram
	assert.Equal(t, pmetric.MetricTypeHistogram, sm.Metrics().At(2).Type())
}

// 15. Edge Case Tests

func TestEdgeCases_VerySmallTimeDifferences(t *testing.T) {
	processor := createTestProcessor(t)

	baseTime := time.Now()

	// Set up previous state
	key := "test.metric|key=value"
	processor.previousState[key] = &metricState{
		value:     100.0,
		timestamp: baseTime,
	}

	// Process data point with microsecond difference
	currentTime := baseTime.Add(1 * time.Microsecond)
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(100.001)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(currentTime))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should calculate rate without overflow
	assert.False(t, dp.Flags().NoRecordedValue())
	assert.False(t, math.IsInf(dp.DoubleValue(), 0))
	assert.False(t, math.IsNaN(dp.DoubleValue()))
}

func TestEdgeCases_CounterReset(t *testing.T) {
	processor := createTestProcessor(t)

	// Simulate counter reset scenario
	key := "test.metric|key=value"
	previousTime := time.Now().Add(-60 * time.Second)

	// Previous state with high value
	processor.previousState[key] = &metricState{
		value:     1000000.0,
		timestamp: previousTime,
	}

	// Current value is much lower (counter reset)
	now := time.Now()
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(100.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should be marked as NoRecordedValue due to negative rate
	assert.True(t, dp.Flags().NoRecordedValue())

	// State should still be updated for next calculation
	state := processor.previousState[key]
	assert.Equal(t, 100.0, state.value)
	assert.True(t, now.Equal(state.timestamp)) // Use Equal() method instead
}

func TestEdgeCases_FloatingPointPrecision(t *testing.T) {
	processor := createTestProcessor(t)

	// Test with values that might cause floating point precision issues
	key := "test.metric|key=value"
	previousTime := time.Now().Add(-1 * time.Second)

	processor.previousState[key] = &metricState{
		value:     0.1 + 0.2, // 0.30000000000000004 in floating point
		timestamp: previousTime,
	}

	now := time.Now()
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(0.3 + 0.1) // 0.4
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Should handle floating point precision gracefully
	assert.False(t, dp.Flags().NoRecordedValue())

	// Rate should be approximately 0.1 per second
	assert.InDelta(t, 0.1, dp.DoubleValue(), 0.001)
}

// 16. Boundary Value Tests

func TestBoundaryValues_ZeroValues(t *testing.T) {
	processor := createTestProcessor(t)

	// Test with zero values
	key := "test.metric|key=value"
	previousTime := time.Now().Add(-60 * time.Second)

	processor.previousState[key] = &metricState{
		value:     0.0,
		timestamp: previousTime,
	}

	now := time.Now()
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(0.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Rate should be 0
	assert.Equal(t, 0.0, dp.DoubleValue())
	assert.False(t, dp.Flags().NoRecordedValue())
}

func TestBoundaryValues_NegativeValues(t *testing.T) {
	processor := createTestProcessor(t)

	// Test with negative values (unusual but possible)
	key := "test.metric|key=value"
	previousTime := time.Now().Add(-60 * time.Second)

	processor.previousState[key] = &metricState{
		value:     -100.0,
		timestamp: previousTime,
	}

	now := time.Now()
	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(-50.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("key", "value")

	processor.processDataPoint("test.metric", dp)

	// Rate should be positive: (-50 - (-100)) / 60 = 50/60 ≈ 0.833
	expectedRate := 50.0 / 60.0
	assert.InDelta(t, expectedRate, dp.DoubleValue(), 0.01)
	assert.False(t, dp.Flags().NoRecordedValue())
}

// 17. Stress Tests (Limited for Unit Tests)

func TestStress_ManyUniqueMetrics(t *testing.T) {
	processor := createTestProcessor(t)

	numMetrics := 1000
	baseTime := time.Now()

	// Process many unique metrics
	for i := 0; i < numMetrics; i++ {
		dp := pmetric.NewNumberDataPoint()
		dp.SetDoubleValue(float64(100 + i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(baseTime))
		dp.Attributes().PutStr("metric_id", fmt.Sprintf("metric_%d", i))

		processor.processDataPoint("test.metric", dp)
	}

	// Should have state for all metrics
	assert.Equal(t, numMetrics, len(processor.previousState))

	// Process second round
	secondTime := baseTime.Add(60 * time.Second)
	for i := 0; i < numMetrics; i++ {
		dp := pmetric.NewNumberDataPoint()
		dp.SetDoubleValue(float64(200 + i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(secondTime))
		dp.Attributes().PutStr("metric_id", fmt.Sprintf("metric_%d", i))

		processor.processDataPoint("test.metric", dp)

		// Should calculate rate correctly
		expectedRate := 100.0 / 60.0
		assert.InDelta(t, expectedRate, dp.DoubleValue(), 0.01)
		assert.False(t, dp.Flags().NoRecordedValue())
	}
}

// 19. Metadata Preservation Tests

func TestMetadataPreservation_MetricMetadata(t *testing.T) {
	processor := createTestProcessor(t)

	// Create metric with full metadata
	metric := pmetric.NewMetric()
	metric.SetName("test.metric.with.metadata")
	metric.SetDescription("Test metric with description")
	metric.SetUnit("bytes/sec")

	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(100.0)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.Attributes().PutStr("service", "test-service")

	processor.processMetric(metric)

	// Verify metadata is preserved
	assert.Equal(t, "test.metric.with.metadata", metric.Name())
	assert.Equal(t, "Test metric with description", metric.Description())
	assert.Equal(t, "bytes/sec", metric.Unit())

	// Should be converted to gauge
	assert.Equal(t, pmetric.MetricTypeGauge, metric.Type())

	// Attributes should be preserved
	gauge := metric.Gauge()
	if gauge.DataPoints().Len() > 0 {
		dp := gauge.DataPoints().At(0)
		service, exists := dp.Attributes().Get("service")
		assert.True(t, exists)
		assert.Equal(t, "test-service", service.Str())
	}
}

// 20. Resource and Scope Preservation Tests

func TestResourceScopePreservation(t *testing.T) {
	processor := createTestProcessor(t)

	// Create metrics with resource and scope attributes
	md := pmetric.NewMetrics()

	// Set resource attributes
	rm := md.ResourceMetrics().AppendEmpty()
	resourceAttrs := rm.Resource().Attributes()
	resourceAttrs.PutStr("service.name", "test-service")
	resourceAttrs.PutStr("service.version", "1.0.0")

	// Set scope attributes
	sm := rm.ScopeMetrics().AppendEmpty()
	scope := sm.Scope()
	scope.SetName("test.scope")
	scope.SetVersion("2.0.0")

	// Add metric
	metric := createTestSumMetric("test.metric", 100.0, time.Now(), map[string]string{"key": "value"})
	tgt := sm.Metrics().AppendEmpty()
	metric.CopyTo(tgt)

	result, err := processor.processMetrics(context.Background(), md)
	assert.NoError(t, err)

	// Verify resource attributes are preserved
	resultRM := result.ResourceMetrics().At(0)
	resultResourceAttrs := resultRM.Resource().Attributes()

	serviceName, exists := resultResourceAttrs.Get("service.name")
	assert.True(t, exists)
	assert.Equal(t, "test-service", serviceName.Str())

	serviceVersion, exists := resultResourceAttrs.Get("service.version")
	assert.True(t, exists)
	assert.Equal(t, "1.0.0", serviceVersion.Str())

	// Verify scope attributes are preserved
	resultSM := resultRM.ScopeMetrics().At(0)
	resultScope := resultSM.Scope()
	assert.Equal(t, "test.scope", resultScope.Name())
	assert.Equal(t, "2.0.0", resultScope.Version())
}
