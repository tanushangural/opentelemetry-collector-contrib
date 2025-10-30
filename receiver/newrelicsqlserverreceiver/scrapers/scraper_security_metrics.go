// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package scrapers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// SecurityScraper handles SQL Server security-level metrics collection
type SecurityScraper struct {
	connection    SQLConnectionInterface
	logger        *zap.Logger
	startTime     pcommon.Timestamp
	engineEdition int
}

// NewSecurityScraper creates a new security scraper
func NewSecurityScraper(conn SQLConnectionInterface, logger *zap.Logger, engineEdition int) *SecurityScraper {
	return &SecurityScraper{
		connection:    conn,
		logger:        logger,
		engineEdition: engineEdition,
	}
}

// ScrapeSecurityPrincipalsMetrics scrapes server principals count metrics
func (s *SecurityScraper) ScrapeSecurityPrincipalsMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics) error {
	s.logger.Debug("Scraping SQL Server security principals metrics")

	var results []models.SecurityPrincipalsModel
	if err := s.connection.Query(ctx, &results, queries.SecurityPrincipalsQuery); err != nil {
		s.logger.Error("Failed to execute security principals query", zap.Error(err))
		return fmt.Errorf("failed to execute security principals query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from security principals query")
		return fmt.Errorf("no results returned from security principals query")
	}

	result := results[0]
	if result.ServerPrincipalsCount == nil {
		s.logger.Error("Security principals metric is null - invalid query result")
		return fmt.Errorf("security principals metric is null in query result")
	}

	if err := s.processSecurityPrincipalsMetrics(result, scopeMetrics); err != nil {
		s.logger.Error("Failed to process security principals metrics", zap.Error(err))
		return fmt.Errorf("failed to process security principals metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped security principals metrics")
	return nil
}

// processSecurityPrincipalsMetrics converts security principals metrics to OpenTelemetry format
func (s *SecurityScraper) processSecurityPrincipalsMetrics(result models.SecurityPrincipalsModel, scopeMetrics pmetric.ScopeMetrics) error {
	resultValue := reflect.ValueOf(result)
	resultType := reflect.TypeOf(result)

	for i := 0; i < resultValue.NumField(); i++ {
		field := resultValue.Field(i)
		fieldType := resultType.Field(i)

		if field.Kind() == reflect.Ptr && field.IsNil() {
			continue
		}

		metricName := fieldType.Tag.Get("metric_name")
		sourceType := fieldType.Tag.Get("source_type")
		description := fieldType.Tag.Get("description")
		unit := fieldType.Tag.Get("unit")

		if metricName == "" {
			continue
		}

		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName(metricName)
		if description != "" {
			metric.SetDescription(description)
		}
		if unit != "" {
			metric.SetUnit(unit)
		}

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dataPoint.SetStartTimestamp(s.startTime)

		fieldValue := field
		if field.Kind() == reflect.Ptr {
			fieldValue = field.Elem()
		}

		if fieldValue.Kind() == reflect.Int64 {
			dataPoint.SetIntValue(fieldValue.Int())
		} else if fieldValue.Kind() == reflect.Float64 {
			dataPoint.SetDoubleValue(fieldValue.Float())
		}

		dataPoint.Attributes().PutStr("metric.type", sourceType)
	}
	return nil
}

// ScrapeSecurityRoleMembersMetrics scrapes server role membership count metrics
func (s *SecurityScraper) ScrapeSecurityRoleMembersMetrics(ctx context.Context, scopeMetrics pmetric.ScopeMetrics) error {
	s.logger.Debug("Scraping SQL Server security role members metrics")

	var results []models.SecurityRoleMembersModel
	if err := s.connection.Query(ctx, &results, queries.SecurityRoleMembersQuery); err != nil {
		s.logger.Error("Failed to execute security role members query", zap.Error(err))
		return fmt.Errorf("failed to execute security role members query: %w", err)
	}

	if len(results) == 0 {
		s.logger.Warn("No results returned from security role members query")
		return fmt.Errorf("no results returned from security role members query")
	}

	result := results[0]
	if result.ServerRoleMembersCount == nil {
		s.logger.Error("Security role members metric is null - invalid query result")
		return fmt.Errorf("security role members metric is null in query result")
	}

	if err := s.processSecurityRoleMembersMetrics(result, scopeMetrics); err != nil {
		s.logger.Error("Failed to process security role members metrics", zap.Error(err))
		return fmt.Errorf("failed to process security role members metrics: %w", err)
	}

	s.logger.Debug("Successfully scraped security role members metrics")
	return nil
}

// processSecurityRoleMembersMetrics converts security role members metrics to OpenTelemetry format
func (s *SecurityScraper) processSecurityRoleMembersMetrics(result models.SecurityRoleMembersModel, scopeMetrics pmetric.ScopeMetrics) error {
	resultValue := reflect.ValueOf(result)
	resultType := reflect.TypeOf(result)

	for i := 0; i < resultValue.NumField(); i++ {
		field := resultValue.Field(i)
		fieldType := resultType.Field(i)

		if field.Kind() == reflect.Ptr && field.IsNil() {
			continue
		}

		metricName := fieldType.Tag.Get("metric_name")
		sourceType := fieldType.Tag.Get("source_type")
		description := fieldType.Tag.Get("description")
		unit := fieldType.Tag.Get("unit")

		if metricName == "" {
			continue
		}

		metric := scopeMetrics.Metrics().AppendEmpty()
		metric.SetName(metricName)
		if description != "" {
			metric.SetDescription(description)
		}
		if unit != "" {
			metric.SetUnit(unit)
		}

		gauge := metric.SetEmptyGauge()
		dataPoint := gauge.DataPoints().AppendEmpty()
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dataPoint.SetStartTimestamp(s.startTime)

		fieldValue := field
		if field.Kind() == reflect.Ptr {
			fieldValue = field.Elem()
		}

		if fieldValue.Kind() == reflect.Int64 {
			dataPoint.SetIntValue(fieldValue.Int())
		} else if fieldValue.Kind() == reflect.Float64 {
			dataPoint.SetDoubleValue(fieldValue.Float())
		}

		dataPoint.Attributes().PutStr("metric.type", sourceType)
	}
	return nil
}
