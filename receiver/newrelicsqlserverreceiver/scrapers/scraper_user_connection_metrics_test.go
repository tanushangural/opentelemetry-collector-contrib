// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/queries"
)

// MockUserConnectionSQLInterface provides a mock implementation for testing user connection metrics
type MockUserConnectionSQLInterface struct {
	queryResults map[string]interface{}
	queryError   error
}

func (m *MockUserConnectionSQLInterface) Query(ctx context.Context, dest interface{}, query string) error {
	if m.queryError != nil {
		return m.queryError
	}

	// For testing, use the first available result of the matching type
	for _, results := range m.queryResults {
		switch v := dest.(type) {
		case *[]models.UserConnectionStatusMetrics:
			if mockResults, ok := results.([]models.UserConnectionStatusMetrics); ok {
				*v = mockResults
				return nil
			}
		case *[]models.LoginLogoutMetrics:
			if mockResults, ok := results.([]models.LoginLogoutMetrics); ok {
				*v = mockResults
				return nil
			}
		case *[]models.LoginLogoutSummary:
			if mockResults, ok := results.([]models.LoginLogoutSummary); ok {
				*v = mockResults
				return nil
			}
		case *[]models.FailedLoginMetrics:
			if mockResults, ok := results.([]models.FailedLoginMetrics); ok {
				*v = mockResults
				return nil
			}
		case *[]models.FailedLoginSummary:
			if mockResults, ok := results.([]models.FailedLoginSummary); ok {
				*v = mockResults
				return nil
			}
		case *[]models.UserConnectionStatusSummary:
			if mockResults, ok := results.([]models.UserConnectionStatusSummary); ok {
				*v = mockResults
				return nil
			}
		case *[]models.UserConnectionUtilization:
			if mockResults, ok := results.([]models.UserConnectionUtilization); ok {
				*v = mockResults
				return nil
			}
		case *[]models.UserConnectionByClientMetrics:
			if mockResults, ok := results.([]models.UserConnectionByClientMetrics); ok {
				*v = mockResults
				return nil
			}
		case *[]models.UserConnectionClientSummary:
			if mockResults, ok := results.([]models.UserConnectionClientSummary); ok {
				*v = mockResults
				return nil
			}
		}
	}

	return nil
}

func (m *MockUserConnectionSQLInterface) Ping(ctx context.Context) error {
	return nil
}

// Helper function to create string pointer
func stringPtr(s string) *string { return &s }

func TestNewUserConnectionScraper(t *testing.T) {
	logger := zap.NewNop()
	mockConn := &MockUserConnectionSQLInterface{}
	engineEdition := queries.StandardSQLServerEngineEdition

	scraper := NewUserConnectionScraper(mockConn, logger, engineEdition)

	assert.NotNil(t, scraper)
	assert.Equal(t, mockConn, scraper.connection)
	assert.Equal(t, logger, scraper.logger)
	assert.Equal(t, engineEdition, scraper.engineEdition)
	assert.NotEqual(t, pcommon.Timestamp(0), scraper.startTime)
}

func TestScrapeUserConnectionStatusMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.UserConnectionStatusMetrics
		mockError      error
		engineEdition  int
		expectError    bool
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "successful_status_metrics_scraping",
			mockResults: []models.UserConnectionStatusMetrics{
				{
					Status:       "running",
					SessionCount: int64Ptr(5),
				},
				{
					Status:       "sleeping",
					SessionCount: int64Ptr(10),
				},
			},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 2,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 2, metrics.Metrics().Len())

				metric1 := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.status.count", metric1.Name())
				assert.Equal(t, "Number of user connections by status", metric1.Description())
				assert.Equal(t, "1", metric1.Unit())
				assert.Equal(t, pmetric.MetricTypeGauge, metric1.Type())

				dataPoint1 := metric1.Gauge().DataPoints().At(0)
				assert.Equal(t, int64(5), dataPoint1.IntValue())

				attrs1 := dataPoint1.Attributes()
				status, exists := attrs1.Get("status")
				assert.True(t, exists)
				assert.Equal(t, "running", status.Str())

				sourceType, exists := attrs1.Get("source_type")
				assert.True(t, exists)
				assert.Equal(t, "user_connections", sourceType.Str())
			},
		},
		{
			name:          "no_results_found",
			mockResults:   []models.UserConnectionStatusMetrics{},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 0,
		},
		{
			name:          "database_query_error",
			mockError:     errors.New("database connection failed"),
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   true,
			expectMetrics: 0,
		},
		{
			name: "nil_session_count",
			mockResults: []models.UserConnectionStatusMetrics{
				{
					Status:       "running",
					SessionCount: nil,
				},
			},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
				queryError: tt.mockError,
			}

			scraper := NewUserConnectionScraper(mockConn, logger, tt.engineEdition)

			// Mock the getQueryForMetric function by setting up results with a known query
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeUserConnectionStatusMetrics(context.Background(), scopeMetrics)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeLoginLogoutMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.LoginLogoutMetrics
		mockError      error
		engineEdition  int
		expectError    bool
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "successful_login_logout_metrics",
			mockResults: []models.LoginLogoutMetrics{
				{
					CounterName: "Logins/sec",
					CntrValue:   int64Ptr(25),
					Username:    stringPtr("testuser"),
					SourceIP:    stringPtr("192.168.1.100"),
				},
				{
					CounterName: "Logouts/sec",
					CntrValue:   int64Ptr(10),
					Username:    stringPtr("testuser2"),
					SourceIP:    stringPtr("192.168.1.101"),
				},
			},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 2,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 2, metrics.Metrics().Len())

				metric1 := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.authentication.rate", metric1.Name())
				assert.Equal(t, "SQL Server authentication rate per second (logins and logouts)", metric1.Description())
				assert.Equal(t, "1/s", metric1.Unit())
				assert.Equal(t, pmetric.MetricTypeGauge, metric1.Type())

				dataPoint1 := metric1.Gauge().DataPoints().At(0)
				assert.Equal(t, int64(25), dataPoint1.IntValue())

				attrs1 := dataPoint1.Attributes()
				counterName, exists := attrs1.Get("counter_name")
				assert.True(t, exists)
				assert.Equal(t, "Logins/sec", counterName.Str())

				username, exists := attrs1.Get("username")
				assert.True(t, exists)
				assert.Equal(t, "testuser", username.Str())

				sourceIP, exists := attrs1.Get("source_ip")
				assert.True(t, exists)
				assert.Equal(t, "192.168.1.100", sourceIP.Str())
			},
		},
		{
			name:          "no_authentication_activity",
			mockResults:   []models.LoginLogoutMetrics{},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 0,
		},
		{
			name:          "query_execution_error",
			mockError:     errors.New("failed to execute query"),
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   true,
			expectMetrics: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
				queryError: tt.mockError,
			}

			scraper := NewUserConnectionScraper(mockConn, logger, tt.engineEdition)
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeLoginLogoutMetrics(context.Background(), scopeMetrics)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeLoginLogoutSummaryMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.LoginLogoutSummary
		mockError      error
		engineEdition  int
		expectError    bool
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "successful_summary_metrics",
			mockResults: []models.LoginLogoutSummary{
				{
					LoginsPerSec:        int64Ptr(50),
					LogoutsPerSec:       int64Ptr(30),
					TotalAuthActivity:   int64Ptr(80),
					ConnectionChurnRate: float64Ptr(60.0),
					Username:            stringPtr("admin"),
					SourceIP:            stringPtr("10.0.0.1"),
				},
			},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 4, // All 4 metrics should be created
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 4, metrics.Metrics().Len())

				// Verify logins per sec metric
				loginsMetric := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.authentication.logins_per_sec", loginsMetric.Name())
				assert.Equal(t, "SQL Server login rate per second", loginsMetric.Description())
				assert.Equal(t, "1/s", loginsMetric.Unit())
				assert.Equal(t, int64(50), loginsMetric.Gauge().DataPoints().At(0).IntValue())

				// Verify churn rate metric (should be the last one)
				churnMetric := metrics.Metrics().At(3)
				assert.Equal(t, "sqlserver.user_connections.authentication.churn_rate", churnMetric.Name())
				assert.Equal(t, "%", churnMetric.Unit())
				assert.Equal(t, 60.0, churnMetric.Gauge().DataPoints().At(0).DoubleValue())
			},
		},
		{
			name: "zero_values_filtered_out",
			mockResults: []models.LoginLogoutSummary{
				{
					LoginsPerSec:        int64Ptr(0), // Should be filtered out
					LogoutsPerSec:       int64Ptr(5),
					TotalAuthActivity:   int64Ptr(0),     // Should be filtered out
					ConnectionChurnRate: float64Ptr(0.0), // Should be included (>= 0)
				},
			},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 2, // Only logouts and churn rate
		},
		{
			name:          "no_summary_data",
			mockResults:   []models.LoginLogoutSummary{},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
				queryError: tt.mockError,
			}

			scraper := NewUserConnectionScraper(mockConn, logger, tt.engineEdition)
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeLoginLogoutSummaryMetrics(context.Background(), scopeMetrics)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeFailedLoginMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.FailedLoginMetrics
		mockError      error
		engineEdition  int
		expectError    bool
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "standard_sql_server_failed_logins",
			mockResults: []models.FailedLoginMetrics{
				{
					LogDate:     stringPtr("2023-10-15 10:30:00"),
					ProcessInfo: stringPtr("Logon"),
					Text:        stringPtr("Login failed for user 'baduser'"),
					Username:    stringPtr("baduser"),
					SourceIP:    stringPtr("192.168.1.200"),
				},
			},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 1,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 1, metrics.Metrics().Len())

				metric := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.authentication.failed_login_event", metric.Name())
				assert.Equal(t, "SQL Server failed login attempt event", metric.Description())
				assert.Equal(t, "1", metric.Unit())
				assert.Equal(t, int64(1), metric.Gauge().DataPoints().At(0).IntValue())

				attrs := metric.Gauge().DataPoints().At(0).Attributes()
				logDate, exists := attrs.Get("log_date")
				assert.True(t, exists)
				assert.Equal(t, "2023-10-15 10:30:00", logDate.Str())

				errorText, exists := attrs.Get("error_text")
				assert.True(t, exists)
				assert.Equal(t, "Login failed for user 'baduser'", errorText.Str())
			},
		},
		{
			name: "azure_sql_database_failed_logins",
			mockResults: []models.FailedLoginMetrics{
				{
					EventType:   stringPtr("connection_failed"),
					Description: stringPtr("Login failed"),
					StartTime:   stringPtr("2023-10-15T10:30:00Z"),
					ClientIP:    stringPtr("203.0.113.1"),
					Username:    stringPtr("testuser"),
				},
			},
			engineEdition: queries.AzureSQLDatabaseEngineEdition,
			expectError:   false,
			expectMetrics: 1,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 1, metrics.Metrics().Len())

				metric := metrics.Metrics().At(0)
				attrs := metric.Gauge().DataPoints().At(0).Attributes()

				eventType, exists := attrs.Get("event_type")
				assert.True(t, exists)
				assert.Equal(t, "connection_failed", eventType.Str())

				clientIP, exists := attrs.Get("client_ip")
				assert.True(t, exists)
				assert.Equal(t, "203.0.113.1", clientIP.Str())
			},
		},
		{
			name:          "no_failed_logins",
			mockResults:   []models.FailedLoginMetrics{},
			engineEdition: queries.StandardSQLServerEngineEdition,
			expectError:   false,
			expectMetrics: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
				queryError: tt.mockError,
			}

			scraper := NewUserConnectionScraper(mockConn, logger, tt.engineEdition)
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeFailedLoginMetrics(context.Background(), scopeMetrics)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeFailedLoginSummaryMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.FailedLoginSummary
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "complete_failed_login_summary",
			mockResults: []models.FailedLoginSummary{
				{
					TotalFailedLogins:   int64Ptr(100),
					RecentFailedLogins:  int64Ptr(15),
					UniqueFailedUsers:   int64Ptr(5),
					UniqueFailedSources: int64Ptr(3),
					Username:            stringPtr("security_audit"),
					SourceIP:            stringPtr("10.0.0.100"),
				},
			},
			expectMetrics: 4,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 4, metrics.Metrics().Len())

				// Check total failed logins metric
				totalMetric := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.authentication.total_failed_logins", totalMetric.Name())
				assert.Equal(t, int64(100), totalMetric.Gauge().DataPoints().At(0).IntValue())

				// Verify common attributes are set
				attrs := totalMetric.Gauge().DataPoints().At(0).Attributes()
				username, exists := attrs.Get("username")
				assert.True(t, exists)
				assert.Equal(t, "security_audit", username.Str())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
			}

			scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeFailedLoginSummaryMetrics(context.Background(), scopeMetrics)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeUserConnectionSummaryMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.UserConnectionStatusSummary
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "complete_connection_summary",
			mockResults: []models.UserConnectionStatusSummary{
				{
					TotalUserConnections: int64Ptr(50),
					SleepingConnections:  int64Ptr(30),
					RunningConnections:   int64Ptr(10),
					SuspendedConnections: int64Ptr(5),
					RunnableConnections:  int64Ptr(3),
					DormantConnections:   int64Ptr(2),
				},
			},
			expectMetrics: 6,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 6, metrics.Metrics().Len())

				// Check total connections metric
				totalMetric := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.total", totalMetric.Name())
				assert.Equal(t, "connections", totalMetric.Unit())
				assert.Equal(t, int64(50), totalMetric.Gauge().DataPoints().At(0).IntValue())

				// Check sleeping connections metric
				sleepingMetric := metrics.Metrics().At(1)
				assert.Equal(t, "sqlserver.user_connections.sleeping", sleepingMetric.Name())
				assert.Equal(t, int64(30), sleepingMetric.Gauge().DataPoints().At(0).IntValue())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
			}

			scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeUserConnectionSummaryMetrics(context.Background(), scopeMetrics)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeUserConnectionUtilizationMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.UserConnectionUtilization
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "complete_utilization_metrics",
			mockResults: []models.UserConnectionUtilization{
				{
					ActiveConnectionRatio:  float64Ptr(0.75),
					IdleConnectionRatio:    float64Ptr(0.20),
					WaitingConnectionRatio: float64Ptr(0.05),
					ConnectionEfficiency:   float64Ptr(0.85),
				},
			},
			expectMetrics: 4,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 4, metrics.Metrics().Len())

				// Check active ratio metric
				activeMetric := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.utilization.active_ratio", activeMetric.Name())
				assert.Equal(t, "ratio", activeMetric.Unit())
				assert.Equal(t, 0.75, activeMetric.Gauge().DataPoints().At(0).DoubleValue())

				// Check efficiency metric
				efficiencyMetric := metrics.Metrics().At(3)
				assert.Equal(t, "sqlserver.user_connections.utilization.efficiency", efficiencyMetric.Name())
				assert.Equal(t, 0.85, efficiencyMetric.Gauge().DataPoints().At(0).DoubleValue())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
			}

			scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeUserConnectionUtilizationMetrics(context.Background(), scopeMetrics)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeUserConnectionByClientMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.UserConnectionByClientMetrics
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "client_connection_breakdown",
			mockResults: []models.UserConnectionByClientMetrics{
				{
					HostName:        "web-server-01",
					ProgramName:     "SQLCMD",
					ConnectionCount: int64Ptr(5),
				},
				{
					HostName:        "app-server-02",
					ProgramName:     "MyApp.exe",
					ConnectionCount: int64Ptr(12),
				},
			},
			expectMetrics: 2,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 2, metrics.Metrics().Len())

				// Check first client metric
				clientMetric := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.client.count", clientMetric.Name())
				assert.Equal(t, "connections", clientMetric.Unit())
				assert.Equal(t, int64(5), clientMetric.Gauge().DataPoints().At(0).IntValue())

				attrs := clientMetric.Gauge().DataPoints().At(0).Attributes()
				hostName, exists := attrs.Get("host_name")
				assert.True(t, exists)
				assert.Equal(t, "web-server-01", hostName.Str())

				programName, exists := attrs.Get("program_name")
				assert.True(t, exists)
				assert.Equal(t, "SQLCMD", programName.Str())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
			}

			scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeUserConnectionByClientMetrics(context.Background(), scopeMetrics)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeUserConnectionClientSummaryMetrics(t *testing.T) {
	tests := []struct {
		name           string
		mockResults    []models.UserConnectionClientSummary
		expectMetrics  int
		validateFields func(t *testing.T, metrics pmetric.ScopeMetrics)
	}{
		{
			name: "complete_client_summary",
			mockResults: []models.UserConnectionClientSummary{
				{
					UniqueHosts:               int64Ptr(10),
					UniquePrograms:            int64Ptr(8),
					TopHostConnectionCount:    int64Ptr(25),
					TopProgramConnectionCount: int64Ptr(30),
					HostsWithMultiplePrograms: int64Ptr(3),
					ProgramsFromMultipleHosts: int64Ptr(2),
				},
			},
			expectMetrics: 6,
			validateFields: func(t *testing.T, metrics pmetric.ScopeMetrics) {
				require.Equal(t, 6, metrics.Metrics().Len())

				// Check unique hosts metric
				hostsMetric := metrics.Metrics().At(0)
				assert.Equal(t, "sqlserver.user_connections.client.unique_hosts", hostsMetric.Name())
				assert.Equal(t, "hosts", hostsMetric.Unit())
				assert.Equal(t, int64(10), hostsMetric.Gauge().DataPoints().At(0).IntValue())

				// Check top program connections metric
				topProgramMetric := metrics.Metrics().At(3)
				assert.Equal(t, "sqlserver.user_connections.client.top_program_connections", topProgramMetric.Name())
				assert.Equal(t, int64(30), topProgramMetric.Gauge().DataPoints().At(0).IntValue())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			mockConn := &MockUserConnectionSQLInterface{
				queryResults: map[string]interface{}{
					"test_query": tt.mockResults,
				},
			}

			scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)
			scopeMetrics := pmetric.NewScopeMetrics()

			err := scraper.ScrapeUserConnectionClientSummaryMetrics(context.Background(), scopeMetrics)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectMetrics, scopeMetrics.Metrics().Len())

			if tt.validateFields != nil {
				tt.validateFields(t, scopeMetrics)
			}
		})
	}
}

func TestScrapeUserConnectionStatsMetrics(t *testing.T) {
	logger := zap.NewNop()
	mockConn := &MockUserConnectionSQLInterface{
		queryResults: map[string]interface{}{
			"test_query": []models.UserConnectionStatusSummary{
				{
					TotalUserConnections: int64Ptr(100),
				},
			},
		},
	}

	scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)
	scopeMetrics := pmetric.NewScopeMetrics()

	err := scraper.ScrapeUserConnectionStatsMetrics(context.Background(), scopeMetrics)

	// This test should pass as it's a composite function calling other scraping methods
	assert.NoError(t, err)
}

func TestGetQueryForMetric(t *testing.T) {
	logger := zap.NewNop()
	mockConn := &MockUserConnectionSQLInterface{}
	scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)

	// This tests the internal getQueryForMetric method indirectly
	// Since it's a private method, we test it through the public scraping methods
	scopeMetrics := pmetric.NewScopeMetrics()

	// Test that a query exists for the standard engine edition
	err := scraper.ScrapeUserConnectionStatusMetrics(context.Background(), scopeMetrics)

	// Should not return an error for valid engine edition with existing queries
	assert.NoError(t, err)
}

func TestProcessingWithNilValues(t *testing.T) {
	logger := zap.NewNop()
	mockConn := &MockUserConnectionSQLInterface{
		queryResults: map[string]interface{}{
			"test_query": []models.UserConnectionStatusMetrics{
				{
					Status:       "running",
					SessionCount: nil, // This should be skipped
				},
			},
		},
	}

	scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)
	scopeMetrics := pmetric.NewScopeMetrics()

	err := scraper.ScrapeUserConnectionStatusMetrics(context.Background(), scopeMetrics)

	assert.NoError(t, err)
	assert.Equal(t, 0, scopeMetrics.Metrics().Len()) // No metrics should be created for nil values
}

func TestTimestampConsistency(t *testing.T) {
	logger := zap.NewNop()
	mockConn := &MockUserConnectionSQLInterface{
		queryResults: map[string]interface{}{
			"test_query": []models.UserConnectionStatusMetrics{
				{
					Status:       "running",
					SessionCount: int64Ptr(5),
				},
			},
		},
	}

	scraper := NewUserConnectionScraper(mockConn, logger, queries.StandardSQLServerEngineEdition)
	scopeMetrics := pmetric.NewScopeMetrics()

	err := scraper.ScrapeUserConnectionStatusMetrics(context.Background(), scopeMetrics)

	assert.NoError(t, err)
	require.Equal(t, 1, scopeMetrics.Metrics().Len())

	dataPoint := scopeMetrics.Metrics().At(0).Gauge().DataPoints().At(0)

	// Verify timestamp is set and reasonable (within last 10 seconds)
	now := time.Now()
	timestamp := dataPoint.Timestamp().AsTime()
	assert.True(t, timestamp.Before(now.Add(time.Second)))
	assert.True(t, timestamp.After(now.Add(-10*time.Second)))
}
