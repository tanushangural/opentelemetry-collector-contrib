// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmssqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmssqlreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// WaitStat represents a wait statistic
type WaitStat struct {
	WaitType          string
	Category          string
	WaitingTasksCount int64
	WaitTimeMs        int64
	MaxWaitTimeMs     int64
}

// LockStat represents a lock statistic
type LockStat struct {
	LockType      string
	LockResource  string
	TimeoutCount  int64
	WaitCount     int64
	WaitTimeMs    int64
}

func (m *mssqlScraper) getInstanceName(ctx context.Context) (string, error) {
	var instanceName sql.NullString
	query := "SELECT ISNULL(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER')"
	
	err := m.db.QueryRowContext(ctx, query).Scan(&instanceName)
	if err != nil {
		return "", err
	}
	
	if instanceName.Valid {
		return instanceName.String, nil
	}
	return "MSSQLSERVER", nil
}

func (m *mssqlScraper) getBufferCacheHitRatio(ctx context.Context) (float64, error) {
	query := `
		SELECT 
			(a.cntr_value * 1.0 / b.cntr_value) * 100.0 as buffer_cache_hit_ratio
		FROM sys.dm_os_performance_counters a 
		JOIN sys.dm_os_performance_counters b ON a.object_name = b.object_name
		WHERE a.counter_name = 'Buffer cache hit ratio'
		AND b.counter_name = 'Buffer cache hit ratio base'
		AND a.object_name LIKE '%Buffer Manager%'`
	
	var ratio float64
	err := m.db.QueryRowContext(ctx, query).Scan(&ratio)
	return ratio, err
}

func (m *mssqlScraper) getPageLifeExpectancy(ctx context.Context) (int64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Page life expectancy'
		AND object_name LIKE '%Buffer Manager%'`
	
	var value int64
	err := m.db.QueryRowContext(ctx, query).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getBatchRequestsPerSec(ctx context.Context) (float64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Batch Requests/sec'
		AND object_name LIKE '%SQL Statistics%'`
	
	var value float64
	err := m.db.QueryRowContext(ctx, query).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getSQLCompilationsPerSec(ctx context.Context) (float64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'SQL Compilations/sec'
		AND object_name LIKE '%SQL Statistics%'`
	
	var value float64
	err := m.db.QueryRowContext(ctx, query).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getSQLRecompilationsPerSec(ctx context.Context) (float64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'SQL Re-Compilations/sec'
		AND object_name LIKE '%SQL Statistics%'`
	
	var value float64
	err := m.db.QueryRowContext(ctx, query).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getProcessesBlocked(ctx context.Context) (int64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Processes blocked'
		AND object_name LIKE '%General Statistics%'`
	
	var value int64
	err := m.db.QueryRowContext(ctx, query).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getUserConnections(ctx context.Context) (int64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'User Connections'
		AND object_name LIKE '%General Statistics%'`
	
	var value int64
	err := m.db.QueryRowContext(ctx, query).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getDatabaseList(ctx context.Context) ([]string, error) {
	query := `
		SELECT name 
		FROM sys.databases 
		WHERE state_desc = 'ONLINE' 
		AND database_id > 4`  // Skip system databases
	
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			continue
		}
		databases = append(databases, dbName)
	}
	
	return databases, rows.Err()
}

func (m *mssqlScraper) getDatabaseDataFileSize(ctx context.Context, dbName string) (int64, error) {
	query := fmt.Sprintf(`
		USE [%s];
		SELECT SUM(CAST(size AS BIGINT) * 8192) as data_file_size
		FROM sys.database_files 
		WHERE type_desc = 'ROWS'`, dbName)
	
	var size int64
	err := m.db.QueryRowContext(ctx, query).Scan(&size)
	return size, err
}

func (m *mssqlScraper) getDatabaseLogFileSize(ctx context.Context, dbName string) (int64, error) {
	query := fmt.Sprintf(`
		USE [%s];
		SELECT SUM(CAST(size AS BIGINT) * 8192) as log_file_size
		FROM sys.database_files 
		WHERE type_desc = 'LOG'`, dbName)
	
	var size int64
	err := m.db.QueryRowContext(ctx, query).Scan(&size)
	return size, err
}

func (m *mssqlScraper) getDatabaseLogFileUsedSize(ctx context.Context, dbName string) (int64, error) {
	query := fmt.Sprintf(`
		USE [%s];
		SELECT SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS BIGINT) * 8192) as used_size
		FROM sys.database_files 
		WHERE type_desc = 'LOG'`, dbName)
	
	var size int64
	err := m.db.QueryRowContext(ctx, query).Scan(&size)
	return size, err
}

func (m *mssqlScraper) getDatabaseLogFileUsedPercentage(ctx context.Context, dbName string) (float64, error) {
	query := fmt.Sprintf(`
		USE [%s];
		SELECT 
			CASE 
				WHEN SUM(CAST(size AS BIGINT)) = 0 THEN 0
				ELSE (SUM(CAST(FILEPROPERTY(name, 'SpaceUsed') AS BIGINT)) * 100.0) / SUM(CAST(size AS BIGINT))
			END as used_percentage
		FROM sys.database_files 
		WHERE type_desc = 'LOG'`, dbName)
	
	var percentage float64
	err := m.db.QueryRowContext(ctx, query).Scan(&percentage)
	return percentage, err
}

func (m *mssqlScraper) getDatabaseTransactionsPerSec(ctx context.Context, dbName string) (float64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Transactions/sec'
		AND instance_name = ?
		AND object_name LIKE '%Databases%'`
	
	var value float64
	err := m.db.QueryRowContext(ctx, query, dbName).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getDatabaseActiveConnections(ctx context.Context, dbName string) (int64, error) {
	query := `
		SELECT COUNT(*) 
		FROM sys.dm_exec_sessions 
		WHERE database_id = DB_ID(?) 
		AND is_user_process = 1`
	
	var count int64
	err := m.db.QueryRowContext(ctx, query, dbName).Scan(&count)
	return count, err
}

func (m *mssqlScraper) getWaitStats(ctx context.Context) ([]WaitStat, error) {
	query := `
		SELECT 
			wait_type,
			CASE 
				WHEN wait_type LIKE '%IO%' OR wait_type LIKE '%READ%' OR wait_type LIKE '%WRITE%' THEN 'IO'
				WHEN wait_type LIKE '%LOCK%' OR wait_type LIKE '%LATCH%' THEN 'Locking'
				WHEN wait_type LIKE '%CPU%' OR wait_type LIKE '%SOS_SCHEDULER%' THEN 'CPU'
				WHEN wait_type LIKE '%NETWORK%' OR wait_type LIKE '%ASYNC%' THEN 'Network'
				WHEN wait_type LIKE '%MEMORY%' OR wait_type LIKE '%RESOURCE%' THEN 'Memory'
				ELSE 'Other'
			END as category,
			waiting_tasks_count,
			wait_time_ms,
			max_wait_time_ms
		FROM sys.dm_os_wait_stats
		WHERE wait_time_ms > 0
		AND wait_type NOT IN (
			'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
			'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR',
			'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH',
			'XE_TIMER_EVENT', 'BROKER_TO_FLUSH', 'BROKER_TASK_STOP',
			'CLR_MANUAL_EVENT', 'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE',
			'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT', 'XE_DISPATCHER_JOIN'
		)
		ORDER BY wait_time_ms DESC`
	
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var waitStats []WaitStat
	for rows.Next() {
		var stat WaitStat
		if err := rows.Scan(&stat.WaitType, &stat.Category, &stat.WaitingTasksCount, &stat.WaitTimeMs, &stat.MaxWaitTimeMs); err != nil {
			continue
		}
		waitStats = append(waitStats, stat)
	}
	
	return waitStats, rows.Err()
}

func (m *mssqlScraper) getLockStats(ctx context.Context) ([]LockStat, error) {
	query := `
		SELECT 
			counter_name as lock_type,
			instance_name as lock_resource,
			cntr_value as timeout_count,
			0 as wait_count,
			0 as wait_time_ms
		FROM sys.dm_os_performance_counters 
		WHERE object_name LIKE '%Locks%'
		AND counter_name IN ('Lock Timeouts/sec', 'Lock Timeouts (timeout > 0)/sec')
		AND instance_name != '_Total'
		
		UNION ALL
		
		SELECT 
			counter_name as lock_type,
			instance_name as lock_resource,
			0 as timeout_count,
			cntr_value as wait_count,
			0 as wait_time_ms
		FROM sys.dm_os_performance_counters 
		WHERE object_name LIKE '%Locks%'
		AND counter_name = 'Lock Waits/sec'
		AND instance_name != '_Total'
		
		UNION ALL
		
		SELECT 
			counter_name as lock_type,
			instance_name as lock_resource,
			0 as timeout_count,
			0 as wait_count,
			cntr_value as wait_time_ms
		FROM sys.dm_os_performance_counters 
		WHERE object_name LIKE '%Locks%'
		AND counter_name = 'Average Wait Time (ms)'
		AND instance_name != '_Total'`
	
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	lockStatsMap := make(map[string]*LockStat)
	
	for rows.Next() {
		var lockType, lockResource string
		var timeoutCount, waitCount, waitTimeMs int64
		
		if err := rows.Scan(&lockType, &lockResource, &timeoutCount, &waitCount, &waitTimeMs); err != nil {
			continue
		}
		
		key := lockType + "|" + lockResource
		if stat, exists := lockStatsMap[key]; exists {
			stat.TimeoutCount += timeoutCount
			stat.WaitCount += waitCount
			stat.WaitTimeMs += waitTimeMs
		} else {
			lockStatsMap[key] = &LockStat{
				LockType:     lockType,
				LockResource: lockResource,
				TimeoutCount: timeoutCount,
				WaitCount:    waitCount,
				WaitTimeMs:   waitTimeMs,
			}
		}
	}
	
	var lockStats []LockStat
	for _, stat := range lockStatsMap {
		lockStats = append(lockStats, *stat)
	}
	
	return lockStats, rows.Err()
}

func (m *mssqlScraper) getIndexPageSplitsPerSec(ctx context.Context, dbName string) (float64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Page Splits/sec'
		AND instance_name = ?
		AND object_name LIKE '%Access Methods%'`
	
	var value float64
	err := m.db.QueryRowContext(ctx, query, dbName).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getIndexPageLookupsPerSec(ctx context.Context, dbName string) (float64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Page lookups/sec'
		AND instance_name = ?
		AND object_name LIKE '%Access Methods%'`
	
	var value float64
	err := m.db.QueryRowContext(ctx, query, dbName).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getIndexPageReadsPerSec(ctx context.Context, dbName string) (float64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Page reads/sec'
		AND instance_name = ?
		AND object_name LIKE '%Buffer Manager%'`
	
	var value float64
	err := m.db.QueryRowContext(ctx, query, dbName).Scan(&value)
	return value, err
}

func (m *mssqlScraper) getIndexPageWritesPerSec(ctx context.Context, dbName string) (float64, error) {
	query := `
		SELECT cntr_value 
		FROM sys.dm_os_performance_counters 
		WHERE counter_name = 'Page writes/sec'
		AND instance_name = ?
		AND object_name LIKE '%Buffer Manager%'`
	
	var value float64
	err := m.db.QueryRowContext(ctx, query, dbName).Scan(&value)
	return value, err
}
