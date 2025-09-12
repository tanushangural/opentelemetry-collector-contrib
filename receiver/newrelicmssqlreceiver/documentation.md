# New Relic MSSQL Receiver Documentation

## Overview

The New Relic MSSQL receiver is designed to collect performance metrics from Microsoft SQL Server instances. It is based on the New Relic MSSQL integration but adapted for OpenTelemetry Collector. This receiver does not include query analysis functionality as per the requirements.

## Features

- **Instance-level metrics**: Buffer cache hit ratio, page life expectancy, batch requests, SQL compilations, blocked processes, user connections
- **Database-level metrics**: File sizes, transaction rates, active connections, database state
- **Wait statistics**: Detailed analysis of SQL Server wait types and times
- **Lock statistics**: Lock timeouts, waits, and wait times
- **Index statistics**: Page operations and performance metrics
- **Azure AD authentication**: Support for Azure SQL Database using service principal authentication
- **SSL/TLS support**: Secure connections with certificate validation options
- **Configurable collection**: Enable/disable specific metric categories

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SQL Server    │────│ New Relic MSSQL │────│  OpenTelemetry  │
│    Instance     │    │    Receiver     │    │   Collector     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

The receiver connects directly to SQL Server using the Microsoft SQL Server driver and executes specific queries to gather performance metrics.

## Metric Categories

### Instance Metrics
These metrics provide server-wide performance indicators:

| Metric | Description | Unit |
|--------|-------------|------|
| `mssql.instance.buffer_cache_hit_ratio` | Percentage of pages found in buffer cache | % |
| `mssql.instance.page_life_expectancy` | Expected time a page stays in buffer pool | seconds |
| `mssql.instance.batch_requests_per_sec` | Number of batch requests per second | requests/s |
| `mssql.instance.sql_compilations_per_sec` | SQL compilations per second | compilations/s |
| `mssql.instance.sql_recompilations_per_sec` | SQL recompilations per second | recompilations/s |
| `mssql.instance.processes_blocked` | Number of blocked processes | processes |
| `mssql.instance.user_connections` | Number of user connections | connections |

### Database Metrics
Per-database performance and size metrics:

| Metric | Description | Unit |
|--------|-------------|------|
| `mssql.database.data_file_size` | Size of database data files | bytes |
| `mssql.database.log_file_size` | Size of database log files | bytes |
| `mssql.database.log_file_used_size` | Used size of log files | bytes |
| `mssql.database.log_file_used_percentage` | Percentage of log file used | % |
| `mssql.database.transactions_per_sec` | Transactions per second | transactions/s |
| `mssql.database.active_connections` | Active connections to database | connections |

### Wait Statistics
SQL Server wait events and performance bottlenecks:

| Metric | Description | Unit |
|--------|-------------|------|
| `mssql.wait_stats.waiting_tasks_count` | Number of waiting tasks | tasks |
| `mssql.wait_stats.wait_time_ms` | Total wait time | milliseconds |
| `mssql.wait_stats.max_wait_time_ms` | Maximum wait time | milliseconds |

### Lock Statistics
Lock-related performance metrics:

| Metric | Description | Unit |
|--------|-------------|------|
| `mssql.lock_stats.timeout_count` | Number of lock timeouts | timeouts |
| `mssql.lock_stats.wait_count` | Number of lock waits | waits |
| `mssql.lock_stats.wait_time_ms` | Total lock wait time | milliseconds |

### Index Statistics
Index and page operation metrics:

| Metric | Description | Unit |
|--------|-------------|------|
| `mssql.index_stats.page_splits_per_sec` | Page splits per second | splits/s |
| `mssql.index_stats.page_lookups_per_sec` | Page lookups per second | lookups/s |
| `mssql.index_stats.page_reads_per_sec` | Page reads per second | reads/s |
| `mssql.index_stats.page_writes_per_sec` | Page writes per second | writes/s |

## Configuration Examples

### Basic SQL Server Authentication
```yaml
receivers:
  newrelicmssql:
    hostname: sqlserver.example.com
    port: 1433
    username: monitoring_user
    password: ${MSSQL_PASSWORD}
    collection_interval: 30s
```

### SSL Configuration
```yaml
receivers:
  newrelicmssql:
    hostname: sqlserver.example.com
    port: 1433
    username: monitoring_user
    password: ${MSSQL_PASSWORD}
    enable_ssl: true
    trust_server_certificate: false
    certificate_location: /path/to/server-cert.pem
    collection_interval: 30s
```

### Azure SQL Database with Service Principal
```yaml
receivers:
  newrelicmssql:
    hostname: myserver.database.windows.net
    port: 1433
    client_id: ${AZURE_CLIENT_ID}
    tenant_id: ${AZURE_TENANT_ID}
    client_secret: ${AZURE_CLIENT_SECRET}
    enable_ssl: true
    collection_interval: 30s
```

### Advanced Configuration with Custom Metrics
```yaml
receivers:
  newrelicmssql:
    hostname: localhost
    port: 1433
    username: sa
    password: ${MSSQL_PASSWORD}
    instance: SQLEXPRESS
    timeout: 60
    enable_buffer_metrics: true
    enable_database_reserve_metrics: true
    enable_disk_metrics_in_bytes: true
    collection_interval: 15s
    custom_metrics_query: |
      SELECT 
        'custom_buffer_pool_size' AS metric_name,
        Count_big(*) * (8*1024) AS metric_value,
        'gauge' as metric_type,
        database_id
      FROM sys.dm_os_buffer_descriptors WITH (nolock)
      GROUP BY database_id
```

## Troubleshooting

### Common Connection Issues

1. **Cannot connect to SQL Server**
   - Verify hostname and port
   - Check firewall settings
   - Ensure SQL Server is accepting TCP connections
   - Verify SQL Server Browser service is running (if using named instances)

2. **Authentication failures**
   - Verify username and password
   - Check if SQL Server authentication is enabled
   - Ensure user has proper permissions
   - For Azure AD, verify service principal credentials

3. **Permission errors**
   - Grant `VIEW SERVER STATE` permission
   - Grant `CONNECT SQL` permission
   - Create user in each database to monitor

4. **SSL/TLS errors**
   - Verify certificate configuration
   - Check certificate trust settings
   - Ensure SSL is properly configured on SQL Server

### Required SQL Server Permissions

Create a dedicated monitoring user with minimal required permissions:

```sql
-- Create login and user
USE master;
CREATE LOGIN newrelic WITH PASSWORD = 'SecurePassword123!';
CREATE USER newrelic FOR LOGIN newrelic;

-- Grant server-level permissions
GRANT CONNECT SQL TO newrelic;
GRANT VIEW SERVER STATE TO newrelic;

-- Create user in each database
DECLARE @name NVARCHAR(max)
DECLARE db_cursor CURSOR FOR
SELECT NAME
FROM master.dbo.sysdatabases
WHERE NAME NOT IN ('master','msdb','tempdb','model')
OPEN db_cursor
FETCH NEXT FROM db_cursor INTO @name WHILE @@FETCH_STATUS = 0
BEGIN
    EXECUTE('USE "' + @name + '"; CREATE USER newrelic FOR LOGIN newrelic;' );
    FETCH next FROM db_cursor INTO @name
END
CLOSE db_cursor
DEALLOCATE db_cursor
```

### Performance Considerations

- **Collection interval**: Default 30 seconds balances freshness with performance impact
- **Query timeouts**: Increase timeout for slow environments
- **Resource usage**: Monitor receiver memory and CPU usage in production
- **Network latency**: Consider proximity to SQL Server for best performance

### Debug and Monitoring

Enable debug logging to troubleshoot issues:

```yaml
service:
  telemetry:
    logs:
      level: debug
      development: true
```

Monitor receiver health through collector metrics:
- Connection success/failure rates
- Query execution times
- Error counts and types

## Compatibility

### Supported Platforms
- Microsoft SQL Server 2012 and later
- Azure SQL Database
- Azure SQL Managed Instance
- SQL Server on Linux

### OpenTelemetry Collector
- Requires OpenTelemetry Collector Contrib v0.115.0 or later
- Compatible with all collector deployment modes (agent, gateway, hybrid)

## Security Best Practices

1. **Use dedicated monitoring accounts** with minimal required permissions
2. **Enable SSL/TLS encryption** for production deployments
3. **Store passwords securely** using environment variables or secret management
4. **Regularly rotate credentials** used by the receiver
5. **Monitor access logs** for unauthorized connection attempts
6. **Use Azure AD authentication** when available for additional security

## Limitations

1. **Query analysis functionality** is not implemented (as per requirements)
2. **Custom queries** support basic functionality only
3. **Windows performance counters** are not directly accessible on Linux
4. **Some metrics** may not be available on all SQL Server editions
5. **Real-time monitoring** is limited by collection interval

## Migration from New Relic Infrastructure

When migrating from New Relic Infrastructure MSSQL integration:

1. **Metric mapping**: Most metrics have equivalent mappings
2. **Configuration changes**: Update connection parameters and authentication
3. **Custom queries**: May need adjustment for OpenTelemetry format
4. **Alerting**: Recreate alerts using new metric names and attributes
5. **Dashboards**: Update visualizations with new metric structure

## Future Enhancements

Potential improvements for future versions:
- Support for Always On Availability Groups metrics
- Enhanced custom query functionality
- Windows performance counter integration
- Real-time alerting capabilities
- Automated discovery of SQL Server instances
