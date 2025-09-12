# New Relic MSSQL Receiver

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [development]         |
| Supported pipeline types | metrics               |
| Distributions            | [contrib]             |

The New Relic MSSQL receiver connects to Microsoft SQL Server instances and collects metrics based on the New Relic MSSQL integration. This receiver provides comprehensive monitoring capabilities for SQL Server performance without query analysis functionality.

## Configuration

### Connection Settings

| Field | Type | Default | Required | Description |
| ----- | ---- | ------- | -------- | ----------- |
| `hostname` | string | | yes | The hostname or IP address of the SQL Server |
| `port` | int | `1433` | no | The port number for SQL Server |
| `username` | string | | yes | Username for SQL Server authentication |
| `password` | string | | yes | Password for SQL Server authentication |
| `instance` | string | | no | SQL Server instance name (alternative to port) |

### SSL Configuration

| Field | Type | Default | Required | Description |
| ----- | ---- | ------- | -------- | ----------- |
| `enable_ssl` | bool | `false` | no | Enable SSL encryption |
| `trust_server_certificate` | bool | `true` | no | Trust server certificate without verification |
| `certificate_location` | string | | no | Path to SSL certificate file |

### Azure AD Authentication

| Field | Type | Default | Required | Description |
| ----- | ---- | ------- | -------- | ----------- |
| `client_id` | string | | no | Azure AD application client ID |
| `tenant_id` | string | | no | Azure AD tenant ID |
| `client_secret` | string | | no | Azure AD application client secret |

### Feature Flags

| Field | Type | Default | Required | Description |
| ----- | ---- | ------- | -------- | ----------- |
| `enable_buffer_metrics` | bool | `true` | no | Enable buffer cache metrics collection |
| `enable_database_reserve_metrics` | bool | `true` | no | Enable database reserve metrics |
| `enable_disk_metrics_in_bytes` | bool | `false` | no | Report disk metrics in bytes instead of pages |
| `timeout` | int | `30` | no | Query timeout in seconds |

### Custom Metrics

| Field | Type | Default | Required | Description |
| ----- | ---- | ------- | -------- | ----------- |
| `custom_metrics_config` | string | | no | Path to YAML file with custom queries |
| `custom_metrics_query` | string | | no | Single custom SQL query |

## Example Configuration

### Basic Configuration

```yaml
receivers:
  newrelicmssql:
    hostname: localhost
    port: 1433
    username: sa
    password: ${MSSQL_PASSWORD}
    collection_interval: 30s
```

### Configuration with SSL

```yaml
receivers:
  newrelicmssql:
    hostname: sqlserver.example.com
    port: 1433
    username: monitoring_user
    password: ${MSSQL_PASSWORD}
    enable_ssl: true
    trust_server_certificate: false
    certificate_location: /path/to/cert.pem
    collection_interval: 30s
```

### Azure AD Authentication

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

### Advanced Configuration

```yaml
receivers:
  newrelicmssql:
    hostname: localhost
    port: 1433
    username: monitoring_user
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

## Metrics

This receiver collects the following types of metrics:

### Instance Metrics
- Buffer cache hit ratio
- Page life expectancy
- Batch requests per second
- SQL compilations per second
- SQL recompilations per second
- Blocked processes
- User connections
- Memory utilization
- CPU time

### Database Metrics
- Data file size
- Log file size and usage
- Transactions per second
- Active connections
- Database state

### Wait Statistics
- Waiting tasks count
- Wait time
- Maximum wait time
- Wait types and categories

### Lock Statistics
- Lock timeouts
- Lock waits
- Lock wait time
- Lock types and resources

### Index Statistics
- Page splits per second
- Page lookups per second
- Page reads per second
- Page writes per second

## Resource Attributes

| Name | Description | Enabled |
| ---- | ----------- | ------- |
| `mssql.instance.name` | SQL Server instance name | true |
| `mssql.database.name` | Database name | true |
| `server.address` | Server hostname/IP | true |
| `server.port` | Server port | true |

## Prerequisites

### SQL Server Permissions

The monitoring user requires the following permissions:

```sql
USE master;
CREATE LOGIN newrelic WITH PASSWORD = 'your_password';
CREATE USER newrelic FOR LOGIN newrelic;
GRANT CONNECT SQL TO newrelic;
GRANT VIEW SERVER STATE TO newrelic;

-- For each user database
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

### Network Access

Ensure the OpenTelemetry Collector can reach the SQL Server on the configured port (default 1433).

## Compatibility

This receiver is compatible with:
- Microsoft SQL Server 2012 and later
- Azure SQL Database
- Azure SQL Managed Instance

## Performance Considerations

- Default collection interval is 30 seconds
- Some queries may impact performance on heavily loaded systems
- Consider adjusting timeout values for slow environments
- Monitor the receiver's resource usage in production

## Troubleshooting

### Common Issues

1. **Connection refused**: Check network connectivity and firewall settings
2. **Authentication failed**: Verify username/password and permissions
3. **Timeout errors**: Increase the timeout value
4. **Missing metrics**: Check that the monitoring user has proper permissions
5. **SSL errors**: Verify certificate configuration and trust settings

### Debug Mode

Enable debug logging to troubleshoot connection and query issues:

```yaml
service:
  telemetry:
    logs:
      level: debug
```
