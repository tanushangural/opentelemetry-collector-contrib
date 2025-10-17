// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package queries provides SQL query definitions for failover cluster-level metrics.
// This file contains SQL queries for collecting SQL Server Always On Availability Group
// replica performance metrics when running in high availability failover cluster environments.
//
// Failover Cluster Metrics Categories:
//
// 1. Database Replica Performance Metrics:
//   - Log bytes received per second from primary replica
//   - Transaction delay on secondary replica
//   - Flow control time for log record processing
//
// Query Sources:
// - sys.dm_os_performance_counters: Always On Database Replica performance counters
//
// Metric Collection Strategy:
// - Uses PIVOT operation to transform performance counter rows into columns
// - Filters for Database Replica object counters specific to Always On Availability Groups
// - Returns structured data for log replication performance monitoring
// - Provides insights into replication lag and flow control behavior
//
// Engine Support:
// - Default: Full failover cluster metrics for SQL Server with Always On Availability Groups
// - AzureSQLDatabase: Not applicable (no Always On AG support in single database)
// - AzureSQLManagedInstance: Limited support (managed service handles AG internally)
//
// Availability:
// - Only available on SQL Server instances configured with Always On Availability Groups
// - Returns empty result set on instances without Always On AG enabled
// - Requires appropriate permissions to query performance counter DMVs
package queries

// FailoverClusterReplicaQuery returns the SQL query for Always On replica performance metrics
// This query uses GROUP BY approach to aggregate performance counter data by instance name
// Source: sys.dm_os_performance_counters for Database Replica performance counters
//
// The query returns:
// - instance_name: Database instance name from performance counters
// - Log Bytes Received/sec: Rate of log records received by secondary replica from primary (bytes/sec)
// - Transaction Delay: Average delay for transactions on the secondary replica (milliseconds)
// - Flow Control Time (ms/sec): Time spent in flow control by log records (milliseconds/sec)
const FailoverClusterReplicaQuery = `SELECT
    instance_name,
    ISNULL(MAX(CASE WHEN counter_name = 'Log Bytes Received/sec' THEN cntr_value END), 0) AS [Log Bytes Received/sec],
    ISNULL(MAX(CASE WHEN counter_name = 'Transaction Delay' THEN cntr_value END), 0) AS [Transaction Delay],
    ISNULL(MAX(CASE WHEN counter_name = 'Flow Control Time (ms/sec)' THEN cntr_value END), 0) AS [Flow Control Time (ms/sec)]
FROM
    sys.dm_os_performance_counters
WHERE
    object_name LIKE '%Database Replica%'
    AND counter_name IN (
        'Log Bytes Received/sec',
        'Transaction Delay',
        'Flow Control Time (ms/sec)'
    )
GROUP BY
    instance_name;`

// FailoverClusterReplicaStateQuery returns the SQL query for Always On replica state metrics with extended information
// This query joins availability replica information with database replica states to provide
// detailed log send/redo queue metrics and synchronization state for each database in the availability group
//
// The query returns:
// - replica_server_name: Name of the server hosting the replica
// - database_name: Name of the database in the availability group
// - log_send_queue_kb: Amount of log records not yet sent to secondary replica (KB)
// - redo_queue_kb: Amount of log records waiting to be redone on secondary replica (KB)
// - redo_rate_kb_sec: Rate at which log records are being redone on secondary replica (KB/sec)
// - database_state_desc: Current state of the database replica
// - synchronization_state_desc: Synchronization state of the database replica
// - is_local: Whether this is a local replica (1) or remote (0)
// - is_primary_replica: Whether this is the primary replica (1) or secondary (0)
// - last_commit_time: Timestamp of the last transaction commit
// - last_sent_time: Timestamp when the last log block was sent
// - last_received_time: Timestamp when the last log block was received
// - last_hardened_time: Timestamp when the last log block was hardened
// - last_redone_lsn: Log sequence number of the last redone log record
// - suspend_reason_desc: Reason why the database is suspended (if applicable)
const FailoverClusterReplicaStateQuery = `SELECT
    ar.replica_server_name,
    d.name AS database_name,
    drs.log_send_queue_size AS log_send_queue_kb,
    drs.redo_queue_size AS redo_queue_kb,
    drs.redo_rate AS redo_rate_kb_sec,
    drs.database_state_desc,
    drs.synchronization_state_desc,
    CAST(drs.is_local AS INT) AS is_local,
    CAST(drs.is_primary_replica AS INT) AS is_primary_replica,
    CONVERT(VARCHAR(23), drs.last_commit_time, 121) AS last_commit_time,
    CONVERT(VARCHAR(23), drs.last_sent_time, 121) AS last_sent_time,
    CONVERT(VARCHAR(23), drs.last_received_time, 121) AS last_received_time,
    CONVERT(VARCHAR(23), drs.last_hardened_time, 121) AS last_hardened_time,
    CONVERT(VARCHAR(25), drs.last_redone_lsn) AS last_redone_lsn,
    drs.suspend_reason_desc
FROM
    sys.dm_hadr_database_replica_states AS drs
JOIN
    sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN
    sys.databases AS d ON drs.database_id = d.database_id;`

// FailoverClusterNodeQuery returns the SQL query for cluster node information
// This query retrieves information about cluster nodes including their status and ownership
//
// The query returns:
// - nodename: Name of the server node in the cluster
// - status_description: Health state of the node (Up, Down, Paused, etc.)
// - is_current_owner: 1 if this is the active node running SQL Server, 0 if passive
const FailoverClusterNodeQuery = `SELECT
    nodename,
    status_description,
    is_current_owner
FROM sys.dm_os_cluster_nodes;`

// FailoverClusterAvailabilityGroupHealthQuery returns the SQL query for Availability Group health status with extended configuration
// This query retrieves health, role, and configuration information for all availability group replicas
//
// The query returns:
// - replica_server_name: Name of the server instance hosting the availability replica
// - role_desc: Current role of the replica (PRIMARY, SECONDARY)
// - synchronization_health_desc: Health of data synchronization (HEALTHY, PARTIALLY_HEALTHY, NOT_HEALTHY)
// - availability_mode_desc: Availability mode (SYNCHRONOUS_COMMIT, ASYNCHRONOUS_COMMIT)
// - failover_mode_desc: Failover mode (AUTOMATIC, MANUAL, EXTERNAL)
// - backup_priority: Backup priority setting (0-100)
// - endpoint_url: Database mirroring endpoint URL
// - read_only_routing_url: Read-only routing URL
// - connected_state_desc: Connection state (CONNECTED, DISCONNECTED)
// - operational_state_desc: Operational state (ONLINE, OFFLINE, etc.)
// - recovery_health_desc: Recovery health (ONLINE, OFFLINE, etc.)
const FailoverClusterAvailabilityGroupHealthQuery = `SELECT
    ar.replica_server_name,
    ars.role_desc,
    ars.synchronization_health_desc,
    ar.availability_mode_desc,
    ar.failover_mode_desc,
    ar.backup_priority,
    ar.endpoint_url,
    ar.read_only_routing_url,
    ars.connected_state_desc,
    ars.operational_state_desc,
    ars.recovery_health_desc
FROM
    sys.dm_hadr_availability_replica_states AS ars
INNER JOIN
    sys.availability_replicas AS ar ON ars.replica_id = ar.replica_id;`

// FailoverClusterAvailabilityGroupHealthQueryAzureMI returns the enhanced query for Azure SQL Managed Instance
// This query uses dm_hadr_database_replica_states which is available in Azure MI and contains the actual sync metrics
// We join with availability_replicas and databases to get comprehensive health information
const FailoverClusterAvailabilityGroupHealthQueryAzureMI = `SELECT
    ISNULL(ar.replica_server_name, @@SERVERNAME) AS replica_server_name,
    d.name AS database_name,
    drs.synchronization_health_desc,
    drs.synchronization_state_desc,
    drs.synchronization_state,
    CAST(drs.is_primary_replica AS int) AS is_primary_replica,
    drs.last_commit_time,
    ISNULL(ar.availability_mode_desc, 'UNKNOWN') AS availability_mode_desc,
    ISNULL(ar.failover_mode_desc, 'UNKNOWN') AS failover_mode_desc,
    ISNULL(ar.backup_priority, 50) AS backup_priority,
    drs.database_state_desc,
    CAST(drs.is_local AS int) AS is_local
FROM
    sys.dm_hadr_database_replica_states AS drs
LEFT JOIN
    sys.availability_replicas AS ar ON drs.replica_id = ar.replica_id
JOIN
    sys.databases AS d ON drs.database_id = d.database_id
WHERE
    drs.is_local = 1;`

// FailoverClusterAvailabilityGroupQuery returns the SQL query for Availability Group configuration metrics
// This query retrieves configuration settings for all availability groups
//
// The query returns:
// - group_name: Name of the availability group
// - automated_backup_preference_desc: Backup preference setting (PRIMARY, SECONDARY_ONLY, SECONDARY, NONE)
// - failure_condition_level: Failure detection level (1-5)
// - health_check_timeout: Health check timeout value in milliseconds
// - cluster_type_desc: Cluster type (WSFC, EXTERNAL, NONE)
// - required_synchronized_secondaries_to_commit: Number of secondary replicas required to be synchronized
const FailoverClusterAvailabilityGroupQuery = `SELECT
    ag.name AS group_name,
    ag.automated_backup_preference_desc,
    ag.failure_condition_level,
    ag.health_check_timeout,
    ag.cluster_type_desc,
    ag.required_synchronized_secondaries_to_commit
FROM
    sys.availability_groups AS ag;`

// FailoverClusterAvailabilityGroupQueryAzureMI returns the SQL query for database availability metrics in Azure SQL Managed Instance
// Since Azure MI has built-in high availability without explicit availability groups,
// this query focuses on database states and available performance metrics
//
// The query returns:
// - server_name: Name of the server instance
// - database_name: Name of the database
// - database_state: Current state of the database (ONLINE/RESTORING/etc)
// - database_state_desc: Description of the database state
// - is_read_only: Whether the database is read-only
// - availability_replica_commit_rate: Database replica commit rate per second
// - log_generation_rate: Log generation rate for the database
const FailoverClusterAvailabilityGroupQueryAzureMI = `SELECT
    @@SERVERNAME AS server_name,
    d.name AS database_name,
    d.state AS database_state,
    d.state_desc AS database_state_desc,
    CAST(d.is_read_only AS int) AS is_read_only,
    ISNULL(MAX(CASE WHEN pc.counter_name = 'Database Replica Commit Rate/sec' THEN pc.cntr_value END), 0) AS availability_replica_commit_rate,
    ISNULL(MAX(CASE WHEN pc.counter_name = 'Log Generation Rate' THEN pc.cntr_value END), 0) AS log_generation_rate
FROM
    sys.databases d
LEFT JOIN sys.dm_os_performance_counters pc ON 
    pc.instance_name = d.name
    AND pc.object_name LIKE '%Database Replica%'
    AND pc.counter_name IN ('Database Replica Commit Rate/sec', 'Log Generation Rate')
WHERE
    d.database_id > 4  -- Exclude system databases
GROUP BY
    d.name, d.state, d.state_desc, d.is_read_only;`

// FailoverClusterRedoQueueQueryAzureMI returns the SQL query for Always On redo queue metrics for Azure SQL Managed Instance
// Since Azure MI has dm_hadr_database_replica_states available, we can use the actual replica state data
// This query includes comprehensive synchronization and redo queue metrics
//
// The query returns:
// - database_name: Name of the database in the replica
// - log_send_queue_kb: Amount of log records not yet sent to secondary replica (KB)
// - redo_queue_kb: Amount of log records waiting to be redone on secondary replica (KB)
// - redo_rate_kb_sec: Rate at which log records are being redone (KB/sec)
// - synchronization_health_desc: Health of synchronization (HEALTHY, PARTIALLY_HEALTHY, NOT_HEALTHY)
// - synchronization_state_desc: State of synchronization (SYNCHRONIZING, SYNCHRONIZED, etc.)
// - synchronization_state: Numeric synchronization state
// - is_primary_replica: Whether this is the primary replica (1) or secondary (0)
// - last_commit_time: Timestamp of the last transaction commit
// - last_hardened_time: Timestamp when the last log block was hardened
const FailoverClusterRedoQueueQueryAzureMI = `SELECT
    d.name AS database_name,
    ISNULL(drs.log_send_queue_size, 0) AS log_send_queue_kb,
    ISNULL(drs.redo_queue_size, 0) AS redo_queue_kb,
    ISNULL(drs.redo_rate, 0) AS redo_rate_kb_sec,
    drs.synchronization_state_desc,
    drs.synchronization_state,
    CAST(drs.is_primary_replica AS int) AS is_primary_replica,
    drs.last_commit_time,
    drs.last_hardened_time,
    drs.database_state_desc,
    CAST(drs.is_local AS int) AS is_local
FROM
    sys.dm_hadr_database_replica_states drs
JOIN
    sys.databases d ON drs.database_id = d.database_id
WHERE
    drs.is_local = 1;`

// FailoverClusterPerformanceCounterQuery returns the SQL query for core Always On performance counters
// This query retrieves the essential performance counters for monitoring replica performance
// using PIVOT operation to transform rows into columns for easier consumption
//
// The query returns columns for each performance counter by database instance:
// - instance_name: Database name or _Total for aggregate counters
// - Log Bytes Received/sec: Rate of log records received by secondary replica from primary (bytes/sec)
// - Transaction Delay: Average delay for transactions on the secondary replica (milliseconds)
// - Flow Control Time (ms/sec): Time spent in flow control by log records (milliseconds/sec)
const FailoverClusterPerformanceCounterQuery = `SELECT
    instance_name,
    ISNULL(MAX(CASE WHEN counter_name = 'Log Bytes Received/sec' THEN cntr_value END), 0) AS [Log Bytes Received/sec],
    ISNULL(MAX(CASE WHEN counter_name = 'Transaction Delay' THEN cntr_value END), 0) AS [Transaction Delay],
    ISNULL(MAX(CASE WHEN counter_name = 'Flow Control Time (ms/sec)' THEN cntr_value END), 0) AS [Flow Control Time (ms/sec)]
FROM
    sys.dm_os_performance_counters
WHERE
    object_name LIKE '%Database Replica%'
    AND counter_name IN (
        'Log Bytes Received/sec',
        'Transaction Delay',
        'Flow Control Time (ms/sec)'
    )
GROUP BY
    instance_name;`
