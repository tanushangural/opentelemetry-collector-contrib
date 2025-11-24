// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package models provides data structures for security-level metrics and monitoring.
// This file defines the data models used to represent SQL Server security-level performance data
// including server principals, role membership, and authentication patterns.
//
// Security-Level Data Structures:
//
// 1. Server Security Metrics:
//
//	type SecurityPrincipalsMetrics struct {
//	    TotalServerPrincipals     int64   // Total number of server principals (logins)
//	    SQLServerPrincipals       int64   // SQL Server authentication principals
//	    WindowsPrincipals         int64   // Windows authentication principals
//	    CertificatePrincipals     int64   // Certificate-based principals
//	    AsymmetricKeyPrincipals   int64   // Asymmetric key-based principals
//	    EnabledPrincipals         int64   // Number of enabled principals
//	    DisabledPrincipals        int64   // Number of disabled principals
//	    SystemPrincipals          int64   // Built-in system principals
//	}
//
// 2. Role Membership Metrics:
//
//	type RoleMembershipMetrics struct {
//	    TotalRoleMembers          int64   // Total number of server role members
//	    SysAdminMembers           int64   // Members of sysadmin role
//	    SecurityAdminMembers      int64   // Members of securityadmin role
//	    ServerAdminMembers        int64   // Members of serveradmin role
//	    SetupAdminMembers         int64   // Members of setupadmin role
//	    ProcessAdminMembers       int64   // Members of processadmin role
//	    DiskAdminMembers          int64   // Members of diskadmin role
//	    DBCreatorMembers          int64   // Members of dbcreator role
//	    BulkAdminMembers          int64   // Members of bulkadmin role
//	}
//
// 3. Authentication Activity:
//
//	type AuthenticationMetrics struct {
//	    SuccessfulLogins          int64   // Number of successful login attempts
//	    FailedLogins              int64   // Number of failed login attempts
//	    LoginSuccessRate          float64 // Login success rate percentage
//	    TotalLoginAttempts        int64   // Total login attempts
//	    UniqueUsersLoggedIn       int64   // Number of unique users logged in
//	    ActiveSessions            int64   // Number of active authenticated sessions
//	}
//
// Data Source Mappings:
// - sys.server_principals: Server-level security principals information
// - sys.server_role_members: Server role membership relationships
// - sys.dm_exec_sessions: Session and authentication data
// - sys.dm_server_audit_status: Security audit status
//
// Engine Support:
// - Standard SQL Server: Full security metrics available
// - Azure SQL Database: Limited (no server-level roles, restricted principals)
// - Azure SQL Managed Instance: Most security metrics available
//
// Usage Patterns:
// - Populated by SecurityScraper from SQL Server security DMVs
// - Converted to OpenTelemetry metrics with appropriate labels
// - Provides structured access to security monitoring data
// - Enables security compliance and monitoring across SQL Server editions
package models

// SecurityPrincipalsModel represents server principals count metrics for SQL Server security monitoring
type SecurityPrincipalsModel struct {
	ServerPrincipalsCount *int64 `db:"server_principals_count" metric_name:"sqlserver.security.server_principals_count" source_type:"gauge" description:"Total number of server principals (logins)" unit:"1"`
}

// SecurityRoleMembersModel represents server role membership metrics for SQL Server security monitoring
type SecurityRoleMembersModel struct {
	ServerRoleMembersCount *int64 `db:"server_role_members_count" metric_name:"sqlserver.security.server_role_members_count" source_type:"gauge" description:"Total number of server role members" unit:"1"`
}
