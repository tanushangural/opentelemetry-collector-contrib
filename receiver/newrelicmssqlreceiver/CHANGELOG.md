# Changelog

All notable changes to the New Relic MSSQL receiver will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial implementation of New Relic MSSQL receiver
- Support for Microsoft SQL Server monitoring based on New Relic MSSQL integration
- Instance-level metrics collection (buffer cache, page life expectancy, batch requests, etc.)
- Database-level metrics collection (file sizes, transactions, connections)
- Wait statistics monitoring with categorization
- Lock statistics monitoring 
- Index statistics and page operation metrics
- Support for SQL Server authentication
- Support for Azure AD Service Principal authentication
- SSL/TLS connection support with certificate validation options
- Configurable metric collection intervals
- Feature flags for enabling/disabling specific metric categories
- Support for custom SQL queries (basic functionality)
- Comprehensive configuration validation
- Unit tests for core functionality
- Detailed documentation and examples

### Security
- Implemented secure credential handling using configopaque.String
- Added SSL/TLS support with certificate validation
- Support for Azure AD authentication for enhanced security

### Performance
- Optimized SQL queries for minimal performance impact
- Configurable query timeouts
- Connection pooling through SQL driver
- Efficient metric collection with proper resource cleanup

### Documentation
- Complete README with configuration examples
- Detailed documentation covering all features
- Troubleshooting guide with common issues and solutions
- Security best practices guide
- Migration guide from New Relic Infrastructure

## Notes

- Query analysis functionality is intentionally not implemented as per requirements
- This receiver is designed to complement existing SQL Server monitoring solutions
- Focuses on performance metrics that are most valuable for observability
