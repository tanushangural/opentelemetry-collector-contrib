// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

// Package newrelicmssqlreceiver implements a receiver that can collect metrics from Microsoft SQL Server
// based on the New Relic MSSQL integration. This receiver does not include query analysis functionality.
package newrelicmssqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmssqlreceiver"
