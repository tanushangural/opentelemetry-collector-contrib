package metadata

import (
	"go.opentelemetry.io/collector/component"
)

var (
	Type      = component.MustNewType("cumulativetorate")
	ScopeName = "github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetorateprocessor"
)

const (
	MetricsStability = component.StabilityLevelBeta
)
