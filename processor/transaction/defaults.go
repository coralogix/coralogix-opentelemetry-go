package transaction

import (
	"os"
	"strconv"
	"time"
)

// Default harvest / trim / holdback values. Constructor options override env;
// env overrides these constants when options are unset.
const (
	DefaultMaxNodes                 = 256
	DefaultMaxRegularTraces         = 1
	DefaultHarvestPeriod            = 60 * time.Second
	DefaultCompletionHoldback       = 100 * time.Millisecond
	DefaultHarvestPeriodMillis      = 60_000
	DefaultCompletionHoldbackMillis = 100
)

// Env vars read by NewTransactionSpanProcessor when the matching Option is omitted.
const (
	EnvMaxNodes                 = "OTEL_CX_TRANSACTION_MAX_NODES"
	EnvMaxRegularTraces         = "OTEL_CX_TRANSACTION_MAX_REGULAR_TRACES"
	EnvHarvestPeriodMillis      = "OTEL_CX_TRANSACTION_HARVEST_PERIOD_MILLIS"
	EnvCompletionHoldbackMillis = "OTEL_CX_TRANSACTION_COMPLETION_HOLDBACK_MILLIS"
)

func defaultsFromEnv() (maxNodes, maxRegularTraces int, harvestPeriod, completionHoldback time.Duration) {
	maxNodes = envInt(EnvMaxNodes, DefaultMaxNodes)
	maxRegularTraces = envInt(EnvMaxRegularTraces, DefaultMaxRegularTraces)
	harvestPeriod = time.Duration(envInt(EnvHarvestPeriodMillis, DefaultHarvestPeriodMillis)) * time.Millisecond
	completionHoldback = time.Duration(envInt(EnvCompletionHoldbackMillis, DefaultCompletionHoldbackMillis)) * time.Millisecond
	return
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return fallback
	}
	return v
}
