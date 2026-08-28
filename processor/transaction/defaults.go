package transaction

import (
	"math"
	"os"
	"strconv"
	"time"
)

// Default trim / holdback values. Constructor options override env;
// env overrides these constants when options are unset.
const (
	DefaultMaxNodes                 = 256
	DefaultCompletionHoldback       = 100 * time.Millisecond
	DefaultCompletionHoldbackMillis = 100
	// DefaultMaxFinalizedNames caps post-export SpanID→txn-name entries retained
	// for late fire-and-forget children.
	DefaultMaxFinalizedNames = 16384
)

// Env vars read by NewTransactionSpanProcessor when the matching Option is omitted.
const (
	EnvMaxNodes                 = "OTEL_CX_TRANSACTION_MAX_NODES"
	EnvCompletionHoldbackMillis = "OTEL_CX_TRANSACTION_COMPLETION_HOLDBACK_MILLIS"
)

// maxDurationMillis is the largest nonnegative millisecond count that can be
// represented as a time.Duration without overflowing to negative.
const maxDurationMillis = int64(math.MaxInt64 / int64(time.Millisecond))

func defaultsFromEnv() (maxNodes int, completionHoldback time.Duration) {
	maxNodes = envInt(EnvMaxNodes, DefaultMaxNodes)
	completionHoldback = envDurationMillis(EnvCompletionHoldbackMillis, DefaultCompletionHoldbackMillis)
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

// envDurationMillis parses a nonnegative millisecond env var into a Duration.
// Values that overflow time.Duration fall back to the documented default.
func envDurationMillis(key string, fallbackMillis int) time.Duration {
	fallback := time.Duration(fallbackMillis) * time.Millisecond
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 || v > maxDurationMillis {
		return fallback
	}
	return time.Duration(v) * time.Millisecond
}
