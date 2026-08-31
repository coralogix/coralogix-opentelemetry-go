package transaction

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestDefaultsFromEnv_InvalidFallsBack(t *testing.T) {
	t.Setenv(EnvCompletionHoldbackMillis, "xyz")

	holdback := defaultsFromEnv()
	assert.Equal(t, DefaultCompletionHoldback, holdback)
}

func TestDefaultsFromEnv_NegativeFallsBack(t *testing.T) {
	t.Setenv(EnvCompletionHoldbackMillis, "-1")

	holdback := defaultsFromEnv()
	assert.Equal(t, DefaultCompletionHoldback, holdback)
}

func TestDefaultsFromEnv_ParsesValues(t *testing.T) {
	t.Setenv(EnvCompletionHoldbackMillis, "250")

	holdback := defaultsFromEnv()
	assert.Equal(t, 250*time.Millisecond, holdback)
}

func TestTransactionLimitEnv(t *testing.T) {
	t.Setenv(EnvMaxTransactionSpans, "12")
	t.Setenv(EnvMaxTraces, "34")

	assert.Equal(t, 12, maxTransactionSpansFromEnv())
	assert.Equal(t, 34, maxTracesFromEnv())
}

func TestTransactionLimitEnv_InvalidFallsBack(t *testing.T) {
	t.Setenv(EnvMaxTransactionSpans, "-1")
	t.Setenv(EnvMaxTraces, "invalid")

	assert.Equal(t, DefaultMaxTransactionSpans, maxTransactionSpansFromEnv())
	assert.Equal(t, DefaultMaxTraces, maxTracesFromEnv())
}

func TestTransactionLimitEnv_ZeroDisablesBuffering(t *testing.T) {
	t.Setenv(EnvMaxTransactionSpans, "0")
	t.Setenv(EnvMaxTraces, "0")

	assert.Zero(t, maxTransactionSpansFromEnv())
	assert.Zero(t, maxTracesFromEnv())
}

func TestDefaultsFromEnv_MillisOverflowFallsBack(t *testing.T) {
	// math.MaxInt64 ms overflows Duration multiplication to a negative value.
	t.Setenv(EnvCompletionHoldbackMillis, "9223372036854775807")

	holdback := defaultsFromEnv()
	assert.Equal(t, DefaultCompletionHoldback, holdback)
	assert.True(t, holdback > 0)
}

func TestDefaultsFromEnv_MillisJustOverMaxFallsBack(t *testing.T) {
	over := strconv.FormatInt(maxDurationMillis+1, 10)
	t.Setenv(EnvCompletionHoldbackMillis, over)

	holdback := defaultsFromEnv()
	assert.Equal(t, DefaultCompletionHoldback, holdback)
}

func TestDefaultsFromEnv_MaxRepresentableMillisAccepted(t *testing.T) {
	raw := strconv.FormatInt(maxDurationMillis, 10)
	t.Setenv(EnvCompletionHoldbackMillis, raw)

	holdback := defaultsFromEnv()
	assert.Equal(t, time.Duration(maxDurationMillis)*time.Millisecond, holdback)
	assert.True(t, holdback > 0)
}

func TestNewTransactionSpanProcessor_OptionsOverrideEnv(t *testing.T) {
	t.Setenv(EnvCompletionHoldbackMillis, "50")

	exporter := sdktracetest.NewInMemoryExporter()
	p := NewTransactionSpanProcessor(exporter,
		WithCompletionHoldback(10*time.Millisecond),
	)
	require.NotNil(t, p)
	assert.Equal(t, 10*time.Millisecond, p.completionHoldback)
}

func TestNewTransactionSpanProcessor_EnvUsedWhenOptionsUnset(t *testing.T) {
	t.Setenv(EnvCompletionHoldbackMillis, "50")

	exporter := sdktracetest.NewInMemoryExporter()
	p := NewTransactionSpanProcessor(exporter)
	require.NotNil(t, p)
	assert.Equal(t, 50*time.Millisecond, p.completionHoldback)
}

func TestNewTransactionSpanProcessor_TransactionLimitsOptionsOverrideEnv(t *testing.T) {
	t.Setenv(EnvMaxTransactionSpans, "50")
	t.Setenv(EnvMaxTraces, "60")

	p := NewTransactionSpanProcessor(sdktracetest.NewInMemoryExporter(),
		WithMaxTransactionSpans(10), WithMaxTraces(20))
	assert.Equal(t, 10, p.maxTransactionSpans)
	assert.Equal(t, 20, p.maxTraces)
}

func TestNewTransactionSpanProcessor_ZeroOptionsDisableBuffering(t *testing.T) {
	p := NewTransactionSpanProcessor(sdktracetest.NewInMemoryExporter(),
		WithMaxTransactionSpans(0), WithMaxTraces(0))
	assert.Zero(t, p.maxTransactionSpans)
	assert.Zero(t, p.maxTraces)
}
