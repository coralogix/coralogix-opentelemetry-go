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
