package transaction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestDefaultsFromEnv_InvalidFallsBack(t *testing.T) {
	t.Setenv(EnvMaxNodes, "not-a-number")
	t.Setenv(EnvMaxRegularTraces, "")
	t.Setenv(EnvHarvestPeriodMillis, "abc")
	t.Setenv(EnvCompletionHoldbackMillis, "xyz")

	maxNodes, maxRegular, harvest, holdback := defaultsFromEnv()
	assert.Equal(t, DefaultMaxNodes, maxNodes)
	assert.Equal(t, DefaultMaxRegularTraces, maxRegular)
	assert.Equal(t, DefaultHarvestPeriod, harvest)
	assert.Equal(t, DefaultCompletionHoldback, holdback)
}

func TestDefaultsFromEnv_NegativeFallsBack(t *testing.T) {
	t.Setenv(EnvMaxNodes, "-1")
	t.Setenv(EnvMaxRegularTraces, "-5")
	t.Setenv(EnvHarvestPeriodMillis, "-100")
	t.Setenv(EnvCompletionHoldbackMillis, "-1")

	maxNodes, maxRegular, harvest, holdback := defaultsFromEnv()
	assert.Equal(t, DefaultMaxNodes, maxNodes)
	assert.Equal(t, DefaultMaxRegularTraces, maxRegular)
	assert.Equal(t, DefaultHarvestPeriod, harvest)
	assert.Equal(t, DefaultCompletionHoldback, holdback)
}

func TestDefaultsFromEnv_ParsesValues(t *testing.T) {
	t.Setenv(EnvMaxNodes, "128")
	t.Setenv(EnvMaxRegularTraces, "0")
	t.Setenv(EnvHarvestPeriodMillis, "5000")
	t.Setenv(EnvCompletionHoldbackMillis, "250")

	maxNodes, maxRegular, harvest, holdback := defaultsFromEnv()
	assert.Equal(t, 128, maxNodes)
	assert.Equal(t, 0, maxRegular)
	assert.Equal(t, 5*time.Second, harvest)
	assert.Equal(t, 250*time.Millisecond, holdback)
}

func TestNewTransactionSpanProcessor_OptionsOverrideEnv(t *testing.T) {
	t.Setenv(EnvMaxNodes, "64")
	t.Setenv(EnvMaxRegularTraces, "3")
	t.Setenv(EnvHarvestPeriodMillis, "1000")
	t.Setenv(EnvCompletionHoldbackMillis, "50")

	exporter := sdktracetest.NewInMemoryExporter()
	p := NewTransactionSpanProcessor(exporter,
		WithMaxNodes(32),
		WithMaxRegularTraces(0),
		WithHarvestPeriod(2*time.Second),
		WithCompletionHoldback(10*time.Millisecond),
	)
	require.NotNil(t, p)
	assert.Equal(t, 32, p.maxNodes)
	assert.Equal(t, 0, p.maxRegularTraces)
	assert.Equal(t, 2*time.Second, p.harvestPeriod)
	assert.Equal(t, 10*time.Millisecond, p.completionHoldback)
}

func TestNewTransactionSpanProcessor_EnvUsedWhenOptionsUnset(t *testing.T) {
	t.Setenv(EnvMaxNodes, "64")
	t.Setenv(EnvMaxRegularTraces, "0")
	t.Setenv(EnvHarvestPeriodMillis, "1000")
	t.Setenv(EnvCompletionHoldbackMillis, "50")

	exporter := sdktracetest.NewInMemoryExporter()
	p := NewTransactionSpanProcessor(exporter)
	require.NotNil(t, p)
	assert.Equal(t, 64, p.maxNodes)
	assert.Equal(t, 0, p.maxRegularTraces)
	assert.Equal(t, time.Second, p.harvestPeriod)
	assert.Equal(t, 50*time.Millisecond, p.completionHoldback)
}
