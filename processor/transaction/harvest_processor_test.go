package transaction

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracecore "go.opentelemetry.io/otel/trace"

	metricsdk "go.opentelemetry.io/otel/sdk/metric"
)

func TestProcessor_HarvestKeepsOnlySlowestTraceOnForceFlush(t *testing.T) {
	reader := metricsdk.NewManualReader()
	meterProvider := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))

	exporter := sdktracetest.NewInMemoryExporter()
	// Long harvest period: rely on ForceFlush, not the ticker, for determinism.
	processor := NewTransactionSpanProcessor(exporter,
		WithMeterProvider(meterProvider),
		WithHarvestPeriod(time.Hour),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("harvest-test")
	base := time.Unix(0, 0)

	_, fast := tracer.Start(context.Background(), "fast-transaction",
		tracecore.WithSpanKind(tracecore.SpanKindServer), tracecore.WithTimestamp(base))
	fast.End(tracecore.WithTimestamp(base.Add(10 * time.Millisecond)))

	_, slow := tracer.Start(context.Background(), "slow-transaction",
		tracecore.WithSpanKind(tracecore.SpanKindServer), tracecore.WithTimestamp(base))
	slow.End(tracecore.WithTimestamp(base.Add(200 * time.Millisecond)))

	require.Len(t, exporter.GetSpans(), 1, "displaced fast loser must stub-export immediately")
	assert.Equal(t, "fast-transaction", exporter.GetSpans()[0].Name)

	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 2, "slowest full waterfall + stub root for the loser")
	names := map[string]bool{}
	for _, s := range spans {
		names[s.Name] = true
	}
	assert.True(t, names["slow-transaction"], "slowest full trace must export")
	assert.True(t, names["fast-transaction"], "loser must still export a root stub")

	rm, err := reader.Collect(context.Background())
	require.NoError(t, err)
	dataPoints := findHistogramDataPoints(t, rm, SelfDurationMetricName)
	require.Len(t, dataPoints, 2, "self-time metric must be recorded for every completed trace")

	require.NoError(t, tp.Shutdown(context.Background()))
}

func TestProcessor_HarvestTickerFlushesWinnersPeriodically(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithHarvestPeriod(20*time.Millisecond), WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("harvest-ticker-test")

	_, span := tracer.Start(context.Background(), "root",
		tracecore.WithSpanKind(tracecore.SpanKindServer))
	span.End()

	assert.Empty(t, exporter.GetSpans(), "trace should be held until the harvest ticker fires")

	require.Eventually(t, func() bool {
		return len(exporter.GetSpans()) == 1
	}, time.Second, 5*time.Millisecond, "harvest ticker must flush the winning trace")

	require.NoError(t, tp.Shutdown(context.Background()))
}

func TestProcessor_MaxRegularTracesZeroExportsEveryCompletedTraceImmediately(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithMaxRegularTraces(0), WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("harvest-disabled-test")

	_, first := tracer.Start(context.Background(), "first",
		tracecore.WithSpanKind(tracecore.SpanKindServer))
	first.End()
	require.Len(t, exporter.GetSpans(), 1)

	_, second := tracer.Start(context.Background(), "second",
		tracecore.WithSpanKind(tracecore.SpanKindServer))
	second.End()
	require.Len(t, exporter.GetSpans(), 2, "every completed trace must export immediately")

	require.NoError(t, tp.Shutdown(context.Background()))
}

func TestProcessor_TrimAppliesBeforeHarvest(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxNodes(2),
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("trim-test")
	base := time.Unix(0, 0)

	rootCtx, root := tracer.Start(context.Background(), "root",
		tracecore.WithSpanKind(tracecore.SpanKindServer), tracecore.WithTimestamp(base))
	_, fastChild := tracer.Start(rootCtx, "fast-child", tracecore.WithTimestamp(base))
	fastChild.End(tracecore.WithTimestamp(base.Add(1 * time.Millisecond)))
	_, slowChild := tracer.Start(rootCtx, "slow-child", tracecore.WithTimestamp(base))
	slowChild.End(tracecore.WithTimestamp(base.Add(90 * time.Millisecond)))
	root.End(tracecore.WithTimestamp(base.Add(100 * time.Millisecond)))

	spans := exporter.GetSpans()
	require.Len(t, spans, 2, "trimmed to maxNodes=2: root + slowest child")

	names := map[string]bool{}
	for _, s := range spans {
		names[s.Name] = true
	}
	assert.True(t, names["root"], "root must always survive trim")
	assert.True(t, names["slow-child"], "slowest non-root span must survive trim")
	assert.False(t, names["fast-child"], "fastest non-root span must be trimmed")

	require.NoError(t, tp.Shutdown(context.Background()))
}
