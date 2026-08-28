package transaction

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracecore "go.opentelemetry.io/otel/trace"

	metricglobal "go.opentelemetry.io/otel/metric/global"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

func TestWithMeterProvider_RecordsSelfDurationHistogram(t *testing.T) {
	reader := metricsdk.NewManualReader()
	meterProvider := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))

	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithMeterProvider(meterProvider), WithCompletionHoldback(0))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
	)
	tracer := tp.Tracer("metric-test")
	base := time.Unix(0, 0)

	rootCtx, rootSpan := tracer.Start(context.Background(), "root-server",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithTimestamp(base),
	)
	_, childSpan := tracer.Start(rootCtx, "child-work", tracecore.WithTimestamp(base.Add(20*time.Millisecond)))
	childSpan.End(tracecore.WithTimestamp(base.Add(80 * time.Millisecond)))
	rootSpan.End(tracecore.WithTimestamp(base.Add(100 * time.Millisecond)))

	require.NoError(t, tp.Shutdown(context.Background()))

	rm, err := reader.Collect(context.Background())
	require.NoError(t, err)

	dataPoints := findHistogramDataPoints(t, rm, SelfDurationMetricName)
	require.Len(t, dataPoints, 2)

	seen := map[string]metricdata.HistogramDataPoint{}
	for _, dp := range dataPoints {
		spanName, ok := dp.Attributes.Value(attribute.Key(SpanNameMetricAttribute))
		require.True(t, ok)
		seen[spanName.AsString()] = dp
	}

	rootDP, ok := seen["root-server"]
	require.True(t, ok)
	assert.Equal(t, uint64(1), rootDP.Count)
	assert.InDelta(t, 0.04, sumHistogram(rootDP), 1e-9)
	rootTxn, _ := rootDP.Attributes.Value(attribute.Key(sampler.TransactionIdentifier))
	assert.Equal(t, "root-server", rootTxn.AsString())
	_, hasRootFlag := rootDP.Attributes.Value(attribute.Key(sampler.TransactionIdentifierRoot))
	assert.True(t, hasRootFlag)

	childDP, ok := seen["child-work"]
	require.True(t, ok)
	assert.InDelta(t, 0.06, sumHistogram(childDP), 1e-9)
	_, hasRootFlagChild := childDP.Attributes.Value(attribute.Key(sampler.TransactionIdentifierRoot))
	assert.False(t, hasRootFlagChild)
}

func TestNewTransactionSpanProcessor_FallsBackToGlobalMeterProvider(t *testing.T) {
	reader := metricsdk.NewManualReader()
	meterProvider := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))

	previous := metricglobal.MeterProvider()
	metricglobal.SetMeterProvider(meterProvider)
	defer metricglobal.SetMeterProvider(previous)

	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("metric-global-fallback-test")

	_, span := tracer.Start(context.Background(), "root-server",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	span.End()

	require.NoError(t, tp.Shutdown(context.Background()))

	rm, err := reader.Collect(context.Background())
	require.NoError(t, err)

	dataPoints := findHistogramDataPoints(t, rm, SelfDurationMetricName)
	require.Len(t, dataPoints, 1)
}

func TestSelfDurationMetrics_OnlyForEligibleBatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		spans   int
		metrics uint64
	}{
		{name: "130 spans", spans: 130, metrics: 130},
		{name: "260 spans", spans: 260, metrics: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := metricsdk.NewManualReader()
			meterProvider := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
			exporter := sdktracetest.NewInMemoryExporter()
			processor := NewTransactionSpanProcessor(exporter, WithMeterProvider(meterProvider), WithCompletionHoldback(0))
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
			tracer := tp.Tracer("self-duration-cap-metric-test")

			rootCtx, root := tracer.Start(context.Background(), "root", tracecore.WithSpanKind(tracecore.SpanKindServer))
			for i := 1; i < tc.spans; i++ {
				_, child := tracer.Start(rootCtx, "child")
				child.End()
			}
			root.End()
			require.NoError(t, tp.Shutdown(context.Background()))

			rm, err := reader.Collect(context.Background())
			require.NoError(t, err)
			var count uint64
			for _, dp := range findHistogramDataPoints(t, rm, SelfDurationMetricName) {
				count += dp.Count
			}
			assert.Equal(t, tc.metrics, count)
		})
	}
}

func findHistogramDataPoints(t *testing.T, rm metricdata.ResourceMetrics, metricName string) []metricdata.HistogramDataPoint {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram)
			require.True(t, ok, "expected %s to be a Histogram aggregation", metricName)
			return hist.DataPoints
		}
	}
	return nil
}

func sumHistogram(dp metricdata.HistogramDataPoint) float64 {
	if dp.Min != nil {
		return *dp.Min
	}
	return 0
}
