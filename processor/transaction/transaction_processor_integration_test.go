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

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

func TestIntegration_ProcessorOnly(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
	)
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()

	tracer := tp.Tracer("integration-test")
	base := time.Unix(0, 0)

	rootCtx, rootSpan := tracer.Start(context.Background(), "root-server",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithTimestamp(base),
	)
	_, childSpan := tracer.Start(rootCtx, "child-work",
		tracecore.WithTimestamp(base.Add(20*time.Millisecond)),
	)
	childSpan.End(tracecore.WithTimestamp(base.Add(80 * time.Millisecond)))
	rootSpan.End(tracecore.WithTimestamp(base.Add(100 * time.Millisecond)))

	spans := exporter.GetSpans()
	require.Len(t, spans, 2)

	var root, child *sdktracetest.SpanStub
	for i := range spans {
		switch spans[i].Name {
		case "root-server":
			root = &spans[i]
		case "child-work":
			child = &spans[i]
		}
	}
	require.NotNil(t, root, "root span not exported")
	require.NotNil(t, child, "child span not exported")

	assertAttribute(t, root.Attributes, sampler.TransactionIdentifier, "root-server")
	assertBoolAttribute(t, root.Attributes, sampler.TransactionIdentifierRoot, true)
	assertNoAttribute(t, root.Attributes, sampler.DistributedTransactionIdentifier)

	assertAttribute(t, child.Attributes, sampler.TransactionIdentifier, "root-server")
	assertNoAttribute(t, child.Attributes, sampler.TransactionIdentifierRoot)
	assertNoAttribute(t, child.Attributes, sampler.DistributedTransactionIdentifier)

	assertFloat64Attribute(t, root.Attributes, SelfDurationAttribute, 0.04)
	assertFloat64Attribute(t, child.Attributes, SelfDurationAttribute, 0.06)
}

func TestIntegration_NestedLocalTransactionFinalizesBeforeOuterEnds(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
	)
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()

	tracer := tp.Tracer("integration-test")
	base := time.Unix(0, 0)

	outerCtx, outerSpan := tracer.Start(context.Background(), "outer-server",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithTimestamp(base),
	)

	innerCtx, innerSpan := tracer.Start(outerCtx, "inner-server",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithTimestamp(base.Add(10*time.Millisecond)),
	)
	_, innerChild := tracer.Start(innerCtx, "inner-child",
		tracecore.WithTimestamp(base.Add(20*time.Millisecond)),
	)
	innerChild.End(tracecore.WithTimestamp(base.Add(50 * time.Millisecond)))
	innerSpan.End(tracecore.WithTimestamp(base.Add(60 * time.Millisecond)))

	beforeOuterEnds := exporter.GetSpans()
	require.Len(t, beforeOuterEnds, 2, "nested local transaction must finalize before outer ends")

	names := map[string]bool{}
	for _, s := range beforeOuterEnds {
		names[s.Name] = true
	}
	assert.True(t, names["inner-server"], "inner-server must have finalized already")
	assert.True(t, names["inner-child"], "inner-child must have finalized already")
	assert.False(t, names["outer-server"], "outer-server must not finalize yet")

	inner := findSpan(t, beforeOuterEnds, "inner-server")
	assertAttribute(t, inner.Attributes, sampler.TransactionIdentifier, "inner-server")
	assertBoolAttribute(t, inner.Attributes, sampler.TransactionIdentifierRoot, true)
	assertFloat64Attribute(t, inner.Attributes, SelfDurationAttribute, 0.02)

	_, outerChild := tracer.Start(outerCtx, "outer-child",
		tracecore.WithTimestamp(base.Add(70*time.Millisecond)),
	)
	outerChild.End(tracecore.WithTimestamp(base.Add(90 * time.Millisecond)))
	outerSpan.End(tracecore.WithTimestamp(base.Add(100 * time.Millisecond)))

	spans := exporter.GetSpans()
	require.Len(t, spans, 4, "outer local transaction must finalize once outer ends, without re-exporting nested spans")

	outer := findSpan(t, spans, "outer-server")
	assertAttribute(t, outer.Attributes, sampler.TransactionIdentifier, "outer-server")
	assertBoolAttribute(t, outer.Attributes, sampler.TransactionIdentifierRoot, true)
	assertFloat64Attribute(t, outer.Attributes, SelfDurationAttribute, 0.03)

	outerChildStub := findSpan(t, spans, "outer-child")
	assertAttribute(t, outerChildStub.Attributes, sampler.TransactionIdentifier, "outer-server")
	assertNoAttribute(t, outerChildStub.Attributes, sampler.TransactionIdentifierRoot)
}

func TestIntegration_OuterEndsBeforeNestedStillSeparateTxns(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()

	tracer := tp.Tracer("integration-test")
	base := time.Unix(0, 0)

	outerCtx, outerSpan := tracer.Start(context.Background(), "outer",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithTimestamp(base),
	)
	nestedCtx, nestedSpan := tracer.Start(outerCtx, "nested",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithTimestamp(base.Add(10*time.Millisecond)),
	)
	_, nestedChild := tracer.Start(nestedCtx, "nested-child",
		tracecore.WithTimestamp(base.Add(20*time.Millisecond)),
	)

	outerSpan.End(tracecore.WithTimestamp(base.Add(40 * time.Millisecond)))
	assert.Empty(t, exporter.GetSpans(), "outer must stay buffered while nested subtree is still live")

	nestedChild.End(tracecore.WithTimestamp(base.Add(50 * time.Millisecond)))
	nestedSpan.End(tracecore.WithTimestamp(base.Add(60 * time.Millisecond)))

	spans := exporter.GetSpans()
	require.Len(t, spans, 3, "nested + outer must export as separate batches")
	assert.NotNil(t, findSpan(t, spans, "nested"))
	assert.NotNil(t, findSpan(t, spans, "nested-child"))
	assert.NotNil(t, findSpan(t, spans, "outer"))

	nestedRoot := findSpan(t, spans, "nested")
	outerRoot := findSpan(t, spans, "outer")
	assertBoolAttribute(t, nestedRoot.Attributes, sampler.TransactionIdentifierRoot, true)
	assertBoolAttribute(t, outerRoot.Attributes, sampler.TransactionIdentifierRoot, true)
	assertAttribute(t, nestedRoot.Attributes, sampler.TransactionIdentifier, "nested")
	assertAttribute(t, outerRoot.Attributes, sampler.TransactionIdentifier, "outer")
}

func TestIntegration_LateChildOfFinalizedNestedExportsWhileOuterLive(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("late-rootless-while-outer")

	outerCtx, outer := tracer.Start(context.Background(), "outer",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	nestedCtx, nested := tracer.Start(outerCtx, "nested",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	nested.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 1)
	assertAttribute(t, exporter.GetSpans()[0].Attributes, sampler.TransactionIdentifier, "nested")
	exporter.Reset()

	// Fire-and-forget child of the already-finalized nested root while outer
	// is still live must not stay buffered until outer ends.
	_, late := tracer.Start(nestedCtx, "late-nested-child",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	late.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	lateSpans := exporter.GetSpans()
	require.Len(t, lateSpans, 1, "late child of finalized nested must export while outer is live")
	assertAttribute(t, lateSpans[0].Attributes, sampler.TransactionIdentifier, "nested")
	assertNoAttribute(t, lateSpans[0].Attributes, sampler.TransactionIdentifierRoot)
	exporter.Reset()

	outer.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	outerSpans := exporter.GetSpans()
	require.Len(t, outerSpans, 1)
	assertAttribute(t, outerSpans[0].Attributes, sampler.TransactionIdentifier, "outer")
}

func TestIntegration_RootlessPartitionsExportIndependently(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("rootless-export-partitions")

	outerCtx, outer := tracer.Start(context.Background(), "gateway",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	aCtx, a := tracer.Start(outerCtx, "txn-a",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	bCtx, b := tracer.Start(outerCtx, "txn-b",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	a.End()
	b.End()
	outer.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 3)
	exporter.Reset()

	_, lateA := tracer.Start(aCtx, "late-a",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	_, lateB := tracer.Start(bCtx, "late-b",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	lateA.End()
	lateB.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 2, "each rootless partition must export its span")
	assertAttribute(t, findSpan(t, spans, "late-a").Attributes, sampler.TransactionIdentifier, "txn-a")
	assertAttribute(t, findSpan(t, spans, "late-b").Attributes, sampler.TransactionIdentifier, "txn-b")
}

func TestIntegration_ExportsEveryCompletedTraceImmediately(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("immediate-export-test")

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

func TestIntegration_EnrichesOnlyEligibleBatch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		spans    int
		enriched bool
	}{
		{name: "130 spans", spans: 130, enriched: true},
		{name: "260 spans", spans: 260, enriched: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exporter := sdktracetest.NewInMemoryExporter()
			processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
			tracer := tp.Tracer("self-duration-cap-test")
			base := time.Unix(0, 0)

			rootCtx, root := tracer.Start(context.Background(), "root", tracecore.WithSpanKind(tracecore.SpanKindServer), tracecore.WithTimestamp(base))
			for i := 1; i < tc.spans; i++ {
				_, child := tracer.Start(rootCtx, "child", tracecore.WithTimestamp(base))
				child.End(tracecore.WithTimestamp(base.Add(time.Millisecond)))
				if tc.spans > DefaultMaxTransactionSpans && i == DefaultMaxTransactionSpans+1 {
					require.Len(t, exporter.GetSpans(), DefaultMaxTransactionSpans+1,
						"the 257th completed span must flush the raw buffered batch")
				}
			}
			root.End(tracecore.WithTimestamp(base.Add(100 * time.Millisecond)))

			spans := exporter.GetSpans()
			require.Len(t, spans, tc.spans, "all spans must export")
			for _, span := range spans {
				if tc.enriched {
					assertHasAttribute(t, span.Attributes, SelfDurationAttribute)
					assertAttribute(t, span.Attributes, sampler.TransactionIdentifier, "root")
				} else {
					assertNoAttribute(t, span.Attributes, SelfDurationAttribute)
					assertNoAttribute(t, span.Attributes, sampler.TransactionIdentifier)
				}
			}
			require.NoError(t, tp.Shutdown(context.Background()))
		})
	}
}

func TestIntegration_TransactionSpanLimitFlushesRaw(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithCompletionHoldback(0), WithMaxTransactionSpans(2))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("transaction-limit-test")

	rootCtx, root := tracer.Start(context.Background(), "root", tracecore.WithSpanKind(tracecore.SpanKindServer))
	for i := 0; i < 3; i++ {
		_, child := tracer.Start(rootCtx, "child")
		child.End()
	}
	require.Len(t, exporter.GetSpans(), 3, "third completed span flushes raw")
	root.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 4)
	for _, span := range spans {
		assertNoAttribute(t, span.Attributes, SelfDurationAttribute)
		assertNoAttribute(t, span.Attributes, sampler.TransactionIdentifier)
	}
	require.NoError(t, tp.Shutdown(context.Background()))
}

func TestIntegration_RawPassthroughPreservesExplicitTransactionAttributes(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithCompletionHoldback(0), WithMaxTransactionSpans(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("raw-explicit-transaction")

	_, span := tracer.Start(context.Background(), "operation", tracecore.WithSpanKind(tracecore.SpanKindServer))
	sampler.StartNewTransaction(span, "explicit-transaction")
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "explicit-transaction")
	assertHasAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot)
	assertHasAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierExplicit)
	assertNoAttribute(t, spans[0].Attributes, SelfDurationAttribute)
	require.NoError(t, tp.Shutdown(context.Background()))
}

func TestIntegration_LateSpansStayRawAfterTraceOverflows(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithCompletionHoldback(0), WithMaxTransactionSpans(1))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("late-raw-passthrough")

	rootCtx, root := tracer.Start(context.Background(), "root", tracecore.WithSpanKind(tracecore.SpanKindServer))
	_, first := tracer.Start(rootCtx, "first")
	_, second := tracer.Start(rootCtx, "second")
	first.End()
	second.End()
	root.End()

	_, late := tracer.Start(rootCtx, "late")
	late.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 4)
	lateSpan := findSpan(t, spans, "late")
	assertNoAttribute(t, lateSpan.Attributes, sampler.TransactionIdentifier)
	assertNoAttribute(t, lateSpan.Attributes, SelfDurationAttribute)
	require.NoError(t, tp.Shutdown(context.Background()))
}

func TestIntegration_MaxTracesUsesRawPassthrough(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithCompletionHoldback(0), WithMaxTraces(1))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("trace-limit-test")

	_, first := tracer.Start(context.Background(), "first", tracecore.WithSpanKind(tracecore.SpanKindServer))
	_, second := tracer.Start(context.Background(), "second", tracecore.WithSpanKind(tracecore.SpanKindServer))
	second.End()
	require.Len(t, exporter.GetSpans(), 1)
	assertNoAttribute(t, exporter.GetSpans()[0].Attributes, sampler.TransactionIdentifier)
	assertNoAttribute(t, exporter.GetSpans()[0].Attributes, SelfDurationAttribute)

	first.End()
	require.Len(t, exporter.GetSpans(), 2)

	_, third := tracer.Start(context.Background(), "third", tracecore.WithSpanKind(tracecore.SpanKindServer))
	third.End()
	require.Len(t, exporter.GetSpans(), 3)
	assertHasAttribute(t, exporter.GetSpans()[2].Attributes, sampler.TransactionIdentifier)
	require.NoError(t, tp.Shutdown(context.Background()))
}

func TestIntegration_ZeroMaxTracesIsUnlimited(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithCompletionHoldback(0), WithMaxTraces(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("zero-trace-limit-test")

	_, span := tracer.Start(context.Background(), "unlimited", tracecore.WithSpanKind(tracecore.SpanKindServer))
	span.End()
	require.Len(t, exporter.GetSpans(), 1)
	assertHasAttribute(t, exporter.GetSpans()[0].Attributes, sampler.TransactionIdentifier)
	assertHasAttribute(t, exporter.GetSpans()[0].Attributes, SelfDurationAttribute)
	require.NoError(t, tp.Shutdown(context.Background()))
}

func assertHasAttribute(t *testing.T, attrs []attribute.KeyValue, key string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			return
		}
	}
	t.Fatalf("attribute %q not found", key)
}

func assertAttribute(t *testing.T, attrs []attribute.KeyValue, key, expected string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			assert.Equal(t, expected, a.Value.AsString())
			return
		}
	}
	t.Fatalf("attribute %q not found", key)
}

func assertBoolAttribute(t *testing.T, attrs []attribute.KeyValue, key string, expected bool) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			assert.Equal(t, expected, a.Value.AsBool())
			return
		}
	}
	t.Fatalf("attribute %q not found", key)
}

func assertFloat64Attribute(t *testing.T, attrs []attribute.KeyValue, key string, expected float64) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			assert.InDelta(t, expected, a.Value.AsFloat64(), 1e-12)
			return
		}
	}
	t.Fatalf("attribute %q not found", key)
}

func assertNoAttribute(t *testing.T, attrs []attribute.KeyValue, key string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key {
			t.Fatalf("attribute %q unexpectedly present", key)
		}
	}
}
