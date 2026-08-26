package transaction

import (
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

func TestRegularTraceHeap_MaxTracesZeroNeverKeepsAnything(t *testing.T) {
	h := newRegularTraceHeap(0)

	stubs := h.witness(harvestTrace{durationNs: 100, spans: []sdktrace.ReadOnlySpan{stubSpanWithRoot(t, "a", 1, 100, true)}})

	assert.Nil(t, stubs)
	assert.Empty(t, h.drain())
}

func TestRegularTraceHeap_KeepsUpToCapacity(t *testing.T) {
	h := newRegularTraceHeap(2)

	assert.Nil(t, h.witness(harvestTrace{durationNs: 10}))
	assert.Nil(t, h.witness(harvestTrace{durationNs: 20}))

	drained := h.drain()
	require.Len(t, drained, 2)
}

func TestRegularTraceHeap_OnlySlowerTraceEvictsShortest(t *testing.T) {
	h := newRegularTraceHeap(1)
	first := stubSpanWithRoot(t, "first", 1, 100, true)
	short := stubSpanWithRoot(t, "short", 2, 50, true)
	long := stubSpanWithRoot(t, "long", 3, 150, true)

	assert.Nil(t, h.witness(harvestTrace{durationNs: 100, spans: []sdktrace.ReadOnlySpan{first}}))
	equalStubs := h.witness(harvestTrace{durationNs: 100, spans: []sdktrace.ReadOnlySpan{stubSpanWithRoot(t, "eq", 4, 100, true)}})
	require.Len(t, equalStubs, 1)
	assert.Equal(t, "eq", equalStubs[0].Name())
	shortStubs := h.witness(harvestTrace{durationNs: 50, spans: []sdktrace.ReadOnlySpan{short}})
	require.Len(t, shortStubs, 1)
	assert.Equal(t, "short", shortStubs[0].Name())
	displaced := h.witness(harvestTrace{durationNs: 150, spans: []sdktrace.ReadOnlySpan{long}})
	require.Len(t, displaced, 1)
	assert.Equal(t, "first", displaced[0].Name(), "evicted previous winner must stub")

	drained := h.drain()
	require.Len(t, drained, 1)
	assert.Equal(t, int64(150), drained[0].durationNs)
}

func TestRegularTraceHeap_DrainClearsHeap(t *testing.T) {
	h := newRegularTraceHeap(1)
	h.witness(harvestTrace{durationNs: 10})

	first := h.drain()
	require.Len(t, first, 1)

	assert.Empty(t, h.drain(), "drain must clear the heap")
}

func TestRegularTraceHeap_RestoreReappliesCapacity(t *testing.T) {
	h := newRegularTraceHeap(1)
	short := stubSpanWithRoot(t, "short", 1, 50, true)
	long := stubSpanWithRoot(t, "long", 2, 200, true)

	stubs := h.restore([]harvestTrace{
		{durationNs: 50, spans: []sdktrace.ReadOnlySpan{short}},
		{durationNs: 200, spans: []sdktrace.ReadOnlySpan{long}},
	})
	require.LessOrEqual(t, h.Len(), 1)
	require.Len(t, stubs, 1)
	assert.Equal(t, "short", stubs[0].Name())

	drained := h.drain()
	require.Len(t, drained, 1)
	assert.Equal(t, int64(200), drained[0].durationNs)
}

func TestRegularTraceHeap_RestoreEvictsAgainstConcurrentRefill(t *testing.T) {
	h := newRegularTraceHeap(1)
	held := stubSpanWithRoot(t, "held", 1, 100, true)
	concurrent := stubSpanWithRoot(t, "concurrent", 2, 150, true)

	require.Nil(t, h.witness(harvestTrace{durationNs: 100, spans: []sdktrace.ReadOnlySpan{held}}))
	drained := h.drain()
	require.Len(t, drained, 1)

	// Simulate another trace entering harvest while the flush export failed.
	require.Nil(t, h.witness(harvestTrace{durationNs: 150, spans: []sdktrace.ReadOnlySpan{concurrent}}))
	stubs := h.restore(drained)

	require.LessOrEqual(t, h.Len(), 1)
	require.Len(t, stubs, 1)
	assert.Equal(t, "held", stubs[0].Name())
	kept := h.drain()
	require.Len(t, kept, 1)
	assert.Equal(t, int64(150), kept[0].durationNs)
}

func TestRegularTraceHeap_KeepsSlowestNOfManyCompetitors(t *testing.T) {
	h := newRegularTraceHeap(2)
	durations := []int64{5, 50, 10, 100, 1, 30}
	for _, d := range durations {
		h.witness(harvestTrace{durationNs: d})
	}

	drained := h.drain()
	require.Len(t, drained, 2)
	got := map[int64]bool{drained[0].durationNs: true, drained[1].durationNs: true}
	assert.True(t, got[100], "slowest trace must survive")
	assert.True(t, got[50], "second slowest trace must survive")
}

func TestHarvestStubSpans_PrefersTransactionRoot(t *testing.T) {
	root := stubSpanWithRoot(t, "root", 1, 100, true)
	child := stubSpanWithRoot(t, "child", 2, 500, false)

	stubs := harvestStubSpans([]sdktrace.ReadOnlySpan{root, child})

	require.Len(t, stubs, 1)
	assert.Equal(t, "root", stubs[0].Name())
}

func TestRootDurationNanos_UsesTransactionRootSpanWhenPresent(t *testing.T) {
	root := stubSpanWithRoot(t, "root", 1, 100, true)
	child := stubSpanWithRoot(t, "child", 2, 500, false)

	got := rootDurationNanos([]sdktrace.ReadOnlySpan{root, child})

	assert.Equal(t, int64(100), got)
}

func TestRootDurationNanos_FallsBackToMaxDurationWhenNoRootTagged(t *testing.T) {
	a := stubSpanWithRoot(t, "a", 1, 100, false)
	b := stubSpanWithRoot(t, "b", 2, 500, false)

	got := rootDurationNanos([]sdktrace.ReadOnlySpan{a, b})

	assert.Equal(t, int64(500), got)
}

func TestRootDurationNanos_UsesMaxAmongMultipleRoots(t *testing.T) {
	shortLast := stubSpanWithRoot(t, "short", 2, 50, true)
	longFirst := stubSpanWithRoot(t, "long", 1, 200, true)

	got := rootDurationNanos([]sdktrace.ReadOnlySpan{longFirst, shortLast})

	assert.Equal(t, int64(200), got)
}

func stubSpanWithRoot(t *testing.T, name string, spanID byte, durationNs int64, isRoot bool) sdktrace.ReadOnlySpan {
	t.Helper()
	base := time.Unix(0, 0)
	stub := sdktracetest.SpanStub{
		Name: name,
		SpanContext: tracecore.NewSpanContext(tracecore.SpanContextConfig{
			TraceID: tracecore.TraceID{0x01},
			SpanID:  tracecore.SpanID{spanID},
		}),
		StartTime: base,
		EndTime:   base.Add(time.Duration(durationNs)),
	}
	if isRoot {
		stub.Attributes = []attribute.KeyValue{attribute.Bool(sampler.TransactionIdentifierRoot, true)}
	}
	return stub.Snapshot()
}
