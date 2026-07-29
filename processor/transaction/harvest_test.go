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

	kept := h.witness(harvestTrace{durationNs: 100})

	assert.False(t, kept)
	assert.Empty(t, h.drain())
}

func TestRegularTraceHeap_KeepsUpToCapacity(t *testing.T) {
	h := newRegularTraceHeap(2)

	assert.True(t, h.witness(harvestTrace{durationNs: 10}))
	assert.True(t, h.witness(harvestTrace{durationNs: 20}))

	drained := h.drain()
	require.Len(t, drained, 2)
}

func TestRegularTraceHeap_OnlySlowerTraceEvictsShortest(t *testing.T) {
	h := newRegularTraceHeap(1)

	assert.True(t, h.witness(harvestTrace{durationNs: 100}))
	assert.False(t, h.witness(harvestTrace{durationNs: 100}))
	assert.False(t, h.witness(harvestTrace{durationNs: 50}))
	assert.True(t, h.witness(harvestTrace{durationNs: 150}))

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
