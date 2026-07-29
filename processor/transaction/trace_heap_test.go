package transaction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracecore "go.opentelemetry.io/otel/trace"
)

func newFakeSpanWithParent(spanID byte, parent tracecore.SpanContext, start, end time.Time) sdktrace.ReadOnlySpan {
	sc := tracecore.NewSpanContext(tracecore.SpanContextConfig{
		TraceID: tracecore.TraceID{0x01},
		SpanID:  tracecore.SpanID{spanID},
	})
	stub := sdktracetest.SpanStub{
		SpanContext: sc,
		Parent:      parent,
		StartTime:   start,
		EndTime:     end,
	}
	return stub.Snapshot()
}

func spanCtx(spanID byte) tracecore.SpanContext {
	return tracecore.NewSpanContext(tracecore.SpanContextConfig{
		TraceID: tracecore.TraceID{0x01},
		SpanID:  tracecore.SpanID{spanID},
	})
}

func TestSelectSlowestSpans_NoTrimWhenUnderLimit(t *testing.T) {
	base := time.Unix(0, 0)
	root := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(100*time.Millisecond))
	child := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(10*time.Millisecond))

	spans := []sdktrace.ReadOnlySpan{root, child}
	out := selectSlowestSpans(spans, 256, spanCtx(1).SpanID())

	require.Len(t, out, 2)
	assert.Equal(t, tracecore.SpanID{1}, out[0].SpanContext().SpanID())
	assert.Equal(t, tracecore.SpanID{2}, out[1].SpanContext().SpanID())
}

func TestSelectSlowestSpans_MaxNodesZeroOrNegativeDisablesTrim(t *testing.T) {
	base := time.Unix(0, 0)
	root := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(100*time.Millisecond))
	child := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(10*time.Millisecond))
	spans := []sdktrace.ReadOnlySpan{root, child}

	assert.Equal(t, spans, selectSlowestSpans(spans, 0, spanCtx(1).SpanID()))
	assert.Equal(t, spans, selectSlowestSpans(spans, -1, spanCtx(1).SpanID()))
}

func TestSelectSlowestSpans_KeepsRootAndSlowestChildren(t *testing.T) {
	base := time.Unix(0, 0)
	root := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(1*time.Millisecond))
	fastChild := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(5*time.Millisecond))
	slowChild := newFakeSpanWithParent(3, spanCtx(1), base, base.Add(50*time.Millisecond))
	mediumChild := newFakeSpanWithParent(4, spanCtx(1), base, base.Add(20*time.Millisecond))

	spans := []sdktrace.ReadOnlySpan{root, fastChild, slowChild, mediumChild}

	out := selectSlowestSpans(spans, 3, spanCtx(1).SpanID())

	require.Len(t, out, 3)
	names := spanIDSet(out)
	assert.Contains(t, names, tracecore.SpanID{1}, "root must be kept")
	assert.Contains(t, names, tracecore.SpanID{3}, "slowest child must be kept")
	assert.Contains(t, names, tracecore.SpanID{4}, "second slowest child must be kept")
	assert.NotContains(t, names, tracecore.SpanID{2}, "fastest child must be evicted")
}

func TestSelectSlowestSpans_ReparentsToNearestKeptAncestor(t *testing.T) {
	base := time.Unix(0, 0)
	root := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(100*time.Millisecond))
	droppedMid := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(1*time.Millisecond))
	grandchild := newFakeSpanWithParent(3, spanCtx(2), base, base.Add(60*time.Millisecond))
	spans := []sdktrace.ReadOnlySpan{root, droppedMid, grandchild}

	out := selectSlowestSpans(spans, 2, spanCtx(1).SpanID())

	require.Len(t, out, 2)
	var kept sdktrace.ReadOnlySpan
	for _, s := range out {
		if s.SpanContext().SpanID() == (tracecore.SpanID{3}) {
			kept = s
		}
	}
	require.NotNil(t, kept, "grandchild must survive (slowest non-root span)")
	assert.Equal(t, tracecore.SpanID{1}, kept.Parent().SpanID(), "grandchild must reparent to root")
}

func TestSelectSlowestSpans_ReparentsToNearestKeptAncestorTwoLevelsUp(t *testing.T) {
	base := time.Unix(0, 0)
	root := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(100*time.Millisecond))
	a := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(1*time.Millisecond))
	b := newFakeSpanWithParent(3, spanCtx(2), base, base.Add(1*time.Millisecond))
	c := newFakeSpanWithParent(4, spanCtx(3), base, base.Add(80*time.Millisecond))
	spans := []sdktrace.ReadOnlySpan{root, a, b, c}

	out := selectSlowestSpans(spans, 2, spanCtx(1).SpanID())

	require.Len(t, out, 2)
	ids := spanIDSet(out)
	assert.Contains(t, ids, tracecore.SpanID{1})
	assert.Contains(t, ids, tracecore.SpanID{4})

	var kept sdktrace.ReadOnlySpan
	for _, s := range out {
		if s.SpanContext().SpanID() == (tracecore.SpanID{4}) {
			kept = s
		}
	}
	require.NotNil(t, kept)
	assert.Equal(t, tracecore.SpanID{1}, kept.Parent().SpanID())
}

func TestSelectSlowestSpans_DroppedSpanWithNoKeptAncestorBecomesRootless(t *testing.T) {
	base := time.Unix(0, 0)
	fastParent := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(1*time.Millisecond))
	slowChild := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(50*time.Millisecond))
	otherSlow := newFakeSpanWithParent(3, tracecore.SpanContext{}, base, base.Add(60*time.Millisecond))
	spans := []sdktrace.ReadOnlySpan{fastParent, slowChild, otherSlow}

	out := selectSlowestSpans(spans, 2, tracecore.SpanID{})

	require.Len(t, out, 2)
	var child sdktrace.ReadOnlySpan
	for _, s := range out {
		if s.SpanContext().SpanID() == (tracecore.SpanID{2}) {
			child = s
		}
	}
	require.NotNil(t, child)
	assert.False(t, child.Parent().IsValid(), "child of an evicted rootless ancestor must lose its parent")
}

func TestSelectSlowestSpans_PreservesOriginalOrder(t *testing.T) {
	base := time.Unix(0, 0)
	root := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(10*time.Millisecond))
	c1 := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(30*time.Millisecond))
	c2 := newFakeSpanWithParent(3, spanCtx(1), base, base.Add(20*time.Millisecond))
	spans := []sdktrace.ReadOnlySpan{root, c1, c2}

	out := selectSlowestSpans(spans, 3, spanCtx(1).SpanID())

	require.Len(t, out, 3)
	assert.Equal(t, tracecore.SpanID{1}, out[0].SpanContext().SpanID())
	assert.Equal(t, tracecore.SpanID{2}, out[1].SpanContext().SpanID())
	assert.Equal(t, tracecore.SpanID{3}, out[2].SpanContext().SpanID())
}

func TestSelectSlowestSpans_ProtectsAllTransactionRoots(t *testing.T) {
	base := time.Unix(0, 0)
	rootA := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(1*time.Millisecond))
	rootB := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(1*time.Millisecond))
	slow := newFakeSpanWithParent(3, spanCtx(1), base, base.Add(100*time.Millisecond))
	spans := []sdktrace.ReadOnlySpan{rootA, rootB, slow}

	out := selectSlowestSpans(spans, 2, spanCtx(1).SpanID(), spanCtx(2).SpanID())

	ids := spanIDSet(out)
	assert.Contains(t, ids, tracecore.SpanID{1})
	assert.Contains(t, ids, tracecore.SpanID{2})
	assert.NotContains(t, ids, tracecore.SpanID{3}, "non-root loses when roots fill maxNodes")
}

func spanIDSet(spans []sdktrace.ReadOnlySpan) map[tracecore.SpanID]struct{} {
	out := make(map[tracecore.SpanID]struct{}, len(spans))
	for _, s := range spans {
		out[s.SpanContext().SpanID()] = struct{}{}
	}
	return out
}
