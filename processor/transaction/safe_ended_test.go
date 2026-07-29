package transaction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"
)

func TestSafeEndedSpans_BlocksFullAncestorChainOfLiveSpan(t *testing.T) {
	base := time.Unix(0, 0)
	a := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(100*time.Millisecond))
	b := newFakeSpanWithParent(2, spanCtx(1), base, base.Add(50*time.Millisecond))
	d := newFakeSpanWithParent(4, spanCtx(1), base, base.Add(10*time.Millisecond))

	liveC := tracecore.SpanID{3}
	tb := &traceBuffer{
		spans: []sdktrace.ReadOnlySpan{a, b, d},
		liveParents: map[tracecore.SpanID]tracecore.SpanID{
			liveC: tracecore.SpanID{2},
		},
	}

	out := tb.safeEndedSpans()
	require.Len(t, out, 1)
	assert.Equal(t, tracecore.SpanID{4}, out[0].SpanContext().SpanID())
}

func TestSafeEndedSpans_NoLiveReturnsAll(t *testing.T) {
	base := time.Unix(0, 0)
	a := newFakeSpanWithParent(1, tracecore.SpanContext{}, base, base.Add(10*time.Millisecond))
	tb := &traceBuffer{
		spans:       []sdktrace.ReadOnlySpan{a},
		liveParents: map[tracecore.SpanID]tracecore.SpanID{},
	}
	assert.Equal(t, tb.spans, tb.safeEndedSpans())
}
