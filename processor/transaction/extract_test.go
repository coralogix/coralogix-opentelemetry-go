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

func fakeRootSpan(name string, spanID, parentID byte, start, end time.Time, root bool) sdktrace.ReadOnlySpan {
	stub := sdktracetest.SpanStub{
		Name: name,
		SpanContext: tracecore.NewSpanContext(tracecore.SpanContextConfig{
			TraceID: tracecore.TraceID{0xde, 0xef},
			SpanID:  tracecore.SpanID{spanID},
		}),
		StartTime: start,
		EndTime:   end,
	}
	if parentID != 0 {
		stub.Parent = tracecore.NewSpanContext(tracecore.SpanContextConfig{
			TraceID: tracecore.TraceID{0xde, 0xef},
			SpanID:  tracecore.SpanID{parentID},
		})
	}
	if root {
		stub.Attributes = []attribute.KeyValue{
			attribute.Bool(sampler.TransactionIdentifierRoot, true),
		}
	}
	return stub.Snapshot()
}

func TestExtractCompleted_RootsDeepestFirstExcludesExtracted(t *testing.T) {
	base := time.Unix(0, 0)
	outer := fakeRootSpan("outer", 1, 0, base, base.Add(100*time.Millisecond), true)
	nested := fakeRootSpan("inner", 2, 1, base.Add(10*time.Millisecond), base.Add(60*time.Millisecond), true)
	child := fakeRootSpan("db", 3, 2, base.Add(20*time.Millisecond), base.Add(50*time.Millisecond), false)

	processor := NewTransactionSpanProcessor(nil, WithMaxRegularTraces(0), WithCompletionHoldback(0))
	tb := &traceBuffer{
		spans:       []sdktrace.ReadOnlySpan{outer, nested, child},
		liveParents: map[tracecore.SpanID]tracecore.SpanID{},
	}

	processor.mu.Lock()
	batches := processor.extractCompletedLocalTransactionsLocked(tb, true)
	processor.mu.Unlock()

	require.Len(t, batches, 2)
	require.Len(t, batches[0], 2)
	assert.Equal(t, "inner", batches[0][0].Name())
	assert.Equal(t, "db", batches[0][1].Name())
	require.Len(t, batches[1], 1)
	assert.Equal(t, "outer", batches[1][0].Name())

	seen := make(map[tracecore.SpanID]struct{})
	for _, batch := range batches {
		for _, s := range batch {
			sid := s.SpanContext().SpanID()
			_, dup := seen[sid]
			assert.False(t, dup, "no span exported twice")
			seen[sid] = struct{}{}
		}
	}
	assert.Empty(t, tb.spans)
}
