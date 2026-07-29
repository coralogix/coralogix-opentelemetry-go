package transaction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracecore "go.opentelemetry.io/otel/trace"
)

// newFakeSpan builds a minimal sdktrace.ReadOnlySpan without a TracerProvider.
func newFakeSpan(spanID, parentID byte, start, end time.Time) sdktrace.ReadOnlySpan {
	sc := tracecore.NewSpanContext(tracecore.SpanContextConfig{
		TraceID: tracecore.TraceID{0x01},
		SpanID:  tracecore.SpanID{spanID},
	})

	stub := sdktracetest.SpanStub{
		SpanContext: sc,
		StartTime:   start,
		EndTime:     end,
	}
	if parentID != 0 {
		stub.Parent = tracecore.NewSpanContext(tracecore.SpanContextConfig{
			TraceID: tracecore.TraceID{0x01},
			SpanID:  tracecore.SpanID{parentID},
		})
	}
	return stub.Snapshot()
}

func TestSelfTimeNanos(t *testing.T) {
	base := time.Unix(0, 0)

	t.Run("leaf span with no children has self time equal to its duration", func(t *testing.T) {
		leaf := newFakeSpan(2, 1, base.Add(20*time.Millisecond), base.Add(80*time.Millisecond))

		self := selfTimeNanos(leaf, nil)

		assert.Equal(t, int64(60*time.Millisecond), self)
	})

	t.Run("parent 0-100 with single child 20-80 has self time 40ms and child 60ms", func(t *testing.T) {
		parent := newFakeSpan(1, 0, base, base.Add(100*time.Millisecond))
		child := newFakeSpan(2, 1, base.Add(20*time.Millisecond), base.Add(80*time.Millisecond))

		parentSelf := selfTimeNanos(parent, []sdktrace.ReadOnlySpan{child})
		childSelf := selfTimeNanos(child, nil)

		assert.Equal(t, int64(40*time.Millisecond), parentSelf)
		assert.Equal(t, int64(60*time.Millisecond), childSelf)
	})

	t.Run("overlapping concurrent children are only counted once", func(t *testing.T) {
		parent := newFakeSpan(1, 0, base, base.Add(100*time.Millisecond))
		childA := newFakeSpan(2, 1, base.Add(10*time.Millisecond), base.Add(50*time.Millisecond))
		childB := newFakeSpan(3, 1, base.Add(30*time.Millisecond), base.Add(70*time.Millisecond))

		self := selfTimeNanos(parent, []sdktrace.ReadOnlySpan{childA, childB})

		assert.Equal(t, int64(40*time.Millisecond), self)
	})

	t.Run("non overlapping children are summed", func(t *testing.T) {
		parent := newFakeSpan(1, 0, base, base.Add(100*time.Millisecond))
		childA := newFakeSpan(2, 1, base.Add(0*time.Millisecond), base.Add(20*time.Millisecond))
		childB := newFakeSpan(3, 1, base.Add(50*time.Millisecond), base.Add(70*time.Millisecond))

		self := selfTimeNanos(parent, []sdktrace.ReadOnlySpan{childA, childB})

		assert.Equal(t, int64(60*time.Millisecond), self)
	})

	t.Run("child extending beyond parent bounds is clamped", func(t *testing.T) {
		parent := newFakeSpan(1, 0, base.Add(10*time.Millisecond), base.Add(90*time.Millisecond))
		child := newFakeSpan(2, 1, base, base.Add(200*time.Millisecond))

		self := selfTimeNanos(parent, []sdktrace.ReadOnlySpan{child})

		assert.Equal(t, int64(0), self)
	})

	t.Run("self time never goes negative", func(t *testing.T) {
		parent := newFakeSpan(1, 0, base, base)
		child := newFakeSpan(2, 1, base, base.Add(10*time.Millisecond))

		self := selfTimeNanos(parent, []sdktrace.ReadOnlySpan{child})

		assert.Equal(t, int64(0), self)
	})
}

func TestChildrenByParentSpanID(t *testing.T) {
	parent := newFakeSpan(1, 0, time.Unix(0, 0), time.Unix(0, 100))
	child1 := newFakeSpan(2, 1, time.Unix(0, 0), time.Unix(0, 50))
	child2 := newFakeSpan(3, 1, time.Unix(0, 50), time.Unix(0, 100))
	grandchild := newFakeSpan(4, 2, time.Unix(0, 0), time.Unix(0, 10))

	byParent := childrenByParentSpanID([]sdktrace.ReadOnlySpan{parent, child1, child2, grandchild})

	parentSpanID := tracecore.SpanID{1}
	child1SpanID := tracecore.SpanID{2}

	assert.Len(t, byParent[parentSpanID], 2)
	assert.Len(t, byParent[child1SpanID], 1)
	assert.Len(t, byParent[tracecore.SpanID{3}], 0)
}
