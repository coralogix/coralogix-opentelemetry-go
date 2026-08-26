package transaction

import (
	"container/heap"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"go.opentelemetry.io/otel/attribute"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

// regularTraceMinHeap is a min-heap by root duration (nanoseconds).
// Head is the shortest / easiest to displace when the harvest capacity is full.
// container/heap provides sift-up / sift-down; see also slowestSpanMinHeap.
type regularTraceMinHeap []harvestTrace

func (h regularTraceMinHeap) Len() int           { return len(h) }
func (h regularTraceMinHeap) Less(i, j int) bool { return h[i].durationNs < h[j].durationNs }
func (h regularTraceMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *regularTraceMinHeap) Push(x any)        { *h = append(*h, x.(harvestTrace)) }
func (h *regularTraceMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type harvestTrace struct {
	durationNs int64
	spans      []sdktrace.ReadOnlySpan
}

// regularTraceHeap is not safe for concurrent use; TransactionSpanProcessor guards with harvestMu.
type regularTraceHeap struct {
	maxTraces int
	heap      regularTraceMinHeap
}

func newRegularTraceHeap(maxTraces int) *regularTraceHeap {
	return &regularTraceHeap{maxTraces: maxTraces}
}

func (r *regularTraceHeap) witness(trace harvestTrace) []sdktrace.ReadOnlySpan {
	if r.maxTraces <= 0 {
		return nil
	}
	if len(r.heap) < r.maxTraces {
		heap.Push(&r.heap, trace)
		return nil
	}
	if trace.durationNs <= r.heap[0].durationNs {
		return harvestStubSpans(trace.spans)
	}
	displaced := heap.Pop(&r.heap).(harvestTrace)
	heap.Push(&r.heap, trace)
	return harvestStubSpans(displaced.spans)
}

func (r *regularTraceHeap) drain() []harvestTrace {
	traces := make([]harvestTrace, len(r.heap))
	copy(traces, r.heap)
	r.heap = r.heap[:0]
	return traces
}

// restore puts drained traces back via witness so maxTraces capacity is
// re-applied (concurrent refill during a failed flush can leave traces in the
// heap). Returns stub spans for any capacity evictions.
func (r *regularTraceHeap) restore(traces []harvestTrace) []sdktrace.ReadOnlySpan {
	var stubs []sdktrace.ReadOnlySpan
	for _, t := range traces {
		stubs = append(stubs, r.witness(t)...)
	}
	return stubs
}

// Len reports how many traces are currently held. Used by tests.
func (r *regularTraceHeap) Len() int {
	return len(r.heap)
}

// harvestStubSpans returns the local-transaction root span(s) for APM presence when
// a completed tree loses harvest (full waterfall is dropped).
func harvestStubSpans(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	if len(spans) == 0 {
		return nil
	}
	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	var stubs []sdktrace.ReadOnlySpan
	for _, s := range spans {
		for _, a := range s.Attributes() {
			if a.Key == rootKey && a.Value.AsBool() {
				stubs = append(stubs, s)
				break
			}
		}
	}
	if len(stubs) > 0 {
		return stubs
	}
	// Fallback: longest span in the trimmed batch (should be rare).
	best := spans[0]
	bestDur := spanDurationNanos(best)
	for _, s := range spans[1:] {
		if d := spanDurationNanos(s); d > bestDur {
			best = s
			bestDur = d
		}
	}
	return []sdktrace.ReadOnlySpan{best}
}

func rootDurationNanos(spans []sdktrace.ReadOnlySpan) int64 {
	var maxRootDuration int64
	foundRoot := false
	var maxDuration int64
	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	for _, s := range spans {
		d := spanDurationNanos(s)
		if d > maxDuration {
			maxDuration = d
		}
		for _, a := range s.Attributes() {
			if a.Key == rootKey && a.Value.AsBool() {
				foundRoot = true
				if d > maxRootDuration {
					maxRootDuration = d
				}
				break
			}
		}
	}
	if foundRoot {
		return maxRootDuration
	}
	return maxDuration
}
