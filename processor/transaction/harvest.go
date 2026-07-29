package transaction

import (
	"container/heap"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

// DefaultMaxRegularTraces is the default harvest capacity.
const DefaultMaxRegularTraces = 1

// DefaultHarvestPeriod is the default harvest flush interval.
const DefaultHarvestPeriod = 60 * time.Second

type harvestTrace struct {
	durationNs int64
	spans      []sdktrace.ReadOnlySpan
}

type harvestMinHeap []harvestTrace

func (h harvestMinHeap) Len() int           { return len(h) }
func (h harvestMinHeap) Less(i, j int) bool { return h[i].durationNs < h[j].durationNs }
func (h harvestMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *harvestMinHeap) Push(x any)        { *h = append(*h, x.(harvestTrace)) }
func (h *harvestMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// regularTraceHeap is not safe for concurrent use; TransactionSpanProcessor guards with harvestMu.
type regularTraceHeap struct {
	maxTraces int
	heap      harvestMinHeap
}

func newRegularTraceHeap(maxTraces int) *regularTraceHeap {
	return &regularTraceHeap{maxTraces: maxTraces}
}

func (r *regularTraceHeap) witness(trace harvestTrace) bool {
	if r.maxTraces <= 0 {
		return false
	}
	if len(r.heap) < r.maxTraces {
		heap.Push(&r.heap, trace)
		return true
	}
	if trace.durationNs <= r.heap[0].durationNs {
		return false
	}
	heap.Pop(&r.heap)
	heap.Push(&r.heap, trace)
	return true
}

func (r *regularTraceHeap) drain() []harvestTrace {
	traces := make([]harvestTrace, len(r.heap))
	copy(traces, r.heap)
	r.heap = r.heap[:0]
	return traces
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
