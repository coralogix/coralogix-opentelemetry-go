package transaction

import (
	"container/heap"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"
)

func spanDurationNanos(span sdktrace.ReadOnlySpan) int64 {
	d := span.EndTime().Sub(span.StartTime()).Nanoseconds()
	if d < 0 {
		return 0
	}
	return d
}

type spanHeapItem struct {
	duration int64
	index    int // insertion order tie-break for stable trims
	span     sdktrace.ReadOnlySpan
}

// slowestSpanMinHeap is a min-heap by span duration. Head is the shortest kept
// span — displace it when a slower candidate appears. container/heap provides
// sift-up / sift-down (same pattern as slowestSpanMinHeap).
type slowestSpanMinHeap []spanHeapItem

func (h slowestSpanMinHeap) Len() int { return len(h) }
func (h slowestSpanMinHeap) Less(i, j int) bool {
	if h[i].duration != h[j].duration {
		return h[i].duration < h[j].duration
	}
	return h[i].index < h[j].index
}
func (h slowestSpanMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *slowestSpanMinHeap) Push(x any)   { *h = append(*h, x.(spanHeapItem)) }
func (h *slowestSpanMinHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// selectSlowestSpans keeps at most maxNodes spans (slowest first). protectedRoots
// are always retained. Dropped spans are reparented to the nearest kept ancestor.
func selectSlowestSpans(spans []sdktrace.ReadOnlySpan, maxNodes int, protectedRoots []tracecore.SpanID) []sdktrace.ReadOnlySpan {
	if maxNodes <= 0 || len(spans) <= maxNodes {
		return spans
	}

	protect := make(map[tracecore.SpanID]struct{}, len(protectedRoots))
	for _, id := range protectedRoots {
		if id.IsValid() {
			protect[id] = struct{}{}
		}
	}

	roots := make([]sdktrace.ReadOnlySpan, 0, len(protect))
	others := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, s := range spans {
		if _, ok := protect[s.SpanContext().SpanID()]; ok {
			roots = append(roots, s)
			continue
		}
		others = append(others, s)
	}

	slots := maxNodes - len(roots)
	if slots < 0 {
		slots = 0
	}
	if slots == 0 {
		return reparentToKeptAncestors(orderKept(spans, roots), spans)
	}

	h := make(slowestSpanMinHeap, 0, slots)
	for i, s := range others {
		item := spanHeapItem{duration: spanDurationNanos(s), index: i, span: s}
		if len(h) < slots {
			heap.Push(&h, item)
			continue
		}
		if item.duration > h[0].duration {
			heap.Pop(&h)
			heap.Push(&h, item)
		}
	}

	kept := make([]sdktrace.ReadOnlySpan, 0, len(h)+len(roots))
	for _, item := range h {
		kept = append(kept, item.span)
	}
	kept = append(kept, roots...)

	return reparentToKeptAncestors(orderKept(spans, kept), spans)
}

func orderKept(original, kept []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	keptIDs := make(map[tracecore.SpanID]struct{}, len(kept))
	for _, s := range kept {
		keptIDs[s.SpanContext().SpanID()] = struct{}{}
	}
	ordered := make([]sdktrace.ReadOnlySpan, 0, len(kept))
	for _, s := range original {
		if _, ok := keptIDs[s.SpanContext().SpanID()]; ok {
			ordered = append(ordered, s)
		}
	}
	return ordered
}

func reparentToKeptAncestors(kept, allSpans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	byID := make(map[tracecore.SpanID]sdktrace.ReadOnlySpan, len(allSpans))
	for _, s := range allSpans {
		byID[s.SpanContext().SpanID()] = s
	}
	keptIDs := make(map[tracecore.SpanID]struct{}, len(kept))
	for _, s := range kept {
		keptIDs[s.SpanContext().SpanID()] = struct{}{}
	}

	result := make([]sdktrace.ReadOnlySpan, 0, len(kept))
	for _, s := range kept {
		newParent := nearestKeptParent(s, byID, keptIDs)
		if newParent.SpanID() == s.Parent().SpanID() {
			result = append(result, s)
			continue
		}
		result = append(result, withParent(s, newParent))
	}
	return result
}

func nearestKeptParent(span sdktrace.ReadOnlySpan, byID map[tracecore.SpanID]sdktrace.ReadOnlySpan, keptIDs map[tracecore.SpanID]struct{}) tracecore.SpanContext {
	parent := span.Parent()
	for parent.IsValid() {
		if _, ok := keptIDs[parent.SpanID()]; ok {
			if keptParent, exists := byID[parent.SpanID()]; exists {
				return keptParent.SpanContext()
			}
			return parent
		}
		ancestor, exists := byID[parent.SpanID()]
		if !exists {
			// Parent is outside this local batch (e.g. remote). Preserve it.
			return parent
		}
		parent = ancestor.Parent()
	}
	return tracecore.SpanContext{}
}
