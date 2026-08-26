package transaction

import (
	"sort"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"
)

// Exclusive self-duration is a span's own wall time minus time covered by its
// direct children. Child intervals are clamped to the parent window, then
// sorted and merged so overlapping/concurrent children are not double-counted.

func selfDurationNanos(span sdktrace.ReadOnlySpan, children []sdktrace.ReadOnlySpan) int64 {
	return selfDurationNanosWithExtraIntervals(span, children, nil)
}

func selfDurationNanosWithExtraIntervals(span sdktrace.ReadOnlySpan, children []sdktrace.ReadOnlySpan, extra []interval) int64 {
	duration := span.EndTime().Sub(span.StartTime()).Nanoseconds()
	if duration < 0 {
		duration = 0
	}
	if len(children) == 0 && len(extra) == 0 {
		return duration
	}

	parentStart := span.StartTime().UnixNano()
	parentEnd := span.EndTime().UnixNano()

	raw := make([]interval, 0, len(children)+len(extra))
	for _, c := range children {
		raw = append(raw, interval{start: c.StartTime().UnixNano(), end: c.EndTime().UnixNano()})
	}
	raw = append(raw, extra...)

	covered := coveredDurationNanos(parentStart, parentEnd, raw)

	self := duration - covered
	if self < 0 {
		self = 0
	}
	return self
}

type interval struct {
	start int64
	end   int64
}

// coveredDurationNanos clamps child intervals to [parentStart, parentEnd],
// sorts them, and merges overlaps. Returns the union length in nanoseconds.
func coveredDurationNanos(parentStart, parentEnd int64, raw []interval) int64 {
	sortedClamped := clampIntervalsToParent(parentStart, parentEnd, raw)
	return mergeSortedIntervalLength(sortedClamped)
}

// clampIntervalsToParent clips each interval to the parent window and drops
// empty/inverted results. Order is not sorted.
func clampIntervalsToParent(parentStart, parentEnd int64, raw []interval) []interval {
	out := make([]interval, 0, len(raw))
	for _, r := range raw {
		start, end := r.start, r.end
		if end < start {
			end = start
		}
		if start < parentStart {
			start = parentStart
		}
		if end > parentEnd {
			end = parentEnd
		}
		if end <= start {
			continue
		}
		out = append(out, interval{start: start, end: end})
	}
	return out
}

// mergeSortedIntervalLength sorts intervals by start, merges overlaps, and
// returns the covered length. Callers typically pass clampIntervalsToParent output.
func mergeSortedIntervalLength(intervals []interval) int64 {
	if len(intervals) == 0 {
		return 0
	}

	sortedClamped := append([]interval(nil), intervals...)
	sort.Slice(sortedClamped, func(i, j int) bool {
		return sortedClamped[i].start < sortedClamped[j].start
	})

	var total int64
	curStart, curEnd := sortedClamped[0].start, sortedClamped[0].end
	for _, iv := range sortedClamped[1:] {
		if iv.start > curEnd {
			total += curEnd - curStart
			curStart, curEnd = iv.start, iv.end
			continue
		}
		if iv.end > curEnd {
			curEnd = iv.end
		}
	}
	total += curEnd - curStart
	return total
}

func childrenByParentSpanID(spans []sdktrace.ReadOnlySpan) map[tracecore.SpanID][]sdktrace.ReadOnlySpan {
	byParent := make(map[tracecore.SpanID][]sdktrace.ReadOnlySpan)
	for _, s := range spans {
		parent := s.Parent()
		if !parent.IsValid() {
			continue
		}
		byParent[parent.SpanID()] = append(byParent[parent.SpanID()], s)
	}
	return byParent
}
