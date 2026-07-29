package transaction

import (
	"sort"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"
)

func selfTimeNanos(span sdktrace.ReadOnlySpan, children []sdktrace.ReadOnlySpan) int64 {
	return selfTimeNanosWithExtraIntervals(span, children, nil)
}

func selfTimeNanosWithExtraIntervals(span sdktrace.ReadOnlySpan, children []sdktrace.ReadOnlySpan, extra []interval) int64 {
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

	covered := unionLengthNanosFromIntervals(parentStart, parentEnd, raw)

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

func unionLengthNanos(parentStart, parentEnd int64, children []sdktrace.ReadOnlySpan) int64 {
	raw := make([]interval, 0, len(children))
	for _, c := range children {
		raw = append(raw, interval{start: c.StartTime().UnixNano(), end: c.EndTime().UnixNano()})
	}
	return unionLengthNanosFromIntervals(parentStart, parentEnd, raw)
}

func unionLengthNanosFromIntervals(parentStart, parentEnd int64, raw []interval) int64 {
	intervals := make([]interval, 0, len(raw))
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
		intervals = append(intervals, interval{start: start, end: end})
	}
	if len(intervals) == 0 {
		return 0
	}

	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].start < intervals[j].start
	})

	var total int64
	curStart, curEnd := intervals[0].start, intervals[0].end
	for _, iv := range intervals[1:] {
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
