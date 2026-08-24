package transaction

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

// acceptCompleted finalizes one completed local-transaction batch:
// stamp cgx.transaction from the root's final name (or override), stamp
// exclusive self-duration + metrics, trim, then harvest or export.
func (p *TransactionSpanProcessor) acceptCompleted(spans []sdktrace.ReadOnlySpan) {
	if len(spans) == 0 {
		return
	}
	defer p.popChildIntervals(spans)

	tracked := p.snapshotMembership(spans)
	named := stampTransactionAttributes(spans, tracked)
	stamped := p.stampSelfDurationAndMetrics(named)

	trimmed := selectSlowestSpans(stamped, p.maxNodes, findTransactionRootSpanIDs(stamped))
	if len(trimmed) == 0 {
		return
	}

	if p.maxRegularTraces <= 0 || p.harvestPeriod <= 0 {
		p.exportSpans(trimmed)
		return
	}

	p.mu.Lock()
	stopped := p.stopped || p.exporterShutdown.Load()
	p.mu.Unlock()
	if stopped {
		p.exportSpans(trimmed)
		return
	}

	p.harvestMu.Lock()
	stubs := p.harvest.witness(harvestTrace{
		durationNs: rootDurationNanos(trimmed),
		spans:      trimmed,
	})
	p.harvestMu.Unlock()
	if len(stubs) > 0 {
		p.exportSpans(stubs)
	}
}

func (p *TransactionSpanProcessor) stampSelfDurationAndMetrics(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	byParent := childrenByParentSpanID(spans)

	p.mu.Lock()
	prior := make(map[tracecore.SpanID][]interval, len(spans))
	for _, s := range spans {
		sid := s.SpanContext().SpanID()
		if ivs := p.childIntervals[sid]; len(ivs) > 0 {
			prior[sid] = append([]interval(nil), ivs...)
		}
	}
	p.mu.Unlock()

	out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, s := range spans {
		sid := s.SpanContext().SpanID()
		children := byParent[sid]
		extra := filterPriorIntervals(prior[sid], children)
		selfDurationNs := selfDurationNanosWithExtraIntervals(s, children, extra)
		stamped := withSelfDuration(s, selfDurationNs)
		out = append(out, stamped)
		p.recordSelfDurationMetric(stamped, selfDurationNs)
	}
	return out
}

func filterPriorIntervals(prior []interval, children []sdktrace.ReadOnlySpan) []interval {
	if len(prior) == 0 {
		return nil
	}
	childSet := make(map[interval]struct{}, len(children))
	for _, c := range children {
		childSet[interval{
			start: c.StartTime().UnixNano(),
			end:   c.EndTime().UnixNano(),
		}] = struct{}{}
	}
	out := make([]interval, 0, len(prior))
	for _, iv := range prior {
		if _, dup := childSet[iv]; !dup {
			out = append(out, iv)
		}
	}
	return out
}

func (p *TransactionSpanProcessor) snapshotMembership(spans []sdktrace.ReadOnlySpan) map[tracecore.SpanID]spanMembership {
	if len(spans) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[tracecore.SpanID]spanMembership, len(spans))
	for _, s := range spans {
		sid := s.SpanContext().SpanID()
		if m, ok := p.membership[sid]; ok {
			out[sid] = m
		}
	}
	return out
}

func (p *TransactionSpanProcessor) popChildIntervals(spans []sdktrace.ReadOnlySpan) {
	p.mu.Lock()
	for _, s := range spans {
		sid := s.SpanContext().SpanID()
		delete(p.childIntervals, sid)
		delete(p.membership, sid)
	}
	p.mu.Unlock()
}

func findTransactionRootSpanIDs(spans []sdktrace.ReadOnlySpan) []tracecore.SpanID {
	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	var roots []tracecore.SpanID
	for _, s := range spans {
		for _, a := range s.Attributes() {
			if a.Key == rootKey && a.Value.AsBool() {
				roots = append(roots, s.SpanContext().SpanID())
				break
			}
		}
	}
	return roots
}

func (p *TransactionSpanProcessor) stopCompleteTimerLocked(tb *traceBuffer) {
	if tb.completeTimer != nil {
		tb.completeTimer.Stop()
		tb.completeTimer = nil
	}
}

func (p *TransactionSpanProcessor) stopNestedCompleteTimerLocked(tb *traceBuffer) {
	if tb.nestedCompleteTimer != nil {
		tb.nestedCompleteTimer.Stop()
		tb.nestedCompleteTimer = nil
	}
}

func (p *TransactionSpanProcessor) scheduleCompletionLocked(traceID tracecore.TraceID, tb *traceBuffer) [][]sdktrace.ReadOnlySpan {
	p.stopCompleteTimerLocked(tb)
	if p.completionHoldback <= 0 {
		batches := p.extractCompletedLocalTransactionsLocked(tb, true)
		if len(tb.spans) == 0 {
			delete(p.traces, traceID)
		}
		return batches
	}
	var timer *time.Timer
	timer = time.AfterFunc(p.completionHoldback, func() {
		p.mu.Lock()
		if p.exporterShutdown.Load() {
			p.mu.Unlock()
			return
		}
		cur, ok := p.traces[traceID]
		// Ignore stale firings: only the currently armed timer may finalize.
		if !ok || cur.liveCount() > 0 || cur.completeTimer != timer {
			p.mu.Unlock()
			return
		}
		cur.completeTimer = nil
		batches := p.extractCompletedLocalTransactionsLocked(cur, true)
		if len(cur.spans) == 0 {
			delete(p.traces, traceID)
		}
		p.pendingFinalize += len(batches)
		p.mu.Unlock()

		for _, batch := range batches {
			p.acceptCompleted(batch)
			p.mu.Lock()
			p.pendingFinalize--
			p.idle.Broadcast()
			p.mu.Unlock()
		}
	})
	tb.completeTimer = timer
	return nil
}

// scheduleNestedCompletionLocked delays nested extract while an outer ancestor
// is still live so late fire-and-forget children under the nested root can join.
func (p *TransactionSpanProcessor) scheduleNestedCompletionLocked(traceID tracecore.TraceID, tb *traceBuffer) [][]sdktrace.ReadOnlySpan {
	if !hasExtractableNestedTransaction(tb) {
		p.stopNestedCompleteTimerLocked(tb)
		return nil
	}
	if tb.nestedCompleteTimer != nil {
		// Already armed; do not reset on unrelated outer activity.
		return nil
	}
	if p.completionHoldback <= 0 {
		return p.extractCompletedLocalTransactionsLocked(tb, false)
	}
	var timer *time.Timer
	timer = time.AfterFunc(p.completionHoldback, func() {
		p.mu.Lock()
		if p.exporterShutdown.Load() {
			p.mu.Unlock()
			return
		}
		cur, ok := p.traces[traceID]
		if !ok || cur.nestedCompleteTimer != timer {
			p.mu.Unlock()
			return
		}
		cur.nestedCompleteTimer = nil
		var batches [][]sdktrace.ReadOnlySpan
		if cur.liveCount() > 0 {
			batches = p.extractCompletedLocalTransactionsLocked(cur, false)
		}
		p.pendingFinalize += len(batches)
		p.mu.Unlock()

		for _, batch := range batches {
			p.acceptCompleted(batch)
			p.mu.Lock()
			p.pendingFinalize--
			p.idle.Broadcast()
			p.mu.Unlock()
		}

		// Re-arm if another nested root became extractable after the fire.
		p.mu.Lock()
		if cur, ok := p.traces[traceID]; ok && cur.liveCount() > 0 {
			_ = p.scheduleNestedCompletionLocked(traceID, cur)
		}
		p.mu.Unlock()
	})
	tb.nestedCompleteTimer = timer
	return nil
}

func (p *TransactionSpanProcessor) flushPendingCompletionsLocked() {
	for {
		var batches [][]sdktrace.ReadOnlySpan
		for id, tb := range p.traces {
			p.stopCompleteTimerLocked(tb)
			p.stopNestedCompleteTimerLocked(tb)
			if tb.liveCount() > 0 {
				batches = append(batches, p.extractCompletedLocalTransactionsLocked(tb, false)...)
				continue
			}
			batches = append(batches, p.extractCompletedLocalTransactionsLocked(tb, true)...)
			if len(tb.spans) == 0 {
				delete(p.traces, id)
			}
		}
		if len(batches) == 0 {
			p.idle.Broadcast()
			return
		}
		p.pendingFinalize += len(batches)
		for _, spans := range batches {
			p.mu.Unlock()
			p.acceptCompleted(spans)
			p.mu.Lock()
			p.pendingFinalize--
		}
		p.idle.Broadcast()
	}
}

func (p *TransactionSpanProcessor) flushHarvest() {
	// Hold exportMu across drain+export so Shutdown cannot shut down between them.
	p.exportMu.Lock()
	defer p.exportMu.Unlock()
	if p.exporterShutdown.Load() {
		return
	}
	p.harvestMu.Lock()
	winners := p.harvest.drain()
	p.harvestMu.Unlock()

	ctx := p.exportCtx
	if ctx == nil {
		ctx = context.Background()
	}
	for _, w := range winners {
		if p.exporter == nil || len(w.spans) == 0 {
			continue
		}
		if err := p.exporter.ExportSpans(ctx, w.spans); err != nil {
			otel.Handle(err)
		}
	}
}

func (p *TransactionSpanProcessor) exportSpans(spans []sdktrace.ReadOnlySpan) {
	if p.exporter == nil || len(spans) == 0 {
		return
	}
	// Only exportMu — never p.mu (Shutdown may hold p.mu waiting pendingFinalize).
	p.exportMu.Lock()
	defer p.exportMu.Unlock()
	if p.exporterShutdown.Load() {
		return
	}
	ctx := p.exportCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.exporter.ExportSpans(ctx, spans); err != nil {
		otel.Handle(err)
	}
}

func (p *TransactionSpanProcessor) recordSelfDurationMetric(span sdktrace.ReadOnlySpan, selfDurationNs int64) {
	if p.selfDurationHistogram == nil {
		return
	}

	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs, attribute.String(SpanNameMetricAttribute, span.Name()))
	for _, a := range span.Attributes() {
		if a.Key == attribute.Key(sampler.TransactionIdentifier) || a.Key == attribute.Key(sampler.TransactionIdentifierRoot) {
			attrs = append(attrs, a)
		}
	}

	p.selfDurationHistogram.Record(context.Background(), float64(selfDurationNs)/float64(1e9), attrs...)
}
