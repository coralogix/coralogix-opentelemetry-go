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

// acceptCompleted finalizes one completed local-transaction batch. Batches at
// within maxTransactionSpans receive transaction tagging, self-duration, and
// metrics. Larger batches normally switch to raw passthrough in OnEnd.
func (p *TransactionSpanProcessor) acceptCompleted(spans []sdktrace.ReadOnlySpan) {
	_ = p.acceptCompletedCtx(context.Background(), spans)
}

// acceptCompletedCtx publishes transaction identity under p.mu, then finishes
// metrics/export with ctx (ForceFlush / Shutdown deadlines).
func (p *TransactionSpanProcessor) acceptCompletedCtx(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if len(spans) == 0 {
		return nil
	}
	p.mu.Lock()
	named := p.publishCompletedIdentityLocked(spans)
	p.mu.Unlock()
	return p.finishCompletedCtx(ctx, named)
}

// publishCompletedIdentityLocked enriches an eligible batch and publishes its
// identity while clearing active membership. Larger batches are exported
// without processor attributes. Caller must hold p.mu.
func (p *TransactionSpanProcessor) publishCompletedIdentityLocked(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	if len(spans) == 0 {
		return nil
	}
	if len(spans) > p.maxTransactionSpans {
		for _, s := range spans {
			delete(p.membership, spanRefFromContext(s.SpanContext()))
		}
		return spans
	}
	tracked := make(map[spanRef]spanMembership, len(spans))
	for _, s := range spans {
		ref := spanRefFromContext(s.SpanContext())
		if m, ok := p.membership[ref]; ok {
			tracked[ref] = m
		}
	}
	named := stampTransactionAttributes(spans, tracked)
	p.retainFinalizedNamesAndClearLocked(named)
	return named
}

// publishCompletedBatchesLocked publishes identity for each extracted batch.
// Caller must hold p.mu.
func (p *TransactionSpanProcessor) publishCompletedBatchesLocked(batches [][]sdktrace.ReadOnlySpan) [][]sdktrace.ReadOnlySpan {
	out := make([][]sdktrace.ReadOnlySpan, 0, len(batches))
	for _, batch := range batches {
		out = append(out, p.publishCompletedIdentityLocked(batch))
	}
	return out
}

// finishCompletedCtx stamps self-duration and records metrics for an eligible
// batch, then exports every span whose transaction identity was published.
func (p *TransactionSpanProcessor) finishCompletedCtx(ctx context.Context, named []sdktrace.ReadOnlySpan) error {
	if len(named) == 0 {
		return nil
	}
	groups := [][]sdktrace.ReadOnlySpan{named}
	if len(named) <= p.maxTransactionSpans {
		groups = groupByTransactionName(named)
		for i, group := range groups {
			groups[i] = p.stampSelfDurationAndMetrics(group)
		}
	}

	// Drop retained child intervals after the stamped prefix has used them.
	p.mu.Lock()
	for _, s := range named {
		delete(p.childIntervals, spanRefFromContext(s.SpanContext()))
	}
	p.mu.Unlock()

	for _, group := range groups {
		if err := p.exportSpansCtx(ctx, group); err != nil {
			return err
		}
	}
	return nil
}

func groupByTransactionName(spans []sdktrace.ReadOnlySpan) [][]sdktrace.ReadOnlySpan {
	if len(spans) == 0 {
		return nil
	}
	groups := make(map[string][]sdktrace.ReadOnlySpan)
	var order []string
	for _, s := range spans {
		name := transactionAttr(s)
		if _, seen := groups[name]; !seen {
			order = append(order, name)
		}
		groups[name] = append(groups[name], s)
	}
	out := make([][]sdktrace.ReadOnlySpan, 0, len(order))
	for _, key := range order {
		out = append(out, groups[key])
	}
	return out
}

func (p *TransactionSpanProcessor) stampSelfDurationAndMetrics(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	byParent := childrenByParentSpanID(spans)

	p.mu.Lock()
	prior := make(map[spanRef][]interval, len(spans))
	for _, s := range spans {
		ref := spanRefFromContext(s.SpanContext())
		if ivs := p.childIntervals[ref]; len(ivs) > 0 {
			prior[ref] = append([]interval(nil), ivs...)
		}
	}
	p.mu.Unlock()

	out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, s := range spans {
		sid := s.SpanContext().SpanID()
		ref := spanRefFromContext(s.SpanContext())
		children := byParent[sid]
		extra := filterPriorIntervals(prior[ref], children)
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

func (p *TransactionSpanProcessor) snapshotMembership(spans []sdktrace.ReadOnlySpan) map[spanRef]spanMembership {
	if len(spans) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[spanRef]spanMembership, len(spans))
	for _, s := range spans {
		ref := spanRefFromContext(s.SpanContext())
		if m, ok := p.membership[ref]; ok {
			out[ref] = m
		}
	}
	return out
}

func (p *TransactionSpanProcessor) retainFinalizedNamesAndClear(spans []sdktrace.ReadOnlySpan) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retainFinalizedNamesAndClearLocked(spans)
}

// retainFinalizedNamesAndClearLocked publishes finalized txn identity and clears
// active membership. Caller must hold p.mu.
func (p *TransactionSpanProcessor) retainFinalizedNamesAndClearLocked(spans []sdktrace.ReadOnlySpan) {
	// Resolve the local transaction root identity before clearing membership.
	txnRoot := tracecore.SpanID{}
	for _, s := range spans {
		if isTransactionRoot(s) {
			txnRoot = s.SpanContext().SpanID()
			break
		}
	}
	if !txnRoot.IsValid() {
		for _, s := range spans {
			if m, ok := p.membership[spanRefFromContext(s.SpanContext())]; ok && m.inheritedFrom.IsValid() {
				txnRoot = m.inheritedFrom
				break
			}
		}
	}

	for _, s := range spans {
		ref := spanRefFromContext(s.SpanContext())
		delete(p.membership, ref)
		if name := transactionAttr(s); name != "" {
			rootID := txnRoot
			if !rootID.IsValid() {
				rootID = ref.span
			}
			p.putFinalizedNameLocked(ref, name, rootID)
		}
	}
}

// putFinalizedNameLocked inserts or updates a finalized spanRef→txn entry and
// evicts the oldest entries when over maxFinalizedNames. Caller must hold p.mu.
func (p *TransactionSpanProcessor) putFinalizedNameLocked(ref spanRef, name string, rootID tracecore.SpanID) {
	if name == "" {
		return
	}
	entry := finalizedTxn{name: name, rootID: rootID}
	if _, exists := p.finalizedNames[ref]; exists {
		p.finalizedNames[ref] = entry
		return
	}
	p.finalizedNames[ref] = entry
	p.finalizedOrder = append(p.finalizedOrder, ref)
	for len(p.finalizedNames) > p.maxFinalizedNames {
		oldest := p.finalizedOrder[0]
		p.finalizedOrder = p.finalizedOrder[1:]
		delete(p.finalizedNames, oldest)
	}
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

// schedulePassthroughCleanupLocked retains the trace marker so late spans
// remain raw passthrough spans.
// Caller must hold p.mu.
func (p *TransactionSpanProcessor) schedulePassthroughCleanupLocked(traceID tracecore.TraceID, tb *traceBuffer) {
	p.stopCompleteTimerLocked(tb)
	if tb.liveCount() > 0 || tb.passthroughTombstone {
		return
	}
	tb.passthroughTombstone = true
	p.passthroughOrder = append(p.passthroughOrder, traceID)
	for len(p.passthroughOrder) > p.maxFinalizedNames {
		oldest := p.passthroughOrder[0]
		p.passthroughOrder = p.passthroughOrder[1:]
		if old, ok := p.traces[oldest]; ok && old.passthrough && old.passthroughTombstone && old.liveCount() == 0 {
			delete(p.traces, oldest)
		} else if old, ok := p.traces[oldest]; ok && old.passthrough {
			// A live trace consumed its stale queue entry; requeue it when the
			// last late span ends instead of evicting its current tombstone.
			old.passthroughTombstone = false
		}
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
		batches = p.publishCompletedBatchesLocked(batches)
		p.pendingFinalize += len(batches)
		p.mu.Unlock()

		for _, batch := range batches {
			_ = p.finishCompletedCtx(context.Background(), batch)
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
	if !p.hasExtractableWhileOuterLive(tb) {
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
		batches = p.publishCompletedBatchesLocked(batches)
		p.pendingFinalize += len(batches)
		p.mu.Unlock()

		for _, batch := range batches {
			_ = p.finishCompletedCtx(context.Background(), batch)
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

func (p *TransactionSpanProcessor) flushPendingCompletionsLocked(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		var batches [][]sdktrace.ReadOnlySpan
		for id, tb := range p.traces {
			p.stopCompleteTimerLocked(tb)
			p.stopNestedCompleteTimerLocked(tb)
			if tb.passthrough {
				continue
			}
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
			return nil
		}
		p.pendingFinalize += len(batches)
		var firstErr error
		batches = p.publishCompletedBatchesLocked(batches)
		for _, spans := range batches {
			p.mu.Unlock()
			// Keep the flush context for every batch so ForceFlush(ctx) can still
			// cancel later exports; always decrement pendingFinalize.
			err := p.finishCompletedCtx(ctx, spans)
			p.mu.Lock()
			p.pendingFinalize--
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		p.idle.Broadcast()
		if firstErr != nil {
			return firstErr
		}
	}
}

// lockExportMu acquires p.exportMu, or returns ctx.Err() if ctx expires first.
func (p *TransactionSpanProcessor) lockExportMu(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if p.exportMu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func (p *TransactionSpanProcessor) exportSpans(spans []sdktrace.ReadOnlySpan) {
	_ = p.exportSpansCtx(context.Background(), spans)
}

// exportSpansCtx exports with ctx for lock wait and ExportSpans. When ctx is
// Background, falls back to p.exportCtx if Shutdown published one.
func (p *TransactionSpanProcessor) exportSpansCtx(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if p.exporter == nil || len(spans) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Only exportMu — never p.mu (Shutdown may hold p.mu waiting pendingFinalize).
	if err := p.lockExportMu(ctx); err != nil {
		return err
	}
	defer p.exportMu.Unlock()
	if p.exporterShutdown.Load() {
		return nil
	}
	exportCtx := ctx
	if p.exportCtx != nil && (ctx == nil || ctx == context.Background()) {
		exportCtx = p.exportCtx
	}
	if err := p.exporter.ExportSpans(exportCtx, spans); err != nil {
		otel.Handle(err)
		return err
	}
	return nil
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
