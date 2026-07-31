// Package transaction provides TransactionSpanProcessor: tags Coralogix
// transactions, stamps self-time, and records cgx.transaction.self_time.
package transaction

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/unit"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

const (
	SelfTimeAttribute       = "cgx.transaction.self_time"
	SelfTimeMetricName      = "cgx.transaction.self_time"
	SelfTimeMetricUnit      = unit.Unit("s")
	SpanNameMetricAttribute = "span.name"

	instrumentationName = "github.com/coralogix/coralogix-opentelemetry-go/processor/transaction"
)

type traceBuffer struct {
	spans         []sdktrace.ReadOnlySpan
	liveParents   map[tracecore.SpanID]tracecore.SpanID
	completeTimer *time.Timer
}

func (tb *traceBuffer) liveCount() int {
	return len(tb.liveParents)
}

func (tb *traceBuffer) safeEndedSpans() []sdktrace.ReadOnlySpan {
	if tb.liveCount() == 0 {
		return tb.spans
	}

	parentOf := make(map[tracecore.SpanID]tracecore.SpanID, len(tb.spans)+len(tb.liveParents))
	for _, s := range tb.spans {
		if pid := s.Parent().SpanID(); pid.IsValid() {
			parentOf[s.SpanContext().SpanID()] = pid
		}
	}
	for id, parentID := range tb.liveParents {
		if parentID.IsValid() {
			parentOf[id] = parentID
		}
	}

	blocked := make(map[tracecore.SpanID]struct{})
	for liveID := range tb.liveParents {
		cur, ok := parentOf[liveID]
		for ok && cur.IsValid() {
			if _, seen := blocked[cur]; seen {
				break
			}
			blocked[cur] = struct{}{}
			cur, ok = parentOf[cur]
		}
	}

	out := make([]sdktrace.ReadOnlySpan, 0, len(tb.spans))
	for _, s := range tb.spans {
		if _, skip := blocked[s.SpanContext().SpanID()]; skip {
			continue
		}
		out = append(out, s)
	}
	return out
}

type TransactionSpanProcessor struct {
	exporter sdktrace.SpanExporter

	mu             sync.Mutex
	traces         map[tracecore.TraceID]*traceBuffer
	childIntervals map[tracecore.SpanID][]interval

	selfTimeHistogram syncfloat64.Histogram

	maxNodes           int
	maxRegularTraces   int
	harvestPeriod      time.Duration
	completionHoldback time.Duration

	harvestMu     sync.Mutex
	harvest       *regularTraceHeap
	harvestTicker *time.Ticker
	harvestStopCh chan struct{}
	harvestDone   chan struct{}

	shutdownOnce sync.Once
	stopped      bool
	// exporterShutdown is atomic so OnStart/OnEnd/timers (p.mu) and export paths (exportMu) share one flag.
	exporterShutdown atomic.Bool
	idle             *sync.Cond
	// pendingFinalize counts acceptCompleted still running outside p.mu; waitForIdle waits for it.
	pendingFinalize int

	// exportMu serializes ExportSpans and exporter.Shutdown.
	exportMu  sync.Mutex
	exportCtx context.Context
}

type Option func(*TransactionSpanProcessor)

// WithMeterProvider sets the MeterProvider for cgx.transaction.self_time.
func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return func(p *TransactionSpanProcessor) {
		if meterProvider == nil {
			return
		}
		if histogram := newSelfTimeHistogram(meterProvider); histogram != nil {
			p.selfTimeHistogram = histogram
		}
	}
}

// WithMaxNodes caps spans kept per completed local trace (slowest first; root always kept).
func WithMaxNodes(maxNodes int) Option {
	return func(p *TransactionSpanProcessor) {
		p.maxNodes = maxNodes
	}
}

// WithMaxRegularTraces caps completed traces held until harvest (0 exports immediately).
func WithMaxRegularTraces(maxRegularTraces int) Option {
	return func(p *TransactionSpanProcessor) {
		p.maxRegularTraces = maxRegularTraces
	}
}

// WithHarvestPeriod sets how often the harvest heap is flushed.
func WithHarvestPeriod(period time.Duration) Option {
	return func(p *TransactionSpanProcessor) {
		p.harvestPeriod = period
	}
}

// DefaultCompletionHoldback is the post-idle delay before finalizing a local trace.
const DefaultCompletionHoldback = 100 * time.Millisecond

// WithCompletionHoldback sets the post-idle delay before finalizing a local trace.
func WithCompletionHoldback(d time.Duration) Option {
	return func(p *TransactionSpanProcessor) {
		p.completionHoldback = d
	}
}

func newSelfTimeHistogram(meterProvider metric.MeterProvider) syncfloat64.Histogram {
	meter := meterProvider.Meter(instrumentationName)
	histogram, err := meter.SyncFloat64().Histogram(
		SelfTimeMetricName,
		instrument.WithUnit(SelfTimeMetricUnit),
		instrument.WithDescription("Exclusive self time per span within a Coralogix transaction"),
	)
	if err != nil {
		otel.Handle(err)
		return nil
	}
	return histogram
}

func NewTransactionSpanProcessor(exporter sdktrace.SpanExporter, opts ...Option) *TransactionSpanProcessor {
	p := &TransactionSpanProcessor{
		exporter:           exporter,
		traces:             make(map[tracecore.TraceID]*traceBuffer),
		childIntervals:     make(map[tracecore.SpanID][]interval),
		maxNodes:           DefaultMaxNodes,
		maxRegularTraces:   DefaultMaxRegularTraces,
		harvestPeriod:      DefaultHarvestPeriod,
		completionHoldback: DefaultCompletionHoldback,
	}
	p.idle = sync.NewCond(&p.mu)
	for _, opt := range opts {
		opt(p)
	}
	if p.selfTimeHistogram == nil {
		p.selfTimeHistogram = newSelfTimeHistogram(global.MeterProvider())
	}
	p.harvest = newRegularTraceHeap(p.maxRegularTraces)
	if p.maxRegularTraces > 0 && p.harvestPeriod > 0 {
		p.startHarvester()
	}
	return p
}

func (p *TransactionSpanProcessor) startHarvester() {
	p.harvestTicker = time.NewTicker(p.harvestPeriod)
	p.harvestStopCh = make(chan struct{})
	p.harvestDone = make(chan struct{})
	go func() {
		defer close(p.harvestDone)
		for {
			select {
			case <-p.harvestTicker.C:
				p.flushHarvest()
			case <-p.harvestStopCh:
				return
			}
		}
	}()
}

func (p *TransactionSpanProcessor) stopHarvester() {
	if p.harvestTicker == nil {
		return
	}
	p.harvestTicker.Stop()
	close(p.harvestStopCh)
	<-p.harvestDone
}

func (p *TransactionSpanProcessor) OnStart(ctx context.Context, s sdktrace.ReadWriteSpan) {
	tagTransaction(ctx, s)

	traceID := s.SpanContext().TraceID()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.exporterShutdown.Load() {
		return
	}
	// After Shutdown, still register children of tracked traces so OnEnd cannot finalize early.
	if p.stopped {
		tb, ok := p.traces[traceID]
		if !ok {
			return
		}
		p.stopCompleteTimerLocked(tb)
		if tb.liveParents == nil {
			tb.liveParents = make(map[tracecore.SpanID]tracecore.SpanID)
		}
		tb.liveParents[s.SpanContext().SpanID()] = s.Parent().SpanID()
		return
	}
	tb, ok := p.traces[traceID]
	if !ok {
		tb = &traceBuffer{liveParents: make(map[tracecore.SpanID]tracecore.SpanID)}
		p.traces[traceID] = tb
	}
	p.stopCompleteTimerLocked(tb)
	if tb.liveParents == nil {
		tb.liveParents = make(map[tracecore.SpanID]tracecore.SpanID)
	}
	tb.liveParents[s.SpanContext().SpanID()] = s.Parent().SpanID()
}

func (p *TransactionSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	traceID := s.SpanContext().TraceID()

	p.mu.Lock()
	if p.exporterShutdown.Load() {
		p.mu.Unlock()
		return
	}
	tb, ok := p.traces[traceID]
	if !ok {
		if p.stopped {
			p.mu.Unlock()
			return
		}
		tb = &traceBuffer{liveParents: make(map[tracecore.SpanID]tracecore.SpanID)}
		p.traces[traceID] = tb
		tb.liveParents[s.SpanContext().SpanID()] = s.Parent().SpanID()
	}

	if parent := s.Parent(); parent.IsValid() {
		p.childIntervals[parent.SpanID()] = append(p.childIntervals[parent.SpanID()], interval{
			start: s.StartTime().UnixNano(),
			end:   s.EndTime().UnixNano(),
		})
	}

	tb.spans = append(tb.spans, s)
	delete(tb.liveParents, s.SpanContext().SpanID())

	var batches [][]sdktrace.ReadOnlySpan
	if tb.liveCount() > 0 {
		batches = p.extractCompletedLocalTransactionsLocked(tb, false)
	} else {
		batches = p.scheduleCompletionLocked(traceID, tb)
	}

	p.pendingFinalize += len(batches)
	if p.totalLiveLocked() == 0 {
		p.idle.Broadcast()
	}
	p.mu.Unlock()

	for _, batch := range batches {
		p.acceptCompleted(batch)
		p.mu.Lock()
		p.pendingFinalize--
		p.idle.Broadcast()
		p.mu.Unlock()
	}
}

func (p *TransactionSpanProcessor) extractCompletedLocalTransactionsLocked(tb *traceBuffer, flushLeftover bool) [][]sdktrace.ReadOnlySpan {
	if len(tb.spans) == 0 {
		return nil
	}

	parentOf := make(map[tracecore.SpanID]tracecore.SpanID, len(tb.spans)+len(tb.liveParents))
	for _, s := range tb.spans {
		if pid := s.Parent().SpanID(); pid.IsValid() {
			parentOf[s.SpanContext().SpanID()] = pid
		}
	}
	for id, pid := range tb.liveParents {
		if pid.IsValid() {
			parentOf[id] = pid
		}
	}

	underRoot := func(spanID, rootID tracecore.SpanID) bool {
		cur := spanID
		seen := make(map[tracecore.SpanID]struct{})
		for cur.IsValid() {
			if _, done := seen[cur]; done {
				break
			}
			if cur == rootID {
				return true
			}
			seen[cur] = struct{}{}
			next, ok := parentOf[cur]
			if !ok {
				break
			}
			cur = next
		}
		return false
	}

	hasLiveInSubtree := func(rootID tracecore.SpanID) bool {
		if _, ok := tb.liveParents[rootID]; ok {
			return true
		}
		for liveID := range tb.liveParents {
			if underRoot(liveID, rootID) {
				return true
			}
		}
		return false
	}

	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	var roots []tracecore.SpanID
	for _, s := range tb.spans {
		for _, a := range s.Attributes() {
			if a.Key == rootKey && a.Value.AsBool() {
				roots = append(roots, s.SpanContext().SpanID())
				break
			}
		}
	}

	rootDepth := func(rootID tracecore.SpanID) int {
		depth := 0
		cur := rootID
		seen := make(map[tracecore.SpanID]struct{})
		for cur.IsValid() {
			if _, done := seen[cur]; done {
				break
			}
			seen[cur] = struct{}{}
			next, ok := parentOf[cur]
			if !ok || !next.IsValid() {
				break
			}
			depth++
			cur = next
		}
		return depth
	}

	sort.SliceStable(roots, func(i, j int) bool {
		return rootDepth(roots[i]) > rootDepth(roots[j])
	})

	var batches [][]sdktrace.ReadOnlySpan
	extracted := make(map[tracecore.SpanID]struct{})

	for _, rootID := range roots {
		if _, done := extracted[rootID]; done {
			continue
		}
		if hasLiveInSubtree(rootID) {
			continue
		}
		var subtree []sdktrace.ReadOnlySpan
		for _, s := range tb.spans {
			sid := s.SpanContext().SpanID()
			if _, done := extracted[sid]; done {
				continue
			}
			if underRoot(sid, rootID) {
				subtree = append(subtree, s)
			}
		}
		if len(subtree) == 0 {
			continue
		}
		for _, s := range subtree {
			extracted[s.SpanContext().SpanID()] = struct{}{}
		}
		batches = append(batches, subtree)
	}

	if len(extracted) > 0 {
		remaining := make([]sdktrace.ReadOnlySpan, 0, len(tb.spans)-len(extracted))
		for _, s := range tb.spans {
			if _, done := extracted[s.SpanContext().SpanID()]; !done {
				remaining = append(remaining, s)
			}
		}
		tb.spans = remaining
	}

	if flushLeftover && tb.liveCount() == 0 && len(tb.spans) > 0 {
		batches = append(batches, tb.spans)
		tb.spans = nil
	}

	return batches
}

func (p *TransactionSpanProcessor) stopCompleteTimerLocked(tb *traceBuffer) {
	if tb.completeTimer != nil {
		tb.completeTimer.Stop()
		tb.completeTimer = nil
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

func (p *TransactionSpanProcessor) flushPendingCompletionsLocked() {
	for {
		var batches [][]sdktrace.ReadOnlySpan
		for id, tb := range p.traces {
			if tb.liveCount() > 0 {
				continue
			}
			p.stopCompleteTimerLocked(tb)
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

func (p *TransactionSpanProcessor) totalLiveLocked() int {
	return p.pendingFinalize + p.liveSpanCountLocked()
}

func (p *TransactionSpanProcessor) liveSpanCountLocked() int {
	n := 0
	for _, tb := range p.traces {
		n += tb.liveCount()
	}
	return n
}

func (p *TransactionSpanProcessor) acceptCompleted(spans []sdktrace.ReadOnlySpan) {
	if len(spans) == 0 {
		return
	}
	defer p.popChildIntervals(spans)

	stamped := p.stampSelfTimeAndMetrics(spans)

	trimmed := selectSlowestSpans(stamped, p.maxNodes, findTransactionRootSpanIDs(stamped)...)
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

func (p *TransactionSpanProcessor) stampSelfTimeAndMetrics(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
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
		selfTimeNs := selfTimeNanosWithExtraIntervals(s, children, extra)
		stamped := withSelfTime(s, selfTimeNs)
		out = append(out, stamped)
		p.recordSelfTimeMetric(stamped, selfTimeNs)
	}
	return out
}

func filterPriorIntervals(prior []interval, children []sdktrace.ReadOnlySpan) []interval {
	if len(prior) == 0 {
		return nil
	}
	out := make([]interval, 0, len(prior))
	for _, iv := range prior {
		dup := false
		for _, c := range children {
			if c.StartTime().UnixNano() == iv.start && c.EndTime().UnixNano() == iv.end {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, iv)
		}
	}
	return out
}

func (p *TransactionSpanProcessor) popChildIntervals(spans []sdktrace.ReadOnlySpan) {
	p.mu.Lock()
	for _, s := range spans {
		delete(p.childIntervals, s.SpanContext().SpanID())
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

func (p *TransactionSpanProcessor) recordSelfTimeMetric(span sdktrace.ReadOnlySpan, selfTimeNs int64) {
	if p.selfTimeHistogram == nil {
		return
	}

	attrs := make([]attribute.KeyValue, 0, 3)
	attrs = append(attrs, attribute.String(SpanNameMetricAttribute, span.Name()))
	for _, a := range span.Attributes() {
		if a.Key == attribute.Key(sampler.TransactionIdentifier) || a.Key == attribute.Key(sampler.TransactionIdentifierRoot) {
			attrs = append(attrs, a)
		}
	}

	p.selfTimeHistogram.Record(context.Background(), float64(selfTimeNs)/float64(1e9), attrs...)
}

func (p *TransactionSpanProcessor) Shutdown(ctx context.Context) error {
	var err error
	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		p.mu.Unlock()

		exportCtx := ctx
		if ctx.Err() != nil {
			exportCtx = context.Background()
		}
		p.exportMu.Lock()
		p.exportCtx = exportCtx
		p.exportMu.Unlock()

		p.stopHarvester()

		p.waitForIdle(ctx)

		p.mu.Lock()
		p.flushPendingCompletionsLocked()
		for p.pendingFinalize > 0 {
			p.idle.Wait()
		}

		var leftover [][]sdktrace.ReadOnlySpan
		for id, tb := range p.traces {
			p.stopCompleteTimerLocked(tb)
			if tb.liveCount() > 0 {
				delete(p.traces, id)
				continue
			}
			leftover = append(leftover, p.extractCompletedLocalTransactionsLocked(tb, true)...)
			delete(p.traces, id)
		}
		p.pendingFinalize += len(leftover)
		for _, batch := range leftover {
			p.mu.Unlock()
			p.acceptCompleted(batch)
			p.mu.Lock()
			p.pendingFinalize--
		}
		for p.pendingFinalize > 0 {
			p.idle.Wait()
		}
		p.mu.Unlock()

		p.flushHarvest()

		p.mu.Lock()
		p.childIntervals = make(map[tracecore.SpanID][]interval)
		p.mu.Unlock()

		p.exportMu.Lock()
		p.exporterShutdown.Store(true)
		if p.exporter != nil {
			err = p.exporter.Shutdown(ctx)
		}
		p.exportCtx = nil
		p.exportMu.Unlock()
	})
	return err
}

func (p *TransactionSpanProcessor) waitForIdle(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.idle.Broadcast()
			p.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	p.mu.Lock()
	defer p.mu.Unlock()
	for p.liveSpanCountLocked() > 0 && ctx.Err() == nil {
		p.idle.Wait()
	}
	for p.pendingFinalize > 0 {
		p.idle.Wait()
	}
}

func (p *TransactionSpanProcessor) ForceFlush(ctx context.Context) error {
	if p.exporterShutdown.Load() {
		return nil
	}

	p.exportMu.Lock()
	p.exportCtx = ctx
	p.exportMu.Unlock()
	defer func() {
		p.exportMu.Lock()
		if p.exportCtx == ctx {
			p.exportCtx = nil
		}
		p.exportMu.Unlock()
	}()

	p.mu.Lock()
	if p.exporterShutdown.Load() {
		p.mu.Unlock()
		return nil
	}
	p.flushPendingCompletionsLocked()
	for p.pendingFinalize > 0 {
		p.idle.Wait()
	}
	p.mu.Unlock()

	p.flushHarvest()
	p.exportMu.Lock()
	defer p.exportMu.Unlock()
	if p.exporterShutdown.Load() {
		return nil
	}
	if flusher, ok := p.exporter.(interface{ ForceFlush(context.Context) error }); ok {
		return flusher.ForceFlush(ctx)
	}
	return nil
}

var _ sdktrace.SpanProcessor = (*TransactionSpanProcessor)(nil)
