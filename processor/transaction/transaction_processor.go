// Package transaction provides TransactionSpanProcessor: tags Coralogix
// transactions, stamps exclusive self-duration, and records
// cgx.transaction.self_duration.
//
// Flow:
//   - OnStart: decide new vs inherit (SERVER / CONSUMER / remote / no local
//     parent txn). Mark cgx.transaction.root on new roots. Track membership.
//     Do not freeze cgx.transaction from the early span name.
//   - OnEnd / holdback: buffer until local transaction trees complete.
//   - acceptCompleted (export finalize): stamp cgx.transaction from the root's
//     final Name() (or pre-set override), stamp self-duration + metrics, trim
//     to maxNodes, then harvest or export.
package transaction

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/unit"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"
)

const (
	SelfDurationAttribute   = "cgx.transaction.self_duration"
	SelfDurationMetricName  = "cgx.transaction.self_duration"
	SelfDurationMetricUnit  = unit.Unit("s")
	SpanNameMetricAttribute = "span.name"

	instrumentationName = "coralogix.opentelemetry.transaction"
)

type spanMembership struct {
	// inheritedName is set when the parent transaction name comes only from
	// TraceState / attrs without a locally tracked root (finalize fallback),
	// or when inheriting from a finalized parent name in the side cache.
	inheritedName string
	// inheritedFrom is the SpanID of the finalized (or TraceState) source that
	// supplied inheritedName. Used as the rootless partition key so two local
	// transactions that share a display name stay separate.
	inheritedFrom tracecore.SpanID
	// startName is the span name observed at OnStart (before UpdateName).
	startName string
	// overrideName is an explicit transaction name that must win over the
	// final span name (Express route template / StartNewTransaction). Sampler
	// echoes of the early span name are not overrides.
	overrideName string
	// finalized marks a post-export tombstone used by late fire-and-forget
	// children to inherit the parent's stamped transaction name. Active
	// membership no longer stores these; see finalizedNames.
	finalized bool
}

type traceBuffer struct {
	spans               []sdktrace.ReadOnlySpan
	liveParents         map[tracecore.SpanID]tracecore.SpanID
	completeTimer       *time.Timer
	nestedCompleteTimer *time.Timer
}

func (tb *traceBuffer) liveCount() int {
	return len(tb.liveParents)
}

// TransactionSpanProcessor wraps a SpanExporter to tag Coralogix transactions,
// stamp cgx.transaction.self_duration, trim, and harvest slowest local traces.
type TransactionSpanProcessor struct {
	exporter sdktrace.SpanExporter

	mu             sync.Mutex
	traces         map[tracecore.TraceID]*traceBuffer
	membership     map[tracecore.SpanID]spanMembership
	childIntervals map[tracecore.SpanID][]interval

	// finalizedNames retains stamped txn names after active membership is
	// cleared so late children can inherit. finalizedOrder is FIFO insertion
	// order for eviction when over maxFinalizedNames.
	finalizedNames    map[tracecore.SpanID]string
	finalizedOrder    []tracecore.SpanID
	maxFinalizedNames int

	selfDurationHistogram syncfloat64.Histogram

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
	shutdownErr  error
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

// WithMeterProvider sets the MeterProvider for cgx.transaction.self_duration.
func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return func(p *TransactionSpanProcessor) {
		if meterProvider == nil {
			return
		}
		if histogram := newSelfDurationHistogram(meterProvider); histogram != nil {
			p.selfDurationHistogram = histogram
		}
	}
}

// WithMaxNodes caps spans kept per completed local trace (slowest first; root always kept).
func WithMaxNodes(maxNodes int) Option {
	return func(p *TransactionSpanProcessor) {
		p.maxNodes = maxNodes
	}
}

// WithMaxRegularTraces caps completed traces held until harvest.
// Default 1 keeps only the slowest completed local waterfall(s) per harvest
// window; losers are stub-exported (root only). Self-duration metrics are still
// recorded for every completed local trace. 0 exports every completed trimmed
// trace immediately.
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

// WithCompletionHoldback sets the post-idle delay before finalizing a local trace.
func WithCompletionHoldback(d time.Duration) Option {
	return func(p *TransactionSpanProcessor) {
		p.completionHoldback = d
	}
}

// WithMaxFinalizedNames caps SpanID→transaction-name entries retained after a
// local batch is finalized (for late fire-and-forget children). When n <= 0,
// DefaultMaxFinalizedNames is used.
func WithMaxFinalizedNames(n int) Option {
	return func(p *TransactionSpanProcessor) {
		p.maxFinalizedNames = n
	}
}

func newSelfDurationHistogram(meterProvider metric.MeterProvider) syncfloat64.Histogram {
	meter := meterProvider.Meter(instrumentationName)
	histogram, err := meter.SyncFloat64().Histogram(
		SelfDurationMetricName,
		instrument.WithUnit(SelfDurationMetricUnit),
		instrument.WithDescription("Exclusive self duration per span within a Coralogix transaction"),
	)
	if err != nil {
		otel.Handle(err)
		return nil
	}
	return histogram
}

// NewTransactionSpanProcessor builds a processor. Options override env vars;
// unset options fall back to OTEL_CX_TRANSACTION_* then package defaults.
func NewTransactionSpanProcessor(exporter sdktrace.SpanExporter, opts ...Option) *TransactionSpanProcessor {
	maxNodes, maxRegularTraces, harvestPeriod, completionHoldback := defaultsFromEnv()
	p := &TransactionSpanProcessor{
		exporter:           exporter,
		traces:             make(map[tracecore.TraceID]*traceBuffer),
		membership:         make(map[tracecore.SpanID]spanMembership),
		childIntervals:     make(map[tracecore.SpanID][]interval),
		finalizedNames:     make(map[tracecore.SpanID]string),
		maxNodes:           maxNodes,
		maxRegularTraces:   maxRegularTraces,
		harvestPeriod:      harvestPeriod,
		completionHoldback: completionHoldback,
	}
	p.idle = sync.NewCond(&p.mu)
	for _, opt := range opts {
		opt(p)
	}
	if p.maxFinalizedNames <= 0 {
		p.maxFinalizedNames = DefaultMaxFinalizedNames
	}
	if p.selfDurationHistogram == nil {
		p.selfDurationHistogram = newSelfDurationHistogram(global.MeterProvider())
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
				_ = p.flushHarvest(context.Background())
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

// OnStart decides new vs inherit and marks roots. Transaction names are stamped
// later in acceptCompleted from the root's final Name() (or override).
func (p *TransactionSpanProcessor) OnStart(ctx context.Context, s sdktrace.ReadWriteSpan) {
	traceID := s.SpanContext().TraceID()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.exporterShutdown.Load() {
		beginTransaction(ctx, s, nil, nil)
		return
	}
	tb, ok := p.traces[traceID]
	if !ok {
		if p.stopped {
			beginTransaction(ctx, s, nil, nil)
			return
		}
		tb = &traceBuffer{
			liveParents: make(map[tracecore.SpanID]tracecore.SpanID),
		}
		p.traces[traceID] = tb
	}
	if tb.liveParents == nil {
		tb.liveParents = make(map[tracecore.SpanID]tracecore.SpanID)
	}
	beginTransaction(ctx, s, p.membership, p.finalizedNames)

	// After Shutdown, still register children of tracked traces so OnEnd cannot finalize early.
	if p.stopped {
		p.stopCompleteTimerLocked(tb)
		p.stopNestedCompleteTimerLocked(tb)
		tb.liveParents[s.SpanContext().SpanID()] = s.Parent().SpanID()
		return
	}

	p.stopCompleteTimerLocked(tb)
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
		tb = &traceBuffer{
			liveParents: make(map[tracecore.SpanID]tracecore.SpanID),
		}
		p.traces[traceID] = tb
		tb.liveParents[s.SpanContext().SpanID()] = s.Parent().SpanID()
		p.membership[s.SpanContext().SpanID()] = spanMembership{}
	}

	// Only retain intervals for parents we track locally. Remote / external
	// parent IDs are never cleaned by acceptCompleted and would leak.
	if parent := s.Parent(); parent.IsValid() && isLocalParent(parent.SpanID(), tb) {
		p.childIntervals[parent.SpanID()] = append(p.childIntervals[parent.SpanID()], interval{
			start: s.StartTime().UnixNano(),
			end:   s.EndTime().UnixNano(),
		})
	}

	tb.spans = append(tb.spans, s)
	delete(tb.liveParents, s.SpanContext().SpanID())

	var batches [][]sdktrace.ReadOnlySpan
	if tb.liveCount() > 0 {
		// Nested local txn finished while an outer ancestor is still live:
		// apply the same completion holdback so late children can join.
		batches = p.scheduleNestedCompletionLocked(traceID, tb)
	} else {
		p.stopNestedCompleteTimerLocked(tb)
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

func isLocalParent(parentID tracecore.SpanID, tb *traceBuffer) bool {
	if _, ok := tb.liveParents[parentID]; ok {
		return true
	}
	for _, sp := range tb.spans {
		if sp.SpanContext().SpanID() == parentID {
			return true
		}
	}
	return false
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

func (p *TransactionSpanProcessor) Shutdown(ctx context.Context) error {
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
			p.stopNestedCompleteTimerLocked(tb)
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

		if err := p.flushHarvest(exportCtx); err != nil && p.shutdownErr == nil {
			p.shutdownErr = err
		}

		p.mu.Lock()
		p.childIntervals = make(map[tracecore.SpanID][]interval)
		p.membership = make(map[tracecore.SpanID]spanMembership)
		p.finalizedNames = make(map[tracecore.SpanID]string)
		p.finalizedOrder = nil
		p.mu.Unlock()

		p.exportMu.Lock()
		p.exporterShutdown.Store(true)
		if p.exporter != nil {
			if err := p.exporter.Shutdown(ctx); err != nil && p.shutdownErr == nil {
				p.shutdownErr = err
			}
		}
		p.exportCtx = nil
		p.exportMu.Unlock()
	})
	return p.shutdownErr
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

	// Do not publish ctx onto p.exportCtx: concurrent OnEnd/acceptCompleted
	// exports must keep using Background (or Shutdown's drain context), not a
	// ForceFlush deadline that may expire mid-export.

	p.mu.Lock()
	if p.exporterShutdown.Load() {
		p.mu.Unlock()
		return nil
	}
	p.flushPendingCompletionsLocked()
	if err := p.waitPendingFinalizeLocked(ctx); err != nil {
		p.mu.Unlock()
		return err
	}
	p.mu.Unlock()

	if err := p.flushHarvest(ctx); err != nil {
		return err
	}
	if err := p.lockExportMu(ctx); err != nil {
		return err
	}
	defer p.exportMu.Unlock()
	if p.exporterShutdown.Load() {
		return nil
	}
	if flusher, ok := p.exporter.(interface{ ForceFlush(context.Context) error }); ok {
		return flusher.ForceFlush(ctx)
	}
	return nil
}

// waitPendingFinalizeLocked waits until pendingFinalize is 0 or ctx is done.
// Caller must hold p.mu. On context expiry returns ctx.Err().
func (p *TransactionSpanProcessor) waitPendingFinalizeLocked(ctx context.Context) error {
	if p.pendingFinalize == 0 {
		return nil
	}
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

	for p.pendingFinalize > 0 && ctx.Err() == nil {
		p.idle.Wait()
	}
	if p.pendingFinalize > 0 {
		return ctx.Err()
	}
	return nil
}

var _ sdktrace.SpanProcessor = (*TransactionSpanProcessor)(nil)
