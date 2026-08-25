package transaction

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracecore "go.opentelemetry.io/otel/trace"
)

func TestForceFlush_DoesNotFinalizeIncompleteTraces(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithMaxRegularTraces(0), WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("force-flush-test")
	base := time.Unix(0, 0)

	rootCtx, root := tracer.Start(context.Background(), "parent",
		tracecore.WithTimestamp(base),
	)
	_, child := tracer.Start(rootCtx, "child",
		tracecore.WithTimestamp(base.Add(20*time.Millisecond)),
	)
	child.End(tracecore.WithTimestamp(base.Add(80 * time.Millisecond)))

	require.NoError(t, processor.ForceFlush(context.Background()))
	assert.Empty(t, exporter.GetSpans(), "incomplete local traces must stay buffered across ForceFlush")

	root.End(tracecore.WithTimestamp(base.Add(100 * time.Millisecond)))
	spans := exporter.GetSpans()
	require.Len(t, spans, 2)

	parent := findSpan(t, spans, "parent")
	assertFloat64Attribute(t, parent.Attributes, SelfDurationAttribute, 0.04)

	require.NoError(t, tp.Shutdown(context.Background()))
}

// stickyExporter keeps spans across Shutdown (InMemoryExporter Reset()s on Shutdown).
type stickyExporter struct {
	mu       sync.Mutex
	spans    sdktracetest.SpanStubs
	shutdown bool
}

func (e *stickyExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.shutdown {
		return errors.New("exporter already shut down")
	}
	e.spans = append(e.spans, sdktracetest.SpanStubsFromReadOnlySpans(spans)...)
	return nil
}

func (e *stickyExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdown = true
	return nil
}

func (e *stickyExporter) get() sdktracetest.SpanStubs {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(sdktracetest.SpanStubs, len(e.spans))
	copy(out, e.spans)
	return out
}

func TestShutdown_WaitsForInFlightSpansBeforeExporterShutdown(t *testing.T) {
	exporter := &stickyExporter{}
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("shutdown-wait-test")
	base := time.Unix(0, 0)

	rootCtx, root := tracer.Start(context.Background(), "parent",
		tracecore.WithTimestamp(base),
	)
	_, child := tracer.Start(rootCtx, "child",
		tracecore.WithTimestamp(base.Add(20*time.Millisecond)),
	)
	child.End(tracecore.WithTimestamp(base.Add(80 * time.Millisecond)))

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(30 * time.Millisecond)
		root.End(tracecore.WithTimestamp(base.Add(100 * time.Millisecond)))
	}()

	require.NoError(t, processor.Shutdown(context.Background()))
	<-done

	spans := exporter.get()
	require.Len(t, spans, 2, "Shutdown must wait for in-flight spans and export before shutting the exporter")
	parent := findSpan(t, spans, "parent")
	assertFloat64Attribute(t, parent.Attributes, SelfDurationAttribute, 0.04)

	_ = tp.Shutdown(context.Background())
}

func TestShutdown_PreservesFirstExporterErrorAcrossCalls(t *testing.T) {
	exporter := &errShutdownExporter{err: errors.New("exporter shutdown failed")}
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0), WithMaxRegularTraces(0))

	first := processor.Shutdown(context.Background())
	second := processor.Shutdown(context.Background())

	require.ErrorIs(t, first, exporter.err)
	require.ErrorIs(t, second, exporter.err)
	assert.Equal(t, first, second)
	assert.Equal(t, 1, exporter.calls, "exporter.Shutdown must run once")
}

type errShutdownExporter struct {
	mu    sync.Mutex
	err   error
	calls int
}

func (e *errShutdownExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return nil
}

func (e *errShutdownExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	return e.err
}

func TestShutdown_ContextCancelDropsIncompleteLocalTraces(t *testing.T) {
	exporter := &stickyExporter{}
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("shutdown-cancel-test")
	base := time.Unix(0, 0)

	rootCtx, root := tracer.Start(context.Background(), "parent",
		tracecore.WithTimestamp(base),
	)
	_, child := tracer.Start(rootCtx, "child",
		tracecore.WithTimestamp(base.Add(10*time.Millisecond)),
	)
	child.End(tracecore.WithTimestamp(base.Add(30 * time.Millisecond)))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: do not wait for parent
	require.NoError(t, processor.Shutdown(ctx))

	assert.Empty(t, exporter.get())

	root.End(tracecore.WithTimestamp(base.Add(50 * time.Millisecond)))
	assert.Empty(t, exporter.get(), "OnEnd after exporter shutdown must not export")

	_ = tp.Shutdown(context.Background())
}

func TestShutdown_ContextCancelSkipsEndedParentsWithLiveChildren(t *testing.T) {
	exporter := &stickyExporter{}
	processor := NewTransactionSpanProcessor(exporter, WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("shutdown-cancel-parent-first")
	base := time.Unix(0, 0)

	rootCtx, root := tracer.Start(context.Background(), "parent",
		tracecore.WithTimestamp(base),
	)
	_, child := tracer.Start(rootCtx, "child",
		tracecore.WithTimestamp(base.Add(10*time.Millisecond)),
	)
	root.End(tracecore.WithTimestamp(base.Add(20 * time.Millisecond)))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, processor.Shutdown(ctx))

	assert.Empty(t, exporter.get(), "ended parent with live child must not be exported (inflated self-time)")

	child.End(tracecore.WithTimestamp(base.Add(40 * time.Millisecond)))
	assert.Empty(t, exporter.get(), "live child ending after exporter shutdown is dropped")

	_ = tp.Shutdown(context.Background())
}

func TestShutdown_PostStopChildPreventsPrematureParentFinalize(t *testing.T) {
	exporter := &stickyExporter{}
	processor := NewTransactionSpanProcessor(exporter, WithMaxRegularTraces(0), WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("shutdown-post-stop-child")
	base := time.Unix(0, 0)

	rootCtx, root := tracer.Start(context.Background(), "parent",
		tracecore.WithTimestamp(base),
	)

	// Begin shutdown without cancelling: waitForIdle blocks until idle.
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- processor.Shutdown(context.Background())
	}()

	// After stopped=true, late children of tracked traces must still be registered.
	time.Sleep(20 * time.Millisecond)
	_, child := tracer.Start(rootCtx, "late-child",
		tracecore.WithTimestamp(base.Add(20*time.Millisecond)),
	)

	root.End(tracecore.WithTimestamp(base.Add(40 * time.Millisecond)))
	time.Sleep(20 * time.Millisecond)
	assert.Empty(t, exporter.get(), "parent must not export while post-stop child is still live")

	child.End(tracecore.WithTimestamp(base.Add(80 * time.Millisecond)))
	require.NoError(t, <-shutdownDone)

	spans := exporter.get()
	require.Len(t, spans, 2)
	parent := findSpan(t, spans, "parent")
	assertFloat64Attribute(t, parent.Attributes, SelfDurationAttribute, 0.02)

	_ = tp.Shutdown(context.Background())
}

// blockingExporter blocks inside ExportSpans until release is closed.
type blockingExporter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingExporter) ExportSpans(_ context.Context, _ []sdktrace.ReadOnlySpan) error {
	e.once.Do(func() { close(e.started) })
	<-e.release
	return nil
}

func (e *blockingExporter) Shutdown(context.Context) error { return nil }

func TestShutdown_WaitsForPendingFinalizeAfterContextCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	exporter := &blockingExporter{started: started, release: release}
	processor := NewTransactionSpanProcessor(exporter, WithMaxRegularTraces(0), WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("shutdown-pending-finalize")

	go func() {
		_, span := tracer.Start(context.Background(), "root")
		span.End()
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("ExportSpans did not start")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- processor.Shutdown(ctx)
	}()

	select {
	case err := <-done:
		t.Fatalf("Shutdown returned while acceptCompleted still in ExportSpans: %v", err)
	case <-time.After(50 * time.Millisecond):
		// Still blocked in pendingFinalize — expected.
	}

	close(release)
	require.NoError(t, <-done)
	_ = tp.Shutdown(context.Background())
}

func TestCompletionHoldback_JoinsFireAndForgetChild(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(50*time.Millisecond),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("holdback-test")
	base := time.Unix(0, 0)

	rootCtx, root := tracer.Start(context.Background(), "parent",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithTimestamp(base),
	)
	root.End(tracecore.WithTimestamp(base.Add(40 * time.Millisecond)))

	time.Sleep(10 * time.Millisecond)
	assert.Empty(t, exporter.GetSpans(), "must not finalize while holdback is open")

	_, child := tracer.Start(rootCtx, "async-child",
		tracecore.WithTimestamp(base.Add(50*time.Millisecond)),
	)
	child.End(tracecore.WithTimestamp(base.Add(80 * time.Millisecond)))

	require.NoError(t, processor.ForceFlush(context.Background()))
	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	assert.NotNil(t, findSpan(t, spans, "parent"))
	assert.NotNil(t, findSpan(t, spans, "async-child"))

	require.NoError(t, tp.Shutdown(context.Background()))
}

// ctxCaptureExporter records the context passed to each ExportSpans call.
type ctxCaptureExporter struct {
	mu    sync.Mutex
	ctxs  []context.Context
	spans sdktracetest.SpanStubs
}

func (e *ctxCaptureExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ctxs = append(e.ctxs, ctx)
	e.spans = append(e.spans, sdktracetest.SpanStubsFromReadOnlySpans(spans)...)
	return nil
}

func (e *ctxCaptureExporter) Shutdown(context.Context) error { return nil }

func TestShutdown_CancelledContextUsesBackgroundForDrainExports(t *testing.T) {
	exporter := &ctxCaptureExporter{}
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(time.Hour),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("shutdown-cancel-export-ctx")
	base := time.Unix(0, 0)

	_, root := tracer.Start(context.Background(), "root",
		tracecore.WithTimestamp(base),
	)
	root.End(tracecore.WithTimestamp(base.Add(10 * time.Millisecond)))

	time.Sleep(10 * time.Millisecond)
	exporter.mu.Lock()
	require.Empty(t, exporter.ctxs, "holdback must defer export until Shutdown drain")
	exporter.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, processor.Shutdown(ctx))

	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	require.NotEmpty(t, exporter.ctxs)
	require.Len(t, exporter.spans, 1)
	for _, c := range exporter.ctxs {
		assert.NoError(t, c.Err(), "drain ExportSpans must not use a cancelled Shutdown context")
	}
	_ = tp.Shutdown(context.Background())
}

func TestShutdown_CancelledContextStillDrainsIdleHoldback(t *testing.T) {
	exporter := &stickyExporter{}
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(time.Hour),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("shutdown-cancel-drain-holdback")
	base := time.Unix(0, 0)

	_, root := tracer.Start(context.Background(), "root",
		tracecore.WithTimestamp(base),
	)
	root.End(tracecore.WithTimestamp(base.Add(10 * time.Millisecond)))

	time.Sleep(10 * time.Millisecond)
	assert.Empty(t, exporter.get(), "holdback must keep the idle trace buffered")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, processor.Shutdown(ctx))

	spans := exporter.get()
	require.Len(t, spans, 1, "cancelled Shutdown must still drain idle holdback leftovers")
	assert.NotNil(t, findSpan(t, spans, "root"))
	_ = tp.Shutdown(context.Background())
}

func TestCompletionHoldback_StaleTimerDoesNotFinalizeEarly(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	holdback := 40 * time.Millisecond
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(holdback),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("stale-timer")
	base := time.Unix(0, 0)

	rootCtx, root := tracer.Start(context.Background(), "parent",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithTimestamp(base),
	)
	root.End(tracecore.WithTimestamp(base.Add(10 * time.Millisecond)))

	// Near first holdback expiry, a child stops that timer and arms a replacement.
	time.Sleep(holdback - 10*time.Millisecond)
	_, child := tracer.Start(rootCtx, "late-child",
		tracecore.WithTimestamp(base.Add(20*time.Millisecond)),
	)
	child.End(tracecore.WithTimestamp(base.Add(30 * time.Millisecond)))

	// Past original deadline; replacement holdback still open.
	time.Sleep(20 * time.Millisecond)
	assert.Empty(t, exporter.GetSpans(), "stale stopped holdback must not finalize after replacement armed")

	time.Sleep(holdback)
	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	assert.NotNil(t, findSpan(t, spans, "parent"))
	assert.NotNil(t, findSpan(t, spans, "late-child"))

	require.NoError(t, tp.Shutdown(context.Background()))
}

// gatedStickyExporter blocks the first ExportSpans until release is closed.
type gatedStickyExporter struct {
	mu       sync.Mutex
	spans    sdktracetest.SpanStubs
	started  chan struct{}
	release  chan struct{}
	once     sync.Once
	shutdown bool
}

func (e *gatedStickyExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.once.Do(func() { close(e.started) })
	<-e.release
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.shutdown {
		return errors.New("exporter already shut down")
	}
	e.spans = append(e.spans, sdktracetest.SpanStubsFromReadOnlySpans(spans)...)
	return nil
}

func (e *gatedStickyExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdown = true
	return nil
}

func (e *gatedStickyExporter) get() sdktracetest.SpanStubs {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(sdktracetest.SpanStubs, len(e.spans))
	copy(out, e.spans)
	return out
}

func TestExporterShutdown_AtomicVisibleAcrossMuAndExportMu(t *testing.T) {
	// Race check: exporterShutdown is atomic.Bool shared across p.mu and exportMu.
	exporter := &stickyExporter{}
	processor := NewTransactionSpanProcessor(exporter, WithMaxRegularTraces(0), WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("exporter-shutdown-atomic")
	base := time.Unix(0, 0)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, span := tracer.Start(context.Background(), "root",
				tracecore.WithTimestamp(base.Add(time.Duration(i)*time.Millisecond)),
			)
			span.End(tracecore.WithTimestamp(base.Add(time.Duration(i+1) * time.Millisecond)))
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = processor.ForceFlush(context.Background())
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(8 * time.Millisecond)
		_ = processor.Shutdown(context.Background())
	}()

	wg.Wait()
	assert.True(t, processor.exporterShutdown.Load())
	_ = tp.Shutdown(context.Background())
}

func TestForceFlush_ReturnsWhenContextExpiresDuringPendingFinalize(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	exporter := &gatedStickyExporter{started: started, release: release}
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(time.Hour),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("flush-ctx-timeout")
	base := time.Unix(0, 0)

	_, span := tracer.Start(context.Background(), "blocked",
		tracecore.WithTimestamp(base),
	)
	span.End(tracecore.WithTimestamp(base.Add(5 * time.Millisecond)))

	// First ForceFlush drains holdback and blocks inside ExportSpans.
	flush1 := make(chan error, 1)
	go func() {
		flush1 <- processor.ForceFlush(context.Background())
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first ForceFlush did not reach ExportSpans")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := processor.ForceFlush(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(release)
	require.NoError(t, <-flush1)
	_ = tp.Shutdown(context.Background())
}

func TestForceFlush_DoesNotPublishContextToConcurrentExports(t *testing.T) {
	exporter := &ctxCaptureExporter{}
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("flush-ctx-isolation")
	base := time.Unix(0, 0)

	flushCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, span := tracer.Start(context.Background(), "root",
		tracecore.WithTimestamp(base),
	)
	span.End(tracecore.WithTimestamp(base.Add(5 * time.Millisecond)))

	require.NoError(t, processor.ForceFlush(flushCtx))
	cancel()

	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	require.NotEmpty(t, exporter.ctxs)
	for _, c := range exporter.ctxs {
		assert.NoError(t, c.Err(), "acceptCompleted export must not use ForceFlush context")
	}
	_ = tp.Shutdown(context.Background())
}

func TestForceFlush_RescansIdleAfterAccept(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	exporter := &gatedStickyExporter{started: started, release: release}
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(time.Hour),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("flush-rescan")
	base := time.Unix(0, 0)

	_, first := tracer.Start(context.Background(), "first",
		tracecore.WithTimestamp(base),
	)
	first.End(tracecore.WithTimestamp(base.Add(5 * time.Millisecond)))

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- processor.ForceFlush(context.Background())
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("ForceFlush did not reach ExportSpans for first idle trace")
	}

	// While first accept is unlocked in ExportSpans, arm holdback on another TraceID.
	_, second := tracer.Start(context.Background(), "second",
		tracecore.WithTimestamp(base.Add(10*time.Millisecond)),
	)
	second.End(tracecore.WithTimestamp(base.Add(15 * time.Millisecond)))

	close(release)
	require.NoError(t, <-flushDone)

	spans := exporter.get()
	require.Len(t, spans, 2, "ForceFlush must re-scan and drain the second idle trace")
	assert.NotNil(t, findSpan(t, spans, "first"))
	assert.NotNil(t, findSpan(t, spans, "second"))

	require.NoError(t, tp.Shutdown(context.Background()))
}
