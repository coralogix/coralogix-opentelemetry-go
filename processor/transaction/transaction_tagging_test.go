package transaction

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	sdktracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracecore "go.opentelemetry.io/otel/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

func newTracerProvider(t *testing.T) (*sdktracetest.InMemoryExporter, tracecore.Tracer, func()) {
	t.Helper()
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter, WithMaxRegularTraces(0), WithCompletionHoldback(0))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	tracer := tp.Tracer("transaction-tagging-test")
	return exporter, tracer, func() { require.NoError(t, tp.Shutdown(context.Background())) }
}

func TestTagTransaction_RootSpanStartsTransaction(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	_, span := tracer.Start(context.Background(), "root")
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "root")
	assertBoolAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot, true)
	assertNoAttribute(t, spans[0].Attributes, sampler.DistributedTransactionIdentifier)
}

func TestTagTransaction_InternalChildContinuesTransaction(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	ctx, root := tracer.Start(context.Background(), "root")
	_, child := tracer.Start(ctx, "child")
	child.End()
	root.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	childStub := findSpan(t, spans, "child")
	assertAttribute(t, childStub.Attributes, sampler.TransactionIdentifier, "root")
	assertNoAttribute(t, childStub.Attributes, sampler.TransactionIdentifierRoot)
}

func TestTagTransaction_ServerKindAlwaysStartsNewTransaction(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	ctx, root := tracer.Start(context.Background(), "root")
	_, server := tracer.Start(ctx, "server-child", tracecore.WithSpanKind(tracecore.SpanKindServer))
	server.End()
	root.End()

	spans := exporter.GetSpans()
	serverStub := findSpan(t, spans, "server-child")
	assertAttribute(t, serverStub.Attributes, sampler.TransactionIdentifier, "server-child")
	assertBoolAttribute(t, serverStub.Attributes, sampler.TransactionIdentifierRoot, true)
}

func TestTagTransaction_ConsumerKindAlwaysStartsNewTransaction(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	ctx, root := tracer.Start(context.Background(), "root")
	_, consumer := tracer.Start(ctx, "consumer-child", tracecore.WithSpanKind(tracecore.SpanKindConsumer))
	consumer.End()
	root.End()

	spans := exporter.GetSpans()
	consumerStub := findSpan(t, spans, "consumer-child")
	assertAttribute(t, consumerStub.Attributes, sampler.TransactionIdentifier, "consumer-child")
	assertBoolAttribute(t, consumerStub.Attributes, sampler.TransactionIdentifierRoot, true)
}

func TestTagTransaction_RemoteParentStartsNewLocalTransaction(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	traceState := tracecore.TraceState{}
	traceState, err := traceState.Insert(sampler.TransactionIdentifierTraceState, "upstream-txn")
	require.NoError(t, err)

	remoteParent := tracecore.NewSpanContext(tracecore.SpanContextConfig{
		TraceID:    tracecore.TraceID{0x01},
		SpanID:     tracecore.SpanID{0x01},
		TraceFlags: tracecore.FlagsSampled,
		TraceState: traceState,
		Remote:     true,
	})
	ctx := tracecore.ContextWithRemoteSpanContext(context.Background(), remoteParent)

	_, span := tracer.Start(ctx, "incoming-request", tracecore.WithSpanKind(tracecore.SpanKindServer))
	span.End()

	spans := exporter.GetSpans()
	stub := findSpan(t, spans, "incoming-request")
	assertAttribute(t, stub.Attributes, sampler.TransactionIdentifier, "incoming-request")
	assertBoolAttribute(t, stub.Attributes, sampler.TransactionIdentifierRoot, true)
	assertNoAttribute(t, stub.Attributes, sampler.DistributedTransactionIdentifier)
}

func TestTagTransaction_StartNewTransactionOverrideIsRespected(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	rootCtx, root := tracer.Start(context.Background(), "root")
	flowCtx, flow := tracer.Start(rootCtx, "flow")
	sampler.StartNewTransaction(flow, "flow")
	_, sub := tracer.Start(flowCtx, "flow-sub")

	sub.End()
	flow.End()
	root.End()

	spans := exporter.GetSpans()
	subStub := findSpan(t, spans, "flow-sub")
	assertAttribute(t, subStub.Attributes, sampler.TransactionIdentifier, "flow")
	assertNoAttribute(t, subStub.Attributes, sampler.TransactionIdentifierRoot)
}

func TestTagTransaction_StartNewTransactionEqualNameSurvivesRename(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	_, span := tracer.Start(context.Background(), "flow",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	sampler.StartNewTransaction(span, "flow")
	span.SetName("renamed-operation")
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "flow")
	assertBoolAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot, true)
	assertNoAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierExplicit)
}

func TestTagTransaction_PreservesPreSetTransactionName(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	_, span := tracer.Start(context.Background(), "GET /users/123",
		tracecore.WithAttributes(
			attribute.String(sampler.TransactionIdentifier, "GET /users/:id"),
		),
	)
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "GET /users/:id")
	assertBoolAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot, true)
}

func TestTagTransaction_UsesFinalSpanNameAfterUpdateName(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	ctx, root := tracer.Start(context.Background(), "GET",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	_, child := tracer.Start(ctx, "handler")
	root.SetName("GET /myroute")
	child.End()
	root.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	rootStub := findSpan(t, spans, "GET /myroute")
	childStub := findSpan(t, spans, "handler")
	assertAttribute(t, rootStub.Attributes, sampler.TransactionIdentifier, "GET /myroute")
	assertBoolAttribute(t, rootStub.Attributes, sampler.TransactionIdentifierRoot, true)
	assertAttribute(t, childStub.Attributes, sampler.TransactionIdentifier, "GET /myroute")
	assertNoAttribute(t, childStub.Attributes, sampler.TransactionIdentifierRoot)
}

func TestTagTransaction_SamplerEchoDoesNotBlockUpdateName(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	// Sampler injects cgx.transaction from the sampling-time name. That echo
	// must not freeze the transaction name before Express-style UpdateName.
	ctx, root := tracer.Start(context.Background(), "GET",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
		tracecore.WithAttributes(
			attribute.String(sampler.TransactionIdentifier, "GET"),
		),
	)
	_, child := tracer.Start(ctx, "handler")
	root.SetName("GET /myroute")
	child.End()
	root.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	rootStub := findSpan(t, spans, "GET /myroute")
	childStub := findSpan(t, spans, "handler")
	assertAttribute(t, rootStub.Attributes, sampler.TransactionIdentifier, "GET /myroute")
	assertAttribute(t, childStub.Attributes, sampler.TransactionIdentifier, "GET /myroute")
}

func TestTagTransaction_StartNewTransactionDifferentNameWins(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	_, span := tracer.Start(context.Background(), "process",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	sampler.StartNewTransaction(span, "fulfill")
	span.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "fulfill")
	assertBoolAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot, true)
}

func TestTagTransaction_LeftoverLateChildrenKeepOwnInheritedNames(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("leftover-partition")

	// Two SERVER roots on the same TraceID (nested under one outer).
	outerCtx, outer := tracer.Start(context.Background(), "gateway",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	aCtx, a := tracer.Start(outerCtx, "txn-a",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	bCtx, b := tracer.Start(outerCtx, "txn-b",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	a.End()
	b.End()
	outer.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 3)
	exporter.Reset()

	// Late INTERNAL children under each finalized root; keep both live so they
	// flush together as one leftover batch on the shared TraceID.
	_, lateA := tracer.Start(aCtx, "late-a",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	_, lateB := tracer.Start(bCtx, "late-b",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	lateA.End()
	lateB.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	assertAttribute(t, findSpan(t, spans, "late-a").Attributes, sampler.TransactionIdentifier, "txn-a")
	assertAttribute(t, findSpan(t, spans, "late-b").Attributes, sampler.TransactionIdentifier, "txn-b")
	assertNoAttribute(t, findSpan(t, spans, "late-a").Attributes, sampler.TransactionIdentifierRoot)
	assertNoAttribute(t, findSpan(t, spans, "late-b").Attributes, sampler.TransactionIdentifierRoot)
}

func TestTagTransaction_LateChildInheritsFinalizedParentName(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("late-child-inherit")

	rootCtx, root := tracer.Start(context.Background(), "parent-op",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	sampler.StartNewTransaction(root, "parent-txn")
	root.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 1)
	exporter.Reset()

	// Fire-and-forget child after the root batch was already finalized. Parent
	// is gone from membership but still readable via Context with attrs.
	_, child := tracer.Start(rootCtx, "late-child",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	child.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "parent-txn")
	assertNoAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot)
}

func TestTagTransaction_LateChildInheritsProcessorOnlyFinalizedName(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("late-child-processor-only")

	// Processor-only root: no StartNewTransaction / sampler attr. Final name
	// exists only on the export wrapper unless we retain a finalized-name entry.
	rootCtx, root := tracer.Start(context.Background(), "GET",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	root.SetName("GET /myroute")
	root.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 1)
	assertAttribute(t, exporter.GetSpans()[0].Attributes, sampler.TransactionIdentifier, "GET /myroute")
	exporter.Reset()

	_, child := tracer.Start(rootCtx, "late-child",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	child.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "GET /myroute")
	assertNoAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot)
}

func TestTagTransaction_LateChildFromFinalizedNonRootInheritsName(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("late-child-non-root")

	rootCtx, root := tracer.Start(context.Background(), "GET",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	root.SetName("GET /orders")
	midCtx, mid := tracer.Start(rootCtx, "handler",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	mid.End()
	root.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 2)
	assertAttribute(t, findSpan(t, exporter.GetSpans(), "handler").Attributes, sampler.TransactionIdentifier, "GET /orders")
	assertNoAttribute(t, findSpan(t, exporter.GetSpans(), "handler").Attributes, sampler.TransactionIdentifierRoot)
	exporter.Reset()

	// Late child started from the ended NON-ROOT parent's context.
	_, late := tracer.Start(midCtx, "late-child",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	late.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "GET /orders")
	assertNoAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot)
}

func TestTagTransaction_LateGrandchildKeepsInheritedName(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("late-grandchild-inherit")

	rootCtx, root := tracer.Start(context.Background(), "GET",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	root.SetName("GET /orders")
	root.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 1)
	exporter.Reset()

	// Late child inherits from finalizedNames; its child starts while the late
	// child is still tracked and must keep the same inherited name.
	lateCtx, late := tracer.Start(rootCtx, "late-child",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	_, grandchild := tracer.Start(lateCtx, "late-grandchild",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	grandchild.End()
	late.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	assertAttribute(t, findSpan(t, spans, "late-child").Attributes, sampler.TransactionIdentifier, "GET /orders")
	assertAttribute(t, findSpan(t, spans, "late-grandchild").Attributes, sampler.TransactionIdentifier, "GET /orders")
	assertNoAttribute(t, findSpan(t, spans, "late-child").Attributes, sampler.TransactionIdentifierRoot)
	assertNoAttribute(t, findSpan(t, spans, "late-grandchild").Attributes, sampler.TransactionIdentifierRoot)
}

func TestTagTransaction_LateGrandchildWaitsForLiveParent(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("late-grandchild-wait")

	outerCtx, outer := tracer.Start(context.Background(), "outer",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	rootCtx, root := tracer.Start(outerCtx, "GET",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	root.SetName("GET /orders")
	root.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 1)
	exporter.Reset()

	lateCtx, late := tracer.Start(rootCtx, "late-child",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	_, grandchild := tracer.Start(lateCtx, "late-grandchild",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	grandchild.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	assert.Empty(t, exporter.GetSpans(), "grandchild must wait for still-live late parent")

	late.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	assertAttribute(t, findSpan(t, spans, "late-child").Attributes, sampler.TransactionIdentifier, "GET /orders")
	assertAttribute(t, findSpan(t, spans, "late-grandchild").Attributes, sampler.TransactionIdentifier, "GET /orders")

	outer.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
}

func TestTagTransaction_SamplerRootClearedOnInherit(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithSampler(sampler.NewCoralogixSampler(sdktrace.AlwaysSample())),
	)
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("sampler-root-clear")

	rootCtx, root := tracer.Start(context.Background(), "parent-txn",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	// Child name equals inherited transaction name — sampler would mark root=true.
	_, child := tracer.Start(rootCtx, "parent-txn",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	child.End()
	root.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 2)
	var rootCount int
	for _, s := range spans {
		for _, a := range s.Attributes {
			if string(a.Key) == sampler.TransactionIdentifierRoot && a.Value.AsBool() {
				rootCount++
			}
		}
	}
	assert.Equal(t, 1, rootCount, "inherited child must not remain a transaction root")
}

func TestTagTransaction_SameNameFinalizedRootsPartitionSeparately(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
		WithMaxNodes(1),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("same-name-partition")

	outerCtx, outer := tracer.Start(context.Background(), "gateway",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	aCtx, a := tracer.Start(outerCtx, "POST /webhook",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	bCtx, b := tracer.Start(outerCtx, "POST /webhook",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	a.End()
	b.End()
	outer.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	require.Len(t, exporter.GetSpans(), 3)
	exporter.Reset()

	_, lateA := tracer.Start(aCtx, "late-a",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	_, lateB := tracer.Start(bCtx, "late-b",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	lateA.End()
	lateB.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 2, "same display name must not merge distinct finalized roots")
	assertAttribute(t, findSpan(t, spans, "late-a").Attributes, sampler.TransactionIdentifier, "POST /webhook")
	assertAttribute(t, findSpan(t, spans, "late-b").Attributes, sampler.TransactionIdentifier, "POST /webhook")
}

func TestTagTransaction_LateChildrenFromSameTxnShareIdentity(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
		WithMaxNodes(1),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("same-txn-identity")

	rootCtx, root := tracer.Start(context.Background(), "GET",
		tracecore.WithSpanKind(tracecore.SpanKindServer),
	)
	root.SetName("GET /orders")
	midCtx, mid := tracer.Start(rootCtx, "handler",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	mid.End()
	root.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	// maxNodes=1 keeps only the root from the first batch; membership names are
	// still retained for both spans before trim.
	require.NotEmpty(t, exporter.GetSpans())
	exporter.Reset()

	// Late children from different spans of the SAME finalized transaction must
	// share one identity (and thus one maxNodes=1 budget), not two.
	_, fromRoot := tracer.Start(rootCtx, "late-from-root",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	_, fromMid := tracer.Start(midCtx, "late-from-mid",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	fromRoot.End()
	fromMid.End()
	require.NoError(t, processor.ForceFlush(context.Background()))

	spans := exporter.GetSpans()
	require.Len(t, spans, 1, "same transaction identity must share one maxNodes budget")
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "GET /orders")
}

func TestTagTransaction_FinalizedNamesCapEvictsOldest(t *testing.T) {
	exporter := sdktracetest.NewInMemoryExporter()
	processor := NewTransactionSpanProcessor(exporter,
		WithMaxRegularTraces(0),
		WithCompletionHoldback(0),
		WithMaxFinalizedNames(2),
	)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	defer func() { require.NoError(t, tp.Shutdown(context.Background())) }()
	tracer := tp.Tracer("finalized-cap")

	// Processor-only SERVER roots so inheritance depends on the finalized cache
	// (live spans never received cgx.transaction).
	var rootCtxs []context.Context
	for i := 0; i < 3; i++ {
		ctx, root := tracer.Start(context.Background(), "GET",
			tracecore.WithSpanKind(tracecore.SpanKindServer),
		)
		root.SetName("GET /route-" + strconv.Itoa(i))
		root.End()
		require.NoError(t, processor.ForceFlush(context.Background()))
		require.Len(t, exporter.GetSpans(), 1)
		exporter.Reset()
		rootCtxs = append(rootCtxs, ctx)
	}

	_, fromOldest := tracer.Start(rootCtxs[0], "late-oldest",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	fromOldest.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	oldestSpans := exporter.GetSpans()
	require.Len(t, oldestSpans, 1)
	assert.NotEqual(t, "GET /route-0", attrString(oldestSpans[0].Attributes, sampler.TransactionIdentifier))
	exporter.Reset()

	_, fromRecent := tracer.Start(rootCtxs[2], "late-recent",
		tracecore.WithSpanKind(tracecore.SpanKindInternal),
	)
	fromRecent.End()
	require.NoError(t, processor.ForceFlush(context.Background()))
	recentSpans := exporter.GetSpans()
	require.Len(t, recentSpans, 1)
	assertAttribute(t, recentSpans[0].Attributes, sampler.TransactionIdentifier, "GET /route-2")
	assertNoAttribute(t, recentSpans[0].Attributes, sampler.TransactionIdentifierRoot)
}

func attrString(attrs []attribute.KeyValue, key string) string {
	for _, a := range attrs {
		if string(a.Key) == key {
			return a.Value.AsString()
		}
	}
	return ""
}

func TestTagTransaction_InheritsFromParentTraceStateWhenParentHasNoAttributes(t *testing.T) {
	exporter, tracer, shutdown := newTracerProvider(t)
	defer shutdown()

	traceState := tracecore.TraceState{}
	traceState, err := traceState.Insert(sampler.TransactionIdentifierTraceState, "from-tracestate")
	require.NoError(t, err)

	parentCtx := tracecore.NewSpanContext(tracecore.SpanContextConfig{
		TraceID:    tracecore.TraceID{0x01},
		SpanID:     tracecore.SpanID{0x01},
		TraceFlags: tracecore.FlagsSampled,
		TraceState: traceState,
		Remote:     false,
	})
	ctx := tracecore.ContextWithSpanContext(context.Background(), parentCtx)

	_, child := tracer.Start(ctx, "internal-child", tracecore.WithSpanKind(tracecore.SpanKindInternal))
	child.End()

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assertAttribute(t, spans[0].Attributes, sampler.TransactionIdentifier, "from-tracestate")
	assertNoAttribute(t, spans[0].Attributes, sampler.TransactionIdentifierRoot)
}

func findSpan(t *testing.T, spans []sdktracetest.SpanStub, name string) *sdktracetest.SpanStub {
	t.Helper()
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	t.Fatalf("span %q not found", name)
	return nil
}
