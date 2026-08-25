package transaction

import (
	"context"
	"testing"

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
