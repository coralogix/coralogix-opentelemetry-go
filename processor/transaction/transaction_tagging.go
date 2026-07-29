package transaction

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

func tagTransaction(ctx context.Context, s sdktrace.ReadWriteSpan) {
	parent := s.Parent()
	txnName, hasLocalTxn := parentTransactionName(ctx, parent)

	starts := !hasLocalTxn ||
		parent.IsRemote() ||
		s.SpanKind() == tracecore.SpanKindServer ||
		s.SpanKind() == tracecore.SpanKindConsumer

	if !starts {
		s.SetAttributes(attribute.String(sampler.TransactionIdentifier, txnName))
		return
	}

	// Prefer a pre-set cgx.transaction name (e.g. sampler template) over the raw span name.
	name := s.Name()
	for _, a := range s.Attributes() {
		if a.Key == attribute.Key(sampler.TransactionIdentifier) {
			if v := a.Value.AsString(); v != "" {
				name = v
			}
			break
		}
	}
	s.SetAttributes(
		attribute.String(sampler.TransactionIdentifier, name),
		attribute.Bool(sampler.TransactionIdentifierRoot, true),
	)
}

func parentTransactionName(ctx context.Context, parent tracecore.SpanContext) (txnName string, hasLocalTxn bool) {
	if parentSpan := tracecore.SpanFromContext(ctx); parentSpan != nil {
		if rw, ok := parentSpan.(sdktrace.ReadWriteSpan); ok {
			for _, a := range rw.Attributes() {
				if a.Key == attribute.Key(sampler.TransactionIdentifier) {
					txnName = a.Value.AsString()
					break
				}
			}
			if txnName != "" {
				return txnName, true
			}
		}
	}

	txnName = parent.TraceState().Get(sampler.TransactionIdentifierTraceState)
	return txnName, txnName != ""
}
