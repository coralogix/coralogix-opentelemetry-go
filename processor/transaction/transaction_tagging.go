package transaction

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

// beginTransaction runs on OnStart.
//
// It only decides new-vs-inherit and marks roots early enough for nested
// extraction. It does NOT freeze cgx.transaction from the early span name —
// Express-style UpdateName (or similar) may still change the root name before
// End. Final names are stamped in stampTransactionAttributes at finalize.
//
// Pre-set cgx.transaction (sampler template / StartNewTransaction) is left on
// the span and treated as an override at finalize.
func beginTransaction(ctx context.Context, s sdktrace.ReadWriteSpan, tracked map[tracecore.SpanID]struct{}) {
	parent := s.Parent()
	hasLocalTxn := hasLocalTransaction(ctx, parent, tracked)

	starts := !hasLocalTxn ||
		parent.IsRemote() ||
		s.SpanKind() == tracecore.SpanKindServer ||
		s.SpanKind() == tracecore.SpanKindConsumer

	if tracked != nil {
		tracked[s.SpanContext().SpanID()] = struct{}{}
	}

	if !starts {
		return
	}

	// Root flag only — do not write cgx.transaction from s.Name().
	s.SetAttributes(attribute.Bool(sampler.TransactionIdentifierRoot, true))
}

// hasLocalTransaction reports whether the parent is already part of a local
// transaction tree. Prefers the OnStart side table (attrs are not stamped on
// inherit children until finalize), then live parent attrs / tracestate.
func hasLocalTransaction(ctx context.Context, parent tracecore.SpanContext, tracked map[tracecore.SpanID]struct{}) bool {
	if parent.IsValid() && tracked != nil {
		if _, ok := tracked[parent.SpanID()]; ok {
			return true
		}
	}
	_, has := parentTransactionName(ctx, parent)
	return has
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
			for _, a := range rw.Attributes() {
				if a.Key == attribute.Key(sampler.TransactionIdentifierRoot) && a.Value.AsBool() {
					return "", true
				}
			}
		}
	}

	txnName = parent.TraceState().Get(sampler.TransactionIdentifierTraceState)
	return txnName, txnName != ""
}

// stampTransactionAttributes sets cgx.transaction on every span in a completed
// local batch from the root's final Name() (or a pre-set override attribute).
func stampTransactionAttributes(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	if len(spans) == 0 {
		return spans
	}

	root := findTransactionRootSpan(spans)
	if root == nil {
		root = spans[0]
	}
	name := resolveTransactionName(root)

	out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, s := range spans {
		out = append(out, withTransaction(s, name, isTransactionRoot(s)))
	}
	return out
}

func resolveTransactionName(root sdktrace.ReadOnlySpan) string {
	if v := transactionAttr(root); v != "" {
		return v
	}
	return root.Name()
}

func transactionAttr(span sdktrace.ReadOnlySpan) string {
	for _, a := range span.Attributes() {
		if a.Key == attribute.Key(sampler.TransactionIdentifier) {
			return a.Value.AsString()
		}
	}
	return ""
}

func isTransactionRoot(span sdktrace.ReadOnlySpan) bool {
	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	for _, a := range span.Attributes() {
		if a.Key == rootKey && a.Value.AsBool() {
			return true
		}
	}
	return false
}

func findTransactionRootSpan(spans []sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if isTransactionRoot(s) {
			return s
		}
	}
	return nil
}
