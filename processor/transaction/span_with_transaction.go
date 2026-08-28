package transaction

import (
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

// spanWithTransaction overlays finalized cgx.transaction (+ optional root flag)
// on a completed ReadOnlySpan. Used at export finalize; OnStart only marks roots.
type spanWithTransaction struct {
	sdktrace.ReadOnlySpan
	txnName string
	isRoot  bool
}

func withTransaction(span sdktrace.ReadOnlySpan, txnName string, isRoot bool) sdktrace.ReadOnlySpan {
	return spanWithTransaction{
		ReadOnlySpan: span,
		txnName:      txnName,
		isRoot:       isRoot,
	}
}

// withoutTransactionAttributes removes processor transaction attributes from
// oversized batches. The root marker is set at OnStart, so it must be removed
// at export time as well.
func withoutTransactionAttributes(spans []sdktrace.ReadOnlySpan) []sdktrace.ReadOnlySpan {
	out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, span := range spans {
		out = append(out, spanWithoutTransactionAttributes{ReadOnlySpan: span})
	}
	return out
}

type spanWithoutTransactionAttributes struct{ sdktrace.ReadOnlySpan }

func (s spanWithoutTransactionAttributes) Attributes() []attribute.KeyValue {
	original := s.ReadOnlySpan.Attributes()
	out := make([]attribute.KeyValue, 0, len(original))
	for _, a := range original {
		if a.Key == attribute.Key(sampler.TransactionIdentifier) ||
			a.Key == attribute.Key(sampler.TransactionIdentifierRoot) ||
			a.Key == attribute.Key(sampler.TransactionIdentifierExplicit) {
			continue
		}
		out = append(out, a)
	}
	return out
}

func (s spanWithTransaction) Attributes() []attribute.KeyValue {
	original := s.ReadOnlySpan.Attributes()
	out := make([]attribute.KeyValue, 0, len(original)+2)
	txnKey := attribute.Key(sampler.TransactionIdentifier)
	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	explicitKey := attribute.Key(sampler.TransactionIdentifierExplicit)
	for _, a := range original {
		if a.Key == txnKey || a.Key == rootKey || a.Key == explicitKey {
			continue
		}
		out = append(out, a)
	}
	out = append(out, attribute.String(sampler.TransactionIdentifier, s.txnName))
	if s.isRoot {
		out = append(out, attribute.Bool(sampler.TransactionIdentifierRoot, true))
	}
	return out
}
