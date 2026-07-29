package transaction

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"
)

type spanWithParent struct {
	sdktrace.ReadOnlySpan
	parent tracecore.SpanContext
}

func withParent(span sdktrace.ReadOnlySpan, parent tracecore.SpanContext) sdktrace.ReadOnlySpan {
	return spanWithParent{
		ReadOnlySpan: span,
		parent:       parent,
	}
}

func (s spanWithParent) Parent() tracecore.SpanContext {
	return s.parent
}
