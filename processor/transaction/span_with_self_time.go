package transaction

import (
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type spanWithSelfTime struct {
	sdktrace.ReadOnlySpan
	selfTimeNs int64
}

func withSelfTime(span sdktrace.ReadOnlySpan, selfTimeNs int64) sdktrace.ReadOnlySpan {
	return spanWithSelfTime{
		ReadOnlySpan: span,
		selfTimeNs:   selfTimeNs,
	}
}

func (s spanWithSelfTime) Attributes() []attribute.KeyValue {
	original := s.ReadOnlySpan.Attributes()
	out := make([]attribute.KeyValue, 0, len(original)+1)
	out = append(out, original...)
	out = append(out, attribute.Float64(SelfTimeAttribute, float64(s.selfTimeNs)/1e9))
	return out
}
