package transaction

import (
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type spanWithSelfDuration struct {
	sdktrace.ReadOnlySpan
	selfDurationNs int64
}

func withSelfDuration(span sdktrace.ReadOnlySpan, selfDurationNs int64) sdktrace.ReadOnlySpan {
	return spanWithSelfDuration{
		ReadOnlySpan:   span,
		selfDurationNs: selfDurationNs,
	}
}

func (s spanWithSelfDuration) Attributes() []attribute.KeyValue {
	original := s.ReadOnlySpan.Attributes()
	out := make([]attribute.KeyValue, 0, len(original)+1)
	key := attribute.Key(SelfDurationAttribute)
	for _, a := range original {
		if a.Key == key {
			continue
		}
		out = append(out, a)
	}
	out = append(out, attribute.Float64(SelfDurationAttribute, float64(s.selfDurationNs)/1e9))
	return out
}
