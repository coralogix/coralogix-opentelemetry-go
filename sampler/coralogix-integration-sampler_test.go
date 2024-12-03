package sampler

import (
	"github.com/stretchr/testify/assert"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	traceCore "go.opentelemetry.io/otel/trace"
	"testing"
)

import (
	"context"
)

const (
	spanName = "spanName"
)

func TestCoralogixSampler_ShouldSample_flow(t *testing.T) {
	t.Run("When_alwaysSampler_Should_AppendAttributesAndState", func(t *testing.T) {
		alwaysSampler := traceSdk.AlwaysSample()
		coralogixSampler := NewCoralogixSampler(alwaysSampler)

		// Act

		tracer := traceSdk.NewTracerProvider(traceSdk.WithSampler(coralogixSampler)).Tracer("test")
		_, span := tracer.Start(context.Background(), "parent")
		testAttribute(t, span, "parent")
		span.End()

	})

	t.Run("When_alwaysSampler_Should_AppendAttributesAndState", func(t *testing.T) {
		alwaysSampler := traceSdk.AlwaysSample()
		coralogixSampler := NewCoralogixSampler(alwaysSampler)

		// Act

		tracer := traceSdk.NewTracerProvider(traceSdk.WithSampler(coralogixSampler)).Tracer("test")
		ctx, span := tracer.Start(context.Background(), "parent")
		ctxFlow1, spanFlow1 := tracer.Start(ctx, "flow1")
		StartNewTransaction(spanFlow1, "flow1")
		_, spanSubFlow1 := tracer.Start(ctxFlow1, "subFlow1")

		testAttribute(t, spanSubFlow1, "flow1")
		ctwFlo2, spanFlow2 := tracer.Start(ctx, "flow2")
		StartNewTransaction(spanFlow2, "flow2")
		_, spanSubFlow2 := tracer.Start(ctwFlo2, "subFlow2")
		testAttribute(t, spanSubFlow2, "flow2")

		attributes := span.(traceSdk.ReadWriteSpan).Attributes()
		found := false
		for _, attribute := range attributes {
			if attribute.Key == "cgx.transaction" {
				assert.Equal(t, attribute.Value.AsString(), "parent")
				found = true
			}
		}
		assert.True(t, found)
		span.End()

	})

	t.Run("When_fatehSampler_Should_AppendAttributesAndState", func(t *testing.T) {
		alwaysSampler := traceSdk.NeverSample()
		coralogixSampler := NewCoralogixSampler(alwaysSampler)

		// Act
		traceState := traceCore.TraceState{}

		parentSpan := traceCore.NewSpanContext(traceCore.SpanContextConfig{
			TraceID:    traceCore.TraceID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F},
			SpanID:     traceCore.SpanID{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8},
			TraceFlags: traceCore.FlagsSampled.WithSampled(false),
			TraceState: traceState,
			Remote:     false,
		})
		parentCtx := traceCore.ContextWithSpanContext(context.Background(), parentSpan)

		tracer := traceSdk.NewTracerProvider(traceSdk.WithSampler(coralogixSampler)).Tracer("test")
		ctx, span := tracer.Start(parentCtx, "parent")

		ctxFlow1, spanFlow1 := tracer.Start(ctx, "flow1")
		StartNewTransaction(spanFlow1, "flow1")
		_, spanSubFlow1 := tracer.Start(ctxFlow1, "subFlow1")

		testAttribute(t, spanSubFlow1, "flow1")
		ctwFlo2, spanFlow2 := tracer.Start(ctx, "flow2")
		StartNewTransaction(spanFlow2, "flow2")
		_, spanSubFlow2 := tracer.Start(ctwFlo2, "subFlow2")
		testAttribute(t, spanSubFlow2, "flow2")

		attributes := span.(traceSdk.ReadWriteSpan).Attributes()
		found := false
		for _, attribute := range attributes {
			if attribute.Key == "cgx.transaction" {
				assert.Equal(t, attribute.Value.AsString(), "parent")
				found = true
			}
		}
		assert.True(t, found)
		span.End()

	})

}

func testAttribute(t *testing.T, span traceCore.Span, attributeVal string) {
	attributes := span.(traceSdk.ReadWriteSpan).Attributes()
	found := false
	for _, attribute := range attributes {
		if attribute.Key == "cgx.transaction" {
			assert.Equal(t, attributeVal, attribute.Value.AsString())
			found = true
		}
	}
	assert.True(t, found)
}
