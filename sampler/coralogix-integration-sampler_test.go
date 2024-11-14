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
