package sampler

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"testing"
)

import (
	"context"
	traceCore "go.opentelemetry.io/otel/trace"
)

const (
	spanName   = "spanName"
	serviceNme = "serviceName"
)

func TestCoralogixSampler_ShouldSample(t *testing.T) {
	t.Run("When_alwaysSampler_Should_AppendAttributesAndState", func(t *testing.T) {
		resource := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceNme))

		alwaysSampler := traceSdk.AlwaysSample()
		coralogixSampler := NewCoralogixSampler(alwaysSampler, resource)

		// Act
		parameters := traceSdk.SamplingParameters{
			ParentContext: context.Background(),
			Name:          spanName,
			Attributes:    []attribute.KeyValue{},
		}
		result := coralogixSampler.ShouldSample(parameters)

		expectedAttributes := []attribute.KeyValue{
			attribute.String(TransactionIdentifier, spanName),
			attribute.String(DistributedTransactionIdentifier, spanName),
			attribute.Bool(TransactionIdentifierRoot, true),
		}

		expectedTraceState := traceCore.TraceState{}

		expectedTraceState, _ = expectedTraceState.Insert(TransactionIdentifierTraceState, spanName)
		expectedTraceState, _ = expectedTraceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)
		expectedTraceState, _ = expectedTraceState.Insert(DistributedTransactionIdentifierTraceState, spanName)

		assert.Equal(t, traceSdk.RecordAndSample, result.Decision)
		assert.ElementsMatch(t, expectedAttributes, result.Attributes)
		assert.Equal(t, expectedTraceState, result.Tracestate)
	})

	t.Run("When_NeverSample_Should_AppendAttributesAndState", func(t *testing.T) {
		neverSampler := traceSdk.NeverSample()
		resource := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceNme))
		coralogixSampler := NewCoralogixSampler(neverSampler, resource)

		// Act
		parameters := traceSdk.SamplingParameters{
			ParentContext: context.Background(),
			Name:          spanName,
			Attributes:    []attribute.KeyValue{},
		}
		result := coralogixSampler.ShouldSample(parameters)

		expectedAttributes := []attribute.KeyValue{
			attribute.String(TransactionIdentifier, spanName),
			attribute.String(DistributedTransactionIdentifier, spanName),
			attribute.Bool(TransactionIdentifierRoot, true),
		}

		expectedTraceState := traceCore.TraceState{}

		expectedTraceState, _ = expectedTraceState.Insert(TransactionIdentifierTraceState, spanName)
		expectedTraceState, _ = expectedTraceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)
		expectedTraceState, _ = expectedTraceState.Insert(DistributedTransactionIdentifierTraceState, spanName)

		assert.Equal(t, traceSdk.Drop, result.Decision)
		assert.ElementsMatch(t, expectedAttributes, result.Attributes)
		assert.Equal(t, expectedTraceState, result.Tracestate)
	})

	t.Run("When_CustomSamplerIsNull_ShouldFailInit", func(t *testing.T) {
		assert.Panics(t, func() {
			resource := resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(serviceNme))
			_ = NewCoralogixSampler(nil, resource)
		})
	})

	t.Run("When_ParentContextExistsAndNotRemote_ShouldCopyParentTraceState", func(t *testing.T) {
		alwaysSampler := traceSdk.AlwaysSample()
		ras := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceNme))
		coralogixSampler := NewCoralogixSampler(alwaysSampler, ras)

		traceState := traceCore.TraceState{}

		traceState, _ = traceState.Insert(TransactionIdentifierTraceState, "fatherSpanName")
		traceState, _ = traceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)
		traceState, _ = traceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")

		parentSpan := traceCore.NewSpanContext(traceCore.SpanContextConfig{
			TraceID:    traceCore.TraceID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F},
			SpanID:     traceCore.SpanID{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8},
			TraceFlags: traceCore.FlagsSampled,
			TraceState: traceState,
			Remote:     false,
		})
		parentCtx := traceCore.ContextWithSpanContext(context.Background(), parentSpan)

		// Act
		parameters := traceSdk.SamplingParameters{
			ParentContext: parentCtx,
			Name:          spanName,
			Attributes:    []attribute.KeyValue{},
		}
		result := coralogixSampler.ShouldSample(parameters)

		expectedAttributes := []attribute.KeyValue{
			attribute.String(TransactionIdentifier, "fatherSpanName"),
			attribute.String(DistributedTransactionIdentifier, "fatherSpanName"),
		}
		expectedTraceState := traceCore.TraceState{}
		expectedTraceState, _ = expectedTraceState.Insert(TransactionIdentifierTraceState, "fatherSpanName")
		expectedTraceState, _ = expectedTraceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)
		expectedTraceState, _ = expectedTraceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")

		assert.ElementsMatch(t, expectedAttributes, result.Attributes)
		assert.Equal(t, expectedTraceState, result.Tracestate)
	})
	t.Run("When_ParentContextExistsAndRemote_ShouldCopyParentTraceState", func(t *testing.T) {
		alwaysSampler := traceSdk.AlwaysSample()
		ras := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceNme))
		coralogixSampler := NewCoralogixSampler(alwaysSampler, ras)

		traceState := traceCore.TraceState{}

		traceState, _ = traceState.Insert(TransactionIdentifierTraceState, "fatherSpanName")
		traceState, _ = traceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)
		traceState, _ = traceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")

		parentSpan := traceCore.NewSpanContext(traceCore.SpanContextConfig{
			TraceID:    traceCore.TraceID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F},
			SpanID:     traceCore.SpanID{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8},
			TraceFlags: traceCore.FlagsSampled,
			TraceState: traceState,
			Remote:     true,
		})
		parentCtx := traceCore.ContextWithSpanContext(context.Background(), parentSpan)

		// Act
		parameters := traceSdk.SamplingParameters{
			ParentContext: parentCtx,
			Name:          spanName,
			Attributes:    []attribute.KeyValue{},
		}
		result := coralogixSampler.ShouldSample(parameters)

		expectedAttributes := []attribute.KeyValue{
			attribute.String(TransactionIdentifier, "spanName"),
			attribute.String(DistributedTransactionIdentifier, "fatherSpanName"),
			attribute.Bool(TransactionIdentifierRoot, true),
		}
		expectedTraceState := traceCore.TraceState{}
		expectedTraceState, _ = expectedTraceState.Insert(TransactionIdentifierTraceState, spanName)
		expectedTraceState, _ = expectedTraceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")
		expectedTraceState, _ = expectedTraceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)

		assert.ElementsMatch(t, expectedAttributes, result.Attributes)
		assert.Equal(t, expectedTraceState.Get(TransactionIdentifierTraceState), result.Tracestate.Get(TransactionIdentifierTraceState))
		assert.Equal(t, expectedTraceState.Get(DistributedTransactionIdentifierTraceState), result.Tracestate.Get(DistributedTransactionIdentifierTraceState))
	})

	t.Run("When_ParentContextExistsAndRemote_ShouldCopyParentTraceState", func(t *testing.T) {
		alwaysSampler := traceSdk.AlwaysSample()
		ras := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceNme))
		coralogixSampler := NewCoralogixSampler(alwaysSampler, ras)

		traceState := traceCore.TraceState{}

		traceState, _ = traceState.Insert(TransactionIdentifierTraceState, "fatherSpanName")
		traceState, _ = traceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")

		parentSpan := traceCore.NewSpanContext(traceCore.SpanContextConfig{
			TraceID:    traceCore.TraceID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F},
			SpanID:     traceCore.SpanID{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8},
			TraceFlags: traceCore.FlagsSampled,
			TraceState: traceState,
			Remote:     true,
		})
		parentCtx := traceCore.ContextWithSpanContext(context.Background(), parentSpan)

		// Act
		parameters := traceSdk.SamplingParameters{
			ParentContext: parentCtx,
			Name:          spanName,
			Attributes:    []attribute.KeyValue{},
		}
		result := coralogixSampler.ShouldSample(parameters)

		expectedAttributes := []attribute.KeyValue{
			attribute.String(TransactionIdentifier, "spanName"),
			attribute.String(DistributedTransactionIdentifier, "fatherSpanName"),
			attribute.Bool(TransactionIdentifierRoot, true),
		}
		expectedTraceState := traceCore.TraceState{}
		expectedTraceState, _ = expectedTraceState.Insert(TransactionIdentifierTraceState, spanName)
		expectedTraceState, _ = expectedTraceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")
		expectedTraceState, _ = expectedTraceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)

		assert.ElementsMatch(t, expectedAttributes, result.Attributes)
		assert.Equal(t, expectedTraceState.Get(TransactionIdentifierTraceState), result.Tracestate.Get(TransactionIdentifierTraceState))
		assert.Equal(t, expectedTraceState.Get(DistributedTransactionIdentifierTraceState), result.Tracestate.Get(DistributedTransactionIdentifierTraceState))
	})

	t.Run("When_ParentContextExistsAndNotRemoteDiffrentSerrice_ShouldNotCopyParentTraceState", func(t *testing.T) {
		alwaysSampler := traceSdk.AlwaysSample()
		ras := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceNme))
		coralogixSampler := NewCoralogixSampler(alwaysSampler, ras)

		traceState := traceCore.TraceState{}

		traceState, _ = traceState.Insert(TransactionIdentifierTraceState, "fatherSpanName")
		traceState, _ = traceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")
		traceState, _ = traceState.Insert(TransactionServiceIdentifierTraceState, "parentServiceName")

		parentSpan := traceCore.NewSpanContext(traceCore.SpanContextConfig{
			TraceID:    traceCore.TraceID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F},
			SpanID:     traceCore.SpanID{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8},
			TraceFlags: traceCore.FlagsSampled,
			TraceState: traceState,
			Remote:     true,
		})
		parentCtx := traceCore.ContextWithSpanContext(context.Background(), parentSpan)

		// Act
		parameters := traceSdk.SamplingParameters{
			ParentContext: parentCtx,
			Name:          spanName,
			Attributes:    []attribute.KeyValue{},
		}
		result := coralogixSampler.ShouldSample(parameters)

		expectedAttributes := []attribute.KeyValue{
			attribute.String(TransactionIdentifier, "spanName"),
			attribute.String(DistributedTransactionIdentifier, "fatherSpanName"),
			attribute.Bool(TransactionIdentifierRoot, true),
		}
		expectedTraceState := traceCore.TraceState{}
		expectedTraceState, _ = expectedTraceState.Insert(TransactionIdentifierTraceState, spanName)
		expectedTraceState, _ = expectedTraceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")
		expectedTraceState, _ = expectedTraceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)

		assert.ElementsMatch(t, expectedAttributes, result.Attributes)
		assert.Equal(t, expectedTraceState.Get(TransactionIdentifierTraceState), result.Tracestate.Get(TransactionIdentifierTraceState))
		assert.Equal(t, expectedTraceState.Get(DistributedTransactionIdentifierTraceState), result.Tracestate.Get(DistributedTransactionIdentifierTraceState))
	})

	t.Run("When_ParentContextExistsAndNotRemoteSameService_ShouldNotCopyParentTraceState", func(t *testing.T) {
		alwaysSampler := traceSdk.AlwaysSample()
		ras := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceNme))
		coralogixSampler := NewCoralogixSampler(alwaysSampler, ras)

		traceState := traceCore.TraceState{}

		traceState, _ = traceState.Insert(TransactionIdentifierTraceState, "fatherSpanName")
		traceState, _ = traceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")
		traceState, _ = traceState.Insert(TransactionServiceIdentifierTraceState, "parentServiceName")

		parentSpan := traceCore.NewSpanContext(traceCore.SpanContextConfig{
			TraceID:    traceCore.TraceID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F},
			SpanID:     traceCore.SpanID{0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8},
			TraceFlags: traceCore.FlagsSampled,
			TraceState: traceState,
			Remote:     false,
		})
		parentCtx := traceCore.ContextWithSpanContext(context.Background(), parentSpan)

		// Act
		parameters := traceSdk.SamplingParameters{
			ParentContext: parentCtx,
			Name:          spanName,
			Attributes:    []attribute.KeyValue{},
		}
		result := coralogixSampler.ShouldSample(parameters)

		expectedAttributes := []attribute.KeyValue{
			attribute.String(TransactionIdentifier, "spanName"),
			attribute.String(DistributedTransactionIdentifier, "fatherSpanName"),
			attribute.Bool(TransactionIdentifierRoot, true),
		}
		expectedTraceState := traceCore.TraceState{}
		expectedTraceState, _ = expectedTraceState.Insert(TransactionIdentifierTraceState, spanName)
		expectedTraceState, _ = expectedTraceState.Insert(DistributedTransactionIdentifierTraceState, "fatherSpanName")
		expectedTraceState, _ = expectedTraceState.Insert(TransactionServiceIdentifierTraceState, serviceNme)

		assert.ElementsMatch(t, expectedAttributes, result.Attributes)
		assert.Equal(t, expectedTraceState.Get(TransactionIdentifierTraceState), result.Tracestate.Get(TransactionIdentifierTraceState))
		assert.Equal(t, expectedTraceState.Get(DistributedTransactionIdentifierTraceState), result.Tracestate.Get(DistributedTransactionIdentifierTraceState))
	})

}
