package sampler

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	traceCore "go.opentelemetry.io/otel/trace"
)

const (
	TransactionIdentifier               = "cgx.transaction"
	TransactionIdentifierTraceState     = "cgx_transaction"
	DistributedTransactionIdentifier    = "cgx.transaction.distributed"
	DistributedTransactionIdentifierTSE = "cgx_transaction_distributed"
)

type CoralogixSampler struct {
	adaptedSampler traceSdk.Sampler
}

func NewCoralogixSampler(adaptedSampler traceSdk.Sampler) *CoralogixSampler {
	if adaptedSampler == nil {
		panic("sampler is null")
	}
	return &CoralogixSampler{
		adaptedSampler: adaptedSampler,
	}
}

func (s *CoralogixSampler) ShouldSample(parameters traceSdk.SamplingParameters) traceSdk.SamplingResult {
	adaptedSamplingResult := s.adaptedSampler.ShouldSample(parameters)

	return s.generateTransactionSamplingResult(parameters.ParentContext, parameters.Name, adaptedSamplingResult)
}

func (s *CoralogixSampler) generateTransactionSamplingResult(ctx context.Context, name string, adaptedSamplingResult traceSdk.SamplingResult) traceSdk.SamplingResult {
	newTracingState := s.generateNewTraceState(ctx, name, adaptedSamplingResult)
	newAttributes := s.injectAttributes(adaptedSamplingResult, newTracingState)
	return traceSdk.SamplingResult{
		Decision:   adaptedSamplingResult.Decision,
		Attributes: newAttributes,
		Tracestate: newTracingState,
	}
}

func (s *CoralogixSampler) injectAttributes(adaptedSamplingResult traceSdk.SamplingResult, newTracingState traceCore.TraceState) []attribute.KeyValue {
	sampledAttributes := adaptedSamplingResult.Attributes

	transactionIdentifier := attribute.String(TransactionIdentifier, newTracingState.Get(TransactionIdentifierTraceState))
	distributedTransactionIdentifier := attribute.String(DistributedTransactionIdentifier, newTracingState.Get(DistributedTransactionIdentifierTSE))

	return append(sampledAttributes, transactionIdentifier, distributedTransactionIdentifier)
}

func (s *CoralogixSampler) getDescription() string {
	return "coralogix-sampler"
}

func (s *CoralogixSampler) generateNewTraceState(ctx context.Context, name string, samplingResult traceSdk.SamplingResult) traceCore.TraceState {
	parentSpanContext := s.getParentSpanContext(ctx)
	parentTraceState := samplingResult.Tracestate

	if !parentSpanContext.IsRemote() && parentTraceState.Get(TransactionIdentifierTraceState) != "" {
		return parentTraceState
	}

	parentTraceState, err := parentTraceState.Insert(TransactionIdentifierTraceState, name)
	if err != nil {
		return parentTraceState
	}
	if parentTraceState.Get(DistributedTransactionIdentifierTSE) == "" {
		parentTraceState, err = parentTraceState.Insert(DistributedTransactionIdentifierTSE, name)
		if err != nil {
			return parentTraceState
		}
	}

	return parentTraceState
}

func (s *CoralogixSampler) getParentSpanContext(ctx context.Context) traceCore.SpanContext {
	span := traceCore.SpanFromContext(ctx)
	if span != nil {
		return span.SpanContext()
	}
	return traceCore.SpanContext{}
}