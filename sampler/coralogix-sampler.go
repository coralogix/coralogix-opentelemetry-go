package sampler

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	traceCore "go.opentelemetry.io/otel/trace"
)

const (
	TransactionIdentifier                      = "cgx.transaction"
	TransactionIdentifierRoot                  = "cgx.transaction.root"
	TransactionIdentifierTraceState            = "cgx_transaction"
	DistributedTransactionIdentifier           = "cgx.transaction.distributed"
	DistributedTransactionIdentifierTraceState = "cgx_transaction_distributed"
	TransactionServiceIdentifier               = "cgx.transaction"
	TransactionServiceIdentifierTraceState     = "cgx_transaction_service"
)

type CoralogixSampler struct {
	adaptedSampler traceSdk.Sampler
	service        string
}

func NewCoralogixSampler(adaptedSampler traceSdk.Sampler, resa *resource.Resource) CoralogixSampler {

	if adaptedSampler == nil {
		panic("sampler is null")
	}
	if resa == nil {
		panic("resource is null")
	}
	attributes := resa.Attributes()
	service := ""
	if attributes != nil {
		for _, attribute := range attributes {
			if attribute.Key == semconv.ServiceNameKey {
				service = attribute.Value.AsString()
			}
		}
	}
	if service == "" {
		panic("service name is empty")
	}
	return CoralogixSampler{
		adaptedSampler: adaptedSampler,
		service:        service,
	}

}
func (s CoralogixSampler) Description() string {
	return "coralogix-sampler"
}

func (s CoralogixSampler) ShouldSample(parameters traceSdk.SamplingParameters) traceSdk.SamplingResult {
	adaptedSamplingResult := s.adaptedSampler.ShouldSample(parameters)

	return s.generateTransactionSamplingResult(parameters.ParentContext, parameters.Name, adaptedSamplingResult, parameters.Kind)
}

func (s CoralogixSampler) generateTransactionSamplingResult(ctx context.Context, name string, adaptedSamplingResult traceSdk.SamplingResult, kind traceCore.SpanKind) traceSdk.SamplingResult {
	newTracingState := s.generateNewTraceState(ctx, name, adaptedSamplingResult, kind)
	newAttributes := s.injectAttributes(adaptedSamplingResult, newTracingState, name)
	return traceSdk.SamplingResult{
		Decision:   adaptedSamplingResult.Decision,
		Attributes: newAttributes,
		Tracestate: newTracingState,
	}
}

func (s CoralogixSampler) injectAttributes(adaptedSamplingResult traceSdk.SamplingResult, newTracingState traceCore.TraceState, name string) []attribute.KeyValue {
	sampledAttributes := adaptedSamplingResult.Attributes

	transactionName := newTracingState.Get(TransactionIdentifierTraceState)

	transactionIdentifier := attribute.String(TransactionIdentifier, transactionName)
	distributedTransactionIdentifier := attribute.String(DistributedTransactionIdentifier, newTracingState.Get(DistributedTransactionIdentifierTraceState))
	if transactionName == name {
		rootTransactionAttribute := attribute.Bool(TransactionIdentifierRoot, true)
		return append(sampledAttributes, transactionIdentifier, distributedTransactionIdentifier, rootTransactionAttribute)
	}
	return append(sampledAttributes, transactionIdentifier, distributedTransactionIdentifier)
}

func (s *CoralogixSampler) getDescription() string {
	return "coralogix-sampler"
}

func (s *CoralogixSampler) generateNewTraceState(ctx context.Context, name string, samplingResult traceSdk.SamplingResult, kind traceCore.SpanKind) traceCore.TraceState {
	parentSpanContext := s.getParentSpanContext(ctx)
	parentTraceState := samplingResult.Tracestate

	if !(kind == traceCore.SpanKindConsumer) && !(kind == traceCore.SpanKindServer) && !parentSpanContext.IsRemote() && parentTraceState.Get(TransactionIdentifierTraceState) != "" && parentTraceState.Get(TransactionServiceIdentifierTraceState) == s.service {
		return parentTraceState
	}

	parentTraceState, err := parentTraceState.Insert(TransactionIdentifierTraceState, name)
	if err != nil {
		return parentTraceState
	}
	parentTraceState, err = parentTraceState.Insert(TransactionServiceIdentifierTraceState, s.service)
	if err != nil {
		return parentTraceState
	}
	if parentTraceState.Get(DistributedTransactionIdentifierTraceState) == "" {
		parentTraceState, err = parentTraceState.Insert(DistributedTransactionIdentifierTraceState, name)
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
