package sampler

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	traceCore "go.opentelemetry.io/otel/trace"
)

const (
	TransactionIdentifier                      = "cgx.transaction"
	TransactionIdentifierRoot                  = "cgx.transaction.root"
	TransactionIdentifierTraceState            = "cgx_transaction"
	DistributedTransactionIdentifier           = "cgx.transaction.distributed"
	DistributedTransactionIdentifierTraceState = "cgx_transaction_distributed"
)

type CoralogixSampler struct {
	adaptedSampler traceSdk.Sampler
}

func NewCoralogixSampler(adaptedSampler traceSdk.Sampler) CoralogixSampler {
	if adaptedSampler == nil {
		panic("sampler is null")
	}
	return CoralogixSampler{
		adaptedSampler: adaptedSampler,
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

	version := attribute.String("cgx.version", "1.4.4")
	transactionIdentifier := attribute.String(TransactionIdentifier, transactionName)
	distributedTransactionIdentifier := attribute.String(DistributedTransactionIdentifier, newTracingState.Get(DistributedTransactionIdentifierTraceState))
	if transactionName == name {
		rootTransactionAttribute := attribute.Bool(TransactionIdentifierRoot, true)
		return append(sampledAttributes, transactionIdentifier, distributedTransactionIdentifier, rootTransactionAttribute, version)
	}
	return append(sampledAttributes, transactionIdentifier, distributedTransactionIdentifier, version)
}

func (s *CoralogixSampler) getDescription() string {
	return "coralogix-sampler"
}

func (s *CoralogixSampler) generateNewTraceState(ctx context.Context, name string, samplingResult traceSdk.SamplingResult, kind traceCore.SpanKind) traceCore.TraceState {
	parentSpanContext := s.getParentSpanContext(ctx)
	parentTraceState := samplingResult.Tracestate

	if !parentSpanContext.IsRemote() && parentTraceState.Get(TransactionIdentifierTraceState) != "" && kind != traceCore.SpanKindServer && !(kind == traceCore.SpanKindConsumer) {
		span := traceCore.SpanFromContext(ctx)
		if span != nil {
			readWriteSpan, ok := span.(traceSdk.ReadWriteSpan)
			if ok {
				attributes := readWriteSpan.Attributes()
				if attributes != nil {
					for _, attribute := range attributes {
						if attribute.Key == TransactionIdentifier {
							parentTraceState, err := parentTraceState.Insert(TransactionIdentifierTraceState, attribute.Value.AsString())
							if err == nil {
								return parentTraceState
							}
						}
					}
				}

			}
		}

		/**/
		return parentTraceState
	}

	parentTraceState, err := parentTraceState.Insert(TransactionIdentifierTraceState, name)
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
func StartNewTransaction(span traceCore.Span, flow string) traceCore.Span {
	span.SetAttributes(attribute.String(TransactionIdentifier, flow))
	span.SetAttributes(attribute.Bool(TransactionIdentifierRoot, true))
	return span
}
