package sampler

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	traceSdk "go.opentelemetry.io/otel/sdk/trace"
	traceCore "go.opentelemetry.io/otel/trace"
)

const (
	TransactionIdentifier            = "cgx.transaction"
	TransactionIdentifierRoot        = "cgx.transaction.root"
	DistributedTransactionIdentifier = "cgx.transaction.distributed"
	VersionIdentifier                = "cgx.version"
	Version                          = "1.4.4"
)

type CoralogixSampler struct {
	adaptedSampler traceSdk.Sampler
}
type TransactionAttributes struct {
	Transaction string
	Distributed string
	Root        bool
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
	transaction := s.generateTransactionAttributes(ctx, name, adaptedSamplingResult, kind)

	if adaptedSamplingResult.Decision == traceSdk.Drop {
		adaptedSamplingResult.Decision = traceSdk.RecordOnly
	}
	newAttributes := s.injectAttributes(adaptedSamplingResult, transaction, name)
	return traceSdk.SamplingResult{
		Decision:   adaptedSamplingResult.Decision,
		Attributes: newAttributes,
		Tracestate: adaptedSamplingResult.Tracestate,
	}
}

func (s CoralogixSampler) injectAttributes(adaptedSamplingResult traceSdk.SamplingResult, transaction TransactionAttributes, name string) []attribute.KeyValue {
	sampledAttributes := adaptedSamplingResult.Attributes

	transactionName := transaction.Transaction

	version := attribute.String(VersionIdentifier, Version)
	transactionIdentifier := attribute.String(TransactionIdentifier, transactionName)
	distributedTransactionIdentifier := attribute.String(DistributedTransactionIdentifier, transaction.Distributed)
	if transactionName == name {
		rootTransactionAttribute := attribute.Bool(TransactionIdentifierRoot, true)
		return append(sampledAttributes, transactionIdentifier, distributedTransactionIdentifier, rootTransactionAttribute, version)
	}
	return append(sampledAttributes, transactionIdentifier, distributedTransactionIdentifier, version)
}

func (s CoralogixSampler) getDescription() string {
	return "coralogix-sampler"
}
func shouldStartNewTransaction(isRemote bool, transactionName string, kind traceCore.SpanKind) bool {
	return isRemote || transactionName == "" || kind == traceCore.SpanKindServer || kind == traceCore.SpanKindConsumer

}
func (s CoralogixSampler) generateTransactionAttributes(ctx context.Context, name string, samplingResult traceSdk.SamplingResult, kind traceCore.SpanKind) TransactionAttributes {
	span := traceCore.SpanFromContext(ctx)

	if span != nil {
		readWriteSpan, ok := span.(traceSdk.ReadOnlySpan)

		if ok {
			parentAttributes := readWriteSpan.Attributes()
			parentTransaction := s.getTransactionAttributes(parentAttributes)

			if !shouldStartNewTransaction(span.SpanContext().IsRemote(), parentTransaction.Transaction, kind) {
				return parentTransaction
			}

		}

		/**/
	}
	newTransaction := TransactionAttributes{Transaction: name, Distributed: name, Root: true}
	return newTransaction

}

func (s CoralogixSampler) getParentSpanContext(ctx context.Context) traceCore.SpanContext {
	span := traceCore.SpanFromContext(ctx)
	if span != nil {
		return span.SpanContext()
	}
	return traceCore.SpanContext{}
}

func (s CoralogixSampler) getTransactionAttributes(attributes []attribute.KeyValue) TransactionAttributes {
	transactionAttributes := TransactionAttributes{}
	for _, keyValue := range attributes {
		if keyValue.Key == TransactionIdentifier {
			transactionAttributes.Transaction = keyValue.Value.AsString()
		}
		if keyValue.Key == DistributedTransactionIdentifier {
			transactionAttributes.Distributed = keyValue.Value.AsString()
		}

	}
	return transactionAttributes
}
func StartNewTransaction(span traceCore.Span, flow string) traceCore.Span {
	span.SetAttributes(attribute.String(TransactionIdentifier, flow))
	span.SetAttributes(attribute.Bool(TransactionIdentifierRoot, true))
	return span
}
