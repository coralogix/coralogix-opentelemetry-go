package transaction

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

// beginTransaction runs on OnStart.
//
// It only decides new-vs-inherit and marks roots early enough for nested
// extraction. It does NOT freeze cgx.transaction from the early span name —
// Express-style UpdateName (or similar) may still change the root name before
// End. Final names are stamped in stampTransactionAttributes at finalize.
//
// Explicit overrides (route template ≠ start name, or later StartNewTransaction
// with a different name) are recorded in membership. Sampler echoes that copy
// the early span name into cgx.transaction are not treated as overrides, so
// UpdateName can still supply the final transaction name.
func beginTransaction(ctx context.Context, s sdktrace.ReadWriteSpan, tracked map[tracecore.SpanID]spanMembership, finalized map[tracecore.SpanID]string) {
	parent := s.Parent()
	hasLocalTxn := hasLocalTransaction(ctx, parent, tracked, finalized)

	starts := !hasLocalTxn ||
		parent.IsRemote() ||
		s.SpanKind() == tracecore.SpanKindServer ||
		s.SpanKind() == tracecore.SpanKindConsumer

	inheritedName := ""
	if !starts {
		name, hasName, hasLocalRoot := resolveParentInfo(ctx, parent, tracked, finalized)
		if hasName && name != "" && !hasLocalRoot {
			inheritedName = name
		}
	}

	startName := s.Name()
	overrideName := ""
	if starts {
		if existing := readWriteTransactionAttr(s); existing != "" && existing != startName {
			overrideName = existing
		}
	}

	if tracked != nil {
		tracked[s.SpanContext().SpanID()] = spanMembership{
			inheritedName: inheritedName,
			startName:     startName,
			overrideName:  overrideName,
		}
	}

	if !starts {
		return
	}

	// Root flag only — do not write cgx.transaction from s.Name().
	s.SetAttributes(attribute.Bool(sampler.TransactionIdentifierRoot, true))
}

// hasLocalTransaction reports whether the parent is already part of a local
// transaction tree. Prefers the OnStart side table (attrs are not stamped on
// inherit children until finalize), then finalized-name cache, then live
// parent attrs / tracestate.
func hasLocalTransaction(ctx context.Context, parent tracecore.SpanContext, tracked map[tracecore.SpanID]spanMembership, finalized map[tracecore.SpanID]string) bool {
	if parent.IsValid() && tracked != nil {
		if _, ok := tracked[parent.SpanID()]; ok {
			return true
		}
	}
	if parent.IsValid() && finalized != nil {
		if _, ok := finalized[parent.SpanID()]; ok {
			return true
		}
	}
	_, has, _ := resolveParentInfo(ctx, parent, tracked, finalized)
	return has
}

// resolveParentInfo returns (name, hasTransaction, hasLocalRoot).
// hasLocalRoot is true when the parent is in the local membership side-table
// (or carries an explicit root attr on a live span).
func resolveParentInfo(
	ctx context.Context,
	parent tracecore.SpanContext,
	tracked map[tracecore.SpanID]spanMembership,
	finalized map[tracecore.SpanID]string,
) (name string, hasTxn bool, hasLocalRoot bool) {
	if parent.IsValid() && tracked != nil {
		if m, ok := tracked[parent.SpanID()]; ok {
			if m.finalized {
				return m.inheritedName, true, false
			}
			return m.inheritedName, true, true
		}
	}
	if parent.IsValid() && finalized != nil {
		if n, ok := finalized[parent.SpanID()]; ok {
			return n, true, false
		}
	}

	if parentSpan := tracecore.SpanFromContext(ctx); parentSpan != nil {
		if rw, ok := parentSpan.(sdktrace.ReadWriteSpan); ok {
			var txnName string
			var isRoot bool
			for _, a := range rw.Attributes() {
				switch a.Key {
				case attribute.Key(sampler.TransactionIdentifier):
					txnName = a.Value.AsString()
				case attribute.Key(sampler.TransactionIdentifierRoot):
					isRoot = a.Value.AsBool()
				}
			}
			if isRoot {
				// hasLocalRoot only when the parent is still in the membership
				// side-table. A finalized root may still expose attrs via Context
				// but must be treated as an inherited name source once membership
				// was cleared.
				sc := rw.SpanContext()
				local := tracked != nil && sc.IsValid()
				if local {
					_, local = tracked[sc.SpanID()]
				}
				return txnName, true, local
			}
			if txnName != "" {
				sc := rw.SpanContext()
				local := tracked != nil && sc.IsValid()
				if local {
					_, local = tracked[sc.SpanID()]
				}
				return txnName, true, local
			}
		}
		// Non-recording parent from ContextWithSpanContext: read TraceState here.
		sc := parentSpan.SpanContext()
		if sc.IsValid() {
			if tsName := sc.TraceState().Get(sampler.TransactionIdentifierTraceState); tsName != "" {
				return tsName, true, false
			}
		}
	}

	if parent.IsValid() {
		tsName := parent.TraceState().Get(sampler.TransactionIdentifierTraceState)
		if tsName != "" {
			return tsName, true, false
		}
	}
	return "", false, false
}

// stampTransactionAttributes sets cgx.transaction on every span in a completed
// local batch from the root's final Name() (or a pre-set override attribute).
func stampTransactionAttributes(
	spans []sdktrace.ReadOnlySpan,
	tracked map[tracecore.SpanID]spanMembership,
) []sdktrace.ReadOnlySpan {
	if len(spans) == 0 {
		return spans
	}

	root := findTransactionRootSpan(spans)
	if root != nil {
		name := resolveTransactionName(root, tracked)
		out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
		for _, s := range spans {
			out = append(out, withTransaction(s, name, isTransactionRoot(s)))
		}
		return out
	}

	// Leftover flush without ROOT markers: partition by inherited transaction
	// so late children from different finalized txns on the same TraceID are
	// not stamped with a single shared name.
	partitions := make(map[string][]sdktrace.ReadOnlySpan)
	var order []string
	for _, s := range spans {
		name := ""
		if tracked != nil {
			if m, ok := tracked[s.SpanContext().SpanID()]; ok {
				name = m.inheritedName
			}
		}
		if _, seen := partitions[name]; !seen {
			order = append(order, name)
		}
		partitions[name] = append(partitions[name], s)
	}

	out := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, key := range order {
		part := partitions[key]
		name := key
		if name == "" {
			fallback := part[0]
			for _, s := range part {
				if !s.Parent().IsValid() {
					fallback = s
					break
				}
			}
			name = resolveTransactionName(fallback, tracked)
		}
		for _, s := range part {
			out = append(out, withTransaction(s, name, isTransactionRoot(s)))
		}
	}
	return out
}

// resolveTransactionName prefers an explicit override over the root's final
// Name(). Sampler-injected cgx.transaction that merely echoed the OnStart name
// is ignored so UpdateName can win. StartNewTransaction sets
// cgx.transaction.explicit so equal-name overrides still win after a rename.
func resolveTransactionName(root sdktrace.ReadOnlySpan, tracked map[tracecore.SpanID]spanMembership) string {
	if hasExplicitTransactionOverride(root) {
		if v := transactionAttr(root); v != "" {
			return v
		}
	}
	if tracked != nil {
		if m, ok := tracked[root.SpanContext().SpanID()]; ok {
			if m.overrideName != "" {
				return m.overrideName
			}
			if v := transactionAttr(root); v != "" && v != m.startName {
				return v
			}
		}
	}
	return root.Name()
}

func hasExplicitTransactionOverride(span sdktrace.ReadOnlySpan) bool {
	key := attribute.Key(sampler.TransactionIdentifierExplicit)
	for _, a := range span.Attributes() {
		if a.Key == key && a.Value.AsBool() {
			return true
		}
	}
	return false
}

func transactionAttr(span sdktrace.ReadOnlySpan) string {
	for _, a := range span.Attributes() {
		if a.Key == attribute.Key(sampler.TransactionIdentifier) {
			return a.Value.AsString()
		}
	}
	return ""
}

func readWriteTransactionAttr(span sdktrace.ReadWriteSpan) string {
	for _, a := range span.Attributes() {
		if a.Key == attribute.Key(sampler.TransactionIdentifier) {
			return a.Value.AsString()
		}
	}
	return ""
}

func isTransactionRoot(span sdktrace.ReadOnlySpan) bool {
	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	for _, a := range span.Attributes() {
		if a.Key == rootKey && a.Value.AsBool() {
			return true
		}
	}
	return false
}

func findTransactionRootSpan(spans []sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if isTransactionRoot(s) {
			return s
		}
	}
	return nil
}
