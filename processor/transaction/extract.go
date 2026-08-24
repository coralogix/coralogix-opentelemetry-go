package transaction

import (
	"sort"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracecore "go.opentelemetry.io/otel/trace"

	"github.com/coralogix/coralogix-opentelemetry-go/sampler"
)

// extractCompletedLocalTransactionsLocked pulls finished local-transaction
// subtrees out of a trace buffer. Nested roots (SERVER/CONSUMER /
// StartNewTransaction) can finalize while an outer ancestor is still live.
func (p *TransactionSpanProcessor) extractCompletedLocalTransactionsLocked(tb *traceBuffer, flushLeftover bool) [][]sdktrace.ReadOnlySpan {
	if len(tb.spans) == 0 {
		return nil
	}

	parentOf := make(map[tracecore.SpanID]tracecore.SpanID, len(tb.spans)+len(tb.liveParents))
	for _, s := range tb.spans {
		if pid := s.Parent().SpanID(); pid.IsValid() {
			parentOf[s.SpanContext().SpanID()] = pid
		}
	}
	for id, pid := range tb.liveParents {
		if pid.IsValid() {
			parentOf[id] = pid
		}
	}

	underRoot := func(spanID, rootID tracecore.SpanID) bool {
		cur := spanID
		seen := make(map[tracecore.SpanID]struct{})
		for cur.IsValid() {
			if _, done := seen[cur]; done {
				break
			}
			if cur == rootID {
				return true
			}
			seen[cur] = struct{}{}
			next, ok := parentOf[cur]
			if !ok {
				break
			}
			cur = next
		}
		return false
	}

	hasLiveInSubtree := func(rootID tracecore.SpanID) bool {
		if _, ok := tb.liveParents[rootID]; ok {
			return true
		}
		for liveID := range tb.liveParents {
			if underRoot(liveID, rootID) {
				return true
			}
		}
		return false
	}

	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	var roots []tracecore.SpanID
	for _, s := range tb.spans {
		for _, a := range s.Attributes() {
			if a.Key == rootKey && a.Value.AsBool() {
				roots = append(roots, s.SpanContext().SpanID())
				break
			}
		}
	}

	rootDepth := func(rootID tracecore.SpanID) int {
		depth := 0
		cur := rootID
		seen := make(map[tracecore.SpanID]struct{})
		for cur.IsValid() {
			if _, done := seen[cur]; done {
				break
			}
			seen[cur] = struct{}{}
			next, ok := parentOf[cur]
			if !ok || !next.IsValid() {
				break
			}
			depth++
			cur = next
		}
		return depth
	}

	sort.SliceStable(roots, func(i, j int) bool {
		return rootDepth(roots[i]) > rootDepth(roots[j])
	})

	var batches [][]sdktrace.ReadOnlySpan
	extracted := make(map[tracecore.SpanID]struct{})

	for _, rootID := range roots {
		if _, done := extracted[rootID]; done {
			continue
		}
		if hasLiveInSubtree(rootID) {
			continue
		}
		var subtree []sdktrace.ReadOnlySpan
		for _, s := range tb.spans {
			sid := s.SpanContext().SpanID()
			if _, done := extracted[sid]; done {
				continue
			}
			if underRoot(sid, rootID) {
				subtree = append(subtree, s)
			}
		}
		if len(subtree) == 0 {
			continue
		}
		for _, s := range subtree {
			extracted[s.SpanContext().SpanID()] = struct{}{}
		}
		batches = append(batches, subtree)
	}

	if len(extracted) > 0 {
		remaining := make([]sdktrace.ReadOnlySpan, 0, len(tb.spans)-len(extracted))
		for _, s := range tb.spans {
			if _, done := extracted[s.SpanContext().SpanID()]; !done {
				remaining = append(remaining, s)
			}
		}
		tb.spans = remaining
	}

	if flushLeftover && tb.liveCount() == 0 && len(tb.spans) > 0 {
		batches = append(batches, tb.spans)
		tb.spans = nil
	}

	return batches
}

// hasExtractableNestedTransaction reports whether a nested transaction root
// subtree is fully ended while an outer ancestor is still live.
func hasExtractableNestedTransaction(tb *traceBuffer) bool {
	if len(tb.spans) == 0 || tb.liveCount() == 0 {
		return false
	}

	parentOf := make(map[tracecore.SpanID]tracecore.SpanID, len(tb.spans)+len(tb.liveParents))
	for _, s := range tb.spans {
		if pid := s.Parent().SpanID(); pid.IsValid() {
			parentOf[s.SpanContext().SpanID()] = pid
		}
	}
	for id, pid := range tb.liveParents {
		if pid.IsValid() {
			parentOf[id] = pid
		}
	}

	underRoot := func(spanID, rootID tracecore.SpanID) bool {
		cur := spanID
		seen := make(map[tracecore.SpanID]struct{})
		for cur.IsValid() {
			if _, done := seen[cur]; done {
				break
			}
			if cur == rootID {
				return true
			}
			seen[cur] = struct{}{}
			next, ok := parentOf[cur]
			if !ok {
				break
			}
			cur = next
		}
		return false
	}

	hasLiveInSubtree := func(rootID tracecore.SpanID) bool {
		if _, ok := tb.liveParents[rootID]; ok {
			return true
		}
		for liveID := range tb.liveParents {
			if underRoot(liveID, rootID) {
				return true
			}
		}
		return false
	}

	rootKey := attribute.Key(sampler.TransactionIdentifierRoot)
	for _, s := range tb.spans {
		for _, a := range s.Attributes() {
			if a.Key == rootKey && a.Value.AsBool() {
				rootID := s.SpanContext().SpanID()
				if !hasLiveInSubtree(rootID) {
					return true
				}
				break
			}
		}
	}
	return false
}
