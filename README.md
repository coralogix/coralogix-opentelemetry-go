# coralogix-opentelemetry-go

Coralogix extensions for OpenTelemetry Go.

## Transaction span processor

`processor/transaction` provides `TransactionSpanProcessor`: tags Coralogix
transactions, stamps exclusive self-duration (`cgx.transaction.self_duration`,
seconds), and records the matching histogram.

### OnStart vs export

- **OnStart**: decide new vs inherit (`SERVER` / `CONSUMER` / remote parent / no
  local parent txn). Mark `cgx.transaction.root` on new roots. Do **not** freeze
  `cgx.transaction` from the early span name (route templating may
  `UpdateName` later).
- **Export finalize** (completed local batch): set `cgx.transaction` from the
  root’s **final** `Name()` (or a pre-set / `StartNewTransaction` override) on
  every span in the batch, then export every span.

### Transaction enrichment limit

Transactions buffer up to **256** completed spans by default. When the next span ends,
the processor immediately exports those buffered spans unchanged and proxies
all later spans unchanged. Transactions that finish at 256 spans or fewer
receive transaction tags, self-duration, and its metric.

### Options and env vars

Invalid env values fall back to defaults.
Set `WithMaxTransactionSpans(0)` to use raw passthrough from the first span.

| Option | Env var | Default | Meaning |
|--------|---------|---------|---------|
| `WithCompletionHoldback` | `OTEL_CX_TRANSACTION_COMPLETION_HOLDBACK_MILLIS` | `100` | Post-idle delay before finalizing a local trace |
| `WithMaxTransactionSpans` | `CORALOGIX_MAX_SPANS_PER_TRACE` | `256` | Completed spans buffered per trace before raw passthrough |
| `WithMaxTraces` | `CORALOGIX_MAX_TRANSACTION_TRACES` | `0` | Transactions retained in memory while live or awaiting completion; positive values cap this buffer, while `0` is unlimited |
| `WithMeterProvider` | — | global | MeterProvider for `cgx.transaction.self_duration` |

```go
import (
    "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"

    "github.com/coralogix/coralogix-opentelemetry-go/processor/transaction"
)

exporter, _ := stdouttrace.New()
tp := sdktrace.NewTracerProvider(
    sdktrace.WithSpanProcessor(transaction.NewTransactionSpanProcessor(exporter)),
)
```
