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
  every span in the batch, then stamp self-duration, trim, and export.

### Trim default

By default the processor keeps at most **256** slowest spans per completed local
trace (`WithMaxNodes` / `OTEL_CX_TRANSACTION_MAX_NODES`). Every completed trimmed
local trace is exported immediately.

### Options and env vars

Constructor options win over env. Invalid env values fall back to defaults.

| Option | Env var | Default | Meaning |
|--------|---------|---------|---------|
| `WithMaxNodes` | `OTEL_CX_TRANSACTION_MAX_NODES` | `256` | Max spans kept per completed local trace (slowest first; root always kept) |
| `WithCompletionHoldback` | `OTEL_CX_TRANSACTION_COMPLETION_HOLDBACK_MILLIS` | `100` | Post-idle delay before finalizing a local trace |
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
