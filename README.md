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
  every span in the batch, then stamp self-duration, trim, and harvest/export.

### Harvest defaults

By default the processor keeps at most **256** slowest spans per local trace
(`WithMaxNodes` / `OTEL_CX_TRANSACTION_MAX_NODES`) and exports only the
**slowest** completed local waterfall every **60s**
(`WithMaxRegularTraces(1)` / `OTEL_CX_TRANSACTION_MAX_REGULAR_TRACES`).

That harvest window is intentional: losers are stub-exported (transaction root
only; full waterfall dropped). Self-duration metrics are still recorded for
**every** completed local trace. Set `WithMaxRegularTraces(0)` (or env `0`) to
export every completed trimmed trace immediately.

### Options and env vars

Constructor options win over env. Invalid env values fall back to defaults.

| Option | Env var | Default | Meaning |
|--------|---------|---------|---------|
| `WithMaxNodes` | `OTEL_CX_TRANSACTION_MAX_NODES` | `256` | Max spans kept per completed local trace (slowest first; root always kept) |
| `WithMaxRegularTraces` | `OTEL_CX_TRANSACTION_MAX_REGULAR_TRACES` | `1` | Slowest-N harvest capacity; `0` = export all immediately |
| `WithHarvestPeriod` | `OTEL_CX_TRANSACTION_HARVEST_PERIOD_MILLIS` | `60000` | Harvest flush interval |
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
