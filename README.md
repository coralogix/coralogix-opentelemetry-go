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
Set `WithMaxTransactionSpans(0)` for unlimited transaction enrichment.

| Option | Env var | Default | Meaning |
|--------|---------|---------|---------|
| `WithCompletionHoldback` | `OTEL_CX_TRANSACTION_COMPLETION_HOLDBACK_MILLIS` | `100` | Post-idle delay before finalizing a local trace |
| `WithMaxTransactionSpans` | `CORALOGIX_MAX_SPANS_PER_TRACE` | `256` | Completed spans buffered per trace before raw passthrough; `0` is unlimited |
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

## Benchmark

To help users estimate resource usage, we ran this benchmark. The first table processes 10,000 traces at each depth from 8 to 2,048 spans to show the impact of increasingly deep transactions. The second table processes traces with a depth of 1,000 spans at increasing trace counts to show the effect of transaction volume.

### 10000 traces by depth

| Depth | Traces | RSS base MiB | RSS peak MiB | RSS delta MiB | Spans/s |
|---:|---:|---:|---:|---:|---:|
|  8  |  10000  | 13.23 | 20.47 | 7.23 | 509739.20 |
|  16  |  10000  | 13.17 | 22.39 | 9.22 | 418712.25 |
|  32  |  10000  | 13.12 | 24.94 | 11.81 | 362343.46 |
|  64  |  10000  | 13.17 | 24.78 | 11.61 | 283227.82 |
|  128  |  10000  | 13.22 | 24.72 | 11.50 | 193021.58 |
|  256  |  10000  | 13.34 | 27.80 | 14.45 | 119076.96 |
|  512  |  10000  | 13.33 | 19.94 | 6.61 | 35123.48 |
|  1024  |  10000  | 13.22 | 21.23 | 8.02 | 35014.33 |
|  2048  |  10000  | 13.22 | 24.19 | 10.97 | 35315.94 |

### Depth 1000 by trace count

| Depth | Traces | RSS base MiB | RSS peak MiB | RSS delta MiB | Spans/s |
|---:|---:|---:|---:|---:|---:|
|  1000  |  100  | 13.17 | 17.62 | 4.45 | 838468.96 |
|  1000  |  1000  | 13.38 | 18.55 | 5.17 | 279183.62 |
|  1000  |  10000  | 13.20 | 21.34 | 8.14 | 34809.55 |
|  1000  |  100000  | 13.27 | 24.58 | 11.31 | 10562.52 |
