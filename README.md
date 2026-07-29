# coralogix-opentelemetry-go

Coralogix extensions for OpenTelemetry Go.

## Transaction span processor

`processor/transaction` provides `TransactionSpanProcessor`: tags Coralogix
transactions, stamps exclusive self-time (`cgx.transaction.self_time`, seconds),
and records the matching histogram.

By default: keep at most **256** slowest
spans per local trace (`WithMaxNodes`), and export only the **slowest** completed
local trace every **60s** (`WithMaxRegularTraces(1)`). Self-time metrics are still
recorded for every completed local trace. Use `WithMaxRegularTraces(0)` to export
every completed (trimmed) trace immediately.

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
