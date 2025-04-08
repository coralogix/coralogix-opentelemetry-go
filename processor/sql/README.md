# Instrumented MySQL Connection with Custom Span Naming (OpenTelemetry)

This project showcases how to set up a MySQL connection in Go with OpenTelemetry instrumentation using `otelsql`, with **custom span name formatting** for SQL queries via a user-defined formatter.

## Key Feature: Custom Span Name Formatter

The most critical part of this setup is:

```go
otelsql.WithSpanNameFormatter(func(ctx context.Context, method otelsql.Method, query string) string {
    return sqlparser.MysqlSpanFormatter(ctx, string(method), query)
}),
```
Example of a full setup

```go
import (
    "context"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/semconv/v1.17.0"
    "github.com/XSAM/otelsql"
    "your_module/sqlparser" // Replace with your actual import path
)

var (
    db       *sql.DB
    err      error
    dsn      string // MySQL DSN: "user:pass@tcp(host:port)/dbname"
    database string
    user     string
    host     string
    port     string
)

db, err = otelsql.Open("mysql", dsn,
    otelsql.WithAttributes(
        semconv.DBSystemMySQL,
        attribute.String("db.name", database),
        attribute.String("db.user", user),
        attribute.String("net.peer.name", host),
        attribute.String("net.peer.port", port),
    ),
    otelsql.WithSpanOptions(otelsql.SpanOptions{
        OmitConnResetSession: true,
        OmitConnPrepare:      true,
        DisableErrSkip:       true,
    }),
    otelsql.WithSpanNameFormatter(func(ctx context.Context, method otelsql.Method, query string) string {
        return sqlparser.MysqlSpanFormatter(ctx, string(method), query)
    }),
)
```

