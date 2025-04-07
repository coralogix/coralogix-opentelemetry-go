package sqlparser

import (
	"context"
)

func MysqlSpanFormatter(ctx context.Context, method string, query string) string {
	if query != "" {
		parsed, err := MysqlParse(&query)
		if err != nil {
			return method
		}
		return parsed
	}
	return method
}

func PostgresqlSpanFormatter(ctx context.Context, method string, query string) string {
	if query != "" {
		parsed, err := PostgresqlParse(&query)
		if err != nil {
			return method
		}
		return parsed
	}
	return method
}
