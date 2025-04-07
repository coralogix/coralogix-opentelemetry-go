package sqlparser

import (
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/auxten/postgresql-parser/pkg/walk"
	mysqlparser "github.com/xwb1989/sqlparser"
	"regexp"
	"strings"
)

func MysqlParse(dbStatementStr *string) (string, error) {
	replaceDollarInsideValues(dbStatementStr)
	stmt, err := mysqlparser.Parse(*dbStatementStr)
	if err != nil {
		return *dbStatementStr, err
	}
	blueprint := mysqlReplaceValuesWithPlaceholder(stmt)
	blueprintStr := mysqlparser.String(blueprint)
	return blueprintStr, nil
}

func PostgresqlParse(dbStatementStr *string) (string, error) {
	replaceDollarInsideValues(dbStatementStr)
	stmts, err := parser.Parse(*dbStatementStr)
	if err != nil {
		return *dbStatementStr, err
	}
	w := &walk.AstWalker{
		Fn: func(_ any, node any) (stop bool) {
			if n, ok := node.(*tree.ComparisonExpr); ok {
				_, leftIsColumn := n.Left.(*tree.ColumnItem)
				_, leftIsUnresolved := n.Left.(*tree.UnresolvedName)
				_, rightIsColumn := n.Right.(*tree.ColumnItem)
				_, rightIsUnresolved := n.Right.(*tree.UnresolvedName)
				if leftIsColumn && !rightIsColumn || leftIsUnresolved && !rightIsUnresolved {
					n.Right = tree.NewStrVal("?")
				}
				if !leftIsColumn && rightIsColumn || !leftIsUnresolved && rightIsUnresolved {
					n.Left = tree.NewStrVal("?")
				}
			}
			return false
		},
	}
	_, _ = w.Walk(stmts, nil)
	blueprintStr := stmts.String()
	return blueprintStr, nil
}
func mysqlReplaceValuesWithPlaceholder(stmt mysqlparser.Statement) mysqlparser.Statement {
	err := mysqlparser.Walk(func(node mysqlparser.SQLNode) (kontinue bool, err error) {
		switch n := node.(type) {

		case *mysqlparser.Insert:
			for i, expr := range n.Rows.(mysqlparser.Values) {
				for j, val := range expr {
					if v, ok := val.(*mysqlparser.ColName); ok {
						v.Name = mysqlparser.NewColIdent("?")
					}
					expr[j] = val
				}
				n.Rows.(mysqlparser.Values)[i] = expr
			}
		case *mysqlparser.SQLVal:
			n.Type = mysqlparser.ValArg
			n.Val = []byte("?")

		case *mysqlparser.ComparisonExpr:
			if n.Operator == mysqlparser.InStr || n.Operator == mysqlparser.NotInStr {
				if v, ok := n.Right.(mysqlparser.ValTuple); ok {
					v = v[0:1]
					n.Right = v
				}
			}
		}
		return true, nil
	}, stmt)
	if err != nil {
		return nil
	}
	return stmt
}

func replaceDollarInsideValues(input *string) {
	if strings.Contains(*input, "$") {
		re := regexp.MustCompile(`(?i)VALUES\s*\(\s*([^)]+)\)`)
		// some libraries implement $1 as a placeholder, for example
		*input = re.ReplaceAllStringFunc(*input, func(match string) string {
			// Replace '$' only inside the matched "VALUES" expression
			return strings.ReplaceAll(match, "$", "")
		})
	}
}
