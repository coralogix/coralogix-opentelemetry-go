package sqlparser

import (
	"testing"
)

func TestMysqlParse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "simple select",
			input:    "SELECT * FROM users WHERE id = 123",
			expected: "select * from users where id = ?",
			wantErr:  false,
		},
		{
			name:     "insert statement",
			input:    "INSERT INTO users (name, age) VALUES ('John', 25)",
			expected: "insert into users(name, age) values (?, ?)",
			wantErr:  false,
		},
		{
			name:     "select with multiple conditions",
			input:    "SELECT * FROM users WHERE age > 18 AND name = 'Alice'",
			expected: "select * from users where age > ? and name = ?",
			wantErr:  false,
		},
		{
			name:     "select with IN clause",
			input:    "SELECT * FROM users WHERE id IN (1, 2, 3, 4)",
			expected: "select * from users where id in (?)",
			wantErr:  false,
		},
		{
			name:     "statement with dollar sign in values",
			input:    "INSERT INTO prices (amount) VALUES ($100.50)",
			expected: "insert into prices(amount) values (?)",
			wantErr:  false,
		},
		{
			name:     "inner join with multiple conditions",
			input:    "SELECT u.name, o.order_date FROM users AS u JOIN orders AS o ON u.id = o.user_id AND o.active = 'active'",
			expected: "select u.name, o.order_date from users as u join orders as o on u.id = o.user_id and o.active = ?",
			wantErr:  false,
		},
		{
			name:     "left join with where clause",
			input:    "SELECT c.name, o.total FROM customers AS c LEFT JOIN orders AS o ON c.id = o.customer_id WHERE o.total > 1000",
			expected: "select c.name, o.total from customers as c left join orders as o on c.id = o.customer_id where o.total > ?",
			wantErr:  false,
		},
		{
			name:     "multiple joins with conditions",
			input:    "SELECT p.name, c.category, o.quantity FROM products AS p JOIN order_items AS o ON p.id = o.product_id JOIN categories AS c ON p.category_id = c.id WHERE o.quantity > 5",
			expected: "select p.name, c.category, o.quantity from products as p join order_items as o on p.id = o.product_id join categories as c on p.category_id = c.id where o.quantity > ?",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MysqlParse(&tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("mysqlParse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("mysqlParse() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestPostgresqlParse(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "simple select",
			input:    "SELECT * FROM users WHERE id = 123",
			expected: "SELECT * FROM users WHERE id = '?'",
			wantErr:  false,
		},
		{
			name:     "select with multiple conditions",
			input:    "SELECT * FROM users WHERE age > 18 AND name = 'Alice'",
			expected: "SELECT * FROM users WHERE (age > '?') AND (name = '?')",
			wantErr:  false,
		},
		{
			name:     "statement with dollar sign in values don't work",
			input:    "INSERT INTO prices (amount) VALUES ($100.50)",
			expected: "INSERT INTO prices(amount) VALUES (100.50)",
			wantErr:  false,
		},
		{
			name:     "complex select with joins",
			input:    "SELECT u.name, o.order_date FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE o.amount > 50.00",
			expected: "SELECT u.name, o.order_date FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE o.amount > '?'",
			wantErr:  false,
		},
		{
			name:     "right join with multiple conditions",
			input:    "SELECT d.department_name, e.name FROM employees AS e RIGHT JOIN departments AS d ON e.dept_id = d.id AND e.status = 'active'",
			expected: "SELECT d.department_name, e.name FROM employees AS e RIGHT JOIN departments AS d ON (e.dept_id = d.id) AND (e.status = '?')",
			wantErr:  false,
		},
		{
			name:     "full outer join",
			input:    "SELECT s.student_name, c.course_name FROM students AS s FULL OUTER JOIN courses AS c ON s.course_id = c.id WHERE s.grade > 80",
			expected: "SELECT s.student_name, c.course_name FROM students AS s FULL JOIN courses AS c ON s.course_id = c.id WHERE s.grade > '?'",
			wantErr:  false,
		},
		{
			name:     "multiple joins with aggregation",
			input:    "SELECT p.category, s.supplier_name, COUNT(*) as count FROM products AS p JOIN suppliers AS s ON p.supplier_id = s.id JOIN orders AS o ON p.id = o.product_id WHERE o.order_date > '2023-01-01' GROUP BY p.category, s.supplier_name",
			expected: "SELECT p.category, s.supplier_name, count(*) AS count FROM products AS p JOIN suppliers AS s ON p.supplier_id = s.id JOIN orders AS o ON p.id = o.product_id WHERE o.order_date > '?' GROUP BY p.category, s.supplier_name",
			wantErr:  false,
		},
		{
			name:    "invalid SQL",
			input:   "SELECT * FROM",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PostgresqlParse(&tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("postgresqlParse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("postgresqlParse() = %v, want %v", got, tt.expected)
			}
		})
	}
}
