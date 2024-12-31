package utility

import (
	"fmt"
	"strings"
)

func ExtractMapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

func ExtractMapValuesByKeys[K comparable, V any](m map[K]V, keys []K) []V {
	values := make([]V, 0, len(keys))
	for _, key := range keys {
		values = append(values, m[key])
	}
	return values
}

func InsertStatement[T any](table string, data []map[string]T) (stmt string, args []T) {
	if len(data) == 0 {
		return
	}

	if len(data) == 1 {
		keys := ExtractMapKeys(data[0])
		args = append(args, ExtractMapValuesByKeys(data[0], keys)...)

		stmt = fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)",
			table,
			strings.Join(keys, ","),
			strings.Repeat("?,", len(keys)-1)+"?",
		)
		return
	}

	stmtBuilder := strings.Builder{}

	keys := ExtractMapKeys(data[0])
	stmtBuilder.WriteString(fmt.Sprintf("INSERT INTO %s(%s) VALUES ", table, strings.Join(keys, ",")))
	for _, d := range data {
		args = append(args, ExtractMapValuesByKeys(d, keys)...)
		stmtBuilder.WriteString(fmt.Sprintf("(%s),", strings.Repeat("?,", len(keys)-1)+"?"))
	}

	stmt = strings.TrimSuffix(stmtBuilder.String(), ",")

	return
}
