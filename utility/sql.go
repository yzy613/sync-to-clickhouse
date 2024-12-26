package utility

import (
	"fmt"
	"strings"
)

func extractMapKeys(m map[string]string) (k []string) {
	k = make([]string, 0, len(m))
	for key := range m {
		k = append(k, key)
	}
	return
}

func extractMapStringValues(m map[string]string, key []string) (v []string) {
	v = make([]string, 0, len(key))
	for _, k := range key {
		v = append(v, m[k])
	}
	return
}

func InsertStatement(table string, data []map[string]string) (stmt string, args []string) {
	if len(data) == 0 {
		return
	}

	if len(data) == 1 {
		k := extractMapKeys(data[0])
		args = append(args, extractMapStringValues(data[0], k)...)

		stmt = fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)",
			table,
			strings.Join(k, ","),
			strings.Repeat("?,", len(k)-1)+"?",
		)
		return
	}

	stmtBuilder := strings.Builder{}

	k := extractMapKeys(data[0])
	stmtBuilder.WriteString(fmt.Sprintf("INSERT INTO %s(%s) VALUES ", table, strings.Join(k, ",")))
	for _, d := range data {
		args = append(args, extractMapStringValues(d, k)...)
		stmtBuilder.WriteString(fmt.Sprintf("(%s),", strings.Repeat("?,", len(k)-1)+"?"))
	}

	stmt = strings.TrimSuffix(stmtBuilder.String(), ",")

	return
}
