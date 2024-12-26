package utility

import (
	"reflect"
	"testing"
)

func TestInsertStatement(t *testing.T) {
	type args struct {
		table string
		data  []map[string]string
	}
	tests := []struct {
		name     string
		args     args
		wantStmt string
		wantArgs []string
	}{
		{
			name: "user",
			args: args{
				table: "users",
				data:  []map[string]string{{"name": "Alice", "age": "20"}},
			},
			wantStmt: "INSERT INTO users(name,age) VALUES (?,?)",
			wantArgs: []string{"Alice", "20"},
		},
		{
			name: "users",
			args: args{
				table: "users",
				data:  []map[string]string{{"name": "Alice", "age": "20"}, {"name": "Bob", "age": "21"}},
			},
			wantStmt: "INSERT INTO users(name,age) VALUES (?,?),(?,?)",
			wantArgs: []string{"Alice", "20", "Bob", "21"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStmt, gotArgs := InsertStatement(tt.args.table, tt.args.data)
			if gotStmt != tt.wantStmt {
				t.Errorf("InsertStatement() gotStmt = %v, want %v", gotStmt, tt.wantStmt)
			}
			if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("InsertStatement() gotArgs = %v, want %v", gotArgs, tt.wantArgs)
			}
		})
	}
}
