// ================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// You can delete these comments if you wish manually maintain this interface file.
// ================================================================================

package service

import (
	"context"

	"github.com/withlin/canal-go/client"
	"github.com/withlin/canal-go/protocol/entry"
)

type (
	ICanal interface {
		NewConnector(ctx context.Context) *client.SimpleCanalConnector
		ParseEntries(ctx context.Context, schemaWhitelist map[string]struct{}, tableWhitelist map[string]struct{}, entries []entry.Entry) (err error)
		ReduceColumns(columns []*entry.Column) (result map[string]string)
	}
)

var (
	localCanal ICanal
)

func Canal() ICanal {
	if localCanal == nil {
		panic("implement not found for interface ICanal, forgot register?")
	}
	return localCanal
}

func RegisterCanal(i ICanal) {
	localCanal = i
}
