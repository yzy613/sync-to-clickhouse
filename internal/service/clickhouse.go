// ================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// You can delete these comments if you wish manually maintain this interface file.
// ================================================================================

package service

import (
	"context"
	"time"
)

type (
	IClickHouse interface {
		SetDBLink(link string) (err error)
		SetAutoFlush(ctx context.Context, count uint, interval time.Duration)
		SetAutoOptimizeTable(ctx context.Context, interval time.Duration, table map[string]struct{})
		OptimizeTable(ctx context.Context, table map[string]struct{}) (err error)
		Flush(ctx context.Context) error
		DumpToDisk(ctx context.Context) (err error)
		RestoreFromDisk(ctx context.Context) (err error)
		Insert(ctx context.Context, table string, data []map[string]string) (err error)
	}
)

var (
	localClickHouse IClickHouse
)

func ClickHouse() IClickHouse {
	if localClickHouse == nil {
		panic("implement not found for interface IClickHouse, forgot register?")
	}
	return localClickHouse
}

func RegisterClickHouse(i IClickHouse) {
	localClickHouse = i
}
