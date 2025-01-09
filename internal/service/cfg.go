// ================================================================================
// Code generated and maintained by GoFrame CLI tool. DO NOT EDIT.
// You can delete these comments if you wish manually maintain this interface file.
// ================================================================================

package service

import (
	"context"
)

type (
	ICfg interface {
		Canal(ctx context.Context) (address string, port int, username string, password string, destination string, soTimeOut int32, idleTimeOut int32)
		CanalFilter(ctx context.Context) (filter string)
		CanalSchema(ctx context.Context) (schema string)
		CanalTable(ctx context.Context) (table string)
		DBLink(ctx context.Context) (link string)
		ClickHouseCountFlush(ctx context.Context) uint
		ClickHouseCrontabFlush(ctx context.Context) string
		IsClickHouseOptimizeTableAfterInsert(ctx context.Context) bool
		ClickHouseCrontabOptimizeTable(ctx context.Context) string
	}
)

var (
	localCfg ICfg
)

func Cfg() ICfg {
	if localCfg == nil {
		panic("implement not found for interface ICfg, forgot register?")
	}
	return localCfg
}

func RegisterCfg(i ICfg) {
	localCfg = i
}
