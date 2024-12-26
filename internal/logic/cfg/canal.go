package cfg

import (
	"context"
	"github.com/gogf/gf/v2/os/gcfg"
)

func (s *sCfg) Canal(ctx context.Context) (
	address string,
	port int,
	username string,
	password string,
	destination string,
	soTimeOut int32,
	idleTimeOut int32,
) {
	const def = ""

	return gcfg.Instance().MustGet(ctx, "canal.address", "127.0.0.1").String(),
		gcfg.Instance().MustGet(ctx, "canal.port", 11111).Int(),
		gcfg.Instance().MustGet(ctx, "canal.username", def).String(),
		gcfg.Instance().MustGet(ctx, "canal.password", def).String(),
		gcfg.Instance().MustGet(ctx, "canal.destination", "example").String(),
		gcfg.Instance().MustGet(ctx, "canal.soTimeOut", 60000).Int32(),
		gcfg.Instance().MustGet(ctx, "canal.idleTimeOut", 60*60*1000).Int32()
}

func (s *sCfg) CanalFilter(ctx context.Context) (filter string) {
	return gcfg.Instance().MustGet(ctx, "canal.filter", ".*\\..*").String()
}

func (s *sCfg) CanalSchema(ctx context.Context) (schema string) {
	return gcfg.Instance().MustGet(ctx, "canal.schema", "").String()
}

func (s *sCfg) CanalTable(ctx context.Context) (table string) {
	return gcfg.Instance().MustGet(ctx, "canal.table", "").String()
}
