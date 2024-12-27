package cfg

import (
	"context"
	"github.com/gogf/gf/v2/os/gcfg"
	"sync-to-clickhouse/internal/service"
)

type sCfg struct{}

func New() *sCfg {
	return &sCfg{}
}

func init() {
	service.RegisterCfg(New())
}

func (s *sCfg) DBLink(ctx context.Context) (link string) {
	return gcfg.Instance().MustGet(ctx, "database.default.link", "").String()
}
