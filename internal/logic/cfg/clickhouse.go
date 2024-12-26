package cfg

import (
	"context"
	"github.com/gogf/gf/v2/os/gcfg"
	"github.com/gogf/gf/v2/os/gtime"
	"time"
)

func (s *sCfg) ClickHouseAutoFlush(ctx context.Context) (count uint, interval time.Duration, err error) {
	count = gcfg.Instance().MustGet(ctx, "clickhouse.autoFlush.count", 1000).Uint()

	intervalStr := gcfg.Instance().MustGet(ctx, "clickhouse.autoFlush.interval", "1m").String()
	interval, err = gtime.ParseDuration(intervalStr)
	return
}
