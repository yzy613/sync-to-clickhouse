package clickhouse

import (
	"context"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gcron"
	"strings"
)

func (s *sClickHouse) lazyInitCrontab() {
	if s.crontab == nil {
		s.crontab = gcron.New()
	}
}

func (s *sClickHouse) SetCrontabFlush(ctx context.Context, crontabExpr string, isEnableOptimizeTable bool) (err error) {
	s.lazyInitCrontab()

	if s.flushEntry != nil {
		s.flushEntry.Close()
		s.flushEntry = nil
	}

	// empty crontab pattern
	if crontabExpr == "" {
		return
	}

	// linux crontab pattern
	if len(strings.Split(crontabExpr, " ")) == 5 {
		crontabExpr = "# " + crontabExpr
	}

	var f func(ctx context.Context)
	if isEnableOptimizeTable {
		f = func(ctx context.Context) {
			if err := s.Flush(ctx); err != nil {
				g.Log().Error(ctx, err)
			}
			if err := s.OptimizeTable(ctx, nil); err != nil {
				g.Log().Error(ctx, err)
			}
		}
	} else {
		f = func(ctx context.Context) {
			if err := s.Flush(ctx); err != nil {
				g.Log().Error(ctx, err)
			}
		}
	}

	entry, err := s.crontab.AddSingleton(
		ctx,
		crontabExpr,
		f,
		"flush",
	)
	if err != nil {
		return
	}

	s.flushEntry = entry

	return
}

func (s *sClickHouse) SetCrontabOptimizeTable(ctx context.Context, crontabExpr string, table map[string]struct{}) (err error) {
	s.lazyInitCrontab()

	if s.optimizeTableEntry != nil {
		s.optimizeTableEntry.Close()
		s.optimizeTableEntry = nil
	}

	// empty crontab pattern
	if crontabExpr == "" {
		return
	}

	// linux crontab pattern
	if len(strings.Split(crontabExpr, " ")) == 5 {
		crontabExpr = "# " + crontabExpr
	}

	entry, err := s.crontab.AddSingleton(
		ctx,
		crontabExpr,
		func(ctx context.Context) {
			if err := s.OptimizeTable(ctx, table); err != nil {
				g.Log().Error(ctx, err)
			}
		},
		"optimize_table",
	)
	if err != nil {
		return
	}

	s.optimizeTableEntry = entry

	return
}
