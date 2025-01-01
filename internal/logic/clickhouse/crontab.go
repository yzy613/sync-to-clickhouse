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

func (s *sClickHouse) rewriteCrontabExpr(expr string) string {
	// linux crontab pattern
	if len(strings.Split(expr, " ")) == 5 {
		return "# " + expr
	}
	return expr
}

func (s *sClickHouse) SetCrontabFlush(
	ctx context.Context,
	crontabExpr string,
	table map[string]struct{},
) (err error) {
	s.lazyInitCrontab()

	if s.flushEntry != nil {
		s.flushEntry.Close()
		s.flushEntry = nil
	}

	// empty crontab pattern
	if crontabExpr == "" {
		return
	}

	entry, err := s.crontab.AddSingleton(
		ctx,
		s.rewriteCrontabExpr(crontabExpr),
		func(ctx context.Context) {
			if err := s.Flush(ctx); err != nil {
				g.Log().Error(ctx, err)
			}
			if table == nil || len(table) == 0 {
				return
			}
			if err := s.OptimizeTable(ctx, table); err != nil {
				g.Log().Error(ctx, err)
			}
		},
		"flush",
	)
	if err != nil {
		return
	}

	s.flushEntry = entry

	return
}

func (s *sClickHouse) SetCrontabOptimizeTable(
	ctx context.Context,
	crontabExpr string,
	table map[string]struct{},
) (err error) {
	s.lazyInitCrontab()

	if s.optimizeTableEntry != nil {
		s.optimizeTableEntry.Close()
		s.optimizeTableEntry = nil
	}

	// empty crontab pattern
	if crontabExpr == "" {
		return
	}

	entry, err := s.crontab.AddSingleton(
		ctx,
		s.rewriteCrontabExpr(crontabExpr),
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
