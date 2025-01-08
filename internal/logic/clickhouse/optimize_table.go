package clickhouse

import (
	"context"
	"github.com/gogf/gf/v2/container/gqueue"
	"github.com/gogf/gf/v2/frame/g"
)

func (s *sClickHouse) lazyInitOptimizeTableQueue() {
	if s.optimizeTableQueue == nil {
		s.optimizeTableQueue = gqueue.New()
	}
}

func (s *sClickHouse) OptimizeTable(ctx context.Context, table map[string]struct{}) (err error) {
	if err = s.hasDB(); err != nil {
		return
	}
	s.lazyInitOptimizeTableQueue()

	for k := range table {
		s.optimizeTableQueue.Push(k)
	}

	if s.optimizeTableMu.TryLock() {
		defer s.optimizeTableMu.Unlock()
		go s.optimizeTable(ctx)
	}

	return
}

func (s *sClickHouse) optimizeTable(ctx context.Context) {
	s.optimizeTableMu.Lock()
	defer s.optimizeTableMu.Unlock()

	ctx = context.WithoutCancel(ctx)

	for v := range s.optimizeTableQueue.C {
		table, ok := v.(string)
		if !ok {
			continue
		}

		if _, err := s.db.Exec(ctx, "OPTIMIZE TABLE "+table+" FINAL"); err != nil {
			g.Log().Error(ctx, err)
		}
	}
}
