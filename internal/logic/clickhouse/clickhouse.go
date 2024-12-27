package clickhouse

import (
	"context"
	"github.com/gogf/gf/v2/container/gqueue"
	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
	"golang.org/x/sync/errgroup"
	"sync"
	"sync-to-clickhouse/internal/service"
	"time"
)

type sClickHouse struct {
	db gdb.DB

	insertQueue     *gqueue.Queue
	insertQueuePath string
	popInsertMu     sync.Mutex

	autoFlushCtx    context.Context
	autoFlushCancel context.CancelFunc
	autoFlushRWMu   sync.RWMutex
	autoFlushCount  uint

	autoOptimizeTableCtx    context.Context
	autoOptimizeTableCancel context.CancelFunc
}

func New() *sClickHouse {
	return &sClickHouse{
		insertQueuePath: "insert_queue.json",
	}
}

func init() {
	service.RegisterClickHouse(New())
}

func (s *sClickHouse) SetDBLink(link string) (err error) {
	s.db, err = gdb.New(gdb.ConfigNode{Link: link})
	return
}

func (s *sClickHouse) SetAutoFlush(ctx context.Context, count uint, interval time.Duration) {
	if s.autoFlushCtx != nil && s.autoFlushCancel != nil {
		s.autoFlushCancel()
	}
	s.autoFlushRWMu.Lock()
	defer s.autoFlushRWMu.Unlock()

	s.autoFlushCtx, s.autoFlushCancel = context.WithCancel(context.Background())
	s.autoFlushCount = count

	if interval == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		localCtx := s.autoFlushCtx

		for {
			select {
			case <-ctx.Done():
				return
			case <-localCtx.Done():
				return
			case <-ticker.C:
				if err := s.Flush(ctx); err != nil {
					g.Log().Error(ctx, err)
				}
			}
		}
	}()
}

func (s *sClickHouse) SetAutoOptimizeTable(ctx context.Context, interval time.Duration, table map[string]struct{}) {
	if s.autoOptimizeTableCtx != nil && s.autoOptimizeTableCancel != nil {
		s.autoOptimizeTableCancel()
	}
	s.autoOptimizeTableCtx, s.autoOptimizeTableCancel = context.WithCancel(context.Background())

	if interval == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		localCtx := s.autoOptimizeTableCtx

		for {
			select {
			case <-ctx.Done():
				return
			case <-localCtx.Done():
				return
			case <-ticker.C:
				if err := s.OptimizeTable(ctx, table); err != nil {
					g.Log().Error(ctx, err)
				}
			}
		}
	}()
}

func (s *sClickHouse) OptimizeTable(ctx context.Context, table map[string]struct{}) (err error) {
	if err = s.hasDB(); err != nil {
		return
	}

	g.Log().Info(ctx, "optimize table")

	for k := range table {
		if _, err = s.db.Exec(ctx, "OPTIMIZE TABLE "+k+" FINAL"); err != nil {
			return
		}
	}
	return
}

func (s *sClickHouse) Flush(ctx context.Context) error {
	return s.flushInsertQueue(ctx)
}

func (s *sClickHouse) DumpToDisk(ctx context.Context) (err error) {
	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return s.dumpInsertQueueToDisk(egCtx)
	})

	if err = eg.Wait(); err != nil {
		return
	}

	return
}

func (s *sClickHouse) RestoreFromDisk(ctx context.Context) (err error) {
	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return s.restoreInsertQueueFromDisk(egCtx)
	})

	if err = eg.Wait(); err != nil {
		return
	}

	return
}

func (s *sClickHouse) hasDB() error {
	if s.db == nil {
		return gerror.New("clickhouse db is nil")
	}
	return nil
}

func (s *sClickHouse) lazyInitInsertQueue() {
	if s.insertQueue == nil {
		s.insertQueue = gqueue.New()
	}
}
