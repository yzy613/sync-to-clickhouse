package clickhouse

import (
	"context"
	"github.com/gogf/gf/v2/container/gqueue"
	"github.com/gogf/gf/v2/database/gdb"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gcron"
	"golang.org/x/sync/errgroup"
	"sync"
	"sync-to-clickhouse/internal/service"
)

type sClickHouse struct {
	db gdb.DB

	insertQueue     *gqueue.Queue
	insertQueuePath string
	popInsertMu     sync.Mutex

	flushCount uint

	crontab            *gcron.Cron
	flushEntry         *gcron.Entry
	optimizeTableEntry *gcron.Entry
}

func New() *sClickHouse {
	return &sClickHouse{
		insertQueuePath: "insert_queue.json",
	}
}

func init() {
	service.RegisterClickHouse(New())
}

func (s *sClickHouse) hasDB() error {
	if s.db == nil {
		return gerror.New("clickhouse db is nil")
	}
	return nil
}

func (s *sClickHouse) SetDBLink(link string) (err error) {
	s.db, err = gdb.New(gdb.ConfigNode{Link: link})
	return
}

func (s *sClickHouse) Flush(ctx context.Context) error {
	return s.flushInsertQueue(ctx)
}

func (s *sClickHouse) OptimizeTable(ctx context.Context, table map[string]struct{}) (err error) {
	if err = s.hasDB(); err != nil {
		return
	}

	g.Log().Info(ctx, "optimize table...")

	for k := range table {
		if _, err = s.db.Exec(ctx, "OPTIMIZE TABLE "+k+" FINAL"); err != nil {
			return
		}
	}

	g.Log().Info(ctx, "optimize table done")

	return
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
