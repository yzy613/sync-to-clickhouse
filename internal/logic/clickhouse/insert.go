package clickhouse

import (
	"context"
	"github.com/bytedance/sonic"
	"github.com/gogf/gf/v2/container/gqueue"
	"github.com/gogf/gf/v2/errors/gerror"
	"github.com/gogf/gf/v2/frame/g"
	"os"
	"sort"
	"sync-to-clickhouse/internal/service"
	"sync-to-clickhouse/utility"
)

func (s *sClickHouse) lazyInitInsertQueue() {
	if s.insertQueue == nil {
		s.insertQueue = gqueue.New()
	}
}

type insertQueueData struct {
	Table string              `json:"table"`
	Data  []map[string]string `json:"data"`
}

type insertQueueDataSlice []insertQueueData

func (s insertQueueDataSlice) Len() int {
	return len(s)
}

func (s insertQueueDataSlice) Less(i, j int) bool {
	return s[i].Table < s[j].Table
}

func (s insertQueueDataSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *sClickHouse) pushInsertQueueDataSlice(data []insertQueueData) {
	s.lazyInitInsertQueue()
	for _, v := range data {
		s.insertQueue.Push(v)
	}
}

func (s *sClickHouse) popInsertQueueDataSlice(ctx context.Context) (data []insertQueueData) {
	s.lazyInitInsertQueue()

	s.popInsertMu.Lock()
	defer s.popInsertMu.Unlock()

	insertQueueLen := s.insertQueue.Len()
	if insertQueueLen == 0 {
		return
	}

	data = make([]insertQueueData, 0, insertQueueLen)
	for range insertQueueLen {
		v := s.insertQueue.Pop()
		if v == nil {
			continue
		}

		d, ok := v.(insertQueueData)
		if !ok {
			g.Log().Panic(ctx, "invalid insert queue data")
		}
		data = append(data, d)
	}

	return
}

func (s *sClickHouse) Insert(ctx context.Context, table string, data []map[string]string) (err error) {
	if table == "" || len(data) == 0 {
		err = gerror.New("invalid insert data")
		return
	}

	if err = s.hasDB(); err != nil {
		return
	}
	s.lazyInitInsertQueue()

	s.insertQueue.Push(insertQueueData{
		Table: table,
		Data:  data,
	})

	// auto flush
	err = func() (err error) {
		if s.flushCount == 0 {
			return
		}

		if s.insertQueue.Len() >= int64(s.flushCount) {
			if err = s.flushInsertQueue(ctx); err != nil {
				return
			}
		}

		return
	}()
	if err != nil {
		return
	}

	return
}

func (s *sClickHouse) flushInsertQueue(ctx context.Context) (err error) {
	if err = s.hasDB(); err != nil {
		return
	}
	s.lazyInitInsertQueue()

	var poppedSlice insertQueueDataSlice = s.popInsertQueueDataSlice(ctx)
	if len(poppedSlice) == 0 {
		return
	}

	g.Log().Info(ctx, "flush insert queue", len(poppedSlice))

	sort.Stable(poppedSlice)

	i := 0
	for i < len(poppedSlice) {
		tableName := poppedSlice[i].Table
		j := i + 1

		for j < len(poppedSlice) && poppedSlice[j].Table == tableName {
			j++
		}

		batch := poppedSlice[i:j]
		i = j

		tmp := make([]map[string]string, 0, len(batch))
		for _, v := range batch {
			tmp = append(tmp, v.Data...)
		}

		if len(tmp) == 0 {
			continue
		}

		stmt, args := utility.InsertStatement(tableName, tmp)

		if _, err := s.db.Exec(ctx, stmt, args); err != nil {
			s.pushInsertQueueDataSlice(batch)

			g.Log().Error(ctx, err)
			continue
		}

		if service.Cfg().IsClickHouseOptimizeTableAfterInsert(ctx) {
			if err := s.OptimizeTable(ctx, map[string]struct{}{tableName: {}}); err != nil {
				g.Log().Error(ctx, err)
			}
		}
	}

	return
}

func (s *sClickHouse) dumpInsertQueueToDisk(ctx context.Context) (err error) {
	if s.insertQueue == nil {
		return
	}

	data := s.popInsertQueueDataSlice(ctx)
	if len(data) == 0 {
		return
	}

	g.Log().Info(ctx, "dump insert queue", len(data))

	dataBytes, err := sonic.Marshal(data)
	if err != nil {
		return
	}

	if err = os.WriteFile(s.insertQueuePath, dataBytes, 0644); err != nil {
		return
	}

	return
}

func (s *sClickHouse) restoreInsertQueueFromDisk(ctx context.Context) (err error) {
	if !utility.IsPathExists(s.insertQueuePath) {
		return
	}

	dataBytes, err := os.ReadFile(s.insertQueuePath)
	if err != nil {
		return
	}

	var data []insertQueueData
	if err = sonic.Unmarshal(dataBytes, &data); err != nil {
		return
	}
	if len(data) == 0 {
		return
	}

	g.Log().Info(ctx, "restore insert queue", len(data))

	s.pushInsertQueueDataSlice(data)

	if err = os.Remove(s.insertQueuePath); err != nil {
		return
	}

	if err = s.flushInsertQueue(ctx); err != nil {
		return err
	}

	return
}
