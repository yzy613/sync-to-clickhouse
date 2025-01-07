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

type insertQueueData struct {
	Table string              `json:"table"`
	Data  []map[string]string `json:"data"`
}

func (s *sClickHouse) lazyInitInsertQueue() {
	if s.insertQueue == nil {
		s.insertQueue = gqueue.New()
	}
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

	var poppedSlice insertQueueDataSlice
	func() {
		s.popInsertMu.Lock()
		defer s.popInsertMu.Unlock()

		insertQueueLen := s.insertQueue.Len()
		if insertQueueLen == 0 {
			return
		}

		poppedSlice = make(insertQueueDataSlice, 0, insertQueueLen)
		for i := int64(0); i < insertQueueLen; i++ {
			v := s.insertQueue.Pop()
			if v == nil {
				continue
			}

			d, ok := v.(insertQueueData)
			if !ok {
				g.Log().Panic(ctx, "invalid insert queue data")
			}
			poppedSlice = append(poppedSlice, d)
		}
	}()
	if poppedSlice == nil || len(poppedSlice) == 0 {
		return
	}

	g.Log().Info(ctx, "flush insert queue", len(poppedSlice))

	sort.Sort(poppedSlice)

	var (
		lastTable string
		lastIdx   = 0
	)
	for i := range poppedSlice {
		if lastTable == poppedSlice[i].Table {
			continue
		}

		if i != 0 {
			arr := poppedSlice[lastIdx:i]

			tmp := make([]map[string]string, 0, len(arr))
			for _, v := range arr {
				tmp = append(tmp, v.Data...)
			}

			if lastTable == "" || len(tmp) == 0 {
				continue
			}

			stmt, args := utility.InsertStatement(lastTable, tmp)

			if _, err = s.db.Exec(ctx, stmt, args); err != nil {
				g.Log().Error(ctx, err)
				s.pushInsertQueueDataSlice(arr)
			} else {
				if service.Cfg().IsClickHouseOptimizeTableAfterInsert(ctx) {
					go func() {
						if err := s.OptimizeTable(
							context.WithoutCancel(ctx),
							map[string]struct{}{lastTable: {}},
						); err != nil {
							g.Log().Error(ctx, err)
						}
					}()
				}
			}
		}

		lastTable = poppedSlice[i].Table
		lastIdx = i
	}

	// last
	if lastTable != "" && lastIdx < len(poppedSlice) {
		arr := poppedSlice[lastIdx:]

		tmp := make([]map[string]string, 0, len(arr))
		for _, v := range arr {
			tmp = append(tmp, v.Data...)
		}

		if len(tmp) == 0 {
			return
		}

		stmt, args := utility.InsertStatement(lastTable, tmp)

		if _, err = s.db.Exec(ctx, stmt, args); err != nil {
			g.Log().Error(ctx, err)
			s.pushInsertQueueDataSlice(arr)
		} else {
			if service.Cfg().IsClickHouseOptimizeTableAfterInsert(ctx) {
				go func() {
					if err := s.OptimizeTable(
						context.WithoutCancel(ctx),
						map[string]struct{}{lastTable: {}},
					); err != nil {
						g.Log().Error(ctx, err)
					}
				}()
			}
		}
	}

	return
}

func (s *sClickHouse) dumpInsertQueueToDisk(ctx context.Context) (err error) {
	if s.insertQueue == nil {
		return
	}

	var data []insertQueueData
	func() {
		s.popInsertMu.Lock()
		defer s.popInsertMu.Unlock()

		insertQueueLen := s.insertQueue.Len()
		if insertQueueLen == 0 {
			return
		}

		data = make([]insertQueueData, 0, insertQueueLen)
		for i := int64(0); i < insertQueueLen; i++ {
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
	}()
	if data == nil || len(data) == 0 {
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
	if err = os.Remove(s.insertQueuePath); err != nil {
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

	if err = s.flushInsertQueue(ctx); err != nil {
		return err
	}

	return
}
