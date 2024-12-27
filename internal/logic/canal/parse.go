package canal

import (
	"context"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/withlin/canal-go/protocol/entry"
	"google.golang.org/protobuf/proto"
	"sync-to-clickhouse/internal/service"
)

func (s *sCanal) ParseEntries(
	ctx context.Context,
	schemaWhitelist,
	tableWhitelist map[string]struct{},
	entries []entry.Entry,
) (err error) {
	for _, e := range entries {
		entryType := e.GetEntryType()
		if entryType == entry.EntryType_TRANSACTIONBEGIN ||
			entryType == entry.EntryType_TRANSACTIONEND {
			continue
		}

		rowChange := &entry.RowChange{}
		if err = proto.Unmarshal(e.GetStoreValue(), rowChange); err != nil {
			return
		}

		if rowChange == nil {
			continue
		}

		header := e.GetHeader()

		if len(schemaWhitelist) > 0 {
			if _, ok := schemaWhitelist[header.GetSchemaName()]; !ok {
				continue
			}
		}
		if len(tableWhitelist) > 0 {
			if _, ok := tableWhitelist[header.GetTableName()]; !ok {
				continue
			}
		}

		eventType := rowChange.GetEventType()

		rowDataSlice := rowChange.GetRowDatas()
		rows := make([]map[string]string, 0, len(rowDataSlice))

		for _, rowData := range rowDataSlice {
			switch eventType {
			case entry.EventType_UPDATE:
				row := s.ReduceColumns(rowData.GetAfterColumns())
				if err = service.ClickHouse().Insert(ctx, header.GetTableName(), []map[string]string{row}); err != nil {
					g.Log().Error(ctx, err)
					continue
				}

			case entry.EventType_DELETE:
				// none

			case entry.EventType_INSERT:
				row := s.ReduceColumns(rowData.GetAfterColumns())
				if row != nil {
					rows = append(rows, row)
				}
			}
		}

		switch eventType {
		case entry.EventType_INSERT:
			if err = service.ClickHouse().Insert(ctx, header.GetTableName(), rows); err != nil {
				g.Log().Error(ctx, err)
				continue
			}
		}
	}

	return
}
