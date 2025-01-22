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
	for _, ent := range entries {
		switch ent.GetEntryType() {
		case entry.EntryType_TRANSACTIONBEGIN, entry.EntryType_TRANSACTIONEND:
			continue
		}

		rowChange := &entry.RowChange{}
		if err = proto.Unmarshal(ent.GetStoreValue(), rowChange); err != nil {
			return
		}
		if rowChange == nil {
			continue
		}

		header := ent.GetHeader()

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
		insertRows := make([]map[string]string, 0, len(rowDataSlice))

		for _, rowData := range rowDataSlice {
			switch eventType {
			case entry.EventType_INSERT, entry.EventType_UPDATE:
				if row := s.ReduceColumns(rowData.GetAfterColumns()); row != nil {
					insertRows = append(insertRows, row)
				}

			case entry.EventType_DELETE:
				// none
			}
		}

		if len(insertRows) > 0 {
			if err = service.ClickHouse().Insert(ctx, header.GetTableName(), insertRows); err != nil {
				g.Log().Error(ctx, err)
			}
		}
	}

	return
}
