package canal

import (
	"github.com/withlin/canal-go/protocol/entry"
)

func (s *sCanal) ReduceColumns(columns []*entry.Column,
) (result map[string]string) {
	result = make(map[string]string, len(columns))
	for _, col := range columns {
		result[col.GetName()] = col.GetValue()
	}
	return
}

//func (s *sCanal) ReduceColumns(columns []*entry.Column,
//) (result map[string]string, key map[string]string) {
//	result = make(map[string]string)
//	key = make(map[string]string)
//	for _, col := range columns {
//		if col.GetIsKey() {
//			key[col.GetName()] = col.GetValue()
//		}
//
//		result[col.GetName()] = col.GetValue()
//	}
//	return
//}
