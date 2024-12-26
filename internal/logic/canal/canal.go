package canal

import (
	"context"
	"github.com/withlin/canal-go/client"
	"sync-mysql-to-clickhouse/internal/service"
)

type sCanal struct{}

func New() *sCanal {
	return &sCanal{}
}

func init() {
	service.RegisterCanal(New())
}

func (s *sCanal) NewConnector(ctx context.Context) *client.SimpleCanalConnector {
	return client.NewSimpleCanalConnector(service.Cfg().Canal(ctx))
}
