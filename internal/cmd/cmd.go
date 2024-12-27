package cmd

import (
	"context"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/os/gcmd"
	"github.com/withlin/canal-go/protocol"
	"os"
	"os/signal"
	"sync-to-clickhouse/internal/consts"
	"sync-to-clickhouse/internal/service"
	"sync-to-clickhouse/utility"
	"syscall"
	"time"
)

var (
	Main = gcmd.Command{
		Name: consts.ProjName,
		Func: func(ctx context.Context, parser *gcmd.Parser) (err error) {
			if err = service.ClickHouse().SetDBLink(service.Cfg().DBLink(ctx)); err != nil {
				return
			}

			if err = service.ClickHouse().RestoreFromDisk(ctx); err != nil {
				return
			}

			{
				var (
					count    uint
					interval time.Duration
				)
				count, interval, err = service.Cfg().ClickHouseAutoFlush(ctx)
				if err != nil {
					return
				}
				service.ClickHouse().SetAutoFlush(ctx, count, interval)
				g.Log().Info(ctx, "auto flush set", count, interval.String())
			}

			signalCh := make(chan os.Signal, 1)
			signal.Notify(signalCh, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

			loopCtx, loopCancel := context.WithCancel(ctx)
			overCh := make(chan struct{}, 1)
			doneCh := make(chan struct{}, 1)

			// handle signal
			go func() {
				<-signalCh

				loopCancel()
				g.Log().Info(ctx, "signal received, exiting...")

				<-overCh

				err = service.ClickHouse().DumpToDisk(ctx)
				g.Log().Info(ctx, "dump to disk")

				doneCh <- struct{}{}
			}()

			schema := utility.CommaStringToSet(service.Cfg().CanalSchema(ctx))
			table := utility.CommaStringToSet(service.Cfg().CanalTable(ctx))

			{
				var interval time.Duration
				interval, err = service.Cfg().ClickHouseOptimizeTable(ctx)
				if err != nil {
					return
				}
				service.ClickHouse().SetAutoOptimizeTable(ctx, interval, table)
				g.Log().Info(ctx, "auto optimize table set", interval.String())
			}

			// canal
		canalLoop:
			for {
				select {
				case <-loopCtx.Done():
					break canalLoop
				default:
				}

				func() {
					connector := service.Canal().NewConnector(ctx)

					g.Log().Info(ctx, "connecting to canal")
					if err = connector.Connect(); err != nil {
						g.Log().Error(ctx, err)
						return
					}

					if err = connector.Subscribe(service.Cfg().CanalFilter(ctx)); err != nil {
						g.Log().Error(ctx, err)
						return
					}

				getLoop:
					for {
						select {
						case <-loopCtx.Done():
							break getLoop
						default:
						}

						var message *protocol.Message
						message, err = connector.Get(100, nil, nil)
						if err != nil {
							break
						}

						if err = service.Canal().ParseEntries(ctx, schema, table, message.Entries); err != nil {
							err = nil
							continue
						}

						if len(message.Entries) == 0 {
							time.Sleep(time.Second)
						}
					}
				}()

				select {
				case <-loopCtx.Done():
					break canalLoop
				default:
				}
				time.Sleep(time.Second)
			}

			overCh <- struct{}{}
			<-doneCh

			return
		},
	}
)
