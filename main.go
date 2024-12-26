package main

import (
	"context"
	_ "github.com/gogf/gf/contrib/drivers/clickhouse/v2"
	"sync-mysql-to-clickhouse/internal/cmd"
	_ "sync-mysql-to-clickhouse/internal/logic"
)

func main() {
	err := cmd.Main.AddCommand(&cmd.Install, &cmd.Uninstall, &cmd.Version)
	if err != nil {
		panic(err)
	}
	cmd.Main.Run(context.Background())
}
