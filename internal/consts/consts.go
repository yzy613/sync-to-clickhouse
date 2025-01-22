package consts

import (
	"github.com/gogf/gf/v2"
	"runtime"
)

const (
	ProjName = "sync-to-clickhouse"
	Version  = "0.1.5"
)

var (
	GitTag      = ""
	GitCommit   = ""
	BuildTime   = ""
	Description = "Version: " + Version +
		"\nGo Version: " + runtime.Version() +
		"\nGoFrame Version: " + gf.VERSION +
		"\nGit Tag: " + GitTag +
		"\nGit Commit: " + GitCommit +
		"\nBuild Time: " + BuildTime
)
