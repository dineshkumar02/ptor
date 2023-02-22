package global

import (
	"ptor/cli"

	"github.com/gammazero/workerpool"
)

var (
	CliOpts           cli.CliArgs
	PrimaryWorkerPool *workerpool.WorkerPool
	RepoWorkerPool    *workerpool.WorkerPool
)
