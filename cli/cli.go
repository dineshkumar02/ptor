package cli

type CliArgs struct {
	RepoPgDsn       string `arg:"--repo-pgdsn,env:REPO_PGDSN" default:"postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"`
	PrimaryPgDsn    string `arg:"--primary-pgdsn,env:PRIMARY_PGDSN" default:"postgres://postgres:postgres@localhost:6432/postgres?sslmode=disable"`
	ParallelWorkers int    `arg:"--parallel-workers,env:PARALLEL_WORKERS" default:"10"`
	Init            bool   `arg:"--init,env:INIT" default:"false"`
	Reset           bool   `arg:"--reset,env:RESET" default:"false"`

	RTOTimeout     int `arg:"--rto-conn-timeout,env:TIMEOUT" default:"10"`
	WarmupDuration int `arg:"--warmup-duration,env:DURATION" default:"10"`

	InsertPercent int `arg:"--insert-percent,env:INSERT_PERCENT" default:"50"`
	UpdatePercent int `arg:"--update-percent,env:UPDATE_PERCENT" default:"30"`
	DeletePercent int `arg:"--delete-percent,env:DELETE_PERCENT" default:"20"`

	FullDataValidation bool `arg:"--full-data-validation,env:FULL_DATA_VALIDATION" default:"true"`
	AsyncRepoMode      bool `arg:"--async-repo-mode,env:ASYNC_REPO_MODE" default:"true"`

	CheckPrimaryLatency bool `arg:"--check-primary-latency,env:CHECK_PRIMARY_LATENCY" default:"false"`
}
