package main

import (
	"fmt"
	"os"
	"ptor/cli"
	"ptor/db"
	"ptor/global"
	"ptor/locks"
	"ptor/pool"
	"ptor/repo"
	"ptor/task"
	"ptor/util"

	"github.com/alexflint/go-arg"
	"github.com/gammazero/workerpool"
)

var args cli.CliArgs

func init() {
	arg.MustParse(&args)
	global.CliOpts = args

	if args.Init {
		err := db.InitSchema()
		if err != nil {
			fmt.Println(err)
		}

		err = repo.InitSchema()
		if err != nil {
			fmt.Println(err)
		}
		os.Exit(0)
	}

	if args.Reset {
		err := db.DropSchema()
		if err != nil {
			fmt.Println(err)
		}

		err = repo.DropSchema()
		if err != nil {
			fmt.Println(err)
		}
		os.Exit(0)
	}

	//Create lock map
	locks.CreatePrimaryRelLocks()
	locks.CreateRepoRelLocks()

	//Create connection pool for the primary
	pool.PrimaryPool = new(pool.Pool)
	err := pool.PrimaryPool.InitPrimaryPool()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//Create connection pool for the repo
	pool.RepoPool = new(pool.Pool)
	err = pool.RepoPool.InitRepoPool()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	util.HandleCntrlC(func() {
		util.QuitNice()
	})

	// Check the percentages of ins,upd and del
	if args.InsertPercent+args.DeletePercent+args.UpdatePercent != 100 {
		fmt.Println("Insert, Update and Delete percentages should add up to 100")
		os.Exit(1)
	}

}

func bootstrapChecks() {
	// Check repo and primary connections
	fmt.Println("Trying to connect to primary instance ...")
	err := db.CheckConnection()
	if err != nil {
		fmt.Println("Error connecting to primary instance: ", err)
		os.Exit(1)
	}

	fmt.Println("Trying to connect to repo instance ...")
	err = repo.CheckConnection()
	if err != nil {
		fmt.Println("Error connecting to repo instance: ", err)
		os.Exit(1)
	}

	// Check if schema present
	fmt.Println("Checking for ptor schema on primary instance ...")
	err = db.CheckPtorSchema()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Checking for ptor schema on repo instance ...")
	err = repo.CheckPtorSchema()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}

func main() {
	global.PrimaryWorkerPool = workerpool.New(global.CliOpts.ParallelWorkers)
	global.RepoWorkerPool = workerpool.New(1)
	bootstrapChecks()

	if global.CliOpts.CheckPrimaryLatency {
		duration, err := task.CheckPrimaryLatency()

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("Primary latency: ", duration)
		os.Exit(0)
	}

	task.Run()
}
