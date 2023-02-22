package util

import (
	"fmt"
	"os"
	"os/signal"
	"ptor/db"
	"ptor/global"
	"syscall"
)

type fn func()

func HandleCntrlC(f fn) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGSEGV)
	go func() {
		<-c
		f()
		os.Exit(1)
	}()
}

func QuitNice() {
	StopTasks()
	os.Exit(0)
}

func StopTasks() {
	global.PrimaryWorkerPool.Stop()
	err := db.ClosePrimaryPool()
	if err != nil {
		fmt.Println(err)
	}
}
