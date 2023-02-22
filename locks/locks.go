package locks

import (
	"ptor/global"
	"sync"
)

var primaryRelationLocks = make(map[int]*sync.RWMutex)
var repoRelationLocks = make(map[int]*sync.RWMutex)

var PrimarySyncLock = &sync.RWMutex{}
var RepoSyncLock = &sync.RWMutex{}

func CreatePrimaryRelLocks() {
	for i := 0; i < global.CliOpts.ParallelWorkers; i++ {
		primaryRelationLocks[i] = &sync.RWMutex{}
	}
}

func AcquirePrimaryWorkerLock(worker_id int) {
	// fmt.Println("AcquireWorkerLock", worker_id)
	primaryRelationLocks[worker_id].Lock()
}

func ReleasePrimaryWorkerLock(worker_id int) {
	//fmt.Println("ReleaseWorkerLock", worker_id)
	primaryRelationLocks[worker_id].Unlock()
}

func CreateRepoRelLocks() {
	for i := 0; i < global.CliOpts.ParallelWorkers; i++ {
		repoRelationLocks[i] = &sync.RWMutex{}
	}
}

func AcquireRepoWorkerLock(worker_id int) {
	// fmt.Println("AcquireWorkerLock", worker_id)
	repoRelationLocks[worker_id].Lock()
}

func ReleaseRepoWorkerLock(worker_id int) {
	//fmt.Println("ReleaseWorkerLock", worker_id)
	repoRelationLocks[worker_id].Unlock()
}
