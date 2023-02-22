package task

import (
	"bytes"
	"fmt"
	"math/rand"
	"ptor/calc"
	"ptor/db"
	"ptor/global"
	"ptor/repo"
	"ptor/util"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/Delta456/box-cli-maker"
	"github.com/briandowns/spinner"
	"github.com/dariubs/percent"
	"github.com/fatih/color"
)

func getRandomNumber() int {
	rand.Seed(time.Now().UnixNano())
	min := 1
	max := 100
	return rand.Intn(max-min+1) + min
}

func Run() {
	task_done := make(chan bool)

	var insert_range int
	var update_range int
	var delete_range int

	insert_count := 0
	update_count := 0
	delete_count := 0

	var workerCnt = 0
	var err error
	var error_time time.Time

	// defer func() {
	// 	fmt.Println("Insert count: ", insert_count)
	// 	fmt.Println("Update count: ", update_count)
	// 	fmt.Println("Delete count: ", delete_count)
	// }()

	insert_range = (int)(percent.Percent(global.CliOpts.InsertPercent, 100))
	update_range = (int)(insert_range + (int)(percent.Percent(global.CliOpts.UpdatePercent, 100)))
	delete_range = (int)(update_range + (int)(percent.Percent(global.CliOpts.DeletePercent, 100)))

	s := spinner.New(spinner.CharSets[9], 100*time.Millisecond)

	go func() {
		s.Prefix = color.HiCyanString("Warming up instance...")
		s.Start()
		time.Sleep(time.Duration(global.CliOpts.WarmupDuration) * time.Second)
		s.Stop()
		task_done <- true
	}()

	for {
		if (workerCnt + 1) == global.CliOpts.ParallelWorkers {
			workerCnt = 0
		} else {
			workerCnt++
		}

		select {
		case <-task_done:
			fmt.Println(color.HiBlueString("\nDo the failover/switchover from Tessell UI"))
			s.Prefix = color.HiCyanString("Data loading...")
			s.Start()
			//util.StopTasks()
			//return
		default:
		}

		// Generate random number between 1 to 100
		random_number := getRandomNumber()
		if random_number <= insert_range {
			if !global.PrimaryWorkerPool.Stopped() {
				global.PrimaryWorkerPool.Submit(func() {
					err = db.DoInsert(workerCnt)
					insert_count++
				})
			}
		} else if random_number <= update_range {
			if !global.PrimaryWorkerPool.Stopped() {
				global.PrimaryWorkerPool.Submit(func() {
					err = db.DoUpdate(workerCnt)
					update_count++
				})
			}
		} else if random_number <= delete_range {
			if !global.PrimaryWorkerPool.Stopped() {
				global.PrimaryWorkerPool.Submit(func() {
					err = db.DoDelete(workerCnt)
					delete_count++
				})
			}
		} else {
			// We should not come here
		}

		if err != nil {
			//Stop spinner
			s.Stop()
			error_time = time.Now()
			fmt.Println(color.HiRedString("Got the connectivity error"))
			util.StopTasks()
			break
		}
	}
	fmt.Println(color.HiGreenString("Checking DNS Availability..."))
	serviceAvailable := calc.ServiceAvailable(error_time)

	// Calculate RTO
	fmt.Println(color.HiGreenString("Checking RTO..."))
	rto := calc.RTO(error_time)
	// Wait for the repo to get sync
	for {
		//Wait until the repo worker pool is empty
		var queueSize = global.RepoWorkerPool.WaitingQueueSize()
		s.Prefix = color.HiCyanString(fmt.Sprintf("Waiting for the repo db sync. Queue size %d", queueSize))
		s.Start()
		if queueSize == 0 {
			s.Stop()
			// Waiting for any pending tasks to complete
			s.Prefix = "Waiting for any pending tasks to get complete..."
			s.Start()
			global.RepoWorkerPool.StopWait()
			// Stop the spinner
			s.Stop()
			break
		}
		time.Sleep(1 * time.Second)
	}

	fmt.Println(color.HiGreenString("Checking RPO..."))
	// Calculate RPO
	rpo, data_los_bytes := calc.RPO()
	buf := new(bytes.Buffer)
	w := tabwriter.NewWriter(buf, 0, 0, 1, ' ', 0)
	fmt.Fprintln(w, "\t", "")
	fmt.Fprintln(w, "SLA\t", fmt.Sprintf("%.5f", percent.PercentOf(86400*1000-int(rto.Milliseconds()), 86400*1000)))
	fmt.Fprintln(w, "DNS Available\t", serviceAvailable)
	fmt.Fprintln(w, "RTO\t", rto)
	fmt.Fprintln(w, "RPO\t", rpo)
	fmt.Fprintln(w, "Quick Data Loss Check (bytes)\t", data_los_bytes)

	w.Flush()

	summary := box.New(box.Config{Px: 1, Py: 1, Type: "Single", Color: "Cyan", TitlePos: "Top", ContentAlign: "Left"})
	summary.Println("Summary", buf.String())

	if global.CliOpts.FullDataValidation {
		s.Prefix = color.HiCyanString("Data validation is in progress...")
		s.Start()
		tab, success := fullDataValidation()

		//Stop the spinner
		s.Stop()
		if success {
			bx := box.New(box.Config{Px: 1, Py: 1, Type: "Single", Color: "Green", TitlePos: "Top", ContentAlign: "Left"})
			bx.Println("Data Loss", "NO DATA LOSS")
		} else {
			bx := box.New(box.Config{Px: 1, Py: 1, Type: "Single", Color: "Red", TitlePos: "Top", ContentAlign: "Left"})
			bx.Println("Data Loss", "DATA LOSS DETECTED")
			fmt.Println(color.HiRedString("Table " + tab + " is not in sync"))
		}
	}
}

func fullDataValidation() (string, bool) {
	for i := 0; i < global.CliOpts.ParallelWorkers; i++ {

		primaryCheckSum, err := db.GetPrimaryRelationCheckSum(i)
		if err != nil {
			fmt.Println(err)
			break
		}
		repoCheckSum, err := repo.GetRepoRelationCheckSum(i)
		if err != nil {
			fmt.Println(err)
			break
		}

		primaryRowCount, err := db.GetPrimaryRowCount(i)
		if err != nil {
			fmt.Println(err)
			break
		}

		repoRowCount, err := repo.GetRepoRowCount(i)
		if err != nil {
			fmt.Println(err)
			break
		}

		if primaryRowCount != repoRowCount {
			return "ptor.worker_" + strconv.Itoa(i), false
		}

		if primaryCheckSum != repoCheckSum {
			return "ptor.worker_" + strconv.Itoa(i), false
		}
	}
	return "", true
}

func CheckPrimaryLatency() (time.Duration, error) {
	// Create a new spinner
	s := spinner.New(spinner.CharSets[9], 100*time.Millisecond)

	// Start the spinner
	s.Prefix = color.HiCyanString("Checking primary latency...")
	s.Start()
	defer s.Stop()

	t, err := db.MakeNewPrimaryReadWriteConn()
	if err != nil {
		return 0, err
	}

	return time.Since(*t), nil
}
