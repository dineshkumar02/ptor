package calc

import (
	"context"
	"fmt"
	"os"
	"ptor/db"
	"ptor/global"
	"ptor/repo"
	"time"
)

func ServiceAvailable(lost_err_time time.Time) time.Duration {
	var service_conn_time *time.Time
	var err error
	// Make a fresh connection to primary/read-write postgres

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(global.CliOpts.RTOTimeout)*time.Millisecond)

	for {

		service_conn_time, err = db.CheckPrimaryConnTime(ctx)
		if err != nil {
			//fmt.Println("Error while making new primary read-write connection: ", err)
		} else {
			break
		}
		select {
		case <- ctx.Done():
			fmt.Println("Timed out waiting for leader recovery, last connection attempt error: ", err.Error())
			os.Exit(1)
		default:
		}
	}

	return service_conn_time.Sub(lost_err_time)
}

func RTO(lost_err_time time.Time) time.Duration {
	var fresh_conn_time *time.Time
	var err error
	// Make a fresh connection to primary/read-write postgres
	for {
		fresh_conn_time, err = db.MakeNewPrimaryReadWriteConn()
		if err != nil {
			//fmt.Println("Error while making new primary read-write connection: ", err)
		} else {
			break
		}
	}

	// If there is a validation delay specified, then add that delay to the RTO
	if global.CliOpts.ValidationDelay > 0 {
		fresh_conn_time.Add(time.Duration(global.CliOpts.ValidationDelay) * time.Second)
	}

	// Calculate RTO
	return fresh_conn_time.Sub(lost_err_time)
}

func RPO() (time.Duration, int) {
	// RPO calculation is bit tricky.
	// Once we get a fresh new connection to primary, then we have to compare the results with the repo results.

	var total_duration time.Duration
	var total_data_los_bytes int

	for i := 0; i < global.CliOpts.ParallelWorkers; i++ {
		// Get the max date from primary
		max_primary_date, err := db.GetMaxDateFromPrimary(i)
		if err != nil {
			fmt.Println("Error while getting max date from primary: ", err)
		}

		// Compare the results with repo
		duration, data_los_bytes, err := repo.CompareDataWithPrimary(i, max_primary_date)
		if err != nil {
			fmt.Println("Error while comparing data with primary: ", err)
		}

		total_duration += duration
		total_data_los_bytes += data_los_bytes
	}

	return total_duration, total_data_los_bytes
}
