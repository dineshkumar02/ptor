package db

import (
	"context"
	"errors"
	"fmt"
	"ptor/catalog"
	"ptor/global"
	"ptor/locks"
	"ptor/pool"
	"ptor/repo"
	"time"

	"github.com/jackc/pgx/v4"
)

func CheckConnection() error {

	conn, err := pool.PrimaryPool.GetConnectionFromPrimaryPool()
	if err != nil {
		return err
	}
	defer conn.Release()

	err = conn.Ping(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func CheckPtorSchema() error {

	conn, err := pool.PrimaryPool.GetConnectionFromPrimaryPool()
	if err != nil {
		return err
	}

	defer conn.Release()

	// Check if ptor schema exists
	var schema string
	err = conn.QueryRow(context.Background(), "SELECT COALESCE(schema_name,'') FROM information_schema.schemata WHERE schema_name = 'ptor'").Scan(&schema)
	if err != nil {

		if err == pgx.ErrNoRows {
			return fmt.Errorf("primary instance is missing the ptor schema, run the initialization command first")
		}

		return err
	}

	return nil
}

func DoInsert(worker_id int) error {

	// Acquire lock on the worker relation
	locks.AcquirePrimaryWorkerLock(worker_id)
	defer locks.ReleasePrimaryWorkerLock(worker_id)

	conn, err := pool.PrimaryPool.GetConnectionFromPrimaryPool()
	if err != nil {
		return err
	}
	defer conn.Release()

	// Start transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	var id int
	var t time.Time

	// Insert record
	err = tx.QueryRow(context.Background(), fmt.Sprintf("INSERT INTO ptor.worker_%d (id, t) VALUES( (SELECT COALESCE(max(id),0)+1 FROM ptor.worker_%d),    'a') RETURNING id,last_update", worker_id, worker_id)).Scan(&id, &t)
	if err != nil {
		return err
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}

	// Save record into repo database asynchronously, if it is successful then commit the primary transaction
	if global.CliOpts.AsyncRepoMode {
		locks.RepoSyncLock.Lock()
		global.RepoWorkerPool.Submit(func() {
			err = repo.SaveInsState(&worker_id, &id, &t)
			if err != nil {
				fmt.Println(err)
			}
		})
		locks.RepoSyncLock.Unlock()
	} else {
		err = repo.SaveInsState(&worker_id, &id, &t)
		if err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

func DoUpdate(worker_id int) error {

	// Acquire lock on the worker relation
	locks.AcquirePrimaryWorkerLock(worker_id)
	defer locks.ReleasePrimaryWorkerLock(worker_id)

	var rec_id int
	var t time.Time

	conn, err := pool.PrimaryPool.GetConnectionFromPrimaryPool()
	if err != nil {
		return err
	}
	defer conn.Release()

	// Start transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	// Update record
	err = tx.QueryRow(context.Background(), fmt.Sprintf(`WITH random_rec AS (SELECT (random() * ((SELECT max(id) FROM ptor.worker_%d)))::int as id) UPDATE ptor.worker_%d SET t='b', last_update=(now() AT TIME ZONE 'UTC'::text) WHERE id = (SELECT CASE WHEN id=0 THEN 1 ELSE id END FROM random_rec) RETURNING id, last_update;`, worker_id, worker_id)).Scan(&rec_id, &t)
	if err != nil {

		//XXX
		// If no rows are updated, then return nil
		// We may get into this situation if the table is empty
		// While we are running the `ptor` first time, then we may get into this situation
		// Where `update` wins over the parallel `insert`
		if err == pgx.ErrNoRows {
			return nil
		}

		return err
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}

	if global.CliOpts.AsyncRepoMode {
		locks.RepoSyncLock.Lock()
		global.RepoWorkerPool.Submit(func() {
			err = repo.SaveUpdState(&worker_id, &rec_id, &t)
			if err != nil {
				fmt.Println(err)
			}
		})
		locks.RepoSyncLock.Unlock()
	} else {
		err = repo.SaveUpdState(&worker_id, &rec_id, &t)
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func DoDelete(worker_id int) error {

	// Acquire lock on the worker relation
	locks.AcquirePrimaryWorkerLock(worker_id)
	defer locks.ReleasePrimaryWorkerLock(worker_id)

	var rec_id int

	conn, err := pool.PrimaryPool.GetConnectionFromPrimaryPool()
	if err != nil {
		return err
	}
	defer conn.Release()

	// Start transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	// Delete Record
	err = tx.QueryRow(context.Background(), fmt.Sprintf(`WITH random_rec AS (SELECT (random() * ( (SELECT max(id) FROM ptor.worker_%d)))::int as id) DELETE FROM ptor.worker_%d WHERE id = (SELECT CASE WHEN id=0 THEN 1 ELSE id END FROM random_rec) RETURNING id;`, worker_id, worker_id)).Scan(&rec_id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return err
	}

	// Delete the record in repo database, if it is successful then commit the primary transaction
	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}

	if global.CliOpts.AsyncRepoMode {
		locks.RepoSyncLock.Lock()
		global.RepoWorkerPool.Submit(func() {
			err = repo.SaveDelState(&worker_id, &rec_id)
			if err != nil {
				fmt.Println(err)
			}
		})
		locks.RepoSyncLock.Unlock()
	} else {
		err = repo.SaveDelState(&worker_id, &rec_id)
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func InitSchema() error {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.PrimaryPgDsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), catalog.SchemaSQL)
	if err != nil {
		return err
	}

	for i := 0; i < global.CliOpts.ParallelWorkers; i++ {
		_, err = conn.Exec(context.Background(), fmt.Sprintf(catalog.TableSQL, i))
		if err != nil {
			return err
		}
	}
	return nil
}

func DropSchema() error {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.PrimaryPgDsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS ptor CASCADE")
	if err != nil {
		return err
	}

	return nil
}

func ClosePrimaryPool() error {
	return pool.PrimaryPool.ClosePrimaryPool()
}

func CloseRepoPool() error {
	return pool.RepoPool.CloseRepoPool()
}

func CheckPrimaryConnTime() (*time.Time, error) {

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(global.CliOpts.RTOTimeout)*time.Millisecond)
	conn, err := pgx.Connect(ctx, global.CliOpts.PrimaryPgDsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	connTime := time.Now()
	return &connTime, nil
}

func MakeNewPrimaryReadWriteConn() (*time.Time, error) {

	ctx, _ := context.WithTimeout(context.Background(), time.Duration(global.CliOpts.RTOTimeout)*time.Millisecond)
	conn, err := pgx.Connect(ctx, global.CliOpts.PrimaryPgDsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	var isRecovery bool
	// Check whether the database is in read-write mode
	err = conn.QueryRow(context.Background(), "SELECT pg_is_in_recovery()").Scan(&isRecovery)
	if err != nil {
		return nil, err
	}

	if isRecovery {
		return nil, errors.New("database is in recovery mode")
	}

	primTime := time.Now()
	return &primTime, nil
}

func GetMaxDateFromPrimary(worker_id int) (time.Time, error) {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.PrimaryPgDsn)
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close(context.Background())

	var maxDate time.Time
	err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT COALESCE(max(last_update), '12-12-1212 12:12:12') FROM ptor.worker_%d", worker_id)).Scan(&maxDate)
	if err != nil {
		return time.Time{}, err
	}

	return maxDate, nil
}

func GetPrimaryRowCount(worker_id int) (int, error) {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.PrimaryPgDsn)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())

	var rowCount int
	err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM ptor.worker_%d", worker_id)).Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func GetPrimaryRelationCheckSum(worker_id int) (string, error) {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.PrimaryPgDsn)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())

	var checkSum string
	err = conn.QueryRow(context.Background(), fmt.Sprintf("WITH sort_data AS (SELECT id||trim(t)||last_update as data FROM ptor.worker_%d ORDER BY last_update DESC) SELECT COALESCE(md5(array_agg(sort_data.data)::text),'') FROM sort_data;", worker_id)).Scan(&checkSum)
	if err != nil {
		return "", err
	}

	return checkSum, nil
}
