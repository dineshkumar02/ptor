package repo

import (
	"context"
	"fmt"
	"ptor/catalog"
	"ptor/global"
	"ptor/locks"
	"ptor/pool"
	"time"

	"github.com/jackc/pgx/v4"
)

func CheckConnection() error {

	conn, err := pool.RepoPool.GetConnectionFromRepoPool()
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

	conn, err := pool.RepoPool.GetConnectionFromRepoPool()
	if err != nil {
		return err
	}

	defer conn.Release()

	// Check if ptor schema exists
	var schema string
	err = conn.QueryRow(context.Background(), "SELECT COALESCE(schema_name,'') FROM information_schema.schemata WHERE schema_name = 'ptor'").Scan(&schema)
	if err != nil {

		if err == pgx.ErrNoRows {
			return fmt.Errorf("repo instance is missing the ptor schema, run the initialization command first")
		}
		return err
	}

	return nil
}

//XXX
//Repo database should always be up and running.
//Repo database should not throw any error.

func SaveInsState(worker_id *int, id *int, t *time.Time) error {

	// Acquire lock on the worker relation
	locks.AcquireRepoWorkerLock(*worker_id)
	defer locks.ReleaseRepoWorkerLock(*worker_id)

	conn, err := pool.RepoPool.GetConnectionFromRepoPool()
	if err != nil {
		return err
	}
	defer conn.Release()

	// Save insert state
	_, err = conn.Exec(context.Background(), fmt.Sprintf("INSERT INTO ptor.worker_%d (id, t, last_update) VALUES( $1, 'a', $2)", *worker_id), *id, *t)
	return err
}

func SaveUpdState(worker_id *int, rec_id *int, t *time.Time) error {
	// Acquire lock on the worker relation
	locks.AcquireRepoWorkerLock(*worker_id)
	defer locks.ReleaseRepoWorkerLock(*worker_id)

	// Connect to repo postgres
	conn, err := pool.RepoPool.GetConnectionFromRepoPool()
	if err != nil {
		return err
	}
	defer conn.Release()

	// Save update state
	_, err = conn.Exec(context.Background(), fmt.Sprintf("UPDATE ptor.worker_%d SET t = 'b', last_update=$2 WHERE id = $1", *worker_id), *rec_id, *t)
	return err
}

func SaveDelState(worker_id *int, rec_id *int) error {
	// Acquire lock on the worker relation
	locks.AcquireRepoWorkerLock(*worker_id)
	defer locks.ReleaseRepoWorkerLock(*worker_id)
	// Connect to repo postgres
	// Connect to repo postgres
	conn, err := pool.RepoPool.GetConnectionFromRepoPool()
	if err != nil {
		return err
	}
	defer conn.Release()

	// Save delete state
	_, err = conn.Exec(context.Background(), fmt.Sprintf("DELETE FROM ptor.worker_%d WHERE id=$1", *worker_id), *rec_id)
	if err != nil {
		return err
	}

	// wait for commit
	return err
}

func InitSchema() error {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.RepoPgDsn)
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
	conn, err := pgx.Connect(context.Background(), global.CliOpts.RepoPgDsn)
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

func GetMaxDateFromRepo(worker_id int) (time.Time, error) {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.RepoPgDsn)
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close(context.Background())

	var maxDate time.Time
	err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT max(last_update) FROM ptor.worker_%d", worker_id)).Scan(&maxDate)
	if err != nil {
		return time.Time{}, err
	}

	return maxDate, nil
}

func CompareDataWithPrimary(worker_id int, primary_max_time time.Time) (duration time.Duration, data_los_bytes int, err error) {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.RepoPgDsn)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close(context.Background())

	var repo_max_time time.Time

	err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT COALESCE(max(last_update),'12-12-1212 12:12:12') FROM ptor.worker_%d WHERE last_update >= $1", worker_id), primary_max_time).Scan(&repo_max_time)
	if err != nil {
		return 0, 0, err
	}

	if repo_max_time == primary_max_time {
		return 0, 0, nil
	}

	err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT sum(a) FROM (SELECT pg_column_size(id)+pg_column_size(t)+pg_column_size(last_update) a FROM ptor.worker_%d WHERE last_update > $1) foo;", worker_id), primary_max_time).Scan(&data_los_bytes)
	if err != nil {
		return 0, 0, err
	}

	return repo_max_time.Sub(primary_max_time), data_los_bytes, nil
}

func GetRepoRowCount(worker_id int) (int, error) {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.RepoPgDsn)
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

func GetRepoRelationCheckSum(worker_id int) (string, error) {
	conn, err := pgx.Connect(context.Background(), global.CliOpts.RepoPgDsn)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())

	var checkSum string
	err = conn.QueryRow(context.Background(), fmt.Sprintf("WITH sort_data AS (SELECT id||trim(t)||last_update as data FROM ptor.worker_%d ORDER BY last_update DESC) SELECT COALESCE(md5(array_agg(sort_data.data)::text), '') FROM sort_data;", worker_id)).Scan(&checkSum)
	if err != nil {
		return "", err
	}

	return checkSum, nil
}
