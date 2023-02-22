package pool

import (
	"context"
	"fmt"
	"ptor/global"

	"github.com/jackc/pgx/v4/pgxpool"
)

var PrimaryPool *Pool
var RepoPool *Pool

type Pool struct {
	pool *pgxpool.Pool
}

func (p *Pool) GetPrimaryPool() *pgxpool.Pool {
	return p.pool
}

func (p *Pool) InitPrimaryPool() error {
	parseConfig, err := pgxpool.ParseConfig(global.CliOpts.PrimaryPgDsn)
	if err != nil {
		return err
	}

	config := parseConfig
	config.MaxConns = int32(global.CliOpts.ParallelWorkers)
	config.LazyConnect = true
	p.pool, err = pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return err
	}

	return nil
}

func (p *Pool) GetConnectionFromPrimaryPool() (*pgxpool.Conn, error) {
	if p.pool == nil {
		return nil, fmt.Errorf("unable to get connection from an empty pool")
	}

	conn, err := p.pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (p *Pool) ClosePrimaryPool() error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

func (p *Pool) GetRepoPool() *pgxpool.Pool {
	return p.pool
}

func (p *Pool) InitRepoPool() error {
	parseConfig, err := pgxpool.ParseConfig(global.CliOpts.RepoPgDsn)
	if err != nil {
		return err
	}

	config := parseConfig
	config.MaxConns = int32(global.CliOpts.ParallelWorkers)
	config.LazyConnect = true
	p.pool, err = pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return err
	}

	return nil
}

func (p *Pool) GetConnectionFromRepoPool() (*pgxpool.Conn, error) {
	if p.pool == nil {
		return nil, fmt.Errorf("unable to get connection from an empty pool")
	}

	conn, err := p.pool.Acquire(context.Background())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (p *Pool) CloseRepoPool() error {
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}
