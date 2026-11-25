package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

type Bucket string

const (
	Raw    Bucket = "raw"
	Minute Bucket = "1m"
	Hour   Bucket = "1h"
)

func ParseBucket(s string) (Bucket, error) {
	switch Bucket(s) {
	case "", Raw:
		return Raw, nil
	case Minute, Hour:
		return Bucket(s), nil
	}
	return "", fmt.Errorf("bucket must be raw, 1m or 1h")
}

type Point struct {
	Time time.Time `json:"time"`
	Avg  float64   `json:"avg"`
	Min  float64   `json:"min"`
	Max  float64   `json:"max"`
}

type Device struct {
	ID       string    `json:"id"`
	LastSeen time.Time `json:"lastSeen"`
	Metrics  int       `json:"metrics"`
}

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Continuous aggregates can't be created inside a transaction, so each statement runs on its own.
var migrations = []string{
	`CREATE EXTENSION IF NOT EXISTS timescaledb`,
	`CREATE TABLE IF NOT EXISTS readings (
		time   timestamptz      NOT NULL,
		device text             NOT NULL,
		metric text             NOT NULL,
		value  double precision NOT NULL
	)`,
	`SELECT create_hypertable('readings', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE)`,
	`CREATE INDEX IF NOT EXISTS readings_device_metric_time ON readings (device, metric, time DESC)`,
	`CREATE MATERIALIZED VIEW IF NOT EXISTS readings_1m WITH (timescaledb.continuous) AS
		SELECT time_bucket('1 minute', time) AS bucket, device, metric,
		       avg(value) AS avg, min(value) AS min, max(value) AS max
		FROM readings GROUP BY bucket, device, metric WITH NO DATA`,
	`CREATE MATERIALIZED VIEW IF NOT EXISTS readings_1h WITH (timescaledb.continuous) AS
		SELECT time_bucket('1 hour', time) AS bucket, device, metric,
		       avg(value) AS avg, min(value) AS min, max(value) AS max
		FROM readings GROUP BY bucket, device, metric WITH NO DATA`,
	`SELECT add_continuous_aggregate_policy('readings_1m', start_offset => INTERVAL '2 days', end_offset => INTERVAL '1 minute', schedule_interval => INTERVAL '1 minute', if_not_exists => TRUE)`,
	`SELECT add_continuous_aggregate_policy('readings_1h', start_offset => INTERVAL '30 days', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '30 minutes', if_not_exists => TRUE)`,
	`ALTER MATERIALIZED VIEW readings_1m SET (timescaledb.materialized_only = false)`,
	`ALTER MATERIALIZED VIEW readings_1h SET (timescaledb.materialized_only = false)`,
	`SELECT add_retention_policy('readings', INTERVAL '30 days', if_not_exists => TRUE)`,
}

// Migrate is safe to call from several replicas at once: a session advisory lock
// makes them take turns, and every statement is idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(7310042)`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(7310042)`)
	for _, q := range migrations {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("migrate: %w\n%s", err, q)
		}
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Write(ctx context.Context, rs []telemetry.Reading) error {
	rows := make([][]any, len(rs))
	for i, r := range rs {
		rows[i] = []any{r.Time, r.Device, r.Metric, r.Value}
	}
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"readings"}, []string{"time", "device", "metric", "value"}, pgx.CopyFromRows(rows))
	return err
}

func (s *Store) Devices(ctx context.Context) ([]Device, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT device, max(time), count(DISTINCT metric)
		FROM readings WHERE time > now() - INTERVAL '7 days'
		GROUP BY device ORDER BY device`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Device, error) {
		var d Device
		err := r.Scan(&d.ID, &d.LastSeen, &d.Metrics)
		return d, err
	})
}

func (s *Store) Latest(ctx context.Context, device string) ([]telemetry.Reading, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (metric) time, device, metric, value
		FROM readings WHERE device = $1 AND time > now() - INTERVAL '7 days'
		ORDER BY metric, time DESC`, device)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[telemetry.Reading])
}

var ErrRange = errors.New("from must be before to")

func (s *Store) Range(ctx context.Context, device, metric string, from, to time.Time, b Bucket, limit int) ([]Point, error) {
	if !from.Before(to) {
		return nil, ErrRange
	}
	var q string
	switch b {
	case Minute:
		q = `SELECT bucket, avg, min, max FROM readings_1m WHERE device=$1 AND metric=$2 AND bucket >= $3 AND bucket < $4 ORDER BY bucket LIMIT $5`
	case Hour:
		q = `SELECT bucket, avg, min, max FROM readings_1h WHERE device=$1 AND metric=$2 AND bucket >= $3 AND bucket < $4 ORDER BY bucket LIMIT $5`
	default:
		q = `SELECT time, value, value, value FROM readings WHERE device=$1 AND metric=$2 AND time >= $3 AND time < $4 ORDER BY time LIMIT $5`
	}
	rows, err := s.pool.Query(ctx, q, device, metric, from, to, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Point])
}
