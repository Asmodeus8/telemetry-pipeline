package ingest

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

type Writer interface {
	Write(ctx context.Context, rs []telemetry.Reading) error
}

// Batcher groups readings into bulk writes, flushing when a batch fills up or the interval passes.
// If the queue is full, new readings are dropped and counted rather than blocking the MQTT client.
type Batcher struct {
	w        Writer
	in       chan telemetry.Reading
	size     int
	interval time.Duration
	retries  int
	Dropped  atomic.Int64
	Written  atomic.Int64
	Failed   atomic.Int64
}

func NewBatcher(w Writer, size int, interval time.Duration, queue int) *Batcher {
	return &Batcher{w: w, in: make(chan telemetry.Reading, queue), size: size, interval: interval, retries: 3}
}

func (b *Batcher) Add(rs ...telemetry.Reading) {
	for _, r := range rs {
		select {
		case b.in <- r:
		default:
			b.Dropped.Add(1)
		}
	}
}

// Run blocks until ctx is cancelled, then writes whatever is still queued.
func (b *Batcher) Run(ctx context.Context) {
	batch := make([]telemetry.Reading, 0, b.size)
	tick := time.NewTicker(b.interval)
	defer tick.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		b.write(batch)
		batch = batch[:0]
	}

	for {
		select {
		case r := <-b.in:
			batch = append(batch, r)
			if len(batch) >= b.size {
				flush()
			}
		case <-tick.C:
			flush()
		case <-ctx.Done():
			for {
				select {
				case r := <-b.in:
					batch = append(batch, r)
					if len(batch) >= b.size {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

func (b *Batcher) write(batch []telemetry.Reading) {
	rs := append([]telemetry.Reading(nil), batch...)
	var err error
	for attempt := 0; attempt <= b.retries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = b.w.Write(ctx, rs)
		cancel()
		if err == nil {
			b.Written.Add(int64(len(rs)))
			return
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	b.Failed.Add(int64(len(rs)))
	slog.Error("batch write failed", "readings", len(rs), "err", err)
}
