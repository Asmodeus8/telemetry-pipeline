package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

type fakeWriter struct {
	mu      sync.Mutex
	batches [][]telemetry.Reading
	fail    int
}

func (f *fakeWriter) Write(_ context.Context, rs []telemetry.Reading) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return errors.New("db down")
	}
	f.batches = append(f.batches, rs)
	return nil
}

func (f *fakeWriter) sizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []int
	for _, b := range f.batches {
		out = append(out, len(b))
	}
	return out
}

func readings(n int) []telemetry.Reading {
	rs := make([]telemetry.Reading, n)
	for i := range rs {
		rs[i] = telemetry.Reading{Device: "d", Metric: "m", Value: float64(i)}
	}
	return rs
}

func TestFlushesFullBatchesAndRemainderOnShutdown(t *testing.T) {
	w := &fakeWriter{}
	b := NewBatcher(w, 10, time.Hour, 100)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()

	b.Add(readings(25)...)
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	total := 0
	for _, n := range w.sizes() {
		total += n
	}
	if total != 25 || b.Written.Load() != 25 {
		t.Fatalf("wrote %d (%v), counter %d", total, w.sizes(), b.Written.Load())
	}
}

func TestFlushesOnInterval(t *testing.T) {
	w := &fakeWriter{}
	b := NewBatcher(w, 1000, 20*time.Millisecond, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	b.Add(readings(3)...)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && len(w.sizes()) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := w.sizes(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("got batches %v", got)
	}
}

func TestRetriesFailedWrites(t *testing.T) {
	w := &fakeWriter{fail: 2}
	b := NewBatcher(w, 5, time.Hour, 10)
	b.write(readings(5))
	if b.Written.Load() != 5 || b.Failed.Load() != 0 {
		t.Fatalf("written %d failed %d", b.Written.Load(), b.Failed.Load())
	}
}

func TestDropsWhenQueueIsFull(t *testing.T) {
	b := NewBatcher(&fakeWriter{}, 10, time.Hour, 2)
	b.Add(readings(5)...)
	if b.Dropped.Load() != 3 {
		t.Fatalf("dropped %d", b.Dropped.Load())
	}
}
