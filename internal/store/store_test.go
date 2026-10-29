package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

// Runs against a real TimescaleDB when TEST_DATABASE_URL is set:
//
//	docker compose up -d db
//	TEST_DATABASE_URL=postgres://pipeline:pipeline@localhost:5434/telemetry go test ./internal/store
func TestStoreAgainstTimescale(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrations should be repeatable: %v", err)
	}

	device := "test-" + time.Now().Format("150405.000")
	base := time.Now().UTC().Truncate(time.Minute).Add(-30 * time.Minute)
	var rs []telemetry.Reading
	for i := 0; i < 120; i++ {
		rs = append(rs, telemetry.Reading{Time: base.Add(time.Duration(i) * 15 * time.Second), Device: device, Metric: "temp", Value: float64(i % 4)})
	}
	rs = append(rs, telemetry.Reading{Time: base.Add(29 * time.Minute), Device: device, Metric: "soc", Value: 55})
	if err := s.Write(ctx, rs); err != nil {
		t.Fatal(err)
	}

	latest, err := s.Latest(ctx, device)
	if err != nil || len(latest) != 2 {
		t.Fatalf("latest: %+v %v", latest, err)
	}

	raw, err := s.Range(ctx, device, "temp", base, base.Add(time.Hour), Raw, 1000)
	if err != nil || len(raw) != 120 {
		t.Fatalf("raw: %d %v", len(raw), err)
	}

	mins, err := s.Range(ctx, device, "temp", base, base.Add(time.Hour), Minute, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(mins) != 30 {
		t.Fatalf("expected 30 one-minute buckets, got %d", len(mins))
	}
	if p := mins[0]; p.Min != 0 || p.Max != 3 || p.Avg != 1.5 {
		t.Fatalf("bucket aggregates wrong: %+v", p)
	}

	if _, err := s.Range(ctx, device, "temp", base, base, Raw, 10); err != ErrRange {
		t.Fatalf("expected ErrRange, got %v", err)
	}

	devices, err := s.Devices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range devices {
		if d.ID == device && d.Metrics == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("device not listed: %+v", devices)
	}
}
