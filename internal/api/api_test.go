package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Asmodeus8/telemetry-pipeline/internal/alerts"
	"github.com/Asmodeus8/telemetry-pipeline/internal/store"
	"github.com/Asmodeus8/telemetry-pipeline/internal/stream"
	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

var now = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

type fakeStore struct {
	lastRange struct {
		device, metric string
		from, to       time.Time
		bucket         store.Bucket
		limit          int
	}
}

func (f *fakeStore) Devices(context.Context) ([]store.Device, error) {
	return []store.Device{{ID: "inverter-1", LastSeen: now, Metrics: 4}}, nil
}

func (f *fakeStore) Latest(_ context.Context, d string) ([]telemetry.Reading, error) {
	if d != "inverter-1" {
		return nil, nil
	}
	return []telemetry.Reading{{Time: now, Device: d, Metric: "battery.soc", Value: 80}}, nil
}

func (f *fakeStore) Range(_ context.Context, d, m string, from, to time.Time, b store.Bucket, limit int) ([]store.Point, error) {
	f.lastRange.device, f.lastRange.metric, f.lastRange.from, f.lastRange.to, f.lastRange.bucket, f.lastRange.limit = d, m, from, to, b, limit
	if !from.Before(to) {
		return nil, store.ErrRange
	}
	return []store.Point{{Time: from, Avg: 1, Min: 0, Max: 2}}, nil
}

func newServer() (*Server, *fakeStore) {
	fs := &fakeStore{}
	return &Server{Store: fs, Hub: stream.NewHub(), Alerts: alerts.NewEngine(nil), Now: func() time.Time { return now }}, fs
}

func get(t *testing.T, h http.Handler, url string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil))
	var body map[string]any
	json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestDevicesAndLatest(t *testing.T) {
	s, _ := newServer()
	h := s.Handler()
	if code, body := get(t, h, "/api/devices"); code != 200 || len(body["devices"].([]any)) != 1 {
		t.Fatalf("devices: %d %v", code, body)
	}
	if code, body := get(t, h, "/api/devices/inverter-1/latest"); code != 200 || len(body["readings"].([]any)) != 1 {
		t.Fatalf("latest: %d %v", code, body)
	}
	if code, _ := get(t, h, "/api/devices/ghost/latest"); code != 404 {
		t.Fatalf("unknown device should 404, got %d", code)
	}
}

func TestReadingsParameters(t *testing.T) {
	s, fs := newServer()
	h := s.Handler()

	code, _ := get(t, h, "/api/devices/inverter-1/readings?metric=battery.soc&from=-6h&bucket=1m&limit=100")
	if code != 200 {
		t.Fatalf("got %d", code)
	}
	r := fs.lastRange
	if r.metric != "battery.soc" || r.bucket != store.Minute || r.limit != 100 || !r.from.Equal(now.Add(-6*time.Hour)) || !r.to.Equal(now) {
		t.Fatalf("store called with %+v", r)
	}

	get(t, h, "/api/devices/inverter-1/readings?metric=x&limit=999999")
	if fs.lastRange.limit != maxPoints {
		t.Fatalf("limit should be capped, got %d", fs.lastRange.limit)
	}

	for _, bad := range []string{
		"/api/devices/inverter-1/readings",
		"/api/devices/inverter-1/readings?metric=x&bucket=5m",
		"/api/devices/inverter-1/readings?metric=x&from=yesterday",
		"/api/devices/inverter-1/readings?metric=x&limit=0",
		"/api/devices/inverter-1/readings?metric=x&from=-1h&to=-2h",
	} {
		if code, _ := get(t, h, bad); code != 400 {
			t.Errorf("%s: expected 400, got %d", bad, code)
		}
	}
}

func TestStreamDeliversReadings(t *testing.T) {
	s, _ := newServer()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/stream?device=inverter-1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Scan()
	for s.Hub.Count() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	s.Hub.Publish(
		telemetry.Reading{Device: "other", Metric: "x", Value: 1},
		telemetry.Reading{Device: "inverter-1", Metric: "battery.soc", Value: 77},
	)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			if !strings.Contains(line, `"device":"inverter-1"`) || !strings.Contains(line, `"value":77`) {
				t.Fatalf("unexpected event %q", line)
			}
			return
		}
	}
	t.Fatal("no event received")
}

func TestAlertsAndStats(t *testing.T) {
	s, _ := newServer()
	s.Stats = func() map[string]int64 { return map[string]int64{"written": 10} }
	h := s.Handler()
	if code, body := get(t, h, "/api/alerts"); code != 200 || body["active"] == nil {
		t.Fatalf("alerts: %d %v", code, body)
	}
	if code, body := get(t, h, "/api/stats"); code != 200 || body["written"].(float64) != 10 {
		t.Fatalf("stats: %d %v", code, body)
	}
}
