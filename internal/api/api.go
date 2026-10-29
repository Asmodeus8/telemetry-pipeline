package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"time"

	"github.com/Asmodeus8/telemetry-pipeline/internal/alerts"
	"github.com/Asmodeus8/telemetry-pipeline/internal/store"
	"github.com/Asmodeus8/telemetry-pipeline/internal/stream"
	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

type Store interface {
	Devices(ctx context.Context) ([]store.Device, error)
	Latest(ctx context.Context, device string) ([]telemetry.Reading, error)
	Range(ctx context.Context, device, metric string, from, to time.Time, b store.Bucket, limit int) ([]store.Point, error)
}

type Stats func() map[string]int64

type Server struct {
	Store  Store
	Hub    *stream.Hub
	Alerts *alerts.Engine
	Stats  Stats
	Static fs.FS
	Now    func() time.Time
}

const maxPoints = 5000

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /api/devices", s.devices)
	mux.HandleFunc("GET /api/devices/{id}/latest", s.latest)
	mux.HandleFunc("GET /api/devices/{id}/readings", s.readings)
	mux.HandleFunc("GET /api/alerts", s.alerts)
	mux.HandleFunc("GET /api/stats", s.stats)
	mux.HandleFunc("GET /api/stream", s.stream)
	if s.Static != nil {
		mux.Handle("GET /", http.FileServerFS(s.Static))
	}
	return mux
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func (s *Server) devices(w http.ResponseWriter, r *http.Request) {
	ds, err := s.Store.Devices(r.Context())
	if err != nil {
		fail(w, http.StatusInternalServerError, "could not load devices")
		return
	}
	if ds == nil {
		ds = []store.Device{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"devices": ds})
}

func (s *Server) latest(w http.ResponseWriter, r *http.Request) {
	rs, err := s.Store.Latest(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, http.StatusInternalServerError, "could not load readings")
		return
	}
	if len(rs) == 0 {
		fail(w, http.StatusNotFound, "no recent readings for this device")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"device": r.PathValue("id"), "readings": rs})
}

func (s *Server) readings(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	metric := q.Get("metric")
	if metric == "" {
		fail(w, http.StatusBadRequest, "metric is required")
		return
	}
	bucket, err := store.ParseBucket(q.Get("bucket"))
	if err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	to := s.now()
	from := to.Add(-time.Hour)
	if v := q.Get("from"); v != "" {
		if from, err = parseTimeParam(v, to); err != nil {
			fail(w, http.StatusBadRequest, "from: "+err.Error())
			return
		}
	}
	if v := q.Get("to"); v != "" {
		if to, err = parseTimeParam(v, s.now()); err != nil {
			fail(w, http.StatusBadRequest, "to: "+err.Error())
			return
		}
	}
	limit := maxPoints
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			fail(w, http.StatusBadRequest, "limit must be a positive number")
			return
		}
		limit = min(n, maxPoints)
	}
	pts, err := s.Store.Range(r.Context(), r.PathValue("id"), metric, from, to, bucket, limit)
	if errors.Is(err, store.ErrRange) {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, "could not load readings")
		return
	}
	if pts == nil {
		pts = []store.Point{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"device": r.PathValue("id"), "metric": metric, "bucket": bucket, "from": from, "to": to, "points": pts})
}

// parseTimeParam accepts RFC 3339 or a relative duration such as -6h.
func parseTimeParam(v string, now time.Time) (time.Time, error) {
	if d, err := time.ParseDuration(v); err == nil {
		return now.Add(d), nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("use RFC 3339 or a duration like -6h")
	}
	return t, nil
}

func (s *Server) alerts(w http.ResponseWriter, r *http.Request) {
	active := []alerts.Event{}
	if s.Alerts != nil {
		if a := s.Alerts.Active(); a != nil {
			active = a
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"active": active})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	out := map[string]int64{"streamClients": int64(s.Hub.Count())}
	if s.Stats != nil {
		for k, v := range s.Stats() {
			out[k] = v
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		fail(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")

	sub := s.Hub.Subscribe(r.URL.Query().Get("device"), 256)
	defer s.Hub.Unsubscribe(sub)

	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case rd, ok := <-sub.C:
			if !ok {
				return
			}
			b, _ := json.Marshal(rd)
			fmt.Fprintf(w, "event: reading\ndata: %s\n\n", b)
			flusher.Flush()
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}
