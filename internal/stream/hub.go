package stream

import (
	"sync"

	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

// Hub fans readings out to live subscribers. A slow subscriber misses readings
// instead of holding up ingestion.
type Hub struct {
	mu   sync.RWMutex
	subs map[*Sub]struct{}
}

type Sub struct {
	C      chan telemetry.Reading
	device string
}

func NewHub() *Hub { return &Hub{subs: map[*Sub]struct{}{}} }

func (h *Hub) Subscribe(device string, buffer int) *Sub {
	s := &Sub{C: make(chan telemetry.Reading, buffer), device: device}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

func (h *Hub) Unsubscribe(s *Sub) {
	h.mu.Lock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		close(s.C)
	}
	h.mu.Unlock()
}

func (h *Hub) Publish(rs ...telemetry.Reading) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs {
		for _, r := range rs {
			if s.device != "" && s.device != r.Device {
				continue
			}
			select {
			case s.C <- r:
			default:
			}
		}
	}
}

func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
