package alerts

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

// Rule fires when a metric crosses a threshold for at least For, or, when Offline is set,
// when a matching device has sent nothing for that long.
type Rule struct {
	Name      string   `json:"name"`
	Device    string   `json:"device"`
	Metric    string   `json:"metric,omitempty"`
	Op        string   `json:"op,omitempty"`
	Threshold float64  `json:"threshold,omitempty"`
	For       Duration `json:"for,omitempty"`
	Offline   Duration `json:"offline,omitempty"`
}

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	d.Duration = v
	return err
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

type State string

const (
	Firing   State = "firing"
	Resolved State = "resolved"
)

type Event struct {
	Rule   string    `json:"rule"`
	Device string    `json:"device"`
	Metric string    `json:"metric,omitempty"`
	State  State     `json:"state"`
	Value  float64   `json:"value"`
	At     time.Time `json:"at"`
}

type key struct{ rule, device string }

type track struct {
	since  time.Time
	firing bool
}

type Engine struct {
	mu       sync.Mutex
	rules    []Rule
	tracks   map[key]*track
	lastSeen map[string]time.Time
}

func Load(file string) ([]Rule, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var rules []Rule
	if err := json.Unmarshal(b, &rules); err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	for _, r := range rules {
		if err := r.validate(); err != nil {
			return nil, fmt.Errorf("rule %q: %w", r.Name, err)
		}
	}
	return rules, nil
}

func (r Rule) validate() error {
	if r.Name == "" || r.Device == "" {
		return fmt.Errorf("name and device are required")
	}
	if _, err := path.Match(r.Device, ""); err != nil {
		return fmt.Errorf("bad device pattern: %w", err)
	}
	if r.Offline.Duration > 0 {
		return nil
	}
	if r.Metric == "" {
		return fmt.Errorf("metric is required")
	}
	switch r.Op {
	case ">", ">=", "<", "<=", "==", "!=":
		return nil
	}
	return fmt.Errorf("unknown op %q", r.Op)
}

func NewEngine(rules []Rule) *Engine {
	return &Engine{rules: rules, tracks: map[key]*track{}, lastSeen: map[string]time.Time{}}
}

func (r Rule) matches(device string) bool {
	ok, _ := path.Match(r.Device, device)
	return ok
}

func compare(op string, v, t float64) bool {
	switch op {
	case ">":
		return v > t
	case ">=":
		return v >= t
	case "<":
		return v < t
	case "<=":
		return v <= t
	case "==":
		return v == t
	case "!=":
		return v != t
	}
	return false
}

// Observe feeds a reading through the threshold rules and returns any state changes.
func (e *Engine) Observe(rd telemetry.Reading) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	var out []Event
	if seen, ok := e.lastSeen[rd.Device]; !ok || rd.Time.After(seen) {
		e.lastSeen[rd.Device] = rd.Time
	}
	for _, r := range e.rules {
		if !r.matches(rd.Device) {
			continue
		}
		if r.Offline.Duration > 0 {
			k := key{r.Name, rd.Device}
			if t := e.tracks[k]; t != nil && t.firing {
				t.firing = false
				out = append(out, Event{Rule: r.Name, Device: rd.Device, State: Resolved, At: rd.Time})
			}
			continue
		}
		if r.Metric != rd.Metric {
			continue
		}
		k := key{r.Name, rd.Device}
		t := e.tracks[k]
		if compare(r.Op, rd.Value, r.Threshold) {
			if t == nil {
				t = &track{since: rd.Time}
				e.tracks[k] = t
			}
			if !t.firing && rd.Time.Sub(t.since) >= r.For.Duration {
				t.firing = true
				out = append(out, Event{Rule: r.Name, Device: rd.Device, Metric: rd.Metric, State: Firing, Value: rd.Value, At: rd.Time})
			}
		} else if t != nil {
			if t.firing {
				out = append(out, Event{Rule: r.Name, Device: rd.Device, Metric: rd.Metric, State: Resolved, Value: rd.Value, At: rd.Time})
			}
			delete(e.tracks, k)
		}
	}
	return out
}

// Tick checks offline rules against the last time each device was heard from.
func (e *Engine) Tick(now time.Time) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	devices := make([]string, 0, len(e.lastSeen))
	for d := range e.lastSeen {
		devices = append(devices, d)
	}
	sort.Strings(devices)

	var out []Event
	for _, r := range e.rules {
		if r.Offline.Duration <= 0 {
			continue
		}
		for _, d := range devices {
			if !r.matches(d) {
				continue
			}
			k := key{r.Name, d}
			t := e.tracks[k]
			if t == nil {
				t = &track{}
				e.tracks[k] = t
			}
			if !t.firing && now.Sub(e.lastSeen[d]) >= r.Offline.Duration {
				t.firing = true
				out = append(out, Event{Rule: r.Name, Device: d, State: Firing, At: now})
			}
		}
	}
	return out
}

// Active lists the alerts that are currently firing.
func (e *Engine) Active() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []Event
	for k, t := range e.tracks {
		if t.firing {
			out = append(out, Event{Rule: k.rule, Device: k.device, State: Firing})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Device != out[j].Device {
			return out[i].Device < out[j].Device
		}
		return out[i].Rule < out[j].Rule
	})
	return out
}
