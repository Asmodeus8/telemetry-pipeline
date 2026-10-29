package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

type Reading struct {
	Time   time.Time `json:"time"`
	Device string    `json:"device"`
	Metric string    `json:"metric"`
	Value  float64   `json:"value"`
}

const MaxMetrics = 200

var (
	idPattern     = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	metricPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,96}$`)
	ErrTopic      = errors.New("topic must look like devices/{id}/telemetry")
)

// DeviceFromTopic takes the device ID from a topic such as devices/inverter-01/telemetry.
func DeviceFromTopic(topic string) (string, error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "devices" || parts[2] != "telemetry" || !idPattern.MatchString(parts[1]) {
		return "", ErrTopic
	}
	return parts[1], nil
}

// Parse accepts {"ts": ..., "metrics": {...}} or a flat object of values.
// Nested objects are flattened with dots, booleans become 1 or 0, and other types are ignored.
func Parse(device string, payload []byte, now time.Time) ([]Reading, error) {
	var root map[string]any
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, fmt.Errorf("payload is not a JSON object: %w", err)
	}

	at := now
	if ts, ok := root["ts"]; ok {
		t, err := parseTime(ts)
		if err != nil {
			return nil, err
		}
		if t.After(now.Add(5 * time.Minute)) {
			return nil, errors.New("timestamp is in the future")
		}
		at = t
		delete(root, "ts")
	}

	body := root
	if m, ok := root["metrics"].(map[string]any); ok {
		body = m
	}

	values := map[string]float64{}
	flatten("", body, values)
	if len(values) == 0 {
		return nil, errors.New("payload has no numeric values")
	}
	if len(values) > MaxMetrics {
		return nil, fmt.Errorf("payload has %d metrics, the limit is %d", len(values), MaxMetrics)
	}

	names := make([]string, 0, len(values))
	for k := range values {
		names = append(names, k)
	}
	sort.Strings(names)

	out := make([]Reading, 0, len(names))
	for _, k := range names {
		if !metricPattern.MatchString(k) {
			return nil, fmt.Errorf("invalid metric name %q", k)
		}
		out = append(out, Reading{Time: at.UTC(), Device: device, Metric: k, Value: values[k]})
	}
	return out, nil
}

func flatten(prefix string, in map[string]any, out map[string]float64) {
	for k, v := range in {
		name := k
		if prefix != "" {
			name = prefix + "." + k
		}
		switch x := v.(type) {
		case float64:
			if !math.IsNaN(x) && !math.IsInf(x, 0) {
				out[name] = x
			}
		case bool:
			if x {
				out[name] = 1
			} else {
				out[name] = 0
			}
		case map[string]any:
			flatten(name, x, out)
		}
	}
}

func parseTime(v any) (time.Time, error) {
	switch x := v.(type) {
	case string:
		t, err := time.Parse(time.RFC3339Nano, x)
		if err != nil {
			return time.Time{}, fmt.Errorf("ts must be RFC 3339: %w", err)
		}
		return t, nil
	case float64:
		if x > 1e12 {
			return time.UnixMilli(int64(x)), nil
		}
		return time.Unix(int64(x), int64((x-math.Floor(x))*1e9)), nil
	}
	return time.Time{}, errors.New("ts must be an RFC 3339 string or a Unix time")
}
