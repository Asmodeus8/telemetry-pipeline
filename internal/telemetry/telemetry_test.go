package telemetry

import (
	"testing"
	"time"
)

var now = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func TestDeviceFromTopic(t *testing.T) {
	id, err := DeviceFromTopic("devices/inverter-01/telemetry")
	if err != nil || id != "inverter-01" {
		t.Fatalf("got %q, %v", id, err)
	}
	for _, bad := range []string{"devices/a/b/telemetry", "sensors/a/telemetry", "devices//telemetry", "devices/a b/telemetry"} {
		if _, err := DeviceFromTopic(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestParseFlatAndNested(t *testing.T) {
	rs, err := Parse("d1", []byte(`{"temp": 21.5, "battery": {"soc": 80, "charging": true}, "label": "x"}`), now)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"battery.charging": 1, "battery.soc": 80, "temp": 21.5}
	if len(rs) != len(want) {
		t.Fatalf("got %d readings: %+v", len(rs), rs)
	}
	for _, r := range rs {
		if want[r.Metric] != r.Value || r.Device != "d1" || !r.Time.Equal(now) {
			t.Errorf("unexpected reading %+v", r)
		}
	}
	if rs[0].Metric != "battery.charging" {
		t.Errorf("readings should be sorted by metric, first is %q", rs[0].Metric)
	}
}

func TestParseEnvelopeWithTimestamp(t *testing.T) {
	rs, err := Parse("d1", []byte(`{"ts": "2026-10-29T11:59:00Z", "metrics": {"pv_w": 1450}}`), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 || rs[0].Value != 1450 || !rs[0].Time.Equal(now.Add(-time.Minute)) {
		t.Fatalf("got %+v", rs)
	}
}

func TestParseUnixTimestamps(t *testing.T) {
	for _, p := range []string{`{"ts": 1793275140, "v": 1}`, `{"ts": 1793275140000, "v": 1}`} {
		rs, err := Parse("d1", []byte(p), now)
		if err != nil {
			t.Fatal(err)
		}
		if rs[0].Time.Unix() != 1793275140 {
			t.Errorf("%s: got %v", p, rs[0].Time)
		}
	}
}

func TestParseRejects(t *testing.T) {
	cases := map[string]string{
		"not json":   `nope`,
		"array":      `[1,2]`,
		"no numbers": `{"a": "b"}`,
		"future ts":  `{"ts": "2026-10-30T00:00:00Z", "v": 1}`,
		"bad ts":     `{"ts": "yesterday", "v": 1}`,
		"bad metric": `{"bad metric!": 1}`,
	}
	for name, p := range cases {
		if _, err := Parse("d1", []byte(p), now); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestParseMetricLimit(t *testing.T) {
	b := []byte("{")
	for i := 0; i <= MaxMetrics; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, []byte(`"m`+itoa(i)+`":1`)...)
	}
	b = append(b, '}')
	if _, err := Parse("d1", b, now); err == nil {
		t.Fatal("expected the metric limit to apply")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for ; i > 0; i /= 10 {
		s = string(rune('0'+i%10)) + s
	}
	return s
}
