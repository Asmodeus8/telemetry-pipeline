package alerts

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

var t0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func rd(device, metric string, v float64, after time.Duration) telemetry.Reading {
	return telemetry.Reading{Time: t0.Add(after), Device: device, Metric: metric, Value: v}
}

func TestThresholdNeedsToHoldForDuration(t *testing.T) {
	e := NewEngine([]Rule{{Name: "low-battery", Device: "inverter-*", Metric: "battery.soc", Op: "<", Threshold: 20, For: Duration{time.Minute}}})

	if ev := e.Observe(rd("inverter-1", "battery.soc", 15, 0)); len(ev) != 0 {
		t.Fatalf("fired too early: %+v", ev)
	}
	if ev := e.Observe(rd("inverter-1", "battery.soc", 14, 30*time.Second)); len(ev) != 0 {
		t.Fatalf("fired before the hold time: %+v", ev)
	}
	ev := e.Observe(rd("inverter-1", "battery.soc", 13, time.Minute))
	if len(ev) != 1 || ev[0].State != Firing || ev[0].Value != 13 {
		t.Fatalf("expected one firing event, got %+v", ev)
	}
	if ev := e.Observe(rd("inverter-1", "battery.soc", 12, 2*time.Minute)); len(ev) != 0 {
		t.Fatalf("should not fire twice: %+v", ev)
	}
	ev = e.Observe(rd("inverter-1", "battery.soc", 40, 3*time.Minute))
	if len(ev) != 1 || ev[0].State != Resolved {
		t.Fatalf("expected a resolved event, got %+v", ev)
	}
}

func TestDipBelowHoldTimeResetsTimer(t *testing.T) {
	e := NewEngine([]Rule{{Name: "hot", Device: "*", Metric: "temp", Op: ">", Threshold: 30, For: Duration{time.Minute}}})
	e.Observe(rd("room", "temp", 31, 0))
	e.Observe(rd("room", "temp", 25, 40*time.Second))
	if ev := e.Observe(rd("room", "temp", 31, 70*time.Second)); len(ev) != 0 {
		t.Fatalf("timer should have restarted: %+v", ev)
	}
}

func TestRulesOnlyMatchTheirDevicesAndMetrics(t *testing.T) {
	e := NewEngine([]Rule{{Name: "hot", Device: "coldroom-*", Metric: "temp", Op: ">", Threshold: 8}})
	if ev := e.Observe(rd("weather-1", "temp", 35, 0)); len(ev) != 0 {
		t.Fatalf("wrong device matched: %+v", ev)
	}
	if ev := e.Observe(rd("coldroom-1", "humidity", 99, 0)); len(ev) != 0 {
		t.Fatalf("wrong metric matched: %+v", ev)
	}
	if ev := e.Observe(rd("coldroom-1", "temp", 9, 0)); len(ev) != 1 {
		t.Fatalf("expected a firing event, got %+v", ev)
	}
}

func TestOfflineFiresAndResolves(t *testing.T) {
	e := NewEngine([]Rule{{Name: "offline", Device: "*", Offline: Duration{2 * time.Minute}}})
	e.Observe(rd("gw-1", "v", 1, 0))
	if ev := e.Tick(t0.Add(time.Minute)); len(ev) != 0 {
		t.Fatalf("fired too early: %+v", ev)
	}
	ev := e.Tick(t0.Add(2 * time.Minute))
	if len(ev) != 1 || ev[0].Device != "gw-1" || ev[0].State != Firing {
		t.Fatalf("expected offline alert, got %+v", ev)
	}
	if len(e.Active()) != 1 {
		t.Fatal("alert should be active")
	}
	if ev := e.Tick(t0.Add(3 * time.Minute)); len(ev) != 0 {
		t.Fatalf("should not fire twice: %+v", ev)
	}
	ev = e.Observe(rd("gw-1", "v", 1, 4*time.Minute))
	if len(ev) != 1 || ev[0].State != Resolved {
		t.Fatalf("expected recovery, got %+v", ev)
	}
	if len(e.Active()) != 0 {
		t.Fatal("no alert should be active")
	}
}

func TestLoadValidatesRules(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.json")
	os.WriteFile(good, []byte(`[{"name":"low","device":"*","metric":"soc","op":"<","threshold":20,"for":"1m"},{"name":"off","device":"*","offline":"90s"}]`), 0o600)
	rules, err := Load(good)
	if err != nil || len(rules) != 2 || rules[0].For.Duration != time.Minute || rules[1].Offline.Duration != 90*time.Second {
		t.Fatalf("got %+v, %v", rules, err)
	}
	bad := filepath.Join(dir, "bad.json")
	os.WriteFile(bad, []byte(`[{"name":"x","device":"*","metric":"soc","op":"~"}]`), 0o600)
	if _, err := Load(bad); err == nil {
		t.Fatal("expected unknown op to be rejected")
	}
}

func TestWebhookAndTelegram(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = append(got, r.URL.Path+" "+string(b))
	}))
	defer srv.Close()

	ev := Event{Rule: "low-battery", Device: "inverter-1", Metric: "battery.soc", State: Firing, Value: 13}
	if err := (Webhook{URL: srv.URL + "/hook"}).Notify(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if err := (Telegram{Token: "T", ChatID: "42", Base: srv.URL}).Notify(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got[0], "/hook ") || !strings.Contains(got[0], `"rule":"low-battery"`) {
		t.Errorf("webhook got %q", got[0])
	}
	if !strings.HasPrefix(got[1], "/botT/sendMessage ") || !strings.Contains(got[1], "chat_id=42") {
		t.Errorf("telegram got %q", got[1])
	}
}
