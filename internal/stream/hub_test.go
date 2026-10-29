package stream

import (
	"testing"

	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
)

func TestFiltersByDeviceAndNeverBlocks(t *testing.T) {
	h := NewHub()
	all := h.Subscribe("", 10)
	one := h.Subscribe("a", 1)

	h.Publish(
		telemetry.Reading{Device: "a", Metric: "x"},
		telemetry.Reading{Device: "b", Metric: "x"},
		telemetry.Reading{Device: "a", Metric: "y"},
	)

	if len(all.C) != 3 {
		t.Errorf("unfiltered subscriber got %d", len(all.C))
	}
	if len(one.C) != 1 {
		t.Errorf("full subscriber should hold 1 and drop the rest, has %d", len(one.C))
	}
	if r := <-one.C; r.Device != "a" {
		t.Errorf("filter leaked device %q", r.Device)
	}
}

func TestUnsubscribeClosesChannel(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("", 1)
	h.Unsubscribe(s)
	h.Unsubscribe(s)
	if _, open := <-s.C; open {
		t.Fatal("channel should be closed")
	}
	if h.Count() != 0 {
		t.Fatal("hub should be empty")
	}
}
