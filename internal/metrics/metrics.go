// Package metrics exposes the pipeline's counters in Prometheus format.
// Most values already live in atomics elsewhere, so they are read on scrape
// with *Func collectors instead of being counted twice.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	reg           *prometheus.Registry
	writeDuration *prometheus.HistogramVec
	batchSize     prometheus.Histogram
	notifications *prometheus.CounterVec
}

func New(role, version string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := &Metrics{
		reg: reg,
		writeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "telemetry_batch_write_duration_seconds",
			Help:    "Time taken by one COPY of a batch into TimescaleDB.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"result"}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "telemetry_batch_size_readings",
			Help:    "Readings per write attempt.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 11),
		}),
		notifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "telemetry_alert_notifications_total",
			Help: "Alert notifications sent, by result.",
		}, []string{"result"}),
	}
	build := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "telemetry_build_info",
		Help:        "Always 1; labels carry the version and role.",
		ConstLabels: prometheus.Labels{"version": version, "role": role},
	})
	build.Set(1)
	reg.MustRegister(m.writeDuration, m.batchSize, m.notifications, build)
	return m
}

func (m *Metrics) ObserveWrite(d time.Duration, n int, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	m.writeDuration.WithLabelValues(result).Observe(d.Seconds())
	m.batchSize.Observe(float64(n))
}

func (m *Metrics) Notified(err error) {
	if err != nil {
		m.notifications.WithLabelValues("error").Inc()
		return
	}
	m.notifications.WithLabelValues("ok").Inc()
}

// Counter registers a counter whose value is read from f on every scrape.
func (m *Metrics) Counter(name, help string, labels prometheus.Labels, f func() float64) {
	m.reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: labels}, f))
}

// Gauge registers a gauge whose value is read from f on every scrape.
func (m *Metrics) Gauge(name, help string, f func() float64) {
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, f))
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}
