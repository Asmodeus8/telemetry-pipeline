package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/Asmodeus8/telemetry-pipeline/internal/alerts"
	"github.com/Asmodeus8/telemetry-pipeline/internal/api"
	"github.com/Asmodeus8/telemetry-pipeline/internal/ingest"
	"github.com/Asmodeus8/telemetry-pipeline/internal/metrics"
	"github.com/Asmodeus8/telemetry-pipeline/internal/store"
	"github.com/Asmodeus8/telemetry-pipeline/internal/stream"
	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
	"github.com/Asmodeus8/telemetry-pipeline/web"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)
	if err := run(); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

// ROLE splits the service so each part scales on its own:
//
//	all     everything in one process (docker compose, local runs)
//	ingest  shared MQTT subscription → TimescaleDB; scale out freely
//	api     REST + live SSE; every replica sees the full stream for its own clients
//	alerts  the rule engine and notifiers; run exactly one
type roles struct{ ingest, api, alerts bool }

func parseRole(r string) (roles, error) {
	switch r {
	case "all":
		return roles{true, true, true}, nil
	case "ingest":
		return roles{ingest: true}, nil
	case "api":
		return roles{api: true}, nil
	case "alerts":
		return roles{alerts: true}, nil
	}
	return roles{}, fmt.Errorf("unknown ROLE %q (want all, ingest, api or alerts)", r)
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	role := env("ROLE", "all")
	on, err := parseRole(role)
	if err != nil {
		return err
	}
	m := metrics.New(role, version)
	slog.Info("starting", "role", role, "version", version)

	var db *store.Store
	if on.ingest || on.api {
		if db, err = openWithRetry(ctx, env("DATABASE_URL", "postgres://pipeline:pipeline@localhost:5432/telemetry"), time.Minute); err != nil {
			return err
		}
		defer db.Close()
	}
	if on.ingest {
		if err := db.Migrate(ctx); err != nil {
			return err
		}
	}

	var engine *alerts.Engine
	events := make(chan alerts.Event, 256)
	if on.alerts {
		var rules []alerts.Rule
		if f := os.Getenv("RULES_FILE"); f != "" {
			if rules, err = alerts.Load(f); err != nil {
				return err
			}
		}
		engine = alerts.NewEngine(rules)
		var notifiers []alerts.Notifier
		if u := os.Getenv("WEBHOOK_URL"); u != "" {
			notifiers = append(notifiers, alerts.Webhook{URL: u})
		}
		if tok, chat := os.Getenv("TELEGRAM_TOKEN"), os.Getenv("TELEGRAM_CHAT_ID"); tok != "" && chat != "" {
			notifiers = append(notifiers, alerts.Telegram{Token: tok, ChatID: chat})
		}
		go dispatch(ctx, events, notifiers, m)
		go tick(ctx, engine, events)
		m.Gauge("telemetry_alerts_firing", "Alerts currently firing.", func() float64 { return float64(len(engine.Active())) })
	}

	// The batcher outlives the MQTT client: on shutdown we stop receiving first,
	// then flush what is queued, so a rolling deploy loses no accepted readings.
	var batcher *ingest.Batcher
	batchCtx, stopBatcher := context.WithCancel(context.Background())
	defer stopBatcher()
	batchDone := make(chan struct{})
	if on.ingest {
		batchSize, _ := strconv.Atoi(env("BATCH_SIZE", "500"))
		batcher = ingest.NewBatcher(db, batchSize, time.Second, 50000)
		batcher.OnWrite = m.ObserveWrite
		go func() { batcher.Run(batchCtx); close(batchDone) }()
		m.Counter("telemetry_readings_written_total", "Readings stored in TimescaleDB.", nil, func() float64 { return float64(batcher.Written.Load()) })
		m.Counter("telemetry_readings_dropped_total", "Readings dropped because the write queue was full.", nil, func() float64 { return float64(batcher.Dropped.Load()) })
		m.Counter("telemetry_readings_failed_total", "Readings lost after every write retry failed.", nil, func() float64 { return float64(batcher.Failed.Load()) })
		m.Gauge("telemetry_batch_queue_length", "Readings waiting to be written.", func() float64 { return float64(batcher.Queued()) })
	} else {
		close(batchDone)
	}

	var hub *stream.Hub
	if on.api {
		hub = stream.NewHub()
		m.Gauge("telemetry_stream_subscribers", "Clients connected to /api/stream.", func() float64 { return float64(hub.Count()) })
	}

	var received, rejected atomic.Int64
	m.Counter("telemetry_mqtt_messages_total", "MQTT messages handled.", map[string]string{"result": "accepted"}, func() float64 { return float64(received.Load()) })
	m.Counter("telemetry_mqtt_messages_total", "MQTT messages handled.", map[string]string{"result": "rejected"}, func() float64 { return float64(rejected.Load()) })

	handle := func(_ mqtt.Client, msg mqtt.Message) {
		device, err := telemetry.DeviceFromTopic(msg.Topic())
		if err == nil {
			var rs []telemetry.Reading
			if rs, err = telemetry.Parse(device, msg.Payload(), time.Now()); err == nil {
				received.Add(1)
				if batcher != nil {
					batcher.Add(rs...)
				}
				if hub != nil {
					hub.Publish(rs...)
				}
				if engine != nil {
					for _, r := range rs {
						for _, e := range engine.Observe(r) {
							events <- e
						}
					}
				}
				return
			}
		}
		rejected.Add(1)
		slog.Warn("rejected message", "topic", msg.Topic(), "err", err)
	}

	topic := env("MQTT_TOPIC", "devices/+/telemetry")
	if on.ingest && !on.api && !on.alerts {
		// Ingest replicas share one subscription, so the broker hands each
		// message to exactly one of them instead of all of them.
		topic = "$share/" + env("MQTT_SHARE_GROUP", "ingest") + "/" + topic
	}
	host, _ := os.Hostname()
	client := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(env("MQTT_URL", "tcp://localhost:1883")).
		SetClientID(env("MQTT_CLIENT_ID", "telemetry-"+role+"-"+host)).
		SetUsername(os.Getenv("MQTT_USERNAME")).
		SetPassword(os.Getenv("MQTT_PASSWORD")).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			if t := c.Subscribe(topic, 1, handle); t.Wait() && t.Error() != nil {
				slog.Error("subscribe failed", "err", t.Error())
				return
			}
			slog.Info("subscribed", "topic", topic)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) { slog.Warn("mqtt connection lost", "err", err) }))
	client.Connect()
	m.Gauge("telemetry_mqtt_connected", "1 while connected to the broker.", func() float64 {
		if client.IsConnectionOpen() {
			return 1
		}
		return 0
	})

	app := &api.Server{
		Hub:    hub,
		Alerts: engine,
		Stats: func() map[string]int64 {
			out := map[string]int64{"messages": received.Load(), "rejected": rejected.Load()}
			if batcher != nil {
				out["written"], out["dropped"], out["failed"] = batcher.Written.Load(), batcher.Dropped.Load(), batcher.Failed.Load()
			}
			return out
		},
	}
	if on.api {
		app.Store = db
		app.Static, _ = fs.Sub(web.Static, "static")
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !client.IsConnectionOpen() {
			http.Error(w, "mqtt not connected", http.StatusServiceUnavailable)
			return
		}
		if db != nil {
			pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := db.Ping(pctx); err != nil {
				http.Error(w, "database: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.Write([]byte("ready"))
	})
	mux.Handle("/", app.Handler())

	srv := &http.Server{Addr: env("HTTP_ADDR", ":8080"), ReadHeaderTimeout: 5 * time.Second, Handler: mux}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shut)
	}()

	slog.Info("listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	client.Disconnect(500)
	stopBatcher()
	<-batchDone
	slog.Info("stopped cleanly")
	return nil
}

func tick(ctx context.Context, engine *alerts.Engine, events chan<- alerts.Event) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			for _, e := range engine.Tick(now) {
				events <- e
			}
		}
	}
}

func openWithRetry(ctx context.Context, url string, wait time.Duration) (*store.Store, error) {
	deadline := time.Now().Add(wait)
	delay := 500 * time.Millisecond
	for {
		db, err := store.Open(ctx, url)
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		slog.Warn("database not ready, retrying", "err", err, "in", delay)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 5*time.Second)
	}
}

func dispatch(ctx context.Context, events <-chan alerts.Event, notifiers []alerts.Notifier, m *metrics.Metrics) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-events:
			slog.Info("alert", "rule", e.Rule, "device", e.Device, "state", e.State, "value", e.Value)
			for _, n := range notifiers {
				nctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := n.Notify(nctx, e)
				cancel()
				m.Notified(err)
				if err != nil {
					slog.Error("notify failed", "err", err)
				}
			}
		}
	}
}
