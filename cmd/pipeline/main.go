package main

import (
	"context"
	"errors"
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
	"github.com/Asmodeus8/telemetry-pipeline/internal/store"
	"github.com/Asmodeus8/telemetry-pipeline/internal/stream"
	"github.com/Asmodeus8/telemetry-pipeline/internal/telemetry"
	"github.com/Asmodeus8/telemetry-pipeline/web"
)

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

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := openWithRetry(ctx, env("DATABASE_URL", "postgres://pipeline:pipeline@localhost:5432/telemetry"), time.Minute)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		return err
	}

	var rules []alerts.Rule
	if f := os.Getenv("RULES_FILE"); f != "" {
		if rules, err = alerts.Load(f); err != nil {
			return err
		}
	}
	engine := alerts.NewEngine(rules)
	var notifiers []alerts.Notifier
	if u := os.Getenv("WEBHOOK_URL"); u != "" {
		notifiers = append(notifiers, alerts.Webhook{URL: u})
	}
	if tok, chat := os.Getenv("TELEGRAM_TOKEN"), os.Getenv("TELEGRAM_CHAT_ID"); tok != "" && chat != "" {
		notifiers = append(notifiers, alerts.Telegram{Token: tok, ChatID: chat})
	}
	events := make(chan alerts.Event, 256)
	go dispatch(ctx, events, notifiers)

	batchSize, _ := strconv.Atoi(env("BATCH_SIZE", "500"))
	batcher := ingest.NewBatcher(db, batchSize, time.Second, 50000)
	batchDone := make(chan struct{})
	go func() { batcher.Run(ctx); close(batchDone) }()

	hub := stream.NewHub()
	var received, rejected atomic.Int64
	handle := func(_ mqtt.Client, m mqtt.Message) {
		device, err := telemetry.DeviceFromTopic(m.Topic())
		if err == nil {
			var rs []telemetry.Reading
			if rs, err = telemetry.Parse(device, m.Payload(), time.Now()); err == nil {
				received.Add(1)
				batcher.Add(rs...)
				hub.Publish(rs...)
				for _, r := range rs {
					for _, e := range engine.Observe(r) {
						events <- e
					}
				}
				return
			}
		}
		rejected.Add(1)
		slog.Warn("rejected message", "topic", m.Topic(), "err", err)
	}

	topic := env("MQTT_TOPIC", "devices/+/telemetry")
	opts := mqtt.NewClientOptions().
		AddBroker(env("MQTT_URL", "tcp://localhost:1883")).
		SetClientID(env("MQTT_CLIENT_ID", "telemetry-pipeline")).
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
		SetConnectionLostHandler(func(_ mqtt.Client, err error) { slog.Warn("mqtt connection lost", "err", err) })
	client := mqtt.NewClient(opts)
	client.Connect()
	defer client.Disconnect(500)

	go func() {
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
	}()

	static, _ := fs.Sub(web.Static, "static")
	srv := &http.Server{
		Addr:              env("HTTP_ADDR", ":8080"),
		ReadHeaderTimeout: 5 * time.Second,
		Handler: (&api.Server{
			Store:  db,
			Hub:    hub,
			Alerts: engine,
			Static: static,
			Stats: func() map[string]int64 {
				return map[string]int64{
					"messages": received.Load(), "rejected": rejected.Load(),
					"written": batcher.Written.Load(), "dropped": batcher.Dropped.Load(), "failed": batcher.Failed.Load(),
				}
			},
		}).Handler(),
	}
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
	<-batchDone
	return nil
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

func dispatch(ctx context.Context, events <-chan alerts.Event, notifiers []alerts.Notifier) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-events:
			slog.Info("alert", "rule", e.Rule, "device", e.Device, "state", e.State, "value", e.Value)
			for _, n := range notifiers {
				nctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				if err := n.Notify(nctx, e); err != nil {
					slog.Error("notify failed", "err", err)
				}
				cancel()
			}
		}
	}
}
