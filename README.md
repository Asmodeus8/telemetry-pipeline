# Telemetry Pipeline

[![test](https://github.com/Asmodeus8/telemetry-pipeline/actions/workflows/test.yml/badge.svg)](https://github.com/Asmodeus8/telemetry-pipeline/actions/workflows/test.yml)

A Go service that takes readings from IoT devices over MQTT, stores them in TimescaleDB, raises alerts when something goes wrong, and serves the data through a REST API, a live stream and a built-in dashboard.

It's the backend half of the hardware I build: ESP32 gateways and sensor nodes publish JSON over MQTT, and this service turns that stream into something you can query, chart and get alerted on.

![Dashboard](docs/dashboard.png)

<p>
  <img src="docs/dashboard-dark.png" width="72%" alt="Dashboard in dark mode" />
  <img src="docs/dashboard-mobile.png" width="24%" alt="Dashboard on a phone" />
</p>

## How it works

![Architecture](docs/architecture.svg)

1. Devices publish to `devices/{id}/telemetry`. The payload can be a flat object (`{"temp_c": 4.2}`) or an envelope with a timestamp (`{"ts": "...", "metrics": {...}}`). Nested objects are flattened (`battery.soc`), and booleans become 1 or 0.
2. Each message is validated once: device IDs and metric names are checked, timestamps more than five minutes in the future are rejected, and there are at most 200 metrics per message. Bad messages are counted and logged, never stored.
3. Valid readings go three ways at once:
   - **Batcher**: queued and written to TimescaleDB with `COPY` in batches of up to 500, or every second. Failed writes are retried. If the database falls behind, the bounded queue drops and counts readings instead of blocking the MQTT client.
   - **Live hub**: pushed to every dashboard or client subscribed to `/api/stream`. A slow client misses readings rather than slowing everyone else down.
   - **Alert engine**: checked against the rules in `rules.json`.
4. TimescaleDB keeps raw readings in a hypertable and maintains 1-minute and 1-hour **continuous aggregates** (avg, min, max). Charts over hours or days read the aggregates, not millions of raw rows. Late data, such as a gateway emptying its offline buffer, is folded into the aggregates for up to two days back. Raw data is kept for 30 days.

## Alerts

```json
[
  { "name": "low-battery",    "device": "inverter-*", "metric": "battery.soc", "op": "<", "threshold": 20, "for": "2m" },
  { "name": "coldroom-warm",  "device": "coldroom-*", "metric": "temp_c",      "op": ">", "threshold": 8,  "for": "1m" },
  { "name": "device-offline", "device": "*",          "offline": "2m" }
]
```

- A threshold has to hold for the whole `for` period before it fires, so a single noisy reading doesn't page anyone.
- Each alert fires once and sends a **resolved** message when the value recovers or the device comes back.
- Device patterns are globs.
- Notifications go to a webhook and/or a Telegram chat.

## API

| Endpoint | |
|---|---|
| `GET /api/devices` | Devices seen in the last 7 days, with last-seen time and metric count |
| `GET /api/devices/{id}/latest` | Latest value of every metric |
| `GET /api/devices/{id}/readings?metric=&from=&to=&bucket=&limit=` | History. `from`/`to` take RFC 3339 or durations like `-6h`. `bucket` is `raw`, `1m` or `1h`. At most 5,000 points |
| `GET /api/alerts` | Alerts currently firing |
| `GET /api/stats` | Messages received, rows written, rejected, dropped, live clients |
| `GET /api/stream?device=` | Server-sent events, one `reading` event per value |
| `GET /healthz` | Liveness |

## Run it

```
docker compose --profile demo up --build
```

Open http://localhost:8080. The `demo` profile adds a simulator that backfills a week of history and then publishes live readings from three devices: a solar inverter in Lekki, a cold room in Yaba and a weather station in Ikeja. Their values follow Lagos daylight hours, a compressor cycle and daily temperature swings.

To point real devices at it, publish to the broker on port 1883 and leave out the `demo` profile.

| Variable | Default |
|---|---|
| `DATABASE_URL` | `postgres://pipeline:pipeline@localhost:5432/telemetry` |
| `MQTT_URL` | `tcp://localhost:1883` |
| `MQTT_TOPIC` | `devices/+/telemetry` |
| `HTTP_ADDR` | `:8080` |
| `RULES_FILE` | `/etc/pipeline/rules.json` in the image |
| `WEBHOOK_URL`, `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID` | unset (notifications off) |

## Tests

```
go test -race ./...
```

Unit tests cover payload parsing, the alert engine (hold times, resets, offline detection, notifiers), the batcher (size and time flushes, retries, back-pressure), the live hub and every API handler, including the SSE stream. The store test runs against a real TimescaleDB when `TEST_DATABASE_URL` is set, and checks the migrations, bulk writes and the 1-minute aggregates:

```
docker compose up -d db
TEST_DATABASE_URL=postgres://pipeline:pipeline@localhost:5434/telemetry go test ./internal/store
```

## Layout

```
cmd/pipeline     service entry point: MQTT, batcher, alerts, HTTP
cmd/simulate     demo device simulator
internal/telemetry  topic and payload parsing
internal/ingest     batched writer
internal/store      TimescaleDB schema, writes and queries
internal/alerts     rule engine, webhook and Telegram notifiers
internal/stream     live fan-out hub
internal/api        REST and SSE handlers
web/static          dashboard (plain HTML, CSS and JS, embedded in the binary)
```

## License

MIT
