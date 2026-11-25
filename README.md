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
| `ROLE` | `all`; see Kubernetes below |

## Kubernetes and GitOps

The same image runs as three Deployments, each with a different `ROLE`, so each part scales on its own:

| Role | Replicas | What it does |
|---|---|---|
| `ingest` | 2–6, autoscaled on CPU | Subscribes through an MQTT **shared subscription** (`$share/ingest/...`), so the broker hands each message to one replica, never all of them. Writes to TimescaleDB. Runs migrations under a Postgres advisory lock. |
| `api` | 2 | REST, SSE and the dashboard. Every replica takes the full stream for its own live clients. |
| `alerts` | exactly 1 | The rule engine keeps hold-time state in memory, so two copies would each fire every alert. The HTTPRoute sends `/api/alerts` here. |

On shutdown a pod first leaves the broker, then flushes its write queue, so a rolling deploy loses no accepted readings. Every pod serves `/metrics` and `/readyz`; readiness checks the broker connection and the database.

```
scripts/bootstrap.ps1        # or scripts/bootstrap.sh
```

The bootstrap creates a three-node [kind](https://kind.sigs.k8s.io) cluster, the two secrets that must never be in Git (database and Grafana passwords), and Argo CD. Argo CD then installs everything else from this repository, as an app of apps:

| Wave | Application | Source |
|---|---|---|
| −2 | Gateway API CRDs | `kubernetes-sigs/gateway-api` v1.7.0 |
| −1 | Argo CD (managing itself), Traefik as the Gateway, metrics-server | Helm charts |
| 0 | kube-prometheus-stack | Helm chart |
| 1 | Routes for the platform UIs | `deploy/platform` |
| 2 | Telemetry pipeline, with the simulator | `deploy/chart` + `deploy/environments/local` |

| | |
|---|---|
| http://telemetry.localhost | dashboard and API |
| http://argocd.localhost | Argo CD (password printed by the bootstrap) |
| http://grafana.localhost | Grafana, with the Telemetry Pipeline dashboard |
| http://prometheus.localhost | Prometheus |
| `localhost:1884` | MQTT for real devices |

![All eight Argo CD applications synced and healthy](docs/k8s-argocd-apps.png)

### Load test

`deploy/load/burst.yaml` replays ten days of readings from four publishers as fast as the broker accepts them:

| | |
|---|---|
| Messages | 1.05 million, peaking at about 2,850 a second |
| Readings written | 3.14 million |
| Dropped or failed | 0 |
| p99 batch write | 214 ms |
| Ingest replicas | autoscaled from 2 to 6 within 40 seconds, back to 2 afterwards |

![Grafana during the load test: messages spread across six ingest pods, write latency, queue depth and broker connections](docs/k8s-grafana.png)

**Release loop:** a push to `main` runs the tests and builds the image. The image goes to `ghcr.io/asmodeus8/telemetry-pipeline:sha-<commit>`, and that tag is committed into `deploy/environments/local/values.yaml`. Argo CD notices the commit and rolls it out. Nothing deploys by running `kubectl` from CI.

**Chart CI:** each change to the chart is linted and schema-checked with kubeconform, including the CRDs. It is then installed on a throwaway kind cluster, where CI runs the smoke test, checks that the shared subscription wrote every reading exactly once, and checks that a rolling restart keeps serving.

**Hardening:** non-root, read-only filesystems, dropped capabilities and seccomp. Also NetworkPolicies (the database accepts only ingest and api; HTTP only from the gateway and monitoring), PodDisruptionBudgets, pods spread across nodes, and `maxUnavailable: 0` rollouts.

**Monitoring:** a ServiceMonitor, a Grafana dashboard loaded from a ConfigMap, and PrometheusRules for stalled ingest, dropped or failed writes, slow writes, queue backlog, lost broker connections and a missing alert engine.

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
internal/metrics    Prometheus metrics
web/static          dashboard (plain HTML, CSS and JS, embedded in the binary)
deploy/chart        Helm chart
deploy/argocd       Argo CD app of apps
deploy/environments values per environment (the image tag CI bumps)
deploy/platform     routes for the platform UIs
deploy/kind         local cluster
scripts             bootstrap
```

## License

MIT
