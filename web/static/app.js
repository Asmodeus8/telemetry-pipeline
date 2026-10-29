const $ = (s) => document.querySelector(s);
const state = { devices: [], device: null, metric: null, range: "15m", latest: {}, history: {}, alerts: [] };
const RANGES = { "15m": { from: "-15m", bucket: "raw" }, "6h": { from: "-6h", bucket: "1m" }, "7d": { from: "-168h", bucket: "1h" } };
const UNITS = [[/power_w$/, "W"], [/soc$/, "%"], [/voltage$/, "V"], [/temp_c$/, "°C"], [/humidity$/, "%"], [/pm25$/, "µg/m³"]];

const unit = (m) => (UNITS.find(([re]) => re.test(m)) || [, ""])[1];
const ACRONYMS = { soc: "SOC", pv: "PV", pm25: "PM2.5", ac: "AC", dc: "DC" };
const label = (m) =>
  m.replace(/_(w|c)$/, "").split(/[._]/).map((w) => ACRONYMS[w] || w[0].toUpperCase() + w.slice(1)).join(" ");
const fmt = (v) => (Math.abs(v) >= 100 ? Math.round(v).toLocaleString() : (+v.toFixed(2)).toString());
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const ago = (t) => {
  const s = Math.max(0, (Date.now() - new Date(t)) / 1000);
  return s < 60 ? `${Math.round(s)}s ago` : s < 3600 ? `${Math.round(s / 60)}m ago` : `${Math.round(s / 3600)}h ago`;
};

async function api(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error((await r.json().catch(() => ({}))).error || r.statusText);
  return r.json();
}

async function loadDevices() {
  const { devices } = await api("/api/devices");
  state.devices = devices;
  if (!state.device && devices.length) await selectDevice(devices[0].id);
  renderDevices();
}

function renderDevices() {
  $("#device-list").innerHTML = state.devices
    .map((d) => {
      const stale = Date.now() - new Date(d.lastSeen) > 120000;
      return `<li><button data-id="${d.id}" aria-current="${d.id === state.device}">
        <span class="state ${stale ? "stale" : ""}"></span>${d.id}<span class="meta">${d.metrics}</span></button></li>`;
    })
    .join("");
}

async function selectDevice(id) {
  state.device = id;
  state.latest = {};
  state.history = {};
  const { readings } = await api(`/api/devices/${encodeURIComponent(id)}/latest`);
  readings.forEach((r) => (state.latest[r.metric] = r));
  await Promise.all(readings.map((r) => loadSpark(r.metric)));
  state.metric = readings.some((r) => r.metric === state.metric) ? state.metric : readings[0]?.metric;
  renderDevices();
  renderHead();
  renderTiles();
  loadChart();
}

async function loadSpark(metric) {
  const { points } = await api(`/api/devices/${encodeURIComponent(state.device)}/readings?metric=${encodeURIComponent(metric)}&from=-10m&limit=300`);
  state.history[metric] = points.map((p) => [new Date(p.time).getTime(), p.avg]);
}

function renderHead() {
  $("#device-name").textContent = state.device || "No devices yet";
  const last = Object.values(state.latest).reduce((a, r) => (a && a > r.time ? a : r.time), null);
  $("#device-seen").textContent = last ? `${Object.keys(state.latest).length} metrics · last reading ${ago(last)}` : "";
  const mine = state.alerts.filter((a) => a.device === state.device);
  $("#alerts").innerHTML = mine.length
    ? mine.map((a) => `<span class="alert">${a.rule.replace(/-/g, " ")}</span>`).join("")
    : state.device ? `<span class="alert clear">All clear</span>` : "";
}

function renderTiles() {
  $("#tiles").innerHTML = Object.keys(state.latest)
    .sort()
    .map((m) => `<button class="tile" data-metric="${m}" aria-pressed="${m === state.metric}">
      <div class="name">${label(m)}</div>
      <div class="value">${fmt(state.latest[m].value)}<span class="unit">${unit(m)}</span></div>
      <canvas></canvas></button>`)
    .join("");
  document.querySelectorAll(".tile").forEach((t) => spark(t.querySelector("canvas"), state.history[t.dataset.metric] || []));
}

function updateTile(r) {
  const t = document.querySelector(`.tile[data-metric="${CSS.escape(r.metric)}"]`);
  if (!t) return renderTiles();
  t.querySelector(".value").innerHTML = `${fmt(r.value)}<span class="unit">${unit(r.metric)}</span>`;
  spark(t.querySelector("canvas"), state.history[r.metric]);
  t.classList.add("flash");
  setTimeout(() => t.classList.remove("flash"), 400);
}

function setup(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  return [ctx, w, h];
}

// Readings further apart than this many times the usual spacing are drawn as a break, not a line.
const GAP = 6;
function gapLimit(ts) {
  const d = ts.slice(1).map((t, i) => t - ts[i]).sort((a, b) => a - b);
  return (d[Math.floor(d.length / 2)] || Infinity) * GAP;
}

function spark(canvas, pts) {
  const [ctx, w, h] = setup(canvas);
  if (pts.length < 2) return;
  const ys = pts.map((p) => p[1]);
  const lo = Math.min(...ys), hi = Math.max(...ys), span = hi - lo || 1;
  const t0 = pts[0][0], t1 = pts[pts.length - 1][0] || t0 + 1;
  const limit = gapLimit(pts.map((p) => p[0]));
  ctx.beginPath();
  pts.forEach(([t, v], i) => {
    const x = ((t - t0) / (t1 - t0 || 1)) * w, y = h - 3 - ((v - lo) / span) * (h - 6);
    i && t - pts[i - 1][0] <= limit ? ctx.lineTo(x, y) : ctx.moveTo(x, y);
  });
  ctx.strokeStyle = css("--chart");
  ctx.lineWidth = 1.5;
  ctx.stroke();
}

async function loadChart() {
  if (!state.metric) return;
  const r = RANGES[state.range];
  const { points } = await api(`/api/devices/${encodeURIComponent(state.device)}/readings?metric=${encodeURIComponent(state.metric)}&from=${r.from}&bucket=${r.bucket}`);
  state.chart = points;
  $("#chart-title").textContent = `${label(state.metric)}${unit(state.metric) ? ` (${unit(state.metric)})` : ""}`;
  $("#chart-note").textContent = `${points.length} ${r.bucket === "raw" ? "raw readings" : `${r.bucket} buckets from a TimescaleDB continuous aggregate`}`;
  drawChart();
}

function drawChart() {
  const canvas = $("#chart");
  const [ctx, w, h] = setup(canvas);
  const pts = state.chart || [];
  const pad = { l: 48, r: 8, t: 10, b: 24 };
  if (pts.length < 2) {
    ctx.fillStyle = css("--muted");
    ctx.font = "13px system-ui";
    ctx.fillText("Waiting for data…", pad.l, h / 2);
    return;
  }
  const xs = pts.map((p) => new Date(p.time).getTime());
  const lo = Math.min(...pts.map((p) => p.min)), hi = Math.max(...pts.map((p) => p.max));
  const span = hi - lo || 1, ymax = hi + span * 0.08;
  const ymin = lo >= 0 ? Math.max(0, lo - span * 0.08) : lo - span * 0.08;
  const X = (t) => pad.l + ((t - xs[0]) / (xs[xs.length - 1] - xs[0] || 1)) * (w - pad.l - pad.r);
  const Y = (v) => pad.t + (1 - (v - ymin) / (ymax - ymin)) * (h - pad.t - pad.b);

  ctx.font = "11px system-ui";
  ctx.fillStyle = css("--muted");
  ctx.strokeStyle = css("--line");
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const v = ymin + ((ymax - ymin) * i) / 4, y = Y(v);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y); ctx.stroke();
    ctx.fillText(fmt(v), 4, y + 4);
  }
  const span_ms = xs[xs.length - 1] - xs[0];
  const ticks = w < 520 ? 3 : 4;
  for (let i = 0; i <= ticks; i++) {
    const t = xs[0] + (span_ms * i) / ticks;
    const d = new Date(t);
    const txt = span_ms > 2 * 864e5 ? d.toLocaleDateString(undefined, { weekday: "short", day: "numeric" }) : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    const x = X(t);
    ctx.fillText(txt, Math.min(Math.max(x - 18, pad.l), w - 60), h - 6);
  }

  const limit = gapLimit(xs);
  const runs = [[0]];
  for (let i = 1; i < pts.length; i++) (xs[i] - xs[i - 1] > limit ? runs.push([i]) : runs[runs.length - 1].push(i));

  if (pts.some((p) => p.max !== p.min)) {
    ctx.fillStyle = css("--band");
    for (const run of runs) {
      ctx.beginPath();
      run.forEach((i, k) => (k ? ctx.lineTo(X(xs[i]), Y(pts[i].max)) : ctx.moveTo(X(xs[i]), Y(pts[i].max))));
      for (let k = run.length - 1; k >= 0; k--) ctx.lineTo(X(xs[run[k]]), Y(pts[run[k]].min));
      ctx.fill();
    }
  }
  ctx.beginPath();
  for (const run of runs) run.forEach((i, k) => (k ? ctx.lineTo(X(xs[i]), Y(pts[i].avg)) : ctx.moveTo(X(xs[i]), Y(pts[i].avg))));
  ctx.strokeStyle = css("--chart");
  ctx.lineWidth = 2;
  ctx.lineJoin = "round";
  ctx.stroke();
}

async function loadStatus() {
  const [{ active }, stats] = await Promise.all([api("/api/alerts"), api("/api/stats")]);
  state.alerts = active;
  renderHead();
  $("#stats").innerHTML = [
    ["messages", "MQTT messages"], ["written", "rows written"], ["rejected", "rejected"], ["dropped", "dropped"], ["streamClients", "live clients"],
  ].map(([k, l]) => `<span><b>${(stats[k] ?? 0).toLocaleString()}</b> ${l}</span>`).join("") + `<span><b>${active.length}</b> active alerts</span>`;
}

function connect() {
  const es = new EventSource("/api/stream");
  es.onopen = () => { $("#live").classList.add("on"); $("#live-text").textContent = "Live"; };
  es.onerror = () => { $("#live").classList.remove("on"); $("#live-text").textContent = "Reconnecting"; };
  es.addEventListener("reading", (e) => {
    const r = JSON.parse(e.data);
    const d = state.devices.find((x) => x.id === r.device);
    if (d) d.lastSeen = r.time;
    else { state.devices.push({ id: r.device, lastSeen: r.time, metrics: 1 }); renderDevices(); }
    if (r.device !== state.device) return;
    state.latest[r.metric] = r;
    const h = (state.history[r.metric] ||= []);
    h.push([new Date(r.time).getTime(), r.value]);
    while (h.length > 300 || (h.length && h[0][0] < Date.now() - 600000)) h.shift();
    updateTile(r);
    if (r.metric === state.metric && state.range === "15m" && state.chart) {
      state.chart.push({ time: r.time, avg: r.value, min: r.value, max: r.value });
      const cutoff = Date.now() - 900000;
      while (state.chart.length && new Date(state.chart[0].time) < cutoff) state.chart.shift();
      drawChart();
    }
  });
}

$("#device-list").addEventListener("click", (e) => {
  const b = e.target.closest("button");
  if (b) selectDevice(b.dataset.id);
});
$("#tiles").addEventListener("click", (e) => {
  const t = e.target.closest(".tile");
  if (!t) return;
  state.metric = t.dataset.metric;
  document.querySelectorAll(".tile").forEach((x) => x.setAttribute("aria-pressed", x === t));
  loadChart();
});
document.querySelector(".ranges").addEventListener("click", (e) => {
  const b = e.target.closest("button");
  if (!b) return;
  state.range = b.dataset.range;
  document.querySelectorAll(".ranges button").forEach((x) => x.classList.toggle("on", x === b));
  loadChart();
});
window.addEventListener("resize", () => { drawChart(); renderTiles(); });

loadDevices().then(loadStatus).catch(console.error);
connect();
setInterval(() => { loadStatus().catch(() => {}); renderDevices(); renderHead(); }, 5000);
