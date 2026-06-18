"use strict";

// ---- local config state (the UI is the source of truth for config) ----
let cfg = null;

// Per-signal metadata driving the generated cards.
const SIGNALS = [
  {
    key: "traces", title: "Traces", unit: "spans/s", rxKey: "spans",
    extras: [
      { id: "childSpans", label: "Child spans / trace", type: "number", min: 0, max: 50 },
      { id: "statusCode", label: "Status code", type: "select", opts: ["Unset", "Ok", "Error"] },
      { id: "spanDuration", label: "Span duration", type: "text" },
      { id: "sizeMB", label: "Min size (MB)", type: "number", min: 0, max: 50 },
    ],
  },
  {
    key: "metrics", title: "Metrics", unit: "points/s", rxKey: "metricPoints",
    extras: [
      { id: "metricType", label: "Metric type", type: "select",
        opts: ["Gauge", "Sum", "Histogram", "ExponentialHistogram"] },
    ],
  },
  {
    key: "logs", title: "Logs", unit: "records/s", rxKey: "logRecords",
    extras: [
      { id: "body", label: "Log body", type: "text" },
      { id: "sizeMB", label: "Min size (MB)", type: "number", min: 0, max: 50 },
    ],
  },
];

const RATE_MAX = 5000;     // per-worker rate slider ceiling
const WORKERS_MAX = 50;
const HIST = 60;           // sparkline history length (samples)
const history = { spans: [], metricPoints: [], logRecords: [] };

// ---- helpers ----
const $ = (sel) => document.querySelector(sel);
const fmt = (n) => {
  if (n === null || n === undefined) return "0";
  if (n >= 1000) return (n / 1000).toFixed(n >= 100000 ? 0 : 1) + "k";
  return n.toFixed(n < 10 && n % 1 !== 0 ? 1 : 0);
};

function attrsToText(obj) {
  return Object.entries(obj || {}).map(([k, v]) => `${k}=${v}`).join("\n");
}
function textToAttrs(text) {
  const out = {};
  for (const line of text.split("\n")) {
    const t = line.trim();
    if (!t) continue;
    const i = t.indexOf("=");
    if (i <= 0) continue;
    out[t.slice(0, i).trim()] = t.slice(i + 1).trim();
  }
  return out;
}

// configured (target) records/sec computed client-side for instant feedback
function configuredRate(key) {
  const s = cfg[key];
  if (!s.enabled) return 0;
  if (!s.rate || s.rate <= 0) return -1; // unlimited
  // telemetrygen's rate limiter caps records/sec at rate*workers for every
  // signal (child-spans does not multiply throughput in rate mode).
  return s.rate * Math.max(1, s.workers);
}

// ---- rendering ----
function buildSignalCards() {
  const root = $(".signals");
  root.innerHTML = "";
  for (const sig of SIGNALS) {
    const s = cfg[sig.key];
    const el = document.createElement("div");
    el.className = `card signal ${sig.key}`;
    el.innerHTML = `
      <div class="signal-head">
        <h3>${sig.title}</h3>
        <label class="switch">
          <input type="checkbox" data-bind="${sig.key}.enabled">
          <span class="slider-toggle"></span>
        </label>
      </div>
      <div class="signal-rate">
        target <b data-target="${sig.key}">0</b> ${sig.unit}
        &nbsp;·&nbsp; received <span data-rx="${sig.key}">0</span> ${sig.unit}
      </div>
      <canvas class="spark" data-spark="${sig.rxKey}"></canvas>

      <div class="field">
        <div class="field-label"><span>Rate (records/s per worker)</span><b data-show="${sig.key}.rate">0</b></div>
        <input type="range" min="0" max="${RATE_MAX}" step="1" data-bind="${sig.key}.rate">
      </div>
      <div class="field">
        <div class="field-label"><span>Workers (goroutines)</span><b data-show="${sig.key}.workers">1</b></div>
        <input type="range" min="1" max="${WORKERS_MAX}" step="1" data-bind="${sig.key}.workers">
      </div>

      <div class="grid2">
        ${sig.extras.map((e) => renderExtra(sig.key, e)).join("")}
      </div>

      <details>
        <summary>Attributes (one key=value per line)</summary>
        <textarea rows="3" data-attrs="${sig.key}" placeholder="env=prod
region=us-east"></textarea>
      </details>
    `;
    root.appendChild(el);

    // initial values
    el.querySelector(`[data-bind="${sig.key}.enabled"]`).checked = s.enabled;
    el.querySelector(`[data-bind="${sig.key}.rate"]`).value = s.rate;
    el.querySelector(`[data-bind="${sig.key}.workers"]`).value = s.workers;
    el.querySelector(`[data-attrs="${sig.key}"]`).value = attrsToText(s.attributes);
    for (const e of sig.extras) {
      const node = el.querySelector(`[data-bind="${sig.key}.${e.id}"]`);
      if (node) node.value = s[e.id] ?? "";
    }
  }
}

function renderExtra(key, e) {
  if (e.type === "select") {
    return `<label>${e.label}
      <select data-bind="${key}.${e.id}">
        ${e.opts.map((o) => `<option value="${o}">${o}</option>`).join("")}
      </select></label>`;
  }
  const attrs = `data-bind="${key}.${e.id}" type="${e.type}"` +
    (e.min !== undefined ? ` min="${e.min}"` : "") +
    (e.max !== undefined ? ` max="${e.max}"` : "");
  return `<label>${e.label}<input ${attrs}></label>`;
}

// ---- two-way binding ----
function setPath(key, field, value) {
  cfg[key][field] = value;
}

function wireInputs() {
  // target collector fields
  bindTop("endpoint", "endpoint", (v) => v);
  bindTop("metricsEndpoint", "metricsEndpoint", (v) => v);
  bindTop("serviceName", "serviceName", (v) => v);
  bindTopCheck("insecure", "insecure");
  bindTopCheck("http", "http");

  // "input" fires immediately for every control type (checkbox, range, number,
  // select, text) — so anything you touch reflects in real time, not on blur.
  document.querySelectorAll("[data-bind]").forEach((node) => {
    const [key, field] = node.dataset.bind.split(".");
    node.addEventListener("input", () => {
      let val;
      if (node.type === "checkbox") val = node.checked;
      else if (node.type === "range" || node.type === "number") val = Number(node.value);
      else val = node.value;
      setPath(key, field, val);
      reflect();
      pushDebounced();
    });
  });

  document.querySelectorAll("[data-attrs]").forEach((node) => {
    node.addEventListener("input", () => {
      cfg[node.dataset.attrs].attributes = textToAttrs(node.value);
      pushDebounced();
    });
  });

  $("#stopAll").addEventListener("click", async () => {
    await fetch("/api/stop", { method: "POST" });
    // reflect disabled state back into the toggles
    for (const sig of SIGNALS) {
      cfg[sig.key].enabled = false;
      const t = document.querySelector(`[data-bind="${sig.key}.enabled"]`);
      if (t) t.checked = false;
    }
    reflect();
  });
}

function bindTop(id, field, conv) {
  const node = $("#" + id);
  node.addEventListener("change", () => { cfg[field] = conv(node.value); pushDebounced(); });
}
function bindTopCheck(id, field) {
  const node = $("#" + id);
  node.addEventListener("input", () => { cfg[field] = node.checked; pushDebounced(); });
}

// reflect updates the live labels that depend purely on local config
function reflect() {
  for (const sig of SIGNALS) {
    const s = cfg[sig.key];
    setText(`[data-show="${sig.key}.rate"]`, s.rate <= 0 ? "∞ (unthrottled)" : fmt(s.rate));
    setText(`[data-show="${sig.key}.workers"]`, s.workers);
    const target = configuredRate(sig.key);
    setText(`[data-target="${sig.key}"]`, target < 0 ? "∞" : fmt(target));
  }
}
function setText(sel, v) {
  const n = document.querySelector(sel);
  if (n) n.textContent = v;
}

// ---- push config (debounced) ----
let pushTimer = null;
function pushDebounced() {
  clearTimeout(pushTimer);
  pushTimer = setTimeout(pushConfig, 250);
}
async function pushConfig() {
  try {
    await fetch("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(cfg),
    });
  } catch (e) {
    console.error("config push failed", e);
  }
}

// ---- live stream (numbers only; never touches form fields) ----
function connectStream() {
  const es = new EventSource("/api/stream");
  es.onopen = () => setStat("#st-backend", true, "Backend connected");
  es.onerror = () => {
    setStat("#st-backend", false, "Backend disconnected");
    setStat("#st-endpoint", false, "Collector endpoint —");
    setStat("#st-metrics", false, "Metrics endpoint —");
  };
  es.onmessage = (msg) => {
    const snap = JSON.parse(msg.data);
    setStat("#st-backend", true, "Backend connected");
    renderLive(snap);
  };
}

// setStat paints a status indicator (ok = green, bad = red) with a label.
function setStat(sel, ok, label) {
  const el = document.querySelector(sel);
  if (!el) return;
  el.className = "stat " + (ok ? "ok" : "bad");
  el.querySelector("span").textContent = label;
}

function renderLive(snap) {
  // connectivity status
  const st = snap.status || {};
  setStat("#st-endpoint", !!st.endpointOnline,
    `Collector ${st.endpoint || "?"} ${st.endpointOnline ? "online" : "offline"}`);
  setStat("#st-metrics", !!st.metricsOnline,
    `Metrics ${st.metricsURL || "?"} ${st.metricsOnline ? "online" : "offline"}`);

  // totals
  setText("#totalConfigured", fmt(snap.configured.total));
  const rx = snap.received;
  setText("#totalReceived", rx.available ? fmt(rx.total) : "—");
  setText("#receivedStatus", rx.available ? "" : "metrics endpoint unreachable");

  // per-signal received + sparkline
  for (const sig of SIGNALS) {
    const v = rx.available ? rx[sig.rxKey] || 0 : 0;
    setText(`[data-rx="${sig.key}"]`, rx.available ? fmt(v) : "—");
    pushHistory(sig.rxKey, v);
    drawSpark(sig.rxKey);
  }

  // logs
  const pre = $("#logs");
  const atBottom = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 10;
  pre.textContent = (snap.logs || []).join("\n");
  if (atBottom) pre.scrollTop = pre.scrollHeight;
}

function pushHistory(key, v) {
  const h = history[key];
  h.push(v);
  if (h.length > HIST) h.shift();
}

function drawSpark(key) {
  const c = document.querySelector(`[data-spark="${key}"]`);
  if (!c) return;
  const dpr = window.devicePixelRatio || 1;
  const w = c.clientWidth, h = c.clientHeight;
  if (c.width !== w * dpr) { c.width = w * dpr; c.height = h * dpr; }
  const ctx = c.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  const data = history[key];
  if (data.length < 2) return;
  const max = Math.max(1, ...data);
  const stepX = w / (HIST - 1);
  const colors = { spans: "#f59e0b", metricPoints: "#34d399", logRecords: "#a78bfa" };
  ctx.beginPath();
  data.forEach((v, i) => {
    const x = i * stepX;
    const y = h - 4 - (v / max) * (h - 8);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = colors[key];
  ctx.lineWidth = 1.8;
  ctx.stroke();
  ctx.lineTo((data.length - 1) * stepX, h);
  ctx.lineTo(0, h);
  ctx.closePath();
  ctx.globalAlpha = 0.12;
  ctx.fillStyle = colors[key];
  ctx.fill();
  ctx.globalAlpha = 1;
}

// ---- bootstrap ----
async function init() {
  const res = await fetch("/api/state");
  const snap = await res.json();
  cfg = snap.config;

  // top fields
  $("#endpoint").value = cfg.endpoint;
  $("#metricsEndpoint").value = cfg.metricsEndpoint || "";
  $("#serviceName").value = cfg.serviceName;
  $("#insecure").checked = cfg.insecure;
  $("#http").checked = cfg.http;

  buildSignalCards();
  wireInputs();
  reflect();
  connectStream();
}

init();
