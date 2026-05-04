"use strict";

console.info("[bubble-zoom] frontend build with manual transform zoom loaded");

const svg = d3.select("#chart");
const meta = document.getElementById("meta");
const dot = document.getElementById("status-dot");
const tooltip = document.getElementById("tooltip");
const root = svg.append("g").attr("class", "root");

const nodes = new Map();
let simulation = null;
let lastSnapshot = null;

// ─── Viewport (CSS-transform style — mirrors timeline.nochaos.io) ─────────────
// World coordinates are centred on (cw/2, ch/2). Screen position of any world
// point P is: cw/2 + panX + (P.x - cw/2) * zoom.
const ZOOM_MIN = 0.05;
const ZOOM_MAX = 8;
const view = { zoom: 1, panX: 0, panY: 0 };

const zoomDisplay = document.getElementById("zoomDisplay");

function applyTransform() {
  const [cw, ch] = size();
  const z = view.zoom;
  // translate(centre + pan) · scale · translate(-centre) — scale around centre.
  root.attr("transform",
    `translate(${cw / 2 + view.panX} ${ch / 2 + view.panY}) ` +
    `scale(${z}) translate(${-cw / 2} ${-ch / 2})`);
  if (zoomDisplay) zoomDisplay.textContent = `${Math.round(z * 100)}%`;
  applyLabelStyles();
}

// Label visibility/size is decided in **screen** pixels: apparent radius is
// `d.r * view.zoom`. Font is counter-scaled by 1/zoom so labels stay at a
// constant ~12 px on screen regardless of zoom level. Tiny bubbles fall back
// to the trailing number (e.g. "16") so something is always readable.
function applyLabelStyles() {
  const z = view.zoom;
  const fontWorld = 12 / z;
  root.selectAll("g.bubble text")
    .each(function (d) {
      const screenR = d.r * z;
      let label = d.key;
      if (screenR < 30) {
        const m = d.key.match(/(\d+)$/);
        if (m) label = m[1];
      }
      this.textContent = label;
      this.style.fontSize = `${fontWorld}px`;
      this.style.opacity = screenR > 4 ? 1 : 0;
    });
}

function setZoom(newZoom, anchorScreenX, anchorScreenY) {
  newZoom = Math.min(Math.max(newZoom, ZOOM_MIN), ZOOM_MAX);
  if (anchorScreenX != null) {
    const [cw, ch] = size();
    // World coord under anchor before zoom.
    const wx = (anchorScreenX - cw / 2 - view.panX) / view.zoom;
    const wy = (anchorScreenY - ch / 2 - view.panY) / view.zoom;
    // Pan so anchor stays put after zoom change.
    view.panX = anchorScreenX - cw / 2 - wx * newZoom;
    view.panY = anchorScreenY - ch / 2 - wy * newZoom;
  } else {
    const ratio = newZoom / view.zoom;
    view.panX *= ratio;
    view.panY *= ratio;
  }
  view.zoom = newZoom;
  applyTransform();
}

function zoomBy(factor, anchorX, anchorY) {
  setZoom(view.zoom * factor, anchorX, anchorY);
}

function resetView() {
  view.zoom = 1; view.panX = 0; view.panY = 0;
  applyTransform();
}

// Wheel handler. Bound to `main` (parent container) so it covers the entire
// canvas area regardless of which child the cursor is over. Also listens at
// document level as a fallback — a non-passive document listener won't go
// through if anything calls preventDefault upstream, but on a clean page it
// fires reliably.
const mainEl = document.querySelector("main");
const onWheel = (e) => {
  if (!e.target.closest || !e.target.closest("main")) return;
  e.preventDefault();
  const rect = svg.node().getBoundingClientRect();
  const mx = e.clientX - rect.left;
  const my = e.clientY - rect.top;
  const factor = e.deltaY > 0 ? 0.92 : 1.08;
  setZoom(view.zoom * factor, mx, my);
};
mainEl.addEventListener("wheel", onWheel, { passive: false });
svg.node().addEventListener("wheel", onWheel, { passive: false });

// Pan: pointer drag anywhere on the canvas (skip bubble circles & UI buttons).
let isPanning = false, panStartX = 0, panStartY = 0, panOriginX = 0, panOriginY = 0;
const onPointerDown = (e) => {
  if (e.button !== 0) return;
  if (e.target.closest("circle, button")) return;
  isPanning = true;
  panStartX = e.clientX; panStartY = e.clientY;
  panOriginX = view.panX; panOriginY = view.panY;
  try { mainEl.setPointerCapture(e.pointerId); } catch {}
  svg.node().classList.add("panning");
};
const onPointerMove = (e) => {
  if (!isPanning) return;
  view.panX = panOriginX + (e.clientX - panStartX);
  view.panY = panOriginY + (e.clientY - panStartY);
  applyTransform();
};
const endPan = () => {
  isPanning = false;
  svg.node().classList.remove("panning");
};
mainEl.addEventListener("pointerdown", onPointerDown);
mainEl.addEventListener("pointermove", onPointerMove);
mainEl.addEventListener("pointerup", endPan);
mainEl.addEventListener("pointercancel", endPan);
mainEl.addEventListener("pointerleave", endPan);

// Double-click empty canvas → reset.
mainEl.addEventListener("dblclick", (e) => {
  if (e.target.closest("circle, button")) return;
  resetView();
});

// Keyboard shortcuts: + − 0.
window.addEventListener("keydown", (e) => {
  if (e.target.closest("input, textarea")) return;
  switch (e.key) {
    case "+": case "=": zoomBy(1.2); break;
    case "-": case "_": zoomBy(1 / 1.2); break;
    case "0": resetView(); break;
  }
});

// HUD buttons.
const bind = (id, fn) => {
  const el = document.getElementById(id);
  if (el) el.addEventListener("click", fn);
};
bind("zoomIn",  () => zoomBy(1.25));
bind("zoomOut", () => zoomBy(1 / 1.25));
bind("zoomReset", resetView);
window.resetView = resetView;

function size() {
  const r = svg.node().getBoundingClientRect();
  return [Math.max(r.width, 320), Math.max(r.height, 240)];
}

function radiusScale(width, height, max) {
  const minDim = Math.min(width, height);
  const cap = Math.max(40, minDim / 3.5);
  return d3.scaleSqrt().domain([0, Math.max(max, 1)]).range([6, cap]);
}

function colorFor(value, max) {
  const t = max > 0 ? value / max : 0;
  return d3.interpolateInferno(0.25 + 0.7 * t);
}

function ensureSim(width, height) {
  if (simulation) return simulation;
  simulation = d3.forceSimulation()
    .force("x", d3.forceX(width / 2).strength(0.05))
    .force("y", d3.forceY(height / 2).strength(0.05))
    .force("charge", d3.forceManyBody().strength(4))
    .force("collide", d3.forceCollide(d => (d.r || 4) + 2).iterations(2))
    .alphaDecay(0.03)
    .velocityDecay(0.35)
    .on("tick", ticked);
  return simulation;
}

function ticked() {
  root.selectAll("g.bubble")
    .attr("transform", d => `translate(${d.x},${d.y})`);
}

function update(snapshot) {
  lastSnapshot = snapshot;
  const [w, h] = size();
  svg.attr("viewBox", `0 0 ${w} ${h}`);
  applyTransform();

  const services = snapshot.services || [];
  const max = d3.max(services, s => s.value) || 1;
  const r = radiusScale(w, h, max);

  const seen = new Set();
  for (const s of services) {
    seen.add(s.service_name);
    const existing = nodes.get(s.service_name);
    if (existing) {
      existing.value = s.value;
      existing.r = r(s.value);
    } else {
      nodes.set(s.service_name, {
        key: s.service_name,
        value: s.value,
        r: r(s.value),
        x: w / 2 + (Math.random() - 0.5) * 80,
        y: h / 2 + (Math.random() - 0.5) * 80,
      });
    }
  }
  for (const k of [...nodes.keys()]) {
    if (!seen.has(k)) nodes.delete(k);
  }

  const data = [...nodes.values()];
  const sim = ensureSim(w, h);
  sim.nodes(data);
  sim.force("collide", d3.forceCollide(d => d.r + 2).iterations(2));
  sim.force("x", d3.forceX(w / 2).strength(0.05));
  sim.force("y", d3.forceY(h / 2).strength(0.05));
  sim.alpha(0.45).restart();

  const groups = root.selectAll("g.bubble").data(data, d => d.key);

  groups.exit()
    .transition().duration(400)
    .style("opacity", 0)
    .remove();

  const entered = groups.enter().append("g").attr("class", "bubble");
  entered.append("circle")
    .attr("r", 0)
    .style("opacity", 0)
    .on("mousemove", showTooltip)
    .on("mouseleave", hideTooltip)
    .on("click", (_e, d) => {
      console.info("[bubble]", d.key, d.value);
    });
  entered.append("text").attr("text-anchor", "middle").attr("dy", "0.35em");

  const merged = entered.merge(groups);
  merged.select("circle")
    .transition().duration(1500).ease(d3.easeCubicOut)
    .attr("r", d => d.r)
    .attr("fill", d => colorFor(d.value, max))
    .style("opacity", 0.92);
  merged.select("text").text(d => d.key);
  applyLabelStyles();

  const sig = snapshot.signal ? snapshot.signal : "all signals";
  const ts = snapshot.ts ? new Date(snapshot.ts * 1000).toLocaleTimeString() : "—";
  meta.textContent =
    `${services.length} services · ${sig} · max ${Math.round(max).toLocaleString()} records · updated ${ts}`;
}

function showTooltip(event, d) {
  tooltip.hidden = false;
  tooltip.textContent = `${d.key} — ${Math.round(d.value).toLocaleString()} records`;
  const rect = svg.node().getBoundingClientRect();
  tooltip.style.left = `${event.clientX - rect.left + 12}px`;
  tooltip.style.top = `${event.clientY - rect.top + 12}px`;
}

function hideTooltip() { tooltip.hidden = true; }

function setStatus(state) {
  dot.classList.remove("live", "dead");
  if (state) dot.classList.add(state);
}

let activeSignal = "all";
let evtSource = null;

function connect() {
  if (evtSource) { try { evtSource.close(); } catch {} }
  evtSource = new EventSource(`/api/stream?signal=${encodeURIComponent(activeSignal)}`);
  evtSource.onopen = () => setStatus("live");
  evtSource.onerror = () => {
    setStatus("dead");
    try { evtSource.close(); } catch {}
    setTimeout(connect, 2000);
  };
  evtSource.onmessage = (e) => {
    try {
      update(JSON.parse(e.data));
    } catch (err) {
      console.error("bad SSE payload", err);
    }
  };
}

function selectSignal(sig) {
  if (sig === activeSignal) return;
  activeSignal = sig;
  // Reset bubble nodes so the new signal's services don't inherit positions
  // and radii from the previous slice (the value scale changes too).
  nodes.clear();
  root.selectAll("g.bubble").remove();
  if (simulation) simulation.nodes([]);
  document.querySelectorAll(".signal-tabs .tab").forEach(t => {
    t.classList.toggle("active", t.dataset.signal === sig);
  });
  connect();
}

document.querySelectorAll(".signal-tabs .tab").forEach((tab) => {
  tab.addEventListener("click", () => selectSignal(tab.dataset.signal));
});

// Periodic per-signal totals → tab counters.
async function refreshTotals() {
  try {
    const r = await fetch("/api/totals", { cache: "no-store" });
    if (!r.ok) return;
    const { totals } = await r.json();
    for (const sig of ["all", "traces", "logs", "metrics"]) {
      const el = document.getElementById(`cnt-${sig}`);
      if (el) el.textContent = formatCount(totals[sig] || 0);
    }
  } catch {}
}
function formatCount(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "k";
  return Math.round(n).toString();
}
refreshTotals();
setInterval(refreshTotals, 3000);

const ro = new ResizeObserver(() => {
  if (lastSnapshot) update(lastSnapshot);
});
ro.observe(svg.node());

connect();
