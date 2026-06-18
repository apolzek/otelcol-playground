"""
OTLP service-bubble backend.

Pulls per-service counters from Prometheus (the metrics that the Flink
otlp-insights-processor job exposes via the Flink Prometheus reporter) and
pushes the top-N snapshot to the browser over Server-Sent Events.

One Prometheus query per poll cycle returns counts broken down by
(service_name, signal). Each subscriber picks one of {all, traces, logs,
metrics}; the slice is computed on demand for each tick.

Why HTTP+SSE instead of gRPC:
  - the consumer is a browser (gRPC-web would need an envoy/grpcwebproxy hop)
  - the stream is unidirectional server -> client
  - the upstream API (Prometheus query) is already HTTP/JSON
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

log = logging.getLogger("bubble-backend")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

PROM_URL = os.getenv("PROMETHEUS_URL", "http://localhost:9090").rstrip("/")
TOP_N = int(os.getenv("TOP_N", "30"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2.0"))
# PromQL regex applied to service_name; only matching services are returned.
SERVICE_REGEX = os.getenv("SERVICE_REGEX", "")

# Counter exposed by the Flink job's PrometheusReporter (see README.md). The
# label-key interleaving in the metric name is a quirk of the reporter; the
# query joins on the label *values*.
_COUNTER = (
    "flink_taskmanager_job_task_operator_signal_service_name"
    "_otlp_signal_records_by_service_total"
)

VALID_SIGNALS = {"all", "traces", "logs", "metrics"}


def _build_query(svc_regex: str = "") -> str:
    sel = f'{{service_name=~"{svc_regex}"}}' if svc_regex else ""
    return f"sum by (service_name, signal) ({_COUNTER}{sel})"


class State:
    """Latest poll snapshot. `records` is keyed by (service_name, signal)."""

    def __init__(self) -> None:
        self.records: dict[tuple[str, str], float] = {}
        self.ts: float = 0.0
        # Each subscriber: (queue, signal). The queue is filled with already-
        # filtered snapshots so the SSE generator doesn't need state-lock work.
        self.subscribers: list[tuple[asyncio.Queue, str]] = []


state = State()


def _normalize_signal(signal: Optional[str]) -> str:
    s = (signal or "").strip().lower()
    return s if s in VALID_SIGNALS else "all"


def compute_snapshot(signal: str) -> dict:
    """Slice `state.records` by signal and return the top-N services."""
    signal = _normalize_signal(signal)
    if signal == "all":
        agg: dict[str, float] = {}
        for (svc, _sig), v in state.records.items():
            agg[svc] = agg.get(svc, 0.0) + v
    else:
        agg = {svc: v for (svc, sig), v in state.records.items() if sig == signal}

    services = sorted(
        ({"service_name": s, "value": v} for s, v in agg.items()),
        key=lambda x: x["value"],
        reverse=True,
    )[:TOP_N]
    return {"services": services, "ts": state.ts, "signal": signal}


def signal_totals() -> dict[str, float]:
    """Total record count per signal — used for tab counters."""
    totals = {"traces": 0.0, "logs": 0.0, "metrics": 0.0}
    for (_svc, sig), v in state.records.items():
        if sig in totals:
            totals[sig] += v
    totals["all"] = sum(totals.values())
    return totals


async def _fetch(client: httpx.AsyncClient) -> dict[tuple[str, str], float]:
    query = _build_query(SERVICE_REGEX)
    try:
        r = await client.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": query},
            timeout=5.0,
        )
        r.raise_for_status()
    except (httpx.HTTPError, httpx.TimeoutException) as exc:
        log.warning("prometheus query failed: %s", exc)
        return {}

    payload = r.json()
    if payload.get("status") != "success":
        log.warning("prometheus non-success: %s", payload)
        return {}

    records: dict[tuple[str, str], float] = {}
    for item in payload.get("data", {}).get("result", []):
        try:
            value = float(item["value"][1])
        except (KeyError, IndexError, ValueError):
            continue
        metric = item.get("metric", {})
        svc = metric.get("service_name", "unknown")
        sig = metric.get("signal", "unknown")
        records[(svc, sig)] = value
    return records


async def _poll_loop() -> None:
    async with httpx.AsyncClient() as client:
        while True:
            records = await _fetch(client)
            state.records = records
            state.ts = time.time()
            for q, sig in list(state.subscribers):
                try:
                    q.put_nowait(compute_snapshot(sig))
                except asyncio.QueueFull:
                    pass
            await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.create_task(_poll_loop(), name="bubble-poll")
    log.info("polling %s every %.1fs (top=%d svc_regex=%s)",
             PROM_URL, POLL_INTERVAL, TOP_N, SERVICE_REGEX or "*")
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan, title="OTLP service bubbles")


@app.middleware("http")
async def no_cache(request, call_next):
    response = await call_next(request)
    if request.url.path.endswith((".js", ".css", ".html", "/")):
        response.headers["Cache-Control"] = "no-store, must-revalidate"
    return response


@app.get("/api/health")
async def health() -> dict:
    return {"ok": True, "prom": PROM_URL, "top_n": TOP_N,
            "signals": sorted(VALID_SIGNALS)}


@app.get("/api/services")
async def services_snapshot(signal: str = Query("all")) -> dict:
    return compute_snapshot(signal)


@app.get("/api/totals")
async def totals_endpoint() -> dict:
    """Per-signal aggregate, for the UI tab counters."""
    return {"ts": state.ts, "totals": signal_totals()}


@app.get("/api/stream")
async def services_stream(signal: str = Query("all")):
    sig = _normalize_signal(signal)

    async def gen():
        q: asyncio.Queue = asyncio.Queue(maxsize=4)
        sub = (q, sig)
        state.subscribers.append(sub)
        try:
            yield f"data: {json.dumps(compute_snapshot(sig))}\n\n"
            while True:
                snap = await q.get()
                yield f"data: {json.dumps(snap)}\n\n"
        finally:
            try:
                state.subscribers.remove(sub)
            except ValueError:
                pass

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


_FRONTEND = Path(__file__).resolve().parent.parent / "frontend"
if _FRONTEND.is_dir():
    app.mount("/", StaticFiles(directory=str(_FRONTEND), html=True), name="frontend")
