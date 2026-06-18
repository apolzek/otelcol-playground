"""Smoke tests for the bubble-chart backend and the geometric load planner.

Runs without docker / kafka / flink: a fake Prometheus is started on a local
port and the FastAPI app is pointed at it via env var before being imported.
"""
from __future__ import annotations

import importlib
import json
import os
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

# Allow tests/run-bubble-load.py to be importable as `run_bubble_load`
import importlib.util
_loader_spec = importlib.util.spec_from_file_location(
    "run_bubble_load", REPO / "tests" / "run-bubble-load.py"
)
run_bubble_load = importlib.util.module_from_spec(_loader_spec)
assert _loader_spec.loader is not None
sys.modules["run_bubble_load"] = run_bubble_load  # @dataclass needs this
_loader_spec.loader.exec_module(run_bubble_load)


# ---------------------------------------------------------------------------
# Geometric plan
# ---------------------------------------------------------------------------
class TestBuildPlan(unittest.TestCase):
    def test_top_matches_target(self):
        plan = run_bubble_load.build_plan(20, 100_000, 0.20, 600)
        self.assertEqual(len(plan), 20)
        self.assertEqual(plan[0].target_records, 100_000)
        self.assertAlmostEqual(plan[0].rate, 100_000 / 600, places=3)

    def test_geometric_decay(self):
        plan = run_bubble_load.build_plan(20, 100_000, 0.20, 600)
        # Each row is 80% of the previous one.
        for prev, curr in zip(plan, plan[1:]):
            ratio = curr.target_records / prev.target_records
            self.assertAlmostEqual(ratio, 0.80, delta=0.01)

    def test_specific_values(self):
        plan = run_bubble_load.build_plan(20, 100_000, 0.20, 600)
        # 100k, 80k, 64k, 51.2k, ...
        expected = [100_000, 80_000, 64_000, 51_200, 40_960]
        for i, want in enumerate(expected):
            self.assertEqual(plan[i].target_records, want)

    def test_names_indexed(self):
        plan = run_bubble_load.build_plan(3, 1000, 0.5, 60, prefix="x")
        self.assertEqual([r.name for r in plan], ["x-01", "x-02", "x-03"])

    def test_invalid_inputs(self):
        with self.assertRaises(ValueError):
            run_bubble_load.build_plan(0, 100, 0.2, 60)
        with self.assertRaises(ValueError):
            run_bubble_load.build_plan(5, 100, 1.0, 60)
        with self.assertRaises(ValueError):
            run_bubble_load.build_plan(5, 100, 0.2, 0)


# ---------------------------------------------------------------------------
# Fake Prometheus + backend
# ---------------------------------------------------------------------------
def _fake_prom_response(records) -> dict:
    """records may be {svc: value} (defaults to signal=traces) or {(svc, sig): value}."""
    items = []
    for key, value in records.items():
        if isinstance(key, tuple):
            svc, sig = key
        else:
            svc, sig = key, "traces"
        items.append({
            "metric": {"service_name": svc, "signal": sig},
            "value": [time.time(), str(value)],
        })
    return {"status": "success", "data": {"resultType": "vector", "result": items}}


class FakeProm:
    """Minimal HTTP server that mimics Prometheus's /api/v1/query."""

    def __init__(self):
        self.records: dict[str, float] = {}
        self.last_query: str | None = None
        self._server: HTTPServer | None = None
        self._thread: threading.Thread | None = None
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path != "/api/v1/query":
                    self.send_error(404)
                    return
                qs = parse_qs(parsed.query)
                outer.last_query = (qs.get("query") or [""])[0]
                payload = _fake_prom_response(outer.records)
                body = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args, **kwargs):  # silence
                return

        self._handler_cls = Handler

    def start(self) -> int:
        self._server = HTTPServer(("127.0.0.1", 0), self._handler_cls)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self._server.server_port

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()


def _import_backend(prom_url: str, poll: float = 0.1, top: int = 30):
    os.environ["PROMETHEUS_URL"] = prom_url
    os.environ["POLL_INTERVAL"] = str(poll)
    os.environ["TOP_N"] = str(top)
    os.environ.pop("SIGNAL_FILTER", None)
    os.environ.pop("SERVICE_REGEX", None)
    # force reload to pick up env vars
    if "app.backend.main" in sys.modules:
        del sys.modules["app.backend.main"]
    return importlib.import_module("app.backend.main")


class TestBackend(unittest.TestCase):
    def setUp(self):
        self.prom = FakeProm()
        self.port = self.prom.start()

    def tearDown(self):
        self.prom.stop()

    def test_health(self):
        backend = _import_backend(f"http://127.0.0.1:{self.port}")
        from fastapi.testclient import TestClient
        client = TestClient(backend.app)
        with client:
            r = client.get("/api/health")
            self.assertEqual(r.status_code, 200)
            body = r.json()
            self.assertTrue(body["ok"])
            self.assertEqual(set(body["signals"]), {"all", "traces", "logs", "metrics"})
            self.assertTrue(body["prom"].endswith(str(self.port)))

    def test_snapshot_endpoint_returns_sorted_top_n(self):
        self.prom.records = {f"svc-{i:02d}": float(1000 - i * 50) for i in range(20)}
        backend = _import_backend(f"http://127.0.0.1:{self.port}", poll=0.05, top=5)
        from fastapi.testclient import TestClient
        client = TestClient(backend.app)
        with client:
            # wait for poll loop to populate snapshot
            deadline = time.time() + 5
            data = None
            while time.time() < deadline:
                r = client.get("/api/services")
                self.assertEqual(r.status_code, 200)
                data = r.json()
                if data["services"]:
                    break
                time.sleep(0.1)
            self.assertIsNotNone(data)
            self.assertGreaterEqual(len(data["services"]), 1)
            values = [s["value"] for s in data["services"]]
            self.assertEqual(values, sorted(values, reverse=True))
            for s in data["services"]:
                self.assertIn("service_name", s)
                self.assertIsInstance(s["value"], float)

    def test_query_groups_by_service_and_signal(self):
        self.prom.records = {"a": 1.0}
        backend = _import_backend(f"http://127.0.0.1:{self.port}", poll=0.05, top=7)
        from fastapi.testclient import TestClient
        client = TestClient(backend.app)
        with client:
            deadline = time.time() + 3
            while time.time() < deadline and self.prom.last_query is None:
                client.get("/api/services")
                time.sleep(0.1)
        self.assertIsNotNone(self.prom.last_query)
        self.assertIn("sum by (service_name, signal)", self.prom.last_query)
        # filtering happens in Python now, not Prometheus, so the query has no
        # signal selector
        self.assertNotIn('signal="', self.prom.last_query)

    def test_signal_slicing(self):
        # heavy on traces, lighter on logs and metrics
        self.prom.records = {
            ("alpha", "traces"): 100.0,
            ("alpha", "logs"): 30.0,
            ("alpha", "metrics"): 10.0,
            ("beta", "traces"): 5.0,
            ("beta", "logs"): 50.0,
        }
        backend = _import_backend(f"http://127.0.0.1:{self.port}", poll=0.05)
        from fastapi.testclient import TestClient
        client = TestClient(backend.app)
        with client:
            # wait for poll
            deadline = time.time() + 3
            while time.time() < deadline and not backend.state.records:
                time.sleep(0.05)

            r = client.get("/api/services?signal=traces")
            self.assertEqual(r.status_code, 200)
            body = r.json()
            self.assertEqual(body["signal"], "traces")
            self.assertEqual(body["services"][0]["service_name"], "alpha")
            self.assertEqual(body["services"][0]["value"], 100.0)
            self.assertEqual(body["services"][1]["service_name"], "beta")
            self.assertEqual(body["services"][1]["value"], 5.0)

            r = client.get("/api/services?signal=logs")
            body = r.json()
            self.assertEqual(body["services"][0]["service_name"], "beta")
            self.assertEqual(body["services"][0]["value"], 50.0)

            r = client.get("/api/services?signal=metrics")
            body = r.json()
            self.assertEqual(len(body["services"]), 1)
            self.assertEqual(body["services"][0]["service_name"], "alpha")

            r = client.get("/api/services?signal=all")
            body = r.json()
            # alpha = 140, beta = 55
            names = [s["service_name"] for s in body["services"]]
            values = {s["service_name"]: s["value"] for s in body["services"]}
            self.assertEqual(names[0], "alpha")
            self.assertEqual(values["alpha"], 140.0)
            self.assertEqual(values["beta"], 55.0)

            # invalid signal should fall back to "all"
            r = client.get("/api/services?signal=garbage")
            self.assertEqual(r.json()["signal"], "all")

    def test_totals_endpoint(self):
        self.prom.records = {
            ("a", "traces"): 10.0, ("a", "logs"): 5.0,
            ("b", "metrics"): 7.0,
        }
        backend = _import_backend(f"http://127.0.0.1:{self.port}", poll=0.05)
        from fastapi.testclient import TestClient
        client = TestClient(backend.app)
        with client:
            deadline = time.time() + 3
            while time.time() < deadline and not backend.state.records:
                time.sleep(0.05)
            r = client.get("/api/totals")
            self.assertEqual(r.status_code, 200)
            t = r.json()["totals"]
            self.assertEqual(t["traces"], 10.0)
            self.assertEqual(t["logs"], 5.0)
            self.assertEqual(t["metrics"], 7.0)
            self.assertEqual(t["all"], 22.0)

    def test_sse_stream_emits_initial_event(self):
        # Boot the real ASGI app in a uvicorn thread and hit it over HTTP —
        # TestClient's sync streaming doesn't play well with infinite SSE
        # generators that block on asyncio.Queue.get().
        self.prom.records = {"alpha": 42.0, "beta": 7.0}
        backend = _import_backend(f"http://127.0.0.1:{self.port}", poll=0.1)
        import socket
        import uvicorn
        import urllib.request

        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()

        cfg = uvicorn.Config(backend.app, host="127.0.0.1", port=port,
                             log_level="warning")
        server = uvicorn.Server(cfg)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()

        try:
            # wait for server up
            deadline = time.time() + 5
            while time.time() < deadline and not server.started:
                time.sleep(0.05)
            self.assertTrue(server.started, "uvicorn never started")

            # wait for first poll to populate state.records
            deadline = time.time() + 5
            while time.time() < deadline:
                if backend.state.records:
                    break
                time.sleep(0.05)
            self.assertTrue(backend.state.records)

            req = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/stream", timeout=5)
            self.assertEqual(req.headers.get("content-type"), "text/event-stream; charset=utf-8")
            buf = b""
            deadline = time.time() + 5
            while time.time() < deadline:
                chunk = req.read(1024)
                if not chunk:
                    break
                buf += chunk
                if b"\n\n" in buf:
                    break
            req.close()
            self.assertIn(b"data: ", buf)
            payload = buf.decode().split("data: ", 1)[1].split("\n\n", 1)[0]
            snap = json.loads(payload)
            names = {s["service_name"] for s in snap["services"]}
            self.assertEqual(names, {"alpha", "beta"})
        finally:
            server.should_exit = True
            thread.join(timeout=5)

    def test_handles_prom_unreachable(self):
        # point at a closed port
        backend = _import_backend("http://127.0.0.1:1", poll=0.05)
        from fastapi.testclient import TestClient
        client = TestClient(backend.app)
        with client:
            time.sleep(0.3)
            r = client.get("/api/services")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["services"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
