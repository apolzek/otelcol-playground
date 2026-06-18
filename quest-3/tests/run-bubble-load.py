#!/usr/bin/env python3
"""
Geometric telemetrygen launcher for the bubble-chart demo.

Spawns N parallel telemetrygen workers, one per synthetic service. The first
service targets `--top-records` records over `--duration` seconds; each
subsequent service sends `(1 - --decay)` × the previous service's volume. The
result feeds Kafka -> Flink -> Prometheus, which the bubble-chart backend
queries.

Defaults: 20 services, top sends 100_000 records over 600s (=> ~167 r/s),
20% decay -> the smallest service ends at ~144 records total.

Examples:
    ./run-bubble-load.py
    ./run-bubble-load.py --duration 300 --signal logs
    ./run-bubble-load.py --dry-run     # show plan, do not launch
"""
from __future__ import annotations

import argparse
import signal as posix_signal
import subprocess
import sys
import time
from dataclasses import dataclass

IMAGE = "ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.147.0"


@dataclass
class PlanRow:
    name: str
    rate: float
    target_records: int


def build_plan(services: int, top_records: int, decay: float,
               duration_seconds: int, prefix: str = "bubble-svc") -> list[PlanRow]:
    if services <= 0:
        raise ValueError("services must be > 0")
    if not 0 <= decay < 1:
        raise ValueError("decay must be in [0, 1)")
    if duration_seconds <= 0:
        raise ValueError("duration must be > 0")

    plan: list[PlanRow] = []
    factor = 1.0
    for i in range(services):
        records = top_records * factor
        rate = records / duration_seconds
        plan.append(PlanRow(
            name=f"{prefix}-{i+1:02d}",
            rate=rate,
            target_records=int(round(records)),
        ))
        factor *= (1.0 - decay)
    return plan


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--services", type=int, default=20)
    p.add_argument("--top-records", type=int, default=100_000,
                   help="records the busiest service sends over --duration")
    p.add_argument("--decay", type=float, default=0.20,
                   help="fraction by which each service decreases vs the previous one (0..1)")
    p.add_argument("--duration", type=int, default=600,
                   help="run length in seconds")
    p.add_argument("--signal", default="traces", choices=["traces", "logs", "metrics"])
    p.add_argument("--endpoint", default="localhost:4317",
                   help="OTLP gRPC endpoint (collector-l1)")
    p.add_argument("--prefix", default="bubble-svc",
                   help="service.name prefix for synthetic services")
    p.add_argument("--dry-run", action="store_true",
                   help="print the plan and exit without launching containers")
    return p.parse_args()


def launch_worker(row: PlanRow, signal_type: str, endpoint: str,
                  duration_seconds: int) -> str:
    cmd = [
        "docker", "run", "--rm", "-d",
        "--network", "host",
        "--name", f"telemetrygen-{row.name}",
        IMAGE, signal_type,
        "--otlp-insecure",
        "--otlp-endpoint", endpoint,
        "--service", row.name,
        "--rate", f"{row.rate:.4f}",
        "--duration", f"{duration_seconds}s",
        "--otlp-attributes", f'deployment.environment="bubble-demo"',
    ]
    return subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode().strip()


def stop_containers(names: list[str]) -> None:
    if not names:
        return
    print(f"\n[stop] removing {len(names)} containers", flush=True)
    subprocess.run(
        ["docker", "rm", "-f", *names],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def main() -> int:
    args = parse_args()
    plan = build_plan(args.services, args.top_records, args.decay,
                      args.duration, args.prefix)

    print(f"[plan] {len(plan)} services · signal={args.signal} · "
          f"endpoint={args.endpoint} · duration={args.duration}s")
    for row in plan:
        print(f"  {row.name:18s}  rate={row.rate:8.3f}/s   "
              f"target_records≈{row.target_records:>8d}")

    if args.dry_run:
        return 0

    containers: list[str] = []
    for row in plan:
        try:
            cid = launch_worker(row, args.signal, args.endpoint, args.duration)
            containers.append(f"telemetrygen-{row.name}")
            print(f"[run] {row.name} → {cid[:12]}", flush=True)
        except subprocess.CalledProcessError as e:
            print(f"[fail] {row.name}: {e.output.decode().strip()}", flush=True)

    if not containers:
        print("[error] no containers launched", flush=True)
        return 1

    stopped = {"done": False}

    def handler(_signum, _frame):
        if stopped["done"]:
            return
        stopped["done"] = True
        stop_containers(containers)
        sys.exit(130)

    posix_signal.signal(posix_signal.SIGINT, handler)
    posix_signal.signal(posix_signal.SIGTERM, handler)

    print(f"[wait] {len(containers)} workers running. Ctrl+C to stop early.", flush=True)
    try:
        while True:
            running = subprocess.run(
                ["docker", "ps", "--filter",
                 f"name=telemetrygen-{args.prefix}-",
                 "--format", "{{.Names}}"],
                capture_output=True, text=True,
            ).stdout.split()
            running = [n for n in running if n in containers]
            if not running:
                print("[done] all workers exited", flush=True)
                break
            print(f"[status] {len(running)}/{len(containers)} running", flush=True)
            time.sleep(15)
    except KeyboardInterrupt:
        handler(posix_signal.SIGINT, None)

    leftover = [
        c for c in containers
        if subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={c}"],
            capture_output=True, text=True,
        ).stdout.strip()
    ]
    stop_containers(leftover)
    return 0


if __name__ == "__main__":
    sys.exit(main())
