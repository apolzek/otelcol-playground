#!/usr/bin/env python3
"""
Spawn parallel telemetrygen processes sending spans, metrics, and logs with
varied OTLP resource attributes.

Each variation is a distinct combination of the 9 resource attributes used by
the Pagmon Java agent (topdomain, domain, applicationslug, systemslug, team,
context_business, vertical, product, segregated). The (topdomain, domain,
product) triple is drawn from tests/variations.json — product maps to the
subdomain key when one exists, otherwise to the domain key. For every
variation the launcher starts one telemetrygen container per signal
(traces/metrics/logs), all running in parallel against the same OTLP endpoint.

Examples:
    ./run-telemetrygen.py
    ./run-telemetrygen.py --variations 12 --duration 30m --rate 10
    ./run-telemetrygen.py --endpoint localhost:4317 --signals traces,logs
    ./run-telemetrygen.py --duration inf          # run until Ctrl+C
"""
import argparse
import json
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

IMAGE = "ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.147.0"

TEAMS = [
    "kanazawa", "osaka", "tokyo", "kyoto", "hokkaido",
    "nara", "sendai", "fukuoka", "sapporo", "nagoya",
]
CONTEXTS = [
    "cash_in", "cash_out", "onboarding", "authentication", "settlement",
    "notification", "reconciliation", "authorization", "acquisition", "risk",
]
VERTICALS = [
    "contapj", "contapf", "enterprise", "retail", "marketplace",
    "acquirer", "issuer", "sme",
]

# Resource-attribute pools used to make the Flink insights panels populate with
# realistic variety (instead of collapsing everything into `unknown`).
SDK_LANGUAGES = ["java", "go", "python", "node", "dotnet", "ruby"]
SDK_VERSIONS  = ["1.32.1", "1.34.1", "1.38.0", "1.42.0", "2.4.0", "2.10.0"]
CLOUD_PROVIDERS = ["aws", "gcp", "azure"]
K8S_CLUSTERS    = ["prod-gt-blue", "prod-gt-green", "qa-gt-blue", "staging-gt-west"]
ENVIRONMENTS    = ["prod", "staging", "dev"]


def topdomains(tree):
    return [t for t in tree if t.get("otype") == "topdomains" and t.get("subs")]


def pick_topdomains(tree, n):
    """Pick N distinct topdomains (without replacement). Falls back to repetition
    if the caller asks for more than the JSON provides."""
    pool = topdomains(tree)
    random.shuffle(pool)
    if n <= len(pool):
        return pool[:n]
    return pool + [random.choice(pool) for _ in range(n - len(pool))]


def find_leaf(tree, target_key):
    """DFS through variations.json for the first node with key == target_key.
    Returns (top, domain, sub) — domain and sub may be None if the match is shallower."""
    for top in tree:
        if top.get("otype") != "topdomains" or not top.get("subs"):
            continue
        if top["key"] == target_key:
            return top, None, None
        for domain in top.get("subs", []) or []:
            if domain.get("key") == target_key:
                return top, domain, None
            for sub in domain.get("subs", []) or []:
                if sub.get("key") == target_key:
                    return top, domain, sub
    return None


def build_heavy_attrs(tree, service):
    """Return (service, attrs) for a heavy service name (e.g. token-service).
    Walks variations.json for a leaf whose key matches the prefix before `-service`;
    falls back to minimal attrs when nothing matches."""
    base = service[:-len("-service")] if service.endswith("-service") else service
    found = find_leaf(tree, base)
    common = {
        "telemetry.sdk.language": random.choice(SDK_LANGUAGES),
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": random.choice(SDK_VERSIONS),
        "cloud.provider": random.choice(CLOUD_PROVIDERS),
        "k8s.cluster.name": random.choice(K8S_CLUSTERS),
        "deployment.environment": random.choice(ENVIRONMENTS),
    }
    if found:
        top, domain, sub = found
        product = (sub or domain or top)["key"]
        domain_key = (domain or top)["key"]
        attrs = {
            "topdomain": top["key"],
            "domain": domain_key,
            "applicationslug": service,
            "systemslug": base,
            "team": random.choice(TEAMS),
            "context_business": random.choice(CONTEXTS),
            "vertical": random.choice(VERTICALS),
            "product": product,
            "segregated": random.choice(["true", "false"]),
            "k8s.namespace.name": f"{top['key']}-platform",
            **common,
        }
    else:
        attrs = {
            "applicationslug": service,
            "systemslug": base,
            "team": random.choice(TEAMS),
            "context_business": random.choice(CONTEXTS),
            "vertical": random.choice(VERTICALS),
            "segregated": random.choice(["true", "false"]),
            "k8s.namespace.name": f"{base}-platform",
            **common,
        }
    return service, attrs


def make_variation(top):
    """Build the 9 resource attributes exactly matching the Pagmon agent contract,
    for a given topdomain node. Domain and subdomain are drawn from that topdomain."""
    domain = random.choice(top["subs"]) if top.get("subs") else None
    sub = random.choice(domain["subs"]) if domain and domain.get("subs") else None
    leaf = sub or domain or top
    service_base = leaf["key"]
    service = f"{service_base}-service"
    product = sub["key"] if sub else (domain["key"] if domain else top["key"])
    attrs = {
        "topdomain": top["key"],
        "domain": (domain or top)["key"],
        "applicationslug": service,
        "systemslug": service_base,
        "team": random.choice(TEAMS),
        "context_business": random.choice(CONTEXTS),
        "vertical": random.choice(VERTICALS),
        "product": product,
        "segregated": random.choice(["true", "false"]),
        "telemetry.sdk.language": random.choice(SDK_LANGUAGES),
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": random.choice(SDK_VERSIONS),
        "cloud.provider": random.choice(CLOUD_PROVIDERS),
        "k8s.cluster.name": random.choice(K8S_CLUSTERS),
        "k8s.namespace.name": f"{top['key']}-platform",
        "deployment.environment": random.choice(ENVIRONMENTS),
    }
    return service, attrs


def build_cmd(signal_type, service, attrs, endpoint_grpc, endpoint_http, rate,
              duration, protocol):
    """protocol is 'grpc' or 'http'. Endpoint is picked accordingly."""
    tag = f"{signal_type}-{protocol}-{service.replace('_','-')}-{random.randint(1000, 9999)}"
    cmd = [
        "docker", "run", "--rm", "-d",
        "--network", "host",
        "--name", f"telemetrygen-{tag}",
        IMAGE,
        signal_type,
        "--otlp-insecure",
        "--service", service,
        "--rate", str(rate),
        "--duration", duration,
    ]
    if protocol == "http":
        cmd.extend(["--otlp-http", "--otlp-endpoint", endpoint_http])
    else:
        cmd.extend(["--otlp-endpoint", endpoint_grpc])
    for k, v in attrs.items():
        cmd.extend(["--otlp-attributes", f'{k}="{v}"'])
    return cmd, tag


def stop_containers(names):
    if not names:
        return
    print(f"\n[stop] stopping {len(names)} telemetrygen containers...")
    subprocess.run(["docker", "rm", "-f", *names],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--variations", type=int, default=20,
                    help="How many distinct topdomain variations to spawn (default: 20)")
    ap.add_argument("--duration", default="10m",
                    help="Duration per worker (e.g. 30s, 10m, inf) (default: 10m)")
    ap.add_argument("--rate", type=float, default=5,
                    help="Records per second per signal per variation (default: 5)")
    ap.add_argument("--endpoint", default="localhost:4317",
                    help="OTLP gRPC endpoint (default: localhost:4317 — collector-l1)")
    ap.add_argument("--endpoint-http", default="localhost:4318",
                    help="OTLP HTTP endpoint (default: localhost:4318 — collector-l1)")
    ap.add_argument("--http-ratio", type=float, default=0.5,
                    help="Fraction of variations sent over HTTP (0=all gRPC, 1=all HTTP; default: 0.5)")
    ap.add_argument("--tree", default=str(Path(__file__).parent / "variations.json"),
                    help="Path to variations.json")
    ap.add_argument("--signals", default="traces,metrics,logs",
                    help="Comma-separated list of signals (default: traces,metrics,logs)")
    ap.add_argument("--seed", type=int, default=None,
                    help="Random seed for reproducible variation picks")
    ap.add_argument("--heavy-services", default="",
                    help="Comma-separated service names that get a boosted rate on top of the "
                         "regular variation loop (e.g. token-service,auth-service)")
    ap.add_argument("--heavy-rate", type=float, default=50.0,
                    help="Rate (records/sec) for each --heavy-services worker (default: 50)")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    tree = json.load(open(args.tree))
    signals = [s.strip() for s in args.signals.split(",") if s.strip()]
    total = args.variations * len(signals)

    print(f"[plan] {args.variations} variations × {len(signals)} signals = {total} telemetrygen containers")
    print(f"[plan] endpoint={args.endpoint} rate={args.rate}/s duration={args.duration}")
    print(f"[plan] image={IMAGE}\n")

    containers = []
    chosen_tops = pick_topdomains(tree, args.variations)
    http_count = round(args.variations * args.http_ratio)
    protocols = ["http"] * http_count + ["grpc"] * (args.variations - http_count)
    random.shuffle(protocols)

    for i, (top, protocol) in enumerate(zip(chosen_tops, protocols)):
        service, attrs = make_variation(top)
        path = f"{attrs['topdomain']}/{attrs['domain']}/{attrs['product']}"
        print(f"[var {i+1:2d}] service={service:42s} path={path:55s} team={attrs['team']:10s} "
              f"vertical={attrs['vertical']:12s} proto={protocol}")

        for sig in signals:
            cmd, tag = build_cmd(sig, service, attrs, args.endpoint, args.endpoint_http,
                                 args.rate, args.duration, protocol)
            try:
                cid = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode().strip()
                containers.append(f"telemetrygen-{tag}")
                print(f"         └─ {sig:7s} {cid[:12]}")
            except subprocess.CalledProcessError as e:
                print(f"         └─ {sig:7s} FAILED: {e.output.decode().strip()}")

    heavy_services = [s.strip() for s in args.heavy_services.split(",") if s.strip()]
    for svc in heavy_services:
        service, attrs = build_heavy_attrs(tree, svc)
        protocol = "http" if random.random() < args.http_ratio else "grpc"
        path = "/".join(v for v in (attrs.get("topdomain"), attrs.get("domain"), attrs.get("product")) if v)
        print(f"[heavy  ] service={service:42s} path={path:55s} team={attrs['team']:10s} "
              f"vertical={attrs['vertical']:12s} proto={protocol} rate={args.heavy_rate}/s")
        for sig in signals:
            cmd, tag = build_cmd(sig, service, attrs, args.endpoint, args.endpoint_http,
                                 args.heavy_rate, args.duration, protocol)
            try:
                cid = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode().strip()
                containers.append(f"telemetrygen-{tag}")
                print(f"         └─ {sig:7s} {cid[:12]}")
            except subprocess.CalledProcessError as e:
                print(f"         └─ {sig:7s} FAILED: {e.output.decode().strip()}")

    if not containers:
        print("\n[error] no containers launched")
        sys.exit(1)

    print(f"\n[run] {len(containers)} containers running. Press Ctrl+C to stop all.\n")

    stopped = {"done": False}
    def handler(signum, _frame):
        if stopped["done"]:
            return
        stopped["done"] = True
        stop_containers(containers)
        sys.exit(130)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    try:
        while True:
            running = subprocess.run(
                ["docker", "ps", "--filter", "name=telemetrygen-", "--format", "{{.Names}}"],
                capture_output=True, text=True,
            ).stdout.strip().splitlines()
            running = [n for n in running if n in containers]
            if not running:
                print("\n[done] all telemetrygen containers exited")
                break
            print(f"[status] {len(running)}/{len(containers)} still running", flush=True)
            time.sleep(15)
    except KeyboardInterrupt:
        handler(signal.SIGINT, None)

    stop_containers([c for c in containers if subprocess.run(
        ["docker", "ps", "-q", "-f", f"name={c}"],
        capture_output=True, text=True).stdout.strip()])


if __name__ == "__main__":
    main()
