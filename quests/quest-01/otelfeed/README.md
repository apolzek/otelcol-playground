# otelfeed

High-throughput replayer for OTLP JSON files. Streams a directory of OTLP exports (metrics / traces / logs) to an OpenTelemetry Collector over **HTTP** and optionally **gRPC** in parallel.

## Run

```bash
cd otelfeed
make tidy
make build
make collector
make run-http
make run-grpc
```

## Flags

| Flag              | Default                  | Purpose                                 |
|-------------------|--------------------------|-----------------------------------------|
| `--dir`           | `samples/otlp-json`      | Input directory                         |
| `--recursive`     | `false`                  | Recurse into subdirs                    |
| `--http`          | `http://localhost:4318`  | OTLP/HTTP endpoint (empty disables)     |
| `--grpc`          | _(empty)_                | OTLP/gRPC `host:port`                   |
| `--grpc-insecure` | `true`                   | Disable TLS for gRPC                    |
| `--grpc-conns`    | `4`                      | Parallel gRPC connections               |
| `--gzip`          | `true`                   | Compress bodies                         |
| `--workers`       | `NumCPU * 4`             | Senders per transport                   |
| `--queue`         | `4096`                   | Fan-out buffer (backpressure)           |
| `--repeat`        | `1`                      | Replay the dataset N times              |
| `--timeout`       | `30s`                    | Per-request timeout                     |
| `--retries`       | `3`                      | HTTP retries on 5xx / 429 / net errors  |
| `--progress`      | `2s`                     | Live progress cadence                   |
| `--header`        | _(repeatable)_           | Extra headers, `key=value`              |
| `--rate`          | `0`                      | Max payloads/second (0 = unlimited)     |
| `--min-interval`  | `0`                      | Minimum spacing between payloads        |

See `./bin/otelfeed --help` for the full list.

## One-off invocation

```bash
./bin/otelfeed \
  --dir ../samples/otlp-json \
  --http http://localhost:4318 \
  --grpc localhost:4317 --grpc-insecure \
  --workers 64 --queue 8192
```

## Load profiles

By default the feed is designed to saturate. Tune pressure via `--workers`,
`--grpc-conns`, and `--queue`. `--workers` is the dominant knob.

When you need to cap throughput to avoid overwhelming the collector, use
`--rate` (payloads/second) or `--min-interval` (spacing between payloads).
Both apply at the fan-out, so HTTP and gRPC see the same pacing:

```bash
# throttle to 50 payloads/s
./bin/otelfeed --dir ../samples/otlp-json --rate 50

# one payload every 200 ms (~5/s), useful for slow collectors
./bin/otelfeed --dir ../samples/otlp-json --min-interval 200ms

# fractional rates: one payload every 2 s
./bin/otelfeed --dir ../samples/otlp-json --rate 0.5
```

```bash
# light (~half the peak) — use to check the collector holds without errors
./bin/otelfeed --dir ../samples/otlp-json \
  --http "" --grpc localhost:4317 --grpc-insecure \
  --workers 8 --grpc-conns 2 --queue 1024

# medium
./bin/otelfeed --dir ../samples/otlp-json \
  --http "" --grpc localhost:4317 --grpc-insecure \
  --workers 16 --grpc-conns 2 --queue 2048

# heavy (stresses the collector) — matches `make run-grpc` defaults
./bin/otelfeed --dir ../samples/otlp-json \
  --http "" --grpc localhost:4317 --grpc-insecure \
  --workers 32 --grpc-conns 4 --queue 4096
```

## Steps

1. **Streaming read.** Each OTLP JSON file (~100 MiB) holds several concatenated objects. `json.Decoder` returns each one as `json.RawMessage`, so the full file is never loaded into memory.
2. **Fan-out.** A forwarder replicates the same `Payload` onto the HTTP queue and (when `--grpc` is enabled) onto the gRPC queue. Both outputs run in parallel over the same data.
3. **HTTP send.** Workers issue `POST /v1/{metrics,traces,logs}` with the raw JSON (no re-serialization). HTTP/2 with keep-alive plus a `sync.Pool` of `gzip.Writer` and buffers eliminates allocations on the hot path.
4. **gRPC send.** Each payload is decoded once at read time using the OTel Collector `pdata` OTLP/JSON unmarshalers (`ptraceotlp` / `pmetricotlp` / `plogotlp`) — required because `protojson` treats `bytes` fields as base64, while OTLP/JSON encodes `trace_id`/`span_id` as hex. Workers hand the already-decoded `ExportRequest` to `*otlp.NewGRPCClient`. Round-robin across `--grpc-conns` connections scales beyond a single NIC queue.
5. **Backpressure.** Buffered channels (`--queue`): a slow collector blocks producers instead of blowing up memory.
6. **Retry.** HTTP uses exponential backoff (100 ms to 5 s) on 5xx/429/network errors; 4xx aborts immediately.

## Signal detection

Signal type (metrics/traces/logs) is inferred from the filename. When ambiguous, the first window of bytes resolves it via `resourceMetrics` / `resourceSpans` / `resourceLogs`.

## Run results

Same dataset (22 files, 1586 payloads — 561 traces + 1025 metrics), local
collector, `--workers 32`, `gzip=true`. Counts come from the pdata decode
and are reported once per payload (no double-counting across transports).

### `make run-http`

```text
==== summary ====
elapsed:          2.629s
files processed:  22
payloads:         total=1586 ok=1586 send_err=0 parse_err=0
  traces:         payloads=561 spans=945293
  metrics:        payloads=1025 instruments=583110 data_points=2072924
  logs:           payloads=0 records=0
transports:
  HTTP requests:  1586  sent=2.05 GiB  (797.68 MiB/s)
  gRPC requests:  0  sent=0 B  (0 B/s) [raw json size; wire is gzipped]
throughput:       603 obj/s
```

### `make run-grpc` (HTTP + gRPC fan-out)

```text
==== summary ====
elapsed:          3.778s
files processed:  22
payloads:         total=1586 ok=1586 send_err=0 parse_err=0
  traces:         payloads=561 spans=945293
  metrics:        payloads=1025 instruments=583110 data_points=2072924
  logs:           payloads=0 records=0
transports:
  HTTP requests:  1586  sent=2.05 GiB  (555.00 MiB/s)
  gRPC requests:  1586  sent=2.05 GiB  (555.00 MiB/s) [raw json size; wire is gzipped]
throughput:       420 obj/s
```

**Note:** The error occurred because OTLP JSON encodes IDs in hexadecimal while standard protobuf expects base64, and it was resolved by using OpenTelemetry’s OTLP-aware unmarshalers that correctly interpret these fields.