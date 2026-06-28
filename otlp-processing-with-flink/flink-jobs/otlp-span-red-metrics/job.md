# otlp-span-red-metrics

Flink streaming job that consumes raw OTLP spans from Kafka and derives **RED metrics** (Rate, Errors, Duration) — including a real Prometheus latency histogram with cumulative buckets — exposed via the Flink PrometheusReporter. It is a Flink-native "spanmetrics connector".

- **Entry point:** `io.nochaos.flink.OtlpSpanRedMetricsJob`
- **Artifact:** `otlp-span-red-metrics-1.0.0.jar` (shaded fat jar, ~22 MB)
- **Flink UI name:** `OTLP Span RED Metrics`
- **Source:** `src/main/java/io/nochaos/flink/OtlpSpanRedMetricsJob.java`
- **Metrics exposure:** Flink `PrometheusReporterFactory` on TaskManager port `9249` (scraped by Prometheus). Metric names are prefixed `flink_taskmanager_job_task_operator_` by the reporter.

---

## 1. Purpose — drop raw traces, keep the golden signals

Storing every raw span purely to power latency and error dashboards is wasteful. A trace store (Tempo / Jaeger / etc.) is typically the most expensive tier in an observability stack, yet the overwhelming majority of dashboard and alerting queries only ever read aggregate **golden signals**, not individual traces.

This job pre-aggregates spans into RED metrics **at ingest time**, directly off the Kafka stream. Once the golden signals live in Prometheus at full fidelity, you can:

- aggressively **downsample or drop the raw traces** (e.g. keep only 1% tail-sampled exemplars for debugging),
- and still serve **latency/error dashboards, SLOs, and error-budget burn-rate alerts** from the cheap metric series.

Metrics are orders of magnitude cheaper to store and query than spans, so the trace store stops being the cost driver while the operational signals stay intact.

---

## 2. The RED model

RED is the golden-signal triad for request-driven services:

| Signal | Meaning | Source on each span |
|---|---|---|
| **R**ate | requests per second | one increment per span |
| **E**rrors | failing requests | `status.code == STATUS_CODE_ERROR (2)` |
| **D**uration | latency distribution | `(endTimeUnixNano − startTimeUnixNano) / 1e9` seconds |

Per span the job extracts:

- `service.name` — from the **resource** attributes
- `span_name` — `span.getName()`
- `span_kind` — `span.getKind()` mapped to `internal / server / client / producer / consumer / unspecified`
- `status_code` — `ok / error / unset`
- `duration_seconds` — as above (negative durations from clock skew are clamped to 0)

The job **only ever increments counters** — never gauges. Rates, error percentages, and quantiles are computed in PromQL at query time.

---

## 3. The cumulative-bucket histogram encoding (load-bearing)

Latency is modelled as a genuine Prometheus histogram so `histogram_quantile()` works **unchanged**.

A Prometheus histogram is a set of **cumulative** counters: each `_bucket{le="X"}` counts every observation whose value is **≤ X**. So for each span of duration `d` the job increments **every** bucket whose boundary `le >= d`, plus the catch-all `le="+Inf"`.

Bucket boundaries (seconds), the Prometheus client-library defaults:

```
0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, +Inf
```

The `le` label is formatted exactly like Prometheus emits it — integral bounds drop the decimal (`"1"`, `"10"`), and the catch-all is the literal string `"+Inf"`.

Because the buckets are nested ("less-than-or-equal"), by construction:

```
bucket[le=0.005] <= bucket[le=0.01] <= ... <= bucket[le=10] <= bucket[le=+Inf]
```

and `bucket[le="+Inf"] == _count`.

### 3.1 Worked example

A single **30 ms** span (`d = 0.030`) produces these increments:

| `le` | inc | why |
|---|---|---|
| `0.005` | +0 | 0.005 < 0.030 |
| `0.01`  | +0 | 0.01 < 0.030 |
| `0.025` | +0 | 0.025 < 0.030 |
| `0.05`  | +1 | 0.05 ≥ 0.030 |
| `0.1`   | +1 | |
| `0.25`  | +1 | |
| `0.5`   | +1 | |
| `1`     | +1 | |
| `2.5`   | +1 | |
| `5`     | +1 | |
| `10`    | +1 | |
| `+Inf`  | +1 | == `_count` |

### 3.2 How `histogram_quantile` reads it

Say after some traffic the cumulative buckets for one endpoint are:

```
le="0.005"=0  le="0.01"=0  le="0.025"=10  le="0.05"=80
le="0.1"=180  le="0.25"=195  le="0.5"=199 ... le="+Inf"=200
```

`histogram_quantile(0.95, ...)` wants the value at rank `0.95 × 200 = 190`. It finds the **first bucket whose cumulative count ≥ 190** — that is `le="0.1"` (180 < 190 ≤ 195 at `le="0.25"`). The crossing happens between `le=0.1` and `le=0.25`, so it **linearly interpolates** inside that band: `0.1 + (0.25 − 0.1) × (190 − 180) / (195 − 180) ≈ 0.20 s`. This only works because the buckets are cumulative — exactly the encoding above.

---

## 4. Topology

```
 otlp-traces ──► KafkaSource ──► SpanRedMetricsProcess ──► (RED counters on port 9249)
                  (latest)         (ProcessFunction, p=2)
```

Single source, single operator. Submitted with `parallelism=2`, so Flink materializes one source+process chain across 2 subtasks. There is no shuffle and no keyed state — each subtask independently accumulates counters for the spans it sees, and the Prometheus reporter on each TaskManager exposes them. Aggregation across subtasks happens in Prometheus (`sum by (...)`).

| Vertex | Parallelism | Role |
|---|---|---|
| `Source: Kafka[traces] -> span-red-metrics` | 2 | consume `otlp-traces`, parse, emit RED counters |

`SpanRedMetricsProcess` is a `ProcessFunction<byte[], Void>` — a terminal sink-shaped operator. It emits nothing downstream; its only output is the Flink metric registry.

### Per-element handling

For each Kafka message:

1. Parse the payload as `ExportTraceServiceRequest`. Parse failures are logged and the batch is skipped.
2. Walk `ResourceSpans → ScopeSpans → Spans`.
3. For each span, extract `service.name` (resource attr), span name, kind, status, and duration.
4. Apply the cardinality guard to `(service_name, span_name)` (see §7).
5. Increment the RED counters (§5), including every cumulative histogram bucket.
6. Update an in-memory per-endpoint tally used only for the 60s log snapshot.

Every 60s each subtask logs a **Top-10 endpoints by request count with their error rate** to the task log, so signals can be spot-checked from the Flink UI without hitting Prometheus.

---

## 5. Complete metric reference

All counters. Labels shown are before the reporter prefixes them. `service_name` and `span_name` are present on every series.

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `otlp_red_requests_total` | counter | `service_name`, `span_name`, `span_kind`, `status_code` | The **R** + the slicing dims. `status_code ∈ {ok, error, unset}`. One increment per span. |
| `otlp_red_errors_total` | counter | `service_name`, `span_name` | The **E**. Incremented only when `status.code == ERROR`. |
| `otlp_red_duration_seconds_bucket` | counter | `service_name`, `span_name`, `le` | The **D** histogram. **Cumulative** buckets — each span bumps every bucket with `le ≥ duration`, plus `le="+Inf"`. |
| `otlp_red_duration_seconds_count` | counter | `service_name`, `span_name` | Total spans per endpoint. Equals `bucket{le="+Inf"}`. |
| `otlp_red_duration_micros_sum` | counter | `service_name`, `span_name` | Sum of durations **in microseconds**. See §5.1. |

### 5.1 Why the sum is in micros

A standard Prometheus histogram has a float `_sum` companion (`otlp_red_duration_seconds_sum`). Flink `Counter`s are **`long`-only**, so a fractional seconds sum cannot be stored as a counter directly.

This job takes the **micros-as-long** approach: `otlp_red_duration_micros_sum` accumulates `Math.round(duration_seconds × 1e6)` per span. To recover the conventional seconds sum in PromQL, **divide by 1e6**:

```promql
otlp_red_duration_micros_sum / 1e6      # == otlp_red_duration_seconds_sum
```

This keeps the histogram exact (`_bucket` + `_count` are unaffected) and gives the average latency where you need it, at the cost of one PromQL division and a non-standard metric name for the sum.

---

## 6. PromQL cookbook

`$svc` / `$ep` are Grafana template variables; `flink_taskmanager_job_task_operator_` is the reporter prefix (elided here for readability — prepend it to the metric names in a real Prometheus instance).

**Request rate (R)**

```promql
# requests/s for one endpoint
sum(rate(otlp_red_requests_total{service_name="$svc", span_name="$ep"}[1m]))

# requests/s per service
sum by (service_name)(rate(otlp_red_requests_total[1m]))

# server-side traffic only
sum by (service_name)(rate(otlp_red_requests_total{span_kind="server"}[1m]))
```

**Error rate % (E)**

```promql
# error ratio for one endpoint
100
* sum(rate(otlp_red_errors_total{service_name="$svc", span_name="$ep"}[5m]))
/ sum(rate(otlp_red_requests_total{service_name="$svc", span_name="$ep"}[5m]))

# equivalently, from the status_code dimension on requests_total
100
* sum by (service_name)(rate(otlp_red_requests_total{status_code="error"}[5m]))
/ sum by (service_name)(rate(otlp_red_requests_total[5m]))
```

**Latency percentiles (D) — `histogram_quantile`**

```promql
# p50 / p95 / p99 latency (seconds) for one endpoint
histogram_quantile(0.50,
  sum by (le)(rate(otlp_red_duration_seconds_bucket{service_name="$svc", span_name="$ep"}[5m])))
histogram_quantile(0.95,
  sum by (le)(rate(otlp_red_duration_seconds_bucket{service_name="$svc", span_name="$ep"}[5m])))
histogram_quantile(0.99,
  sum by (le)(rate(otlp_red_duration_seconds_bucket{service_name="$svc", span_name="$ep"}[5m])))

# p95 per service (aggregate buckets across endpoints AND subtasks before quantile)
histogram_quantile(0.95,
  sum by (service_name, le)(rate(otlp_red_duration_seconds_bucket[5m])))
```

> Always `sum by (le)` (and any grouping labels) **before** `histogram_quantile`, so the per-subtask buckets are merged first.

**Average latency**

```promql
# mean latency (seconds) = sum / count, with the micros sum converted
sum(rate(otlp_red_duration_micros_sum{service_name="$svc"}[5m])) / 1e6
/ sum(rate(otlp_red_duration_seconds_count{service_name="$svc"}[5m]))
```

**Apdex** (satisfied ≤ T, tolerating ≤ 4T; here T = 0.25 s ⇒ 4T = 1 s)

```promql
(
  sum(rate(otlp_red_duration_seconds_bucket{service_name="$svc", le="0.25"}[5m]))
  + sum(rate(otlp_red_duration_seconds_bucket{service_name="$svc", le="1"}[5m]))
) / 2
/ sum(rate(otlp_red_duration_seconds_count{service_name="$svc"}[5m]))
```

(`(satisfied + tolerating/2) / total`, reading the cumulative buckets directly at the Apdex thresholds — pick `le` values that exist in §3's bucket set.)

---

## 7. Cardinality guard

`span_name` is the dangerous label — an un-templated HTTP route (`GET /users/12345`) or a raw SQL string explodes cardinality.

The job caps the number of distinct `(service_name, span_name)` pairs at **`MAX_SERIES = 2000`** per subtask:

- A pair already admitted keeps its real `span_name`.
- A new pair is admitted while the admitted set is under the cap.
- Once the cap is reached, any **new** pair has its `span_name` folded into the literal `"__overflow__"`, so all runaway names collapse into a single low-cost series per service.

This bounds worst-case series at roughly `MAX_SERIES × (#kinds × #status)` for `requests_total` and `MAX_SERIES × (#buckets+1)` for the histogram, per subtask, regardless of how pathological the incoming span names are. The guard is per-subtask in-memory state (a `HashSet`); it is **not** checkpointed and resets on restart.

---

## 8. Runtime configuration

| Setting | Value | Where |
|---|---|---|
| Global parallelism | `2` | `env.setParallelism(2)` |
| Kafka bootstrap | `kafka:29092` | `KAFKA_BOOTSTRAP_SERVERS` |
| Topic | `otlp-traces` | single source |
| Consumer group | `flink-otlp-span-red-metrics` | — |
| Kafka offsets start | `latest` | `OffsetsInitializer.latest()` |
| Watermarks | none | `WatermarkStrategy.noWatermarks()` |
| Deserializer | raw `byte[]` | `ByteArrayDeserializationSchema` |
| Log snapshot interval | `60 000 ms` | `LOG_SNAPSHOT_INTERVAL_MS` |
| Max distinct series | `2000` | `MAX_SERIES` |
| Overflow label value | `__overflow__` | `OVERFLOW` |
| Bucket bounds (s) | `0.005…10, +Inf` | `BUCKET_BOUNDS_S` |
| Max label value length | `120` | `MAX_LABEL_VALUE_LEN` |
| Java | `11` | pom `java.version` |

---

## 9. Build

Requires Docker only (Maven runs in a throwaway container):

```bash
docker run --rm \
  -v "$(pwd)/flink-jobs/otlp-span-red-metrics:/app" \
  -w /app \
  maven:3.9.6-eclipse-temurin-11 \
  mvn package -q -DskipTests

cp flink-jobs/otlp-span-red-metrics/target/otlp-span-red-metrics-1.0.0.jar flink-jobs/
```

The `flink-job-submitter` container picks up any `*.jar` in `./flink-jobs/` on startup and submits it with `parallelism=2`.

### Dependencies

Shaded into the jar: `flink-connector-kafka:3.3.0-1.20`, `opentelemetry-proto:1.3.2-alpha`.
Marked `provided` (supplied by the Flink distro): `flink-streaming-java`, `flink-clients`, `slf4j-api`, `log4j-slf4j-impl`.

---

## 10. Limitations and gotchas

- **No state durability.** Counters live in the Flink metric registry; the cardinality-guard `HashSet` and the log-snapshot tally are plain in-memory fields. A job restart resets everything (Prometheus sees a counter reset — `rate()` handles this gracefully).
- **Per-subtask metrics.** Each of the 2 subtasks exposes its own counters on its TaskManager's port 9249. Always aggregate across subtasks in PromQL (`sum by (le)` etc.) before computing rates or quantiles.
- **Fixed bucket layout.** The 11 default buckets + `+Inf` are hard-coded. Latencies far above 10 s land only in `+Inf`, so `histogram_quantile` cannot resolve tail percentiles beyond the last finite bucket — it returns the `+Inf` boundary (effectively "≥ 10 s"). Add coarser buckets if you serve long requests.
- **Non-standard sum metric.** The duration sum is `otlp_red_duration_micros_sum` (microseconds), not the conventional `_seconds_sum`. Divide by `1e6` in PromQL (§5.1). Tools that auto-detect histograms by the `_sum`/`_count`/`_bucket` naming convention may not recognize the sum.
- **Cardinality guard is per-subtask and lossy.** The 2000-pair cap is enforced independently per subtask, so the global distinct count can be up to `2 × 2000`. Once a subtask overflows, late-arriving but legitimate endpoints get folded into `__overflow__` and cannot be recovered without a restart.
- **`latest` offsets.** Historical spans already in Kafka before the job starts are skipped. Switch to `OffsetsInitializer.earliest()` for replay.
- **Silent parse errors.** A malformed trace batch is logged at WARN and dropped; no error metric is emitted for parse failures.
- **Status `unset` counts as success.** Spans with `STATUS_CODE_UNSET` are *not* errors. If a service never sets span status on failures, those failures won't surface in `otlp_red_errors_total` — this matches OTLP semantics but is worth knowing for error-rate SLOs.
- **Negative durations clamped.** Spans with `end < start` (clock skew) are clamped to duration 0, landing in the smallest bucket.
- **Kafka consumer warning** `'[client.id.prefix, partition.discovery.interval.ms]' were supplied but are not used yet` is harmless (upstream `flink-connector-kafka:3.3.0-1.20` quirk).
