# otlp-log-dedup

Flink streaming job that consumes raw OTLP **logs** from Kafka, fingerprints every log body into a Drain-style template, counts repetitions, detects floods ("log storms"), and exposes the noisiest templates plus how much deduplication would save — as Flink-native Prometheus metrics on TaskManager port `9249`.

- **Entry point:** `io.nochaos.flink.OtlpLogDedupJob`
- **Artifact:** `otlp-log-dedup-1.0.0.jar` (shaded fat jar)
- **Flink UI name:** `OTLP Log Dedup`
- **Source:** `src/main/java/io/nochaos/flink/OtlpLogDedupJob.java`

---

## 1. Purpose & the high-volume pain

A single bad deploy can emit the **same** ERROR log line millions of times — a *log storm*. The bodies usually differ only in volatile tokens (timestamps, request IDs, IP addresses, counters), so to a human they are one message repeated, but to the logging backend they are millions of distinct events:

- ingestion cost explodes (you pay per GB / per event),
- full-text indices bloat,
- and the real signal drowns in the noise.

This job does **not** drop logs. It *measures* the repetition so teams can:

1. quantify how much of their log volume is pure duplication (`dedup_ratio`),
2. identify the exact templates responsible (`template_occurrences_total`),
3. get alerted the moment a storm starts (`flood_events_total`),
4. estimate the savings of enabling dedup at the collector (`bytes_saved_total`).

It mirrors the concept of the OpenTelemetry Collector community **log deduplication processor** (`logdedupprocessor`), which collapses identical consecutive log records into a single record carrying an occurrence count — and the IBM **Drain** online log-template parser, whose token-masking idea the fingerprinter below is based on.

---

## 2. The fingerprinting algorithm

Each `LogRecord.body` (an OTLP `AnyValue`, coerced to string) is normalized into a **template** by masking the parts that vary between otherwise-identical messages, then collapsing whitespace. Masking is applied **in this order** (order matters — masks that contain digits run before the broad `<NUM>` rule):

| Step | Pattern | Replacement |
|---|---|---|
| 1 | quoted substrings `"..."` / `'...'` | `<STR>` |
| 2 | dotted IPv4 (optional `:port`) | `<IP>` |
| 3 | UUID (`8-4-4-4-12` hex) | `<HEX>` |
| 4 | hex-ish token, len ≥ 8, containing an `a–f` letter (optional `0x`) | `<HEX>` |
| 5 | runs of digits | `<NUM>` |
| 6 | whitespace runs | single space, trimmed |

The template's `String.hashCode()` rendered via `Integer.toHexString(...)` is the stable, compact **`template_hash`**.

Step 4 deliberately requires a hex *letter* so ordinary decimals (e.g. `42`) are masked as `<NUM>` (step 5), not `<HEX>` — this keeps numeric counters and hashes/ids in separate template families.

### Before / after examples

```
raw : User 12345 failed login from 10.0.3.17:55012 token "abc-xyz" id=4f9a1c2b8e7d6543
tmpl: User <NUM> failed login from <IP> token <STR> id=<HEX>

raw : GET /orders/9981 -> 503 in 142ms (req 7c1e9f0a-2b3c-4d5e-8f10-aabbccddeeff)
tmpl: GET /orders/<NUM> -> <NUM> in <NUM>ms (req <HEX>)

raw : retrying connection 3 of 5
tmpl: retrying connection <NUM> of <NUM>
```

All three raw families collapse so that the *millions* of distinct concrete bodies map onto a *handful* of templates.

---

## 3. Topology

```
 otlp-logs ─► KafkaSource ─► ProcessFunction (LogDedupProcess) ─► (Void sink)
                                     │
                                     ├─ counters/gauges via MetricGroup ─► PrometheusReporter :9249
                                     └─ 60s Top-10 snapshot ─► task log
```

Single `KafkaSource` on the `otlp-logs` topic feeding one `ProcessFunction`. Submitted with `parallelism=2`, so Flink materializes 2 source subtasks → 2 `LogDedupProcess` subtasks. The function emits no downstream records (`Collector<Void>`); all output is metrics + logs.

| Vertex | Parallelism | Role |
|---|---|---|
| `Source: Kafka[logs] -> Process` | 2 | consume `otlp-logs`, run `LogDedupProcess` |

Each subtask keeps its **own** in-memory template map (no `keyBy`, no shared state). With `parallelism=2`, the same template seen on both subtasks is counted independently and exposed as two series differing by Flink's TaskManager/subtask scope. Aggregate in PromQL with `sum by (...)`.

---

## 4. Complete metric reference

All metrics are exposed through the Flink PrometheusReporter and arrive in Prometheus prefixed `flink_taskmanager_job_task_operator_`.

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `otlp_logdedup_records_total` | counter | — | Every log record seen (all templates, incl. overflow) |
| `otlp_logdedup_unique_templates` | gauge `<Integer>` | — | Distinct templates currently tracked (≤ `MAX_TEMPLATES`) |
| `otlp_logdedup_dedup_ratio` | gauge `<Double>` | — | `1 - unique_templates/records_total`; fraction of volume that is repetition. `0.0` until first record |
| `otlp_logdedup_template_occurrences_total` | counter | `service_name`, `severity`, `template_hash` | Per-template occurrences — **cardinality-guarded** (§6). Series created only once a template crosses `MIN_OCCURRENCES_FOR_SERIES`; the full cumulative count is backfilled at creation |
| `otlp_logdedup_flood_events_total` | counter | `service_name` | +1 each time a template's count within a rolling `FLOOD_WINDOW_MS` crosses `FLOOD_THRESHOLD` |
| `otlp_logdedup_bytes_saved_total` | counter | — | Estimated bytes perfect dedup would save = Σ over templates of `(occurrences-1) * avg_body_bytes`, accumulated incrementally |

---

## 5. PromQL cookbook

```promql
# overall log volume rate
sum(rate(otlp_logdedup_records_total[1m]))

# DEDUP RATIO — fraction of volume that is repetition (0..1). Near 1.0 == heavy storm.
avg(otlp_logdedup_dedup_ratio)

# distinct templates currently tracked
sum(otlp_logdedup_unique_templates)

# TOP NOISY TEMPLATES by recent rate (only templates that earned a series)
topk(10, sum by (template_hash, service_name) (rate(otlp_logdedup_template_occurrences_total[5m])))

# noisiest templates by cumulative weight
topk(10, sum by (template_hash) (otlp_logdedup_template_occurrences_total))

# which services own the most noisy-template volume
topk(10, sum by (service_name) (rate(otlp_logdedup_template_occurrences_total[5m])))

# FLOOD ALERT — any service tripped a storm in the last 5m
sum by (service_name) (increase(otlp_logdedup_flood_events_total[5m])) > 0

# BYTES SAVED rate — savings/sec if duplicates were collapsed
sum(rate(otlp_logdedup_bytes_saved_total[5m]))

# savings as a share of ingested volume (needs an ingest-bytes metric, e.g. from otlp-stream-processor)
sum(rate(otlp_logdedup_bytes_saved_total[5m]))
  / sum(rate(flink_otlp_bytes_total{telemetry_type="logs"}[5m]))
```

Suggested alert:

```promql
# storm in progress: high dedup ratio AND a flood event fired
- alert: LogStorm
  expr: avg(otlp_logdedup_dedup_ratio) > 0.95
        and sum(increase(otlp_logdedup_flood_events_total[5m])) > 0
  for: 2m
```

---

## 6. Configuration constants & the cardinality guard

| Constant | Value | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka bootstrap |
| `MAX_TEMPLATES` | `20000` | Cap on distinct templates held in memory; beyond it, occurrences go to a single `overflow` tally (logged, not a series) |
| `MIN_OCCURRENCES_FOR_SERIES` | `100` | A template earns its own labeled `template_occurrences_total` series only after this many cumulative occurrences |
| `FLOOD_WINDOW_MS` | `60000` | Rolling window for flood detection |
| `FLOOD_THRESHOLD` | `1000` | Occurrences of one template inside the window that trip a flood event |
| `LOG_SNAPSHOT_INTERVAL_MS` | `60000` | Top-10 snapshot cadence to the task log |
| `MAX_TEMPLATE_SAMPLE_LEN` | `200` | Truncation of stored/logged template text |

### Cardinality guard (the central tradeoff)

A naive `template_occurrences_total{template_hash}` would create **one Prometheus series per distinct template**. But floods and malformed bodies frequently produce a long tail of near-unique templates — which is *exactly* the cardinality blow-up we want to detect, not cause. So:

- A labeled series is created **only once a template's cumulative count crosses `MIN_OCCURRENCES_FOR_SERIES` (=100)**. At that moment the full cumulative count is backfilled, then each subsequent occurrence increments by 1.
- Rare templates are still fully reflected in the global aggregates (`records_total`, `unique_templates`, `dedup_ratio`, `bytes_saved_total`); they simply never get their own time series.
- The in-memory template map is independently bounded at `MAX_TEMPLATES (=20000)`; further distinct templates land in an `overflow` bucket so JVM heap stays bounded under a pathological flood.

**Cost:** you cannot chart a low-volume template individually — by design. The series population is restricted to "templates that are actually noisy", which is the set an operator cares about. Tune `MIN_OCCURRENCES_FOR_SERIES` up to emit fewer series, or down for more visibility into the tail.

---

## 7. Build

```bash
docker run --rm \
  -v "$(pwd)/flink-jobs/otlp-log-dedup:/app" \
  -w /app \
  maven:3.9.6-eclipse-temurin-11 \
  mvn package -q -DskipTests

cp flink-jobs/otlp-log-dedup/target/otlp-log-dedup-1.0.0.jar flink-jobs/
```

The `flink-job-submitter` container picks up any `*.jar` in `./flink-jobs/` on startup and submits it with `parallelism=2`.

### Dependencies shaded into the jar
- `flink-connector-kafka:3.3.0-1.20`
- `opentelemetry-proto:1.3.2-alpha`

Marked `provided` (supplied by the Flink distro): `flink-streaming-java`, `flink-clients`, `slf4j-api`, `log4j-slf4j-impl`.

---

## 8. Limitations & gotchas

- **State is not checkpointed.** Template maps and counters live in plain instance fields initialized in `open()`. A job cancel/resubmit resets everything; counters drop to 0 then climb. Fine for a research/demo workload.
- **Per-subtask state, not global.** No `keyBy` — each of the 2 subtasks fingerprints its own slice of the partitions and keeps an independent template map. Always aggregate in PromQL with `sum by (...)`. A given template can therefore appear as up to `parallelism` series, and `unique_templates` summed across subtasks may exceed the true global distinct count (overlap is double-counted).
- **`dedup_ratio` is an approximation.** `1 - unique/total` treats every distinct template as one "kept" record. It is a volume-repetition proxy, not the exact bytes a real deduper would keep (which depends on flush cadence and consecutive-grouping).
- **`bytes_saved_total` uses a running average body size** per template. Bodies of wildly different sizes within one template skew the estimate; it is an estimate, not a guarantee.
- **Flood detection is a fixed tumbling bucket, not a true sliding window.** The per-template window resets when `now - windowStart >= FLOOD_WINDOW_MS`; a storm split across a bucket boundary can register one tick late. The event fires once per window crossing (on the exact `== FLOOD_THRESHOLD` occurrence), not continuously.
- **Latest-offset start.** `OffsetsInitializer.latest()` skips historical messages on first startup; change to `earliest()` for replay.
- **Fingerprinting is heuristic.** Drain-style masking can over- or under-collapse (e.g. a body that is *all* digits, or hostnames that look like hex). It is tuned for typical app logs, not adversarial inputs.
- **Overflow is opaque.** Past `MAX_TEMPLATES` distinct templates, occurrences are tallied only in the logged `overflow` counter, with no per-template visibility.
