# otlp-cost-attribution

Flink streaming job that turns raw OTLP telemetry in Kafka into a **FinOps chargeback / cost-attribution** feed: it attributes ingested bytes and records back to the org dimensions that own them (team / vertical / service / signal) and estimates spend via a configurable price model. All output is exposed as Flink-native counters/gauges through the PrometheusReporterFactory on TaskManager port `9249`.

- **Entry point:** `io.nochaos.flink.OtlpCostAttributionJob`
- **Artifact:** `otlp-cost-attribution-1.0.0.jar` (shaded fat jar, ~23 MB)
- **Flink UI name:** `OTLP Cost Attribution`
- **Source:** `src/main/java/io/nochaos/flink/OtlpCostAttributionJob.java`

---

## 1. Purpose & the FinOps pain

Ingest-priced observability backends (Datadog, Grafana Cloud, New Relic, Splunk, Chronosphere) charge by **GB ingested** and/or **records/events** — and the invoice never says *who* generated the spend. The platform team receives one giant bill; the product teams generate the volume; nobody can:

- **bill back** spend to the owning team / vertical (chargeback / showback),
- **find waste** — debug logs left on in prod, a runaway service, a noisy metric,
- **forecast** next month's bill per team.

This job answers "who owns the spend?" by attributing every ingested byte and record to the resource attributes the load generator stamps on telemetry — `team`, `vertical`, `service.name` — split by `signal` (traces / logs / metrics), and converts bytes to an estimated dollar figure.

---

## 2. Byte-attribution method (the key design decision)

A Kafka record is one OTLP **envelope** (`ExportTraceServiceRequest` / `ExportLogsServiceRequest` / `ExportMetricsServiceRequest`). One envelope can carry **many** `ResourceSpans` / `ResourceLogs` / `ResourceMetrics` blocks, each owned by a different team/service. So the raw Kafka message size cannot be attributed to a single owner.

Two ways to split it:

1. **Proportional split** — take the envelope byte size and divide it among resources by their record share. Simple, but records ≠ bytes (a single fat log line can dwarf 100 spans), so it mis-attributes.
2. **Per-resource serialized size (chosen)** — call protobuf's `ResourceXxx.getSerializedSize()` on each resource block and use that as its byte estimate. Protobuf computes this for free as the on-wire length of that sub-message.

We use **option 2**.

### Tradeoff of `getSerializedSize()`

- ✅ Exact for the resource's *own* bytes (its resource attributes + all scopes + all spans/logs/datapoints), so attribution tracks real wire cost, not a record-count proxy.
- ⚠️ It is the size of the `ResourceXxx` sub-message **only**. It excludes the handful of envelope-framing bytes the parent message spends per resource (the repeated-field tag + the varint length prefix — typically 2–6 bytes per resource). So `sum(per-resource sizes)` is slightly **less** than the full Kafka payload.
- ⚠️ It measures the **decoded OTLP/protobuf** size, not the Kafka-stored bytes. If the topic used compression, on-disk bytes differ; if the collector re-encoded (e.g. it accepted OTLP/JSON and forwarded protobuf), the billed wire size at your *vendor* may differ again. Treat these numbers as a **consistent internal attribution basis**, not a byte-exact reproduction of the vendor invoice.

The framing error is single-digit bytes against KB–MB payloads — negligible — and we trade it for exact per-owner attribution with no proportional fudging.

---

## 3. Price model & how to tune it

```java
private static final double PRICE_PER_GB_INGESTED = 0.50;   // USD/GB — PLACEHOLDER
private static final double BYTES_PER_GB          = 1e9;
```

`PRICE_PER_GB_INGESTED = 0.50` USD/GB is a **placeholder** in the Datadog/Grafana-style ingest-pricing ballpark. The per-team gauge is:

```
otlp_cost_estimated_usd{team} = cumulative_team_bytes / 1e9 * PRICE_PER_GB_INGESTED
```

### This gauge is CUMULATIVE, not a monthly rate

It grows monotonically from job start (it resets on job restart — state is not checkpointed). It answers "how many dollars of ingest has this team accumulated since the job started running?" — useful for a live demo, **not** a monthly bill.

To get a **spend rate / monthly projection**, ignore the gauge and use the byte **counter** in PromQL (see §6). E.g. projected monthly bill per team from the last hour's rate:

```promql
sum by (team)(rate(otlp_cost_bytes_total[1h])) / 1e9 * 0.50 * 730
```

### Tuning

- Change the `0.50` constant to your contracted USD/GB and rebuild.
- Different signals often have different vendor prices (logs vs spans vs custom metrics). To model that, either (a) compute per-signal dollars purely in PromQL by multiplying `otlp_cost_bytes_total{signal="..."}` by the right constant per signal, or (b) extend the job to hold a `Map<signal, price>`.
- Many vendors price by **records/events** (e.g. $/M log events, $/M custom-metric series), not bytes. Use `otlp_cost_records_total` with a per-record price in PromQL for that model.

---

## 4. Topology

```
 otlp-traces  ─► KafkaSource ─► TracesCostProcess   (p=2) ─► counters/gauges (port 9249)
 otlp-logs    ─► KafkaSource ─► LogsCostProcess     (p=2) ─► counters/gauges + waste counter
 otlp-metrics ─► KafkaSource ─► MetricsCostProcess  (p=2) ─► counters/gauges
```

Three independent `KafkaSource`s, one per signal, each feeding its own `ProcessFunction`. No keyed state, no sink — every `ProcessFunction` registers Flink metrics in `open()` and updates them in `processElement()`; the PrometheusReporter scrapes them off the TaskManager. Mirrors the structure of `otlp-insights-processor`.

| Vertex | Parallelism | Role |
|---|---|---|
| `Source: Kafka[traces] -> cost-traces` | 2 | consume `otlp-traces`, attribute spans + bytes |
| `Source: Kafka[logs] -> cost-logs` | 2 | consume `otlp-logs`, attribute log records + bytes, count debug-log waste |
| `Source: Kafka[metrics] -> cost-metrics` | 2 | consume `otlp-metrics`, attribute data points + bytes |

### Per-element work

For each resource block in the envelope:

- **records** = spans (`ScopeSpans.getSpansCount()`) / log records / metric data points (Gauge+Sum+Histogram+ExponentialHistogram+Summary, via `dataPointCount`).
- **bytes** = `ResourceXxx.getSerializedSize()` (see §2).
- **labels** = `team`, `vertical`, `service.name` pulled from the resource attributes (missing → `unknown`).
- logs only: any record with `0 < severityNumber < 9` (below INFO → DEBUG/TRACE) increments the waste counter.

Every 60s each subtask logs a Top-10 teams-by-bytes (with estimated USD) + Top-10 services-by-bytes snapshot to the task log.

---

## 5. Metric reference

All metrics carry the Flink reporter prefix `flink_taskmanager_job_task_operator_` once scraped. The `signal` label is `traces` / `logs` / `metrics`. Label cardinality is kept deliberately small — `team`/`vertical` are bounded org dimensions; `service_name` lives on its **own** metric so it is never crossed with team/vertical (avoids a giant label product).

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `otlp_cost_bytes_total` | counter | `signal`, `team`, `vertical` | Attributed ingest bytes (per-resource `getSerializedSize()`) |
| `otlp_cost_records_total` | counter | `signal`, `team`, `vertical` | Spans / log records / metric data points |
| `otlp_cost_bytes_by_service_total` | counter | `signal`, `service_name` | Bytes per service — separate metric to cap cardinality |
| `otlp_cost_estimated_usd` | gauge (Double) | `team` | `cumulative_team_bytes / 1e9 * 0.50`. **Cumulative since start**, not a rate (see §3). No `signal` label — sums across signals per team |
| `otlp_cost_waste_debug_logs_total` | counter | `signal=logs`, `service_name` | Log records below INFO (`severityNumber < 9`) — waste candidate |

> Note: `otlp_cost_estimated_usd` is registered under the global operator metric group (no `signal` scope) so each signal's subtask accumulates into the same per-team series. Because there are three operators (one per signal) updating per-team gauges that are scraped per TaskManager subtask, treat the gauge as a coarse live indicator and prefer the byte counters for anything precise.

---

## 6. PromQL cookbook

**Cost per team — spend rate (the chargeback number)**
```promql
# bytes/s per team, all signals
sum by (team)(rate(otlp_cost_bytes_total[5m]))

# $/hour per team
sum by (team)(rate(otlp_cost_bytes_total[5m])) / 1e9 * 0.50 * 3600

# projected monthly bill per team (730h/month)
sum by (team)(rate(otlp_cost_bytes_total[1h])) / 1e9 * 0.50 * 730
```

**$ per signal (where the money goes)**
```promql
sum by (signal)(rate(otlp_cost_bytes_total[5m])) / 1e9 * 0.50 * 730
```

**Top spenders**
```promql
# top-10 teams by current ingest rate
topk(10, sum by (team)(rate(otlp_cost_bytes_total[5m])))

# top-10 services by bytes
topk(10, sum by (service_name)(rate(otlp_cost_bytes_by_service_total[5m])))

# top spenders by vertical
topk(5, sum by (vertical)(rate(otlp_cost_bytes_total[5m])))
```

**Waste — debug logs share**
```promql
# debug-log records/s by service
sum by (service_name)(rate(otlp_cost_waste_debug_logs_total[5m]))

# fraction of a service's log records that are debug/trace (0..1) — high = budget waste
  sum by (service_name)(rate(otlp_cost_waste_debug_logs_total[5m]))
/ sum by (service_name)(rate(otlp_cost_records_total{signal="logs"}[5m]))
```

**Bytes/record efficiency (fat payloads = expensive)**
```promql
# avg bytes per record per team — outliers ship bloated telemetry
  sum by (team)(rate(otlp_cost_bytes_total[5m]))
/ sum by (team)(rate(otlp_cost_records_total[5m]))

# bytes per record per signal
  sum by (signal)(rate(otlp_cost_bytes_total[5m]))
/ sum by (signal)(rate(otlp_cost_records_total[5m]))
```

**Cumulative gauge (demo / spot-check only)**
```promql
sum by (team)(otlp_cost_estimated_usd)     # $ accrued since job start
```

---

## 7. Configuration constants

| Setting | Value | Where |
|---|---|---|
| Global parallelism | `2` | `env.setParallelism(2)` |
| Kafka bootstrap | `kafka:29092` | `KAFKA_BOOTSTRAP_SERVERS` |
| Kafka offsets start | `latest` | `OffsetsInitializer.latest()` |
| Watermarks | none | `WatermarkStrategy.noWatermarks()` |
| Consumer groups | `flink-otlp-cost-{traces,logs,metrics}` | `kafka(...)` |
| Price per GB | `0.50` USD | `PRICE_PER_GB_INGESTED` |
| Bytes per GB | `1e9` | `BYTES_PER_GB` |
| INFO severity threshold | `9` | `SEVERITY_INFO` (records below = waste) |
| Log snapshot interval | `60 000 ms` | `LOG_SNAPSHOT_INTERVAL_MS` |
| Max label value length | `120` | `MAX_LABEL_VALUE_LEN` |
| Java | `11` | — |

---

## 8. Build

Requires Docker only (Maven runs in a throwaway container):

```bash
docker run --rm \
  -v "$(pwd)/flink-jobs/otlp-cost-attribution:/app" \
  -w /app \
  maven:3.9.6-eclipse-temurin-11 \
  mvn package -q -DskipTests

cp flink-jobs/otlp-cost-attribution/target/otlp-cost-attribution-1.0.0.jar flink-jobs/
```

The `flink-job-submitter` container picks up any `*.jar` in `./flink-jobs/` on startup and submits it with `parallelism=2`.

### Dependencies shaded into the jar
- `flink-connector-kafka:3.3.0-1.20`
- `opentelemetry-proto:1.3.2-alpha`

Marked `provided` (supplied by the Flink distro): `flink-streaming-java`, `flink-clients`, `slf4j-api`, `log4j-slf4j-impl`.

---

## 9. Limitations & caveats

- **Byte basis ≠ vendor invoice.** `getSerializedSize()` is the decoded OTLP/protobuf wire size of each resource block. It excludes per-resource envelope framing (~2–6 bytes each) and ignores Kafka compression and any re-encoding the collector did. It is a *consistent internal attribution basis*, not a byte-exact reproduction of what Datadog/Grafana bills. Calibrate `PRICE_PER_GB_INGESTED` against a real invoice to absorb the constant-factor difference.
- **The `_usd` gauge is cumulative-since-start, not a rate**, and resets on job restart. Use the byte counter + `rate()` for real spend (see §3, §6).
- **No state durability.** No checkpointed state; all counters/gauges live in operator memory. Job cancel/resubmit resets everything (new operator UIDs → Prometheus sees a drop then re-climb). Fine for demo/research.
- **`latest` offsets** — historical Kafka data is skipped on first start; switch to `OffsetsInitializer.earliest()` for replay.
- **Silent parse errors** — a malformed batch is logged at WARN and contributes nothing; there is no error metric.
- **Collector batching** — the L1 batch processor coalesces requests, so byte attribution is per *resource block within a batch*, which is exactly what we want; just don't expect record counts to map 1:1 to user requests.
- **Per-subtask gauge semantics** — with `parallelism=2`, each of the two subtasks reports its own per-team gauge series; the reporter distinguishes them by subtask scope. Aggregate in PromQL with `sum by (team)(...)`. The byte/record **counters** aggregate cleanly the same way.
- **`unknown` bucket** — resources missing `team` / `vertical` / `service.name` land in an `unknown` label. A large `unknown` share means untagged telemetry that can't be charged back — fix the instrumentation/collector enrichment.
- **Waste heuristic is logs-only** — debug/trace logs are one obvious waste class; high-cardinality metrics and oversampled traces are other big ones not covered here (see `otlp-stream-processor` for cardinality detection).
