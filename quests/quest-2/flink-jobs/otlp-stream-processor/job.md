# otlp-stream-processor

Flink streaming job that consumes raw OTLP telemetry from Kafka, computes three families of analytics on the fly, and writes the results to Prometheus via Remote Write.

- **Entry point:** `io.nochaos.flink.OtlpStreamProcessorJob`
- **Artifact:** `otlp-stream-processor-1.0.0.jar` (shaded fat jar, ~25 MB)
- **Flink UI name:** `OTLP Stream Processor`
- **Source:** `src/main/java/io/nochaos/flink/OtlpStreamProcessorJob.java`

---

## 1. What the job delivers

Three feature families, all emitted as Prometheus Remote Write every 10 seconds:

### 1.1 Cumulative volume counters (`flink_otlp_*`)

Per signal type, the job tracks how much telemetry has flowed through the pipeline since the job started (or since the last successful checkpoint restore):

- `flink_otlp_messages_total{telemetry_type}` — number of OTLP batches (Kafka messages)
- `flink_otlp_bytes_total{telemetry_type}` — raw payload bytes
- `flink_otlp_spans_total{telemetry_type}` — individual spans (populated only for `traces`)
- `flink_job_alive{job="otlp-stream-processor"}` — heartbeat (`1` while the job is emitting)

### 1.2 Top-100 services by volume (`otlp_records_by_service_total`)

For each signal type (`traces`, `logs`, `metrics`), the top 100 services by record count, derived from the `service.name` resource attribute. One time series per `(telemetry_type, service_name)`. Cumulative counter.

Lets you answer: "*which services are flooding my pipeline right now?*" — across traces, logs, and metrics — without paying for a full TSDB label cardinality.

### 1.3 Top-10 high-cardinality metric attributes (`otlp_metric_attr_cardinality`)

For each `(metric_name, attribute_key)` pair on OTLP metric data points, the job keeps a distinct-value set (capped at 10 000) and emits the top 10 by size every tick. Accompanied by `otlp_metric_attr_observations_total` which counts how many data points were seen — useful to compute the *ratio* distinct/total.

Lets you answer: "*which attribute is about to blow up my TSDB cardinality budget?*" — surfaces CPFs, UUIDs, request IDs, etc. that were never supposed to be labels.

---

## 2. Data flow (topology)

```
 otlp-traces  ┐                                            ┌──► keyBy(telemetry_type, p=2)
 otlp-logs    ┤─► KafkaSource ─► ProcessFunction ──main────┤    └──► CumulativeCounterFunction ──┐
 otlp-metrics ┘                    (TagAndFork)            │                                     │
                                        │                  │                                     ├──► union ──► PrometheusSink
                                        └── side output ───┴──► keyBy("singleton", p=1)          │      (Remote Write)
                                                                 └──► AnalyticsFunction ─────────┘
```

One `KafkaSource` per signal type. All three feed into `TagAndFork`, which emits two things per input record:

- **Main output**: a lightweight `TelemetryCount(type, 1, bytes, spans)` for the counter branch.
- **Side output**: the raw OTLP `byte[]` + type tag, forwarded to the analytics branch.

The two branches are independent. Their outputs are `union`ed and written to a single `PrometheusSink`.

### Vertices in the Flink job graph

Submitted with `parallelism=2`. Flink materializes 6 vertices:

| Vertex | Parallelism | Role |
|---|---|---|
| `Source: Kafka [traces] -> Process` | 2 | consume `otlp-traces`, run `TagAndFork` |
| `Source: Kafka [logs] -> Process` | 2 | consume `otlp-logs`, run `TagAndFork` |
| `Source: Kafka [metrics] -> Process` | 2 | consume `otlp-metrics`, run `TagAndFork` |
| `KeyedProcess` (counters) | 2 | `CumulativeCounterFunction`, keyed by signal type |
| `KeyedProcess` (analytics) | **1** | `AnalyticsFunction`, singleton key — pinned via `.setParallelism(1)` |
| `Sink: Writer` | 2 | `PrometheusSink` (async HTTP client) |

Analytics is pinned to parallelism 1 so a single subtask sees every key and can compute a global top-N without a secondary reduce step.

---

## 3. Runtime configuration

| Setting | Value | Where |
|---|---|---|
| Global parallelism | `2` | `env.setParallelism(2)` |
| Analytics parallelism | `1` | `.setParallelism(1)` on analytics stage |
| Checkpoint interval | `10 000 ms` | `env.enableCheckpointing(10_000L)` |
| Checkpoint storage | `JobManagerCheckpointStorage` (heap) | `setCheckpointStorage(...)` |
| State backend | `HashMapStateBackend` (default) | — |
| Emit interval | `10 000 ms` | `EMIT_INTERVAL_MS` |
| Kafka bootstrap | `kafka:29092` | `KAFKA_BOOTSTRAP_SERVERS` |
| Kafka offsets start | `latest` | `OffsetsInitializer.latest()` |
| Watermarks | none | `WatermarkStrategy.noWatermarks()` |
| Prometheus URL | `http://prometheus:9090/api/v1/write` | `PROMETHEUS_REMOTE_WRITE_URL` |

---

## 4. Stage-by-stage detail

### 4.1 Kafka sources

Three independent sources, one per signal type. Each uses:

- `bootstrapServers = kafka:29092`
- `topics = {"otlp-" + type}` (single topic per source)
- `groupId = "flink-otlp-stream-processor-" + type`
- `startingOffsets = OffsetsInitializer.latest()` — historical data is skipped
- `valueOnlyDeserializer = ByteArrayDeserializationSchema` — raw pass-through, no parsing at the source

Topics are created upfront by the `kafka-init` container with 3 partitions each, replication factor 1.

### 4.2 `TagAndFork` (ProcessFunction)

Purpose: split each Kafka record into two downstream streams in one pass over the bytes.

For each record:

1. If `type == "traces"`, parse the payload as `ExportTraceServiceRequest` and sum spans across all `ResourceSpans → ScopeSpans → Spans`. Parse failures are swallowed silently (the message still counts toward volume).
2. Emit `TelemetryCount(type, 1, bytes, spans)` on the main output.
3. If `value != null`, emit `(type, bytes)` on the `ANALYTICS_TAG` side output.

Spans are computed once here (for the counter) and the raw bytes are parsed a second time downstream (for analytics) — duplicating the protobuf decode is a deliberate trade-off to keep the two branches loosely coupled and independently evolvable.

### 4.3 `CumulativeCounterFunction` (counters branch)

`KeyedProcessFunction<String, TelemetryCount, PrometheusTimeSeries>`, keyed by `telemetry_type`. Three keys → three independent keyed state cells per TaskManager subtask.

State (all `ValueState<Long>`):

- `totalMessages` — cumulative Kafka messages
- `totalBytes` — cumulative raw bytes
- `totalSpans` — cumulative spans (only grows on the `traces` key)
- `timerTs` — last scheduled processing-time timer

On each element: increment the three counters, and register a processing-time timer for `now + 10 s` if none exists. On timer fire: emit four `PrometheusTimeSeries` (messages, bytes, spans, `flink_job_alive`), then re-arm the timer for `+10 s`.

This state is keyed and checkpointed via the default `HashMapStateBackend`. It survives TaskManager crashes (restored from the last checkpoint). It does **not** survive JobManager loss because checkpoints live on the JM heap. See §7 for the upgrade path.

### 4.4 `AnalyticsFunction` (analytics branch)

`KeyedProcessFunction<String, Tuple2<String, byte[]>, PrometheusTimeSeries>`, keyed by a constant (`"singleton"`), parallelism 1. All records flow to a single subtask that holds all analytics state in plain Java maps:

```java
Map<String /*type*/,  Map<String /*service*/, Long /*count*/>> perServiceRecords;
Map<String /*metric\0attr_key*/, Set<String> /*distinct values, cap 10 000*/> attrDistinctValues;
Map<String /*metric\0attr_key*/, Long /*total obs*/>                          attrObservations;
```

**This state is not checkpointed.** It lives in non-transient instance fields initialized in `open()`. A cancel + resubmit of the job resets everything to empty.

#### Per-element handling

Switches on `type.f0`:

- **traces** — walk `req.getResourceSpansList()`, extract `service.name` from the Resource, sum spans per Resource. Add to `perServiceRecords["traces"][service]`.
- **logs** — same pattern, counting log records.
- **metrics** — walk resources, then for each metric: add data-point count to `perServiceRecords["metrics"][service]`, and for each data point's attributes call `trackAttribute(metric_name, attr_key, attr_value_as_string)`. Attribute values are coerced to String via `anyValueAsString` (handles `STRING / INT / DOUBLE / BOOL / BYTES`; other AnyValue variants become empty).

Data-point attribute extraction covers all metric types: Gauge, Sum, Histogram, ExponentialHistogram, Summary.

After processing, a one-shot processing-time timer is registered (`now + 10 s`) if not already scheduled.

#### Per-tick emission

On timer fire:

1. **Top-100 services per type.** For each of `{traces, logs, metrics}`, sort the inner `Map<service_name, Long>` descending by count, take the first 100, emit one `otlp_records_by_service_total` series per row with labels `{telemetry_type, service_name}`.
2. **Top-10 cardinality.** Sort `attrDistinctValues` entries descending by `HashSet.size()`, take the first 10, for each emit:
   - `otlp_metric_attr_cardinality{metric, attribute_key} = distinctCount` (gauge)
   - `otlp_metric_attr_observations_total{metric, attribute_key} = obsCount` (counter)

Sample timestamps are `System.currentTimeMillis()` at emit time.

Then re-arm the timer for `+10 s`.

### 4.5 `PrometheusSink`

`flink-connector-prometheus:1.0.0-1.20`. Async sink that batches `PrometheusTimeSeries` writes into protobuf + snappy + HTTP POST to `/api/v1/write`. Retries and backpressure are handled by the connector. Parallelism 2 — both subtasks write concurrently to Prometheus.

The counters branch and the analytics branch are `union`ed and feed the same sink instance.

---

## 5. Complete metric reference

| Metric | Type | Labels | Cadence | Notes |
|---|---|---|---|---|
| `flink_otlp_messages_total` | counter | `telemetry_type` | 10 s per type (3 series) | OTLP batch count (= Kafka messages) |
| `flink_otlp_bytes_total` | counter | `telemetry_type` | 10 s per type (3 series) | Raw payload bytes |
| `flink_otlp_spans_total` | counter | `telemetry_type` | 10 s per type (3 series) | Spans parsed from `ExportTraceServiceRequest`; always 0 for `logs`/`metrics` |
| `flink_job_alive` | gauge | `job` | 10 s per type (emits 3×, dedups to 1 series) | `1` while emitting |
| `otlp_records_by_service_total` | counter | `telemetry_type`, `service_name` | 10 s, top-100 per type | Spans/records/points per service |
| `otlp_metric_attr_cardinality` | gauge | `metric`, `attribute_key` | 10 s, top-10 global | Distinct values, cap 10 000 |
| `otlp_metric_attr_observations_total` | counter | `metric`, `attribute_key` | 10 s, alongside top-10 cardinality | Data-point observations bearing the attribute |

**Steady-state volume**: ≤ `4 + (3 × 100) + (2 × 10) = 324` series emitted every 10 s ≈ 32 writes/s.

---

## 6. PromQL cookbook

**Volume and health**

```promql
# job alive
flink_job_alive{job="otlp-stream-processor"}

# total throughput
sum(rate(flink_otlp_messages_total[1m]))
sum(rate(flink_otlp_bytes_total[1m]))

# per signal
sum by (telemetry_type)(rate(flink_otlp_messages_total[1m]))

# spans specifically
sum(rate(flink_otlp_spans_total{telemetry_type="traces"}[1m]))

# flink consumer lag
kafka_consumergroup_lag_sum{consumergroup=~"flink-otlp-stream-processor.*"}
```

**Top services**

```promql
# top-20 heaviest services right now (traces)
topk(20, otlp_records_by_service_total{telemetry_type="traces"})

# top-10 by recent rate (detects spikes, not cumulative weight)
topk(10, rate(otlp_records_by_service_total{telemetry_type="traces"}[1m]))

# cross-signal: which services send the most total volume?
topk(10, sum by (service_name)(rate(otlp_records_by_service_total[5m])))
```

**Cardinality**

```promql
# top offenders (already pre-filtered by the job)
otlp_metric_attr_cardinality

# any attribute saturated at the 10 000 cap (probable true cardinality is ≥ cap)
otlp_metric_attr_cardinality >= 10000

# distinct/observation ratio — values near 1.0 mean "unique per data point" (CPF/UUID)
otlp_metric_attr_cardinality
  / on(metric,attribute_key)
otlp_metric_attr_observations_total

# rate of new distinct values (cardinality leak detector — uncomment for a 5m window)
# deriv(otlp_metric_attr_cardinality[5m]) > 0
```

---

## 7. State durability — current state and upgrade path

### 7.1 What's durable today

| State | Location | Checkpointed? | Survives TM crash? | Survives JM crash? |
|---|---|---|---|---|
| Counter branch (`totalMessages/Bytes/Spans/timerTs`) | `ValueState` (heap) | ✅ | ✅ | ❌ (JM-memory checkpoint storage) |
| Analytics branch (maps) | plain instance fields | ❌ | ❌ | ❌ |

### 7.2 What this actually costs you in practice

- Counter series (`flink_otlp_*_total`) survive TaskManager restarts cleanly as long as JM stays up.
- On job cancel + resubmit, Flink generates fresh operator UIDs → counters effectively reset (Prometheus will show a drop then climb).
- Analytics series (`otlp_records_by_service_total`, `otlp_metric_attr_cardinality`) reset on any job restart, even from a crash.

For a research/demo workload this is fine. For anything production-ish you'd want state to survive restarts.

### 7.3 Minimal upgrade (no RocksDB)

The `flink:1.20.1-scala_2.12-java11` Docker image **does not ship RocksDB** — neither in `/opt/flink/lib/` nor `/opt/flink/opt/`. But you don't need it unless the state outgrows heap. The cheap upgrade uses only the built-in `HashMapStateBackend` with durable checkpoint storage:

1. In `docker-compose.yaml`, add a volume for Flink checkpoints:
   ```yaml
   volumes:
     - flink-checkpoints:/flink-checkpoints
   ```
2. In the job code:
   ```java
   import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
   env.getCheckpointConfig().setCheckpointStorage(
       new FileSystemCheckpointStorage("file:///flink-checkpoints"));
   ```
3. Port `AnalyticsFunction` from plain maps to keyed state:
   - Key the analytics stream by `(telemetry_type, service_name)` instead of the singleton, and by `(metric_name, attribute_key)` for the cardinality path. This means splitting into two sub-operators.
   - Replace `Map<service, Long>` with `ValueState<Long>` per keyed scope; `HashSet<String>` becomes `MapState<String, Boolean>` (MapState is the canonical way to store a set in Flink).
   - The top-N step can no longer iterate "all keys" locally. Two options:
     a. Use a tumbling window with a `ProcessWindowFunction` that collects counts across keys and emits only the top-N per window fire.
     b. Add a second operator with `keyBy(constant)` and `parallelism=1` that consumes the periodic emits of the first stage and does the sort — same pattern as today, but now the upstream state is durable.

**Effort estimate**: infra change is ~5 minutes (1 volume + 1 line in job). Code refactor of `AnalyticsFunction` is ~half a day — the top-N restructure is the non-trivial piece.

### 7.4 When you actually need RocksDB

RocksDB becomes useful when keyed state exceeds what TaskManager heap can hold (tens of GB+). This job's measured state is ~20 KB under test load and would need *orders of magnitude* more services or attributes before RocksDB matters. If/when needed:

1. Drop `flink-statebackend-rocksdb-1.20.1.jar` into `/opt/flink/lib/` (Dockerfile `RUN wget ...` or a volume-mounted jar).
2. Also need `frocksdbjni` native deps — bundled in the statebackend jar for Linux x86-64 and arm64.
3. `flink-conf.yaml`: `state.backend: rocksdb` (or `env.setStateBackend(new EmbeddedRocksDBStateBackend())` in code).
4. RocksDB writes SSTables to local disk per TaskManager — mount a volume at `/tmp/rocksdb` (or whatever the configured path is) or accept that state is lost if the TM container is recreated.
5. Checkpoints themselves should still land on durable storage (`FileSystemCheckpointStorage` or S3).

---

## 8. Build

Requires Docker only (Maven runs in a throwaway container):

```bash
docker run --rm \
  -v "$(pwd)/flink-jobs/otlp-stream-processor:/app" \
  -w /app \
  maven:3.9.6-eclipse-temurin-11 \
  mvn package -q -DskipTests

cp flink-jobs/otlp-stream-processor/target/otlp-stream-processor-1.0.0.jar flink-jobs/
```

The `flink-job-submitter` container picks up any `*.jar` in `./flink-jobs/` on startup and submits it to the JobManager with `parallelism=2`.

### Dependencies shaded into the jar

- `flink-connector-base:1.20.1`
- `flink-connector-kafka:3.3.0-1.20`
- `flink-connector-prometheus:1.0.0-1.20`
- `opentelemetry-proto:1.3.2-alpha` (used for trace + logs + metrics decoding)
- `jackson-databind:2.15.2`

Marked `provided` (supplied by the Flink distro): `flink-streaming-java`, `flink-clients`, `slf4j-api`, `log4j-slf4j-impl`.

---

## 9. Redeploy an updated job

The `flink-job-submitter` container only runs once at stack startup. To push a new jar onto a running stack:

```bash
# Cancel the running job
JOBID=$(curl -s http://localhost:8082/jobs | python3 -c "import sys,json;print(json.load(sys.stdin)['jobs'][0]['id'])")
curl -sf -X PATCH "http://localhost:8082/jobs/$JOBID?mode=cancel"

# Upload + submit the new jar
resp=$(curl -sf -X POST http://localhost:8082/jars/upload -H 'Expect:' \
  -F "jarfile=@flink-jobs/otlp-stream-processor-1.0.0.jar")
jar_id=$(echo "$resp" | python3 -c "import sys,json;print(json.load(sys.stdin)['filename'].split('/')[-1])")
curl -sf -X POST "http://localhost:8082/jars/$jar_id/run" \
  -H 'Content-Type: application/json' -d '{"parallelism": 2}'
```

Counter state is lost on resubmit (new operator UIDs); analytics state is lost on any restart. See §7 for making this survive.

---

## 10. Observed behavior (from the test runs)

End-to-end tested against the bundled `docker-compose.yaml`. See `test.md` for reproducible scripts.

### 10.1 Smoke test (5 batches per signal)

- collector-l1 received 10 spans / 5 logs / 5 metric points ✅
- Kafka: **1 message per topic** (collector batch processor coalesced the bursts)
- Flink emitted: `messages=1`, `spans=10` (traces), `0` (logs/metrics), bytes as expected ✅
- Sending 3 additional spaced trace batches grew `messages` to 4 and `spans` to 13 monotonically ✅

### 10.2 Load test (`telemetrygen`, 4 workers × 500 rate × 30 s per signal)

- collector-l1 exporter sent 60 021 spans / 60 004 logs / 60 004 metric points
- Flink reported exact parity: `flink_otlp_spans_total{telemetry_type="traces"} = 60021` ✅
- Kafka offsets matched `flink_otlp_messages_total` exactly (59 / 57 / 52) ✅
- Consumer lag returned to 0 within seconds
- Checkpoints: 140 completed, 0 failed, state size ~12 KB, end-to-end duration 3 ms

### 10.3 Top-N services (12 heavies + 150 tinies via telemetrygen `--service`)

- All 12 `heavy-svc-*` always in the top-15 with 400 records each (as expected)
- 130 of 150 `tiny-svc-*` appeared in the top-100 across ticks (ties cause rotation; see §11)
- `otlp_records_by_service_total` grew monotonically ✅

### 10.4 Cardinality (200 unique CPFs/UUIDs vs low-card attrs)

```
checkout.latency[cpf]      distinct=200   ← high-cardinality detected
login.attempts[user_id]    distinct=200   ← high-cardinality detected
login.attempts[region]     distinct=5
orders.total[region]       distinct=5
checkout.latency[status]   distinct=3
```

Ranking is correct: unique identifiers at the top, enum-like attributes at the bottom. `observations_total` = 200 across all pairs, matching the 200 data points sent per metric.

---

## 11. Known limitations and gotchas

### 11.1 Prometheus Remote Write

- **Intermittent `400 Bad Request: out of order sample`** from the `HttpResponseCallback`. Prometheus requires strictly increasing timestamps per series; `System.currentTimeMillis()` can produce equal or backward-moving values across operators or checkpoints. Counters still track correctly but graphs may have gaps. Observed frequency under 1 kmsg/s load: ~40–70 warnings per minute, with samples delivered on the next tick.
- Writes are async — the sink buffers and retries. Backpressure propagates upstream if Prometheus is slow.

### 11.2 Collector batching

- The L1 batch processor coalesces rapid OTLP requests into a single Kafka message. `flink_otlp_messages_total` therefore counts **batches**, not user requests. For volume measurements, prefer `flink_otlp_spans_total` / `flink_otlp_bytes_total`.

### 11.3 Flink metrics reporter misconfiguration

- `docker-compose.yaml` sets `metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporter` — wrong class. Should be `PrometheusReporterFactory`. Consequence: no Flink engine metrics (JVM, checkpoint durations, `numRecordsIn`) on port 9249, and Flink REST `vertices[].metrics.read-records` shows 0 for source vertices even under load. The job's own emitted metrics are unaffected (they go through `PrometheusSink`, not the reporter).

### 11.4 Semantics quirks

- **`flink_job_alive` appears only after the first message per key.** Before any traffic, `absent(flink_job_alive)` returns true even though the job is running.
- **`flink_otlp_spans_total` for `logs`/`metrics` is always 0** by design.
- **Latest-offset start.** On first startup, historical messages are not replayed. Change to `OffsetsInitializer.earliest()` for replay semantics.
- **Protobuf parse errors are silent.** A malformed trace batch contributes `spans=0` but still counts toward `messages` and `bytes`. No error metric is emitted.

### 11.5 Top-N tie-breaking

- `HashMap.entrySet()` iteration order is undefined. When many services tie at the same count (e.g. 150 services with 10 records each), successive ticks pick different subsets of 100 to emit. Over a retention window Prometheus will end up storing *more* than 100 service series.
- For cardinality this matters less — tied distinct counts are rare and rankings are usually dominated by a few true offenders.

### 11.6 Kafka consumer warnings

- `'[client.id.prefix, partition.discovery.interval.ms]' were supplied but are not used yet` — harmless, upstream bug in `flink-connector-kafka:3.3.0-1.20`.

### 11.7 Scalability

- Counter branch: 3 keys, max useful parallelism is 3. Current parallelism 2 means one subtask holds 2 keys and the other holds 1.
- Analytics branch: hard pin to parallelism 1. Measured to keep up cleanly at ~1 k batches/s. Above that, the singleton operator becomes the bottleneck and upstream lag grows.
- State on the analytics branch is bounded in practice by the number of distinct services (`O(hundreds)`) and distinct `(metric, attr_key)` pairs (`O(tens)`), each times the 10 k cap on distinct values. Worst-case heap: `~10 k × 10 pairs × 40 B ≈ 4 MB`. Negligible.
