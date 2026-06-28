# otlp-tail-sampler

Flink streaming job that performs **tail-based trace sampling decisions** on raw OTLP traces from Kafka, then exports Flink-native Prometheus counters/gauges describing what *would* be kept versus dropped — and how much storage that saves. It is a **decision + savings-metrics engine**; it does not (yet) re-emit the kept traces.

- **Entry point:** `io.nochaos.flink.OtlpTailSamplerJob`
- **Artifact:** `otlp-tail-sampler-1.0.0.jar` (shaded fat jar, ~22 MB)
- **Flink UI name:** `OTLP Tail-Based Trace Sampler`
- **Source:** `src/main/java/io/nochaos/flink/OtlpTailSamplerJob.java`
- **Metrics path:** Flink `PrometheusReporterFactory` on TaskManager port `9249` (scraped by Prometheus). Counters/gauges are prefixed `flink_taskmanager_job_task_operator_`.

---

## 1. The high-volume pain this solves

Storing every trace is prohibitively expensive. Trace volume scales 1:1 with request volume, but the overwhelming majority of traces are **boring**: fast, successful, and near-identical to thousands of neighbours. Teams want to **KEEP the interesting traces** — errors, slow outliers, rare/new services — and **DROP the boring majority**.

The hard part is *when* you decide:

- **Head sampling** decides at the root span, before any outcome is known. It is cheap but blind — it cannot keep "the traces that errored" because the error hasn't happened yet.
- **Tail sampling** decides *after* the whole trace is seen. A span can look perfectly healthy while a sibling three hops away threw an exception. Only by buffering all spans of a trace and scoring the assembled whole can you make an outcome-aware keep/drop decision.

This job implements the tail-based decision: it buffers spans per `trace_id`, waits a completion window, scores the trace, and records the decision. The exported metrics let you quantify the storage you would save before committing to dropping anything.

---

## 2. Data flow (topology)

```
 otlp-traces ─► KafkaSource ─► SpanExtractProcess ─► keyBy(trace_id) ─► TailSamplerProcess
                (byte[], p=2)   (SpanFact stream, p=2)                   (KeyedProcessFunction, p=2)
```

One `KafkaSource` on `otlp-traces` only. Each Kafka message (an `ExportTraceServiceRequest`) is exploded into one lightweight `SpanFact` per span. The stream is keyed by `trace_id` so all spans of a trace land on the same subtask, where a `KeyedProcessFunction` buffers them and fires a per-trace timer.

### Vertices in the Flink job graph

Submitted with `parallelism=2`. Flink materializes:

| Vertex | Parallelism | Role |
|---|---|---|
| `Source: Kafka[traces] -> extract-span-facts` | 2 | consume `otlp-traces`, run `SpanExtractProcess` |
| `KeyedProcess` (tail-sampler-decision) | 2 | `TailSamplerProcess`, keyed by `trace_id` |

---

## 3. Runtime configuration

| Setting | Value | Constant / where |
|---|---|---|
| Global parallelism | `2` | `env.setParallelism(2)` |
| Kafka bootstrap | `kafka:29092` | `KAFKA_BOOTSTRAP_SERVERS` |
| Topic | `otlp-traces` | `TRACES_TOPIC` |
| Consumer group | `flink-otlp-tail-sampler` | `GROUP_ID` |
| Kafka offsets start | `latest` | `OffsetsInitializer.latest()` |
| Watermarks | none | `WatermarkStrategy.noWatermarks()` |
| Trace completion window | `10 000 ms` | `DECISION_DELAY_MS` |
| Slow threshold | `1 000 ms` | `SLOW_THRESHOLD_MS` |
| Baseline sample rate | `0.05` (5%) | `BASELINE_SAMPLE_PCT = 5` |
| Rare seen-set cap | `50 000` services | `SEEN_SERVICES_CAP` |
| Log snapshot interval | `60 000 ms` | `LOG_SNAPSHOT_INTERVAL_MS` |
| State backend | `HashMapStateBackend` (default, heap) | — |

---

## 4. Stage-by-stage detail

### 4.1 Kafka source

Single source on `otlp-traces`:

- `bootstrapServers = kafka:29092`
- `topics = {"otlp-traces"}`
- `groupId = "flink-otlp-tail-sampler"`
- `startingOffsets = OffsetsInitializer.latest()` — historical data is skipped
- `valueOnlyDeserializer = ByteArrayDeserializationSchema` — raw pass-through

### 4.2 `SpanExtractProcess` (ProcessFunction)

Parses each `ExportTraceServiceRequest` and emits one `SpanFact` per span:

- `traceId` — hex of `span.getTraceId()` (the key)
- `serviceName` — `service.name` from the owning Resource attributes
- `durationNanos` — `endTimeUnixNano - startTimeUnixNano` (clamped to ≥ 0)
- `error` — `status.code == ERROR(2)` **or** the span carries an event named `exception`
- `bytesEstimate` — see byte approximation below

**Byte approximation.** An OTLP batch holds many spans; we don't serialize each span individually. Per-span bytes are estimated as `messageBytes / spanCount` for the batch — total bytes are conserved across the kept/dropped split, which is all the savings metric needs. Individual span sizes vary, so do not read a single span's byte attribution as exact.

Parse failures are logged at WARN and the batch is skipped.

### 4.3 `TailSamplerProcess` (KeyedProcessFunction, keyed by `trace_id`)

**Keyed state per trace:**

- `spanBuffer : ListState<SpanFact>` — every span fact for the trace
- `timerArmed : ValueState<Long>` — the scheduled completion-window timer (null ⇒ none)

**Per-element:** append the fact to `spanBuffer`; if no timer is armed yet (first span of the trace), register a processing-time timer at `now + DECISION_DELAY_MS` and record it. This is the **trace completion window** — we assume a trace is "done" 10s after its first span arrives at this operator.

**On timer fire:** fold the buffered spans into `(spanCount, byteSum, maxDuration, anyError, serviceName)`, score, increment metrics, then `clear()` both state cells.

**Operator-wide (NOT keyed, NOT checkpointed):** a `HashSet<String> seenServices` for rare detection, plus `keptTraces` / `totalTraces` running totals backing the `kept_ratio` gauge.

### 4.4 Scoring — decision logic (priority order)

The first matching rule wins:

| Priority | Reason | Condition | Decision |
|---|---|---|---|
| 1 | `error` | any span `status=ERROR` or has an `exception` event | **KEEP** |
| 2 | `slow` | `maxDuration > SLOW_THRESHOLD_MS` (1000ms) | **KEEP** |
| 3 | `rare` | first time this `service.name` is seen by the operator (bounded set, cap 50 000) | **KEEP** |
| 4 | `probabilistic` | `(Math.abs(traceId.hashCode()) % 100) < 5` | **KEEP** |
| 5 | `boring` | otherwise | **DROP** |

The probabilistic coin flip is **deterministic** on `trace_id` — `Math.random()` is deliberately avoided because it is non-deterministic and breaks replay/at-least-once reasoning. The same trace id always lands on the same side of the 5% line.

**Rare detection** records each new service in `seenServices`; once the set reaches `SEEN_SERVICES_CAP`, all further services are treated as not-rare so the set cannot grow unbounded.

### 4.5 Log snapshot

Every 60s the subtask writes a snapshot to the task log (`[tail-sampler] ...`): total/kept traces, current `kept_ratio`, seen-service count, and the top-10 entries of each labeled counter — so decisions can be spot-checked from the Flink UI without Prometheus.

---

## 5. Complete metric reference

All metrics are emitted via the Flink `MetricGroup` and surface through the `PrometheusReporterFactory` on TM port `9249`, prefixed `flink_taskmanager_job_task_operator_`.

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `otlp_tracesampler_traces_total` | counter | `decision`, `reason` | One increment per scored trace. `decision ∈ {kept, dropped}`; `reason ∈ {error, slow, rare, probabilistic, boring}` |
| `otlp_tracesampler_spans_total` | counter | `decision` | Spans belonging to kept vs dropped traces |
| `otlp_tracesampler_bytes_total` | counter | `decision` | **Estimated** serialized bytes (per-span = `messageBytes / spanCount`), summed per kept/dropped trace |
| `otlp_tracesampler_kept_ratio` | gauge | — | `kept_traces / total_traces` since operator start (0.0 before the first decision) |

Label cardinality is tiny and bounded: `decision` has 2 values, `reason` has 5 → at most 10 `traces_total` series, 2 each for `spans_total`/`bytes_total`, plus 1 gauge.

---

## 6. PromQL cookbook

> The Prometheus reporter prefixes metric names with `flink_taskmanager_job_task_operator_`. Examples below use the bare name; prepend the prefix (or `{__name__=~".*otlp_tracesampler_.*"}`) for your setup. Sum across the 2 subtasks to get job-wide numbers.

**Effective sampling rate (fraction of traces kept)**

```promql
# instantaneous, over the last 5m
sum(rate(otlp_tracesampler_traces_total{decision="kept"}[5m]))
  /
sum(rate(otlp_tracesampler_traces_total[5m]))

# cumulative since start (matches the kept_ratio gauge)
avg(otlp_tracesampler_kept_ratio)
```

**Projected storage savings (= 1 − kept_ratio)**

```promql
# fraction of trace bytes you would NOT store
1 - (
  sum(rate(otlp_tracesampler_bytes_total{decision="kept"}[5m]))
    /
  sum(rate(otlp_tracesampler_bytes_total[5m]))
)

# simpler, by trace count
1 - avg(otlp_tracesampler_kept_ratio)

# absolute bytes/sec saved by dropping
sum(rate(otlp_tracesampler_bytes_total{decision="dropped"}[5m]))
```

**Drops and keeps by reason**

```promql
# why are traces being kept? (error vs slow vs rare vs probabilistic)
sum by (reason)(rate(otlp_tracesampler_traces_total{decision="kept"}[5m]))

# boring drop rate
sum(rate(otlp_tracesampler_traces_total{decision="dropped", reason="boring"}[5m]))

# share of kept traces that are kept ONLY because of the 5% baseline
sum(rate(otlp_tracesampler_traces_total{reason="probabilistic"}[5m]))
  /
sum(rate(otlp_tracesampler_traces_total{decision="kept"}[5m]))
```

**Throughput and span-level view**

```promql
# traces scored per second
sum(rate(otlp_tracesampler_traces_total[1m]))

# spans kept vs dropped per second
sum by (decision)(rate(otlp_tracesampler_spans_total[1m]))

# consumer lag for this job
kafka_consumergroup_lag_sum{consumergroup="flink-otlp-tail-sampler"}
```

---

## 7. Re-emitting kept traces (extension — not implemented)

This build is metrics-only. To actually feed a downstream collector with the sampled subset:

1. In `SpanExtractProcess`, also carry the original `ExportTraceServiceRequest` bytes (or a per-trace byte slice) so the keyed function can reconstruct the trace, not just its facts. Buffer them in a second keyed `ListState<byte[]>`.
2. In the **KEEP** branch of `onTimer`, re-assemble a single `ExportTraceServiceRequest` for the trace, serialize it, and emit it to a side output (`KEPT_BYTES_TAG`).
3. Attach a `KafkaSink<byte[]>` writing to a new topic `otlp-traces-sampled`:
   ```java
   KafkaSink<byte[]> sink = KafkaSink.<byte[]>builder()
       .setBootstrapServers("kafka:29092")
       .setRecordSerializer(KafkaRecordSerializationSchema.builder()
           .setTopic("otlp-traces-sampled")
           .setValueSerializationSchema(new ByteArraySerializationSchema())
           .build())
       .build();
   decisions.getSideOutput(KEPT_BYTES_TAG).sinkTo(sink);
   ```
4. Point a second collector pipeline (or your trace store's OTLP ingest) at `otlp-traces-sampled`.

Buffering full request bytes roughly **doubles** per-trace state size, which is why it is omitted here.

---

## 8. State durability — current state and caveats

| State | Location | Checkpointed? | Survives TM crash? | Survives JM crash? |
|---|---|---|---|---|
| `spanBuffer` / `timerArmed` (per trace) | keyed `ListState`/`ValueState` (heap) | ✅ | ✅ | ❌ (JM-memory checkpoint storage) |
| `seenServices` (rare detection) | plain operator field | ❌ | ❌ | ❌ |
| `keptTraces` / `totalTraces` (gauge backing) | plain operator field | ❌ | ❌ | ❌ |

What this costs in practice:

- On TaskManager restart, in-flight per-trace buffers restore from the last checkpoint, but the `seenServices` set is empty again — so for a brief window every service re-triggers the `rare` rule once.
- On job cancel + resubmit (new operator UIDs), all state resets; the cumulative counters in Prometheus drop then climb.

The durable upgrade is the same as the sibling jobs: add a `FileSystemCheckpointStorage("file:///flink-checkpoints")` + a checkpoint volume in compose, and (for cross-restart rare detection) promote `seenServices` to keyed/broadcast `MapState`. RocksDB is unnecessary here — the bounded seen-set (≤ 50 000 short strings) and short-lived per-trace buffers fit comfortably in heap.

---

## 9. Build

Requires Docker only (Maven runs in a throwaway container):

```bash
docker run --rm \
  -v "$(pwd)/flink-jobs/otlp-tail-sampler:/app" \
  -w /app \
  maven:3.9.6-eclipse-temurin-11 \
  mvn package -q -DskipTests

cp flink-jobs/otlp-tail-sampler/target/otlp-tail-sampler-1.0.0.jar flink-jobs/
```

The `flink-job-submitter` container picks up any `*.jar` in `./flink-jobs/` on startup and submits it with `parallelism=2`.

### Dependencies shaded into the jar

- `flink-connector-kafka:3.3.0-1.20` (+ `flink-connector-base`)
- `opentelemetry-proto:1.3.2-alpha` (trace decoding only)

Marked `provided` (supplied by the Flink distro): `flink-streaming-java`, `flink-clients`, `slf4j-api`, `log4j-slf4j-impl`.

---

## 10. Known limitations and gotchas

- **Fixed completion window.** A trace is scored 10s after its *first* span reaches the operator. Spans arriving after the timer fires (very long traces, or a late retry) form a *new* trace buffer and are scored again — double-counting that trace. Tune `DECISION_DELAY_MS` to your p99 trace duration.
- **Processing-time, not event-time.** The window keys off wall-clock arrival, so backpressure or replay can group spans differently than their real timing. This is intentional (`noWatermarks()`), trading correctness-under-replay for simplicity.
- **Per-operator rare detection.** `seenServices` is local to each of the 2 subtasks and not shared or checkpointed. A service is "rare" the first time *each subtask* sees it, and again after any restart. It approximates novelty; it is not a global exactly-once "first seen".
- **Byte estimate is approximate.** Per-span bytes are the batch size divided evenly across spans. Good enough for aggregate savings ratios, wrong for any single span.
- **Latest-offset start.** Historical traces are not replayed on first startup. Switch to `OffsetsInitializer.earliest()` for replay.
- **No re-emit.** Kept traces are counted, not forwarded. See §7.
- **Decision metrics reset on resubmit.** New operator UIDs on cancel+resubmit reset all counters/gauges (Prometheus sees a counter reset).
