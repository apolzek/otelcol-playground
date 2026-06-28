package io.nochaos.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tail-based trace sampling decision engine driven off the raw OTLP {@code otlp-traces}
 * Kafka topic.
 *
 * <h3>Why this exists (the high-volume pain)</h3>
 * Storing every trace is prohibitively expensive: trace volume scales with request volume,
 * yet the overwhelming majority of traces are "boring" — fast, successful, and identical to
 * thousands of their neighbours. Teams want to <b>KEEP the interesting traces</b> (errors,
 * slow outliers, rare services) and <b>DROP the boring majority</b>. The catch is that
 * "interesting" can only be judged once the <i>whole</i> trace is seen — a span may look fine
 * but a sibling span three hops away errored. That is the defining property of
 * <b>tail-based</b> sampling versus head sampling, which decides at the root span before any
 * outcome is known.
 *
 * <p>This job is a <b>decision + savings-metrics engine</b>. It does not re-emit traces; it
 * reconstructs each trace from its spans, scores it after a completion window, and exports
 * Flink-native Prometheus counters/gauges describing what <i>would</i> be kept versus dropped
 * and how much storage that saves. See {@code job.md} and §"Re-emit extension" below for how
 * you would wire the kept traces back to Kafka to feed a downstream collector.
 *
 * <h3>Topology</h3>
 * <pre>
 *   otlp-traces ─► KafkaSource ─► SpanExtractProcess ─► keyBy(trace_id) ─► TailSamplerProcess
 *                  (byte[])        (SpanFact stream)                        (KeyedProcessFunction)
 * </pre>
 * {@code SpanExtractProcess} parses each {@link ExportTraceServiceRequest} into one lightweight
 * {@link SpanFact} per span (trace_id hex, service.name, duration nanos, error flag, byte
 * estimate). The keyed function buffers facts per trace in keyed {@link ListState}, arms a
 * 10s processing-time timer on the first span of a trace ("trace completion window"), and on
 * timer fire scores the buffered trace, increments decision metrics, and clears state.
 *
 * <h3>Decision logic (priority order)</h3>
 * <ol>
 *   <li><b>error</b>  — any span has {@code status.code=ERROR(2)} or an {@code exception} event ⇒ KEEP</li>
 *   <li><b>slow</b>   — max span duration &gt; {@code SLOW_THRESHOLD_MS} (1000ms) ⇒ KEEP</li>
 *   <li><b>rare</b>   — first time this {@code service.name} has been seen by this operator
 *       (bounded seen-set, cap {@code SEEN_SERVICES_CAP}=50000) ⇒ KEEP</li>
 *   <li><b>probabilistic</b> — KEEP with {@code BASELINE_SAMPLE_RATE} (5%) else DROP "boring".
 *       The coin flip is deterministic: {@code (Math.abs(traceId.hashCode()) % 100) < 5}.
 *       {@code Math.random()} is intentionally avoided (it is unavailable / non-deterministic
 *       in Flink operator scripts and would break replayability).</li>
 * </ol>
 *
 * <h3>Metrics (Flink counters/gauges via {@link MetricGroup}, prefixed
 * {@code flink_taskmanager_job_task_operator_} by the Prometheus reporter)</h3>
 * <ul>
 *   <li>{@code otlp_tracesampler_traces_total{decision, reason}} — decision in {kept,dropped}</li>
 *   <li>{@code otlp_tracesampler_spans_total{decision}} — spans belonging to kept/dropped traces</li>
 *   <li>{@code otlp_tracesampler_bytes_total{decision}} — estimated serialized bytes of kept/dropped traces</li>
 *   <li>{@code otlp_tracesampler_kept_ratio} — {@code Gauge<Double>} = kept_traces / total_traces</li>
 * </ul>
 *
 * <h3>Re-emit extension (NOT implemented here)</h3>
 * To actually feed a downstream collector you would, in the KEEP branch of the timer, emit the
 * buffered trace and attach a {@code KafkaSink<byte[]>} writing to a new {@code otlp-traces-sampled}
 * topic, then point a second collector pipeline at that topic. The cheapest faithful approach is
 * to buffer the original {@code ExportTraceServiceRequest} bytes (not just {@link SpanFact}s) in a
 * second keyed {@link ListState}, re-assemble a single {@code ExportTraceServiceRequest} per kept
 * trace, serialize it, and {@code out.collect(bytes)} into a sink:
 * <pre>
 *   KafkaSink&lt;byte[]&gt; sink = KafkaSink.&lt;byte[]&gt;builder()
 *       .setBootstrapServers("kafka:29092")
 *       .setRecordSerializer(KafkaRecordSerializationSchema.builder()
 *           .setTopic("otlp-traces-sampled")
 *           .setValueSerializationSchema(new ByteArraySerializationSchema())
 *           .build())
 *       .build();
 *   decisions.getSideOutput(KEPT_BYTES_TAG).sinkTo(sink);
 * </pre>
 * Buffering full request bytes roughly doubles state size, which is the reason it is left out of
 * this metrics-only build.
 *
 * <h3>State durability caveat</h3>
 * Per-trace buffers live in keyed {@link ListState}/{@link ValueState} on the default
 * {@code HashMapStateBackend} with JobManager-heap checkpoint storage: they survive a
 * TaskManager crash (restored from the last checkpoint) but <b>not</b> a JobManager loss, and a
 * job cancel + resubmit resets everything (new operator UIDs). The "seen services" set is a plain
 * in-operator field and is <b>never</b> checkpointed — after any restart every service looks "rare"
 * again for one trace. For a research/demo workload this is acceptable; see {@code job.md} for the
 * durable-checkpoint upgrade path.
 */
public class OtlpTailSamplerJob {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpTailSamplerJob.class);

    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String TRACES_TOPIC            = "otlp-traces";
    private static final String GROUP_ID                = "flink-otlp-tail-sampler";

    private static final long   LOG_SNAPSHOT_INTERVAL_MS = 60_000L;
    private static final String UNKNOWN                  = "unknown";
    private static final int    MAX_LABEL_VALUE_LEN      = 120;

    /* ---- sampling configuration constants ---- */
    private static final long   DECISION_DELAY_MS    = 10_000L;   // trace completion window
    private static final long   SLOW_THRESHOLD_MS    = 1_000L;    // max span duration ⇒ "slow"
    private static final long   SLOW_THRESHOLD_NANOS = SLOW_THRESHOLD_MS * 1_000_000L;
    private static final int    BASELINE_SAMPLE_PCT  = 5;         // 5% baseline keep (0.05)
    private static final int    SEEN_SERVICES_CAP    = 50_000;    // bounded rare-detection set

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(TRACES_TOPIC)
            .setGroupId(GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
            .build();

        DataStream<SpanFact> facts = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka[traces]")
            .process(new SpanExtractProcess())
            .name("extract-span-facts");

        facts.keyBy((KeySelector<SpanFact, String>) f -> f.traceId)
            .process(new TailSamplerProcess())
            .name("tail-sampler-decision");

        env.execute("OTLP Tail-Based Trace Sampler");
    }

    /* ---------- shared resource-attribute helpers ---------- */

    private static Map<String, String> resourceAttrs(List<KeyValue> kvs) {
        Map<String, String> m = new HashMap<>();
        for (KeyValue kv : kvs) {
            String v = stringValue(kv.getValue());
            if (v != null) m.put(kv.getKey(), v);
        }
        return m;
    }

    private static String stringValue(AnyValue v) {
        switch (v.getValueCase()) {
            case STRING_VALUE: return v.getStringValue();
            case INT_VALUE:    return Long.toString(v.getIntValue());
            case BOOL_VALUE:   return Boolean.toString(v.getBoolValue());
            case DOUBLE_VALUE: return Double.toString(v.getDoubleValue());
            default:           return null;
        }
    }

    private static String attr(Map<String, String> attrs, String key) {
        String v = attrs.get(key);
        return v == null || v.isEmpty() ? UNKNOWN : v;
    }

    private static String safeLabel(String v) {
        if (v == null || v.isEmpty()) return UNKNOWN;
        String t = v.replace('\r', ' ').replace('\n', ' ');
        return t.length() > MAX_LABEL_VALUE_LEN ? t.substring(0, MAX_LABEL_VALUE_LEN) : t;
    }

    private static String hex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return UNKNOWN;
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            int hi = (b >> 4) & 0xF;
            int lo = b & 0xF;
            sb.append(Character.forDigit(hi, 16)).append(Character.forDigit(lo, 16));
        }
        return sb.toString();
    }

    /* ---------- labeled counter (Prometheus-style) ---------- */

    /**
     * Lazy factory over {@link Counter}s keyed by label-value tuple. Each unique tuple
     * becomes its own Flink counter under chained {@link MetricGroup#addGroup(String, String)}
     * scopes, which the Prometheus reporter flattens into labels.
     */
    static final class LabeledCounter {
        private static final char LABEL_SEP = '\u0001';

        private final String metricName;
        private final String[] labelNames;
        private final ConcurrentHashMap<String, Counter> cache = new ConcurrentHashMap<>();
        private MetricGroup root;

        LabeledCounter(String metricName, String... labelNames) {
            this.metricName = metricName;
            this.labelNames = labelNames;
        }

        void bind(MetricGroup root) {
            this.root = root;
        }

        void inc(long n, String... labelValues) {
            if (labelValues.length != labelNames.length) {
                throw new IllegalArgumentException(
                    "label arity mismatch for " + metricName
                        + ": expected " + labelNames.length
                        + " got " + labelValues.length);
            }
            String cacheKey = join(labelValues);
            Counter c = cache.get(cacheKey);
            if (c == null) {
                c = cache.computeIfAbsent(cacheKey, k -> {
                    MetricGroup g = root;
                    for (int i = 0; i < labelNames.length; i++) {
                        g = g.addGroup(labelNames[i], safeLabel(labelValues[i]));
                    }
                    return g.counter(metricName);
                });
            }
            c.inc(n);
        }

        Map<String, Counter> snapshot() {
            return new HashMap<>(cache);
        }

        String metricName() {
            return metricName;
        }

        private static String join(String[] parts) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) sb.append(LABEL_SEP);
                sb.append(parts[i] == null ? UNKNOWN : parts[i]);
            }
            return sb.toString();
        }
    }

    /* ---------- lightweight per-span record ---------- */

    /**
     * One row per span on the keyed stream. Deliberately tiny: just what the scorer needs so the
     * per-trace ListState stays cheap.
     */
    public static class SpanFact {
        public String  traceId;       // hex trace id (the key)
        public String  serviceName;   // service.name from the resource
        public long    durationNanos; // endTimeUnixNano - startTimeUnixNano (clamped >= 0)
        public boolean error;         // status ERROR or carries an "exception" event
        public long    bytesEstimate; // estimated serialized bytes for this span

        public SpanFact() { }

        public SpanFact(String traceId, String serviceName, long durationNanos,
                        boolean error, long bytesEstimate) {
            this.traceId = traceId;
            this.serviceName = serviceName;
            this.durationNanos = durationNanos;
            this.error = error;
            this.bytesEstimate = bytesEstimate;
        }
    }

    /* ---------- span extraction ---------- */

    /**
     * Parses each OTLP trace batch into one {@link SpanFact} per span.
     *
     * <p>Byte attribution: an OTLP batch holds many spans, so we approximate per-span bytes by
     * dividing the Kafka message length evenly across the spans it contains
     * ({@code messageBytes / spanCount}). This is an estimate — span sizes vary — but it conserves
     * total bytes across the kept/dropped split, which is what the savings metric needs.
     */
    public static class SpanExtractProcess extends ProcessFunction<byte[], SpanFact> {

        @Override
        public void processElement(byte[] value, Context ctx, Collector<SpanFact> out) {
            if (value == null) return;
            try {
                ExportTraceServiceRequest req = ExportTraceServiceRequest.parseFrom(value);

                int totalSpans = 0;
                for (ResourceSpans rs : req.getResourceSpansList()) {
                    for (ScopeSpans ss : rs.getScopeSpansList()) {
                        totalSpans += ss.getSpansCount();
                    }
                }
                if (totalSpans == 0) return;
                long perSpanBytes = Math.max(1L, (long) value.length / totalSpans);

                for (ResourceSpans rs : req.getResourceSpansList()) {
                    Resource res = rs.getResource();
                    Map<String, String> attrs = resourceAttrs(res.getAttributesList());
                    String svc = attr(attrs, "service.name");

                    for (ScopeSpans ss : rs.getScopeSpansList()) {
                        for (Span span : ss.getSpansList()) {
                            String traceId = hex(span.getTraceId().toByteArray());
                            long dur = span.getEndTimeUnixNano() - span.getStartTimeUnixNano();
                            if (dur < 0) dur = 0;

                            boolean error =
                                span.getStatus().getCode() == Status.StatusCode.STATUS_CODE_ERROR
                                    || hasExceptionEvent(span);

                            out.collect(new SpanFact(traceId, svc, dur, error, perSpanBytes));
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("[tail-sampler] failed to parse OTLP trace batch: {}", e.toString());
            }
        }

        private static boolean hasExceptionEvent(Span span) {
            for (Span.Event ev : span.getEventsList()) {
                if ("exception".equals(ev.getName())) return true;
            }
            return false;
        }
    }

    /* ---------- tail sampling decision ---------- */

    /**
     * Buffers span facts per trace, then on a processing-time completion-window timer scores the
     * trace and emits a keep/drop decision into the sampler metrics. Keyed by {@code trace_id}.
     */
    public static class TailSamplerProcess
            extends KeyedProcessFunction<String, SpanFact, Void> {

        private static final String DECISION_KEPT    = "kept";
        private static final String DECISION_DROPPED = "dropped";

        // keyed per-trace buffer state
        private transient ListState<SpanFact> spanBuffer;
        private transient ValueState<Long>    timerArmed;   // null => no timer scheduled

        // operator-wide rare-detection set (NOT checkpointed) + running totals for the gauge
        private transient java.util.HashSet<String> seenServices;
        private transient long keptTraces;
        private transient long totalTraces;

        // metrics
        private transient LabeledCounter tracesTotal;  // {decision, reason}
        private transient LabeledCounter spansTotal;   // {decision}
        private transient LabeledCounter bytesTotal;   // {decision}
        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            spanBuffer = getRuntimeContext().getListState(
                new ListStateDescriptor<>("span-buffer", SpanFact.class));
            timerArmed = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer-armed", Long.class));

            seenServices = new java.util.HashSet<>();
            keptTraces = 0L;
            totalTraces = 0L;

            MetricGroup root = getRuntimeContext().getMetricGroup();
            tracesTotal = new LabeledCounter("otlp_tracesampler_traces_total", "decision", "reason");
            tracesTotal.bind(root);
            spansTotal = new LabeledCounter("otlp_tracesampler_spans_total", "decision");
            spansTotal.bind(root);
            bytesTotal = new LabeledCounter("otlp_tracesampler_bytes_total", "decision");
            bytesTotal.bind(root);

            // kept_ratio = kept_traces / total_traces, recomputed on scrape
            root.gauge("otlp_tracesampler_kept_ratio", (Gauge<Double>) () ->
                totalTraces == 0L ? 0.0 : (double) keptTraces / (double) totalTraces);

            nextLogAtMs = System.currentTimeMillis() + LOG_SNAPSHOT_INTERVAL_MS;
        }

        @Override
        public void processElement(SpanFact fact, Context ctx, Collector<Void> out)
                throws Exception {
            spanBuffer.add(fact);
            // Arm a single completion-window timer on the first span of this trace.
            if (timerArmed.value() == null) {
                long fireAt = ctx.timerService().currentProcessingTime() + DECISION_DELAY_MS;
                ctx.timerService().registerProcessingTimeTimer(fireAt);
                timerArmed.update(fireAt);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out)
                throws Exception {
            String traceId = ctx.getCurrentKey();

            long spanCount = 0L;
            long byteSum = 0L;
            long maxDuration = 0L;
            boolean anyError = false;
            String serviceName = UNKNOWN;

            for (SpanFact f : spanBuffer.get()) {
                spanCount++;
                byteSum += f.bytesEstimate;
                if (f.durationNanos > maxDuration) maxDuration = f.durationNanos;
                if (f.error) anyError = true;
                if (f.serviceName != null && !UNKNOWN.equals(f.serviceName)) {
                    serviceName = f.serviceName;
                }
            }

            // Empty trace guard (shouldn't happen — timer is only armed after a span lands).
            if (spanCount == 0) {
                spanBuffer.clear();
                timerArmed.clear();
                return;
            }

            // ----- score (priority order: error > slow > rare > probabilistic) -----
            boolean keep;
            String reason;
            if (anyError) {
                keep = true;  reason = "error";
            } else if (maxDuration > SLOW_THRESHOLD_NANOS) {
                keep = true;  reason = "slow";
            } else if (isRare(serviceName)) {
                keep = true;  reason = "rare";
            } else if ((Math.abs(traceId.hashCode()) % 100) < BASELINE_SAMPLE_PCT) {
                keep = true;  reason = "probabilistic";
            } else {
                keep = false; reason = "boring";
            }

            String decision = keep ? DECISION_KEPT : DECISION_DROPPED;
            tracesTotal.inc(1L, decision, reason);
            spansTotal.inc(spanCount, decision);
            bytesTotal.inc(byteSum, decision);

            totalTraces++;
            if (keep) keptTraces++;

            spanBuffer.clear();
            timerArmed.clear();

            maybeLog();
        }

        /**
         * True the first time a service is observed (and records it), bounded at
         * {@link #SEEN_SERVICES_CAP}. Once the cap is hit, every further service is treated as
         * "not rare" so the set can never grow unbounded.
         */
        private boolean isRare(String serviceName) {
            if (serviceName == null || UNKNOWN.equals(serviceName)) return false;
            if (seenServices.contains(serviceName)) return false;
            if (seenServices.size() >= SEEN_SERVICES_CAP) return false;
            seenServices.add(serviceName);
            return true;
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;

            double ratio = totalTraces == 0L ? 0.0 : (double) keptTraces / (double) totalTraces;
            LOG.info("[tail-sampler] total_traces={} kept={} kept_ratio={} seen_services={}",
                totalTraces, keptTraces, String.format("%.4f", ratio), seenServices.size());
            logTop(tracesTotal);
            logTop(spansTotal);
            logTop(bytesTotal);
        }

        private static void logTop(LabeledCounter lc) {
            Map<String, Counter> s = lc.snapshot();
            if (s.isEmpty()) {
                LOG.info("[tail-sampler] {} = (empty)", lc.metricName());
                return;
            }
            s.entrySet().stream()
                .sorted(Comparator.comparingLong(
                    (Map.Entry<String, Counter> e) -> e.getValue().getCount()).reversed())
                .limit(10)
                .forEach(e -> LOG.info(
                    "[tail-sampler] {} [{}] = {}",
                    lc.metricName(),
                    e.getKey().replace('\u0001', '|'),
                    e.getValue().getCount()));
        }
    }

    /* ---------- deserialization ---------- */

    public static class ByteArrayDeserializationSchema implements DeserializationSchema<byte[]> {
        @Override
        public byte[] deserialize(byte[] message) throws IOException {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return TypeInformation.of(new TypeHint<byte[]>() {});
        }
    }
}
