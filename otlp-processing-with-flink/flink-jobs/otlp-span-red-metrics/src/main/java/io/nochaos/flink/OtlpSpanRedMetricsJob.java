package io.nochaos.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Flink-native "spanmetrics connector": derives Prometheus RED metrics from raw OTLP spans
 * landing in Kafka, so the raw traces themselves become disposable.
 *
 * <h3>Why RED, and why this lets you drop raw traces</h3>
 * RED is the golden-signal triad for request-driven services:
 * <ul>
 *   <li><b>R</b>ate — how many requests per second (span throughput)</li>
 *   <li><b>E</b>rrors — how many of them failed ({@code status.code == ERROR})</li>
 *   <li><b>D</b>uration — the latency distribution (span end-minus-start)</li>
 * </ul>
 * Storing every raw span purely to power latency/error dashboards and SLO burn-rate alerts is
 * wasteful: a trace store (Tempo/Jaeger/etc.) is the most expensive tier in the stack, yet 99%
 * of dashboard and alerting queries only ever need the aggregate golden signals, not individual
 * traces. By pre-aggregating spans into RED metrics <i>at ingest</i>, you can aggressively
 * downsample or drop the raw traces (keep, say, 1% tail-sampled exemplars for debugging) while
 * the dashboards, SLOs and error budgets keep reading the full-fidelity metrics. Metrics are
 * orders of magnitude cheaper to store and query than spans.
 *
 * <h3>The cumulative-bucket histogram encoding (this is the load-bearing part)</h3>
 * Latency is modelled as a real Prometheus histogram so {@code histogram_quantile()} works
 * unchanged. A Prometheus histogram is a set of <b>cumulative</b> counters: each
 * {@code _bucket{le="X"}} counts every observation whose value is <i>&le; X</i>. Therefore for
 * each span of duration {@code d} we increment <b>every</b> bucket whose boundary {@code le >= d},
 * including the catch-all {@code le="+Inf"}. The buckets are nested ("less-than-or-equal"), so
 * by construction {@code bucket[le=0.005] <= bucket[le=0.01] <= ... <= bucket[le=+Inf]}, and
 * {@code +Inf == _count}.
 *
 * <p>Worked example — a single 30 ms span ({@code d = 0.030}):
 * <pre>
 *   le="0.005"  +0     (0.005 &lt; 0.030)   le="0.25"  +1
 *   le="0.01"   +0     (0.01  &lt; 0.030)   le="0.5"   +1
 *   le="0.025"  +0     (0.025 &lt; 0.030)   le="1"     +1
 *   le="0.05"   +1     (0.05  &gt;= 0.030)  le="2.5"   +1
 *   le="0.1"    +1                          le="5"     +1
 *                                           le="10"    +1
 *                                           le="+Inf"  +1   (== _count)
 * </pre>
 * {@code histogram_quantile(0.95, ...)} later picks the bucket where the cumulative count crosses
 * the 95th-percentile rank and linearly interpolates within that {@code le} band — which only
 * works because the buckets are cumulative, exactly as encoded here.
 *
 * <h3>Counters exposed</h3>
 * All via the Flink PrometheusReporter on TaskManager port 9249 (prefixed
 * {@code flink_taskmanager_job_task_operator_}). This job only ever increments counters.
 * <ul>
 *   <li>{@code otlp_red_requests_total{service_name, span_name, span_kind, status_code}}
 *       — request rate, {@code status_code in {ok, error, unset}}</li>
 *   <li>{@code otlp_red_errors_total{service_name, span_name}} — error count
 *       (spans with {@code status.code == ERROR})</li>
 *   <li>{@code otlp_red_duration_seconds_bucket{service_name, span_name, le}}
 *       — CUMULATIVE latency histogram buckets</li>
 *   <li>{@code otlp_red_duration_seconds_count{service_name, span_name}}
 *       — total spans (identical to the {@code le="+Inf"} bucket)</li>
 *   <li>{@code otlp_red_duration_micros_sum{service_name, span_name}}
 *       — sum of durations in MICROSECONDS (Flink counters are {@code long}, so the float
 *       seconds sum is accumulated as {@code Math.round(d * 1e6)} micros; divide by 1e6 in
 *       PromQL to recover seconds)</li>
 * </ul>
 *
 * <h3>Cardinality guard</h3>
 * Distinct {@code (service_name, span_name)} pairs are capped at {@link #MAX_SERIES}=2000. Once
 * the cap is hit, any new pair has its {@code span_name} folded into {@code "__overflow__"} so a
 * runaway high-cardinality span name (e.g. an un-templated URL with an ID in it) cannot blow up
 * the TSDB. Already-admitted pairs keep their real name.
 *
 * <p>Every 60s each subtask writes a Top-10-endpoints-by-request-count snapshot (with per-endpoint
 * error rate) to the task log so the signals can be spot-checked from the Flink UI without hitting
 * Prometheus.
 */
public class OtlpSpanRedMetricsJob {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpSpanRedMetricsJob.class);

    private static final String KAFKA_BOOTSTRAP_SERVERS  = "kafka:29092";
    private static final long   LOG_SNAPSHOT_INTERVAL_MS = 60_000L;
    private static final String UNKNOWN                  = "unknown";
    private static final int    MAX_LABEL_VALUE_LEN      = 120;

    /** Cap on distinct (service_name, span_name) pairs before span_name folds to __overflow__. */
    private static final int    MAX_SERIES               = 2000;
    private static final String OVERFLOW                 = "__overflow__";

    /**
     * Prometheus histogram bucket boundaries in seconds (excluding the implicit {@code +Inf}).
     * These are the Prometheus client-library default latency buckets. Each span increments every
     * bucket whose {@code le >= duration}.
     */
    private static final double[] BUCKET_BOUNDS_S = {
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
    };

    /** {@code le} label values, Prometheus-formatted, parallel to {@link #BUCKET_BOUNDS_S} plus +Inf. */
    private static final String[] BUCKET_LE_LABELS = buildLeLabels(BUCKET_BOUNDS_S);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromSource(
                kafka("otlp-traces", "flink-otlp-span-red-metrics"),
                WatermarkStrategy.noWatermarks(),
                "Kafka[traces]")
            .process(new SpanRedMetricsProcess())
            .name("span-red-metrics");

        env.execute("OTLP Span RED Metrics");
    }

    private static KafkaSource<byte[]> kafka(String topic, String groupId) {
        return KafkaSource.<byte[]>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
            .build();
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

    /* ---------- span -> RED metrics ---------- */

    public static class SpanRedMetricsProcess extends ProcessFunction<byte[], Void> {

        private transient LabeledCounter requests;     // otlp_red_requests_total
        private transient LabeledCounter errors;        // otlp_red_errors_total
        private transient LabeledCounter durationBucket;// otlp_red_duration_seconds_bucket
        private transient LabeledCounter durationCount;  // otlp_red_duration_seconds_count
        private transient LabeledCounter durationMicrosSum; // otlp_red_duration_micros_sum

        /** Admitted (service_name, span_name) pairs — the cardinality-guard whitelist. */
        private transient Set<String> admittedSeries;
        /** Per-endpoint request/error tallies, for the 60s log snapshot only. */
        private transient Map<String, long[]> endpointTally; // key -> [requests, errors]

        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            MetricGroup root = getRuntimeContext().getMetricGroup().addGroup("signal", "traces");
            requests = bind(new LabeledCounter(
                "otlp_red_requests_total", "service_name", "span_name", "span_kind", "status_code"), root);
            errors = bind(new LabeledCounter(
                "otlp_red_errors_total", "service_name", "span_name"), root);
            durationBucket = bind(new LabeledCounter(
                "otlp_red_duration_seconds_bucket", "service_name", "span_name", "le"), root);
            durationCount = bind(new LabeledCounter(
                "otlp_red_duration_seconds_count", "service_name", "span_name"), root);
            durationMicrosSum = bind(new LabeledCounter(
                "otlp_red_duration_micros_sum", "service_name", "span_name"), root);

            admittedSeries = new HashSet<>();
            endpointTally = new HashMap<>();
            nextLogAtMs = System.currentTimeMillis() + LOG_SNAPSHOT_INTERVAL_MS;
        }

        private static LabeledCounter bind(LabeledCounter lc, MetricGroup root) {
            lc.bind(root);
            return lc;
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Void> out) {
            if (value == null) return;
            try {
                ExportTraceServiceRequest req = ExportTraceServiceRequest.parseFrom(value);
                for (ResourceSpans rs : req.getResourceSpansList()) {
                    Resource res = rs.getResource();
                    Map<String, String> attrs = resourceAttrs(res.getAttributesList());
                    String svc = attr(attrs, "service.name");

                    for (ScopeSpans ss : rs.getScopeSpansList()) {
                        for (Span span : ss.getSpansList()) {
                            recordSpan(svc, span);
                        }
                    }
                }
                maybeLog();
            } catch (Exception e) {
                LOG.warn("[span-red] failed to parse OTLP batch: {}", e.toString());
            }
        }

        private void recordSpan(String svc, Span span) {
            // Cardinality guard: cap distinct (service, span_name) pairs; fold overflow.
            String rawName = span.getName();
            String spanName = guardSpanName(svc, rawName);

            String kind = spanKind(span.getKind());
            String statusCode = statusCode(span.getStatus());
            boolean isError = span.getStatus().getCode() == Status.StatusCode.STATUS_CODE_ERROR;

            // R — request rate (also split by kind/status for richer slicing).
            requests.inc(1, svc, spanName, kind, statusCode);

            // E — errors.
            if (isError) {
                errors.inc(1, svc, spanName);
            }

            // D — duration. OTLP times are unsigned nanos since epoch.
            long durNanos = span.getEndTimeUnixNano() - span.getStartTimeUnixNano();
            if (durNanos < 0) durNanos = 0; // clock skew / clamped negative spans
            double durSeconds = durNanos / 1e9;

            // Cumulative histogram: increment every bucket whose le >= duration, then +Inf.
            for (int i = 0; i < BUCKET_BOUNDS_S.length; i++) {
                if (durSeconds <= BUCKET_BOUNDS_S[i]) {
                    durationBucket.inc(1, svc, spanName, BUCKET_LE_LABELS[i]);
                }
            }
            durationBucket.inc(1, svc, spanName, BUCKET_LE_LABELS[BUCKET_LE_LABELS.length - 1]); // +Inf
            durationCount.inc(1, svc, spanName);
            durationMicrosSum.inc(Math.round(durSeconds * 1e6), svc, spanName);

            // Snapshot tally (per endpoint) for the periodic log.
            String key = svc + '\u0001' + spanName;
            long[] t = endpointTally.computeIfAbsent(key, k -> new long[2]);
            t[0]++;
            if (isError) t[1]++;
        }

        /**
         * Admit the pair if we are under the cap or it is already known; otherwise fold the
         * span_name into {@code __overflow__}. The overflow pair itself counts as one series.
         */
        private String guardSpanName(String svc, String rawName) {
            String name = (rawName == null || rawName.isEmpty()) ? UNKNOWN : rawName;
            String key = svc + '\u0001' + name;
            if (admittedSeries.contains(key)) {
                return name;
            }
            if (admittedSeries.size() < MAX_SERIES) {
                admittedSeries.add(key);
                return name;
            }
            // Cap reached: fold to overflow (and remember the overflow pair so it stays cheap).
            admittedSeries.add(svc + '\u0001' + OVERFLOW);
            return OVERFLOW;
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;
            logTopEndpoints();
        }

        private void logTopEndpoints() {
            LOG.info("[span-red] distinct_series={} (cap={})", admittedSeries.size(), MAX_SERIES);
            if (endpointTally.isEmpty()) {
                LOG.info("[span-red] top-10 endpoints = (empty)");
                return;
            }
            endpointTally.entrySet().stream()
                .sorted(Comparator.comparingLong((Map.Entry<String, long[]> e) -> e.getValue()[0]).reversed())
                .limit(10)
                .forEach(e -> {
                    long reqs = e.getValue()[0];
                    long errs = e.getValue()[1];
                    double errRate = reqs == 0 ? 0.0 : (100.0 * errs / reqs);
                    LOG.info("[span-red] {} requests={} errors={} error_rate={}%",
                        e.getKey().replace('\u0001', '|'), reqs, errs, String.format("%.2f", errRate));
                });
        }
    }

    /* ---------- span field mapping ---------- */

    private static String spanKind(Span.SpanKind kind) {
        switch (kind) {
            case SPAN_KIND_INTERNAL: return "internal";
            case SPAN_KIND_SERVER:   return "server";
            case SPAN_KIND_CLIENT:   return "client";
            case SPAN_KIND_PRODUCER: return "producer";
            case SPAN_KIND_CONSUMER: return "consumer";
            default:                 return "unspecified";
        }
    }

    private static String statusCode(Status status) {
        switch (status.getCode()) {
            case STATUS_CODE_OK:    return "ok";
            case STATUS_CODE_ERROR: return "error";
            default:                return "unset";
        }
    }

    /** Build Prometheus-formatted {@code le} labels (e.g. "0.005", "10", "+Inf"). */
    private static String[] buildLeLabels(double[] bounds) {
        String[] labels = new String[bounds.length + 1];
        for (int i = 0; i < bounds.length; i++) {
            labels[i] = formatLe(bounds[i]);
        }
        labels[bounds.length] = "+Inf";
        return labels;
    }

    /** Format a bucket boundary like Prometheus does: integral values lose their ".0". */
    private static String formatLe(double v) {
        if (v == Math.rint(v) && !Double.isInfinite(v)) {
            return Long.toString((long) v);
        }
        // Trim a trailing zero from values like 0.50 -> 0.5; the chosen bounds are already clean.
        String s = Double.toString(v);
        return s;
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
