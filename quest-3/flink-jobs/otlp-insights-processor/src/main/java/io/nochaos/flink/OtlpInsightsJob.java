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

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
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
 * Derives insight-grade Prometheus counters from OTLP traffic landing in Kafka.
 *
 * <p>Counters exposed through the Flink PrometheusReporter on TaskManager port 9249
 * (prefixed {@code flink_taskmanager_job_task_operator_}). Rates are computed by
 * Prometheus / Grafana; this job only ever increments counters — never gauges.
 *
 * <h3>Signals produced</h3>
 * Shared across {@code signal in {traces, logs, metrics}}:
 * <ul>
 *   <li>{@code otlp_signal_records_total} — spans, log records, or metric data points</li>
 *   <li>{@code otlp_signal_records_by_service_total{service_name}}</li>
 *   <li>{@code otlp_signal_records_by_transport_total{transport}}
 *       — grpc / http/protobuf / http/json — sourced from {@code otlp.transport} the L1
 *       collector stamps on each resource</li>
 *   <li>{@code otlp_signal_records_by_sdk_language_total{sdk_language}}</li>
 *   <li>{@code otlp_signal_records_by_sdk_total{sdk_name, sdk_version}}</li>
 *   <li>{@code otlp_signal_records_by_cloud_total{cloud_provider}}</li>
 *   <li>{@code otlp_signal_records_by_k8s_total{k8s_cluster_name, k8s_namespace_name}}</li>
 *   <li>{@code otlp_signal_records_by_environment_total{deployment_environment}}</li>
 * </ul>
 * Traces-only:
 * <ul>
 *   <li>{@code otlp_spans_errors_total{service_name}} — spans with {@code status.code=ERROR(2)}</li>
 *   <li>{@code otlp_spans_with_exceptions_total{service_name, exception_type}} — spans
 *       that carry a {@code name=exception} event, bucketed by {@code exception.type}</li>
 * </ul>
 *
 * <p>Every 60s each subtask writes a Top-10 snapshot to the task log so the insights
 * can be spot-checked from the Flink UI without hitting Prometheus.
 */
public class OtlpInsightsJob {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpInsightsJob.class);

    private static final String   KAFKA_BOOTSTRAP_SERVERS   = "kafka:29092";
    private static final long     LOG_SNAPSHOT_INTERVAL_MS  = 60_000L;
    private static final String   UNKNOWN                   = "unknown";
    private static final int      MAX_LABEL_VALUE_LEN       = 120;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromSource(
                kafka("otlp-traces", "flink-otlp-insights-traces"),
                WatermarkStrategy.noWatermarks(),
                "Kafka[traces]")
            .process(new TracesInsightsProcess())
            .name("insights-traces");

        env.fromSource(
                kafka("otlp-logs", "flink-otlp-insights-logs"),
                WatermarkStrategy.noWatermarks(),
                "Kafka[logs]")
            .process(new LogsInsightsProcess())
            .name("insights-logs");

        env.fromSource(
                kafka("otlp-metrics", "flink-otlp-insights-metrics"),
                WatermarkStrategy.noWatermarks(),
                "Kafka[metrics]")
            .process(new MetricsInsightsProcess())
            .name("insights-metrics");

        env.execute("OTLP Insights Processor");
    }

    private static KafkaSource<byte[]> kafka(String topic, String groupId) {
        return KafkaSource.<byte[]>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
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

    /* ---------- common counter wiring for all three signals ---------- */

    /**
     * Bundle of the labeled counters every signal carries. Trace-specific counters
     * sit on {@link TracesInsightsProcess} directly.
     */
    static final class InsightsCounters {
        final Counter total;
        final LabeledCounter byService;
        final LabeledCounter byTransport;
        final LabeledCounter byLanguage;
        final LabeledCounter bySdk;
        final LabeledCounter byCloud;
        final LabeledCounter byK8s;
        final LabeledCounter byEnvironment;

        InsightsCounters(MetricGroup root) {
            total       = root.counter("otlp_signal_records_total");
            byService   = bind(new LabeledCounter("otlp_signal_records_by_service_total", "service_name"), root);
            byTransport = bind(new LabeledCounter("otlp_signal_records_by_transport_total", "transport"), root);
            byLanguage  = bind(new LabeledCounter("otlp_signal_records_by_sdk_language_total", "sdk_language"), root);
            bySdk       = bind(new LabeledCounter("otlp_signal_records_by_sdk_total", "sdk_name", "sdk_version"), root);
            byCloud     = bind(new LabeledCounter("otlp_signal_records_by_cloud_total", "cloud_provider"), root);
            byK8s       = bind(new LabeledCounter("otlp_signal_records_by_k8s_total", "k8s_cluster_name", "k8s_namespace_name"), root);
            byEnvironment = bind(new LabeledCounter("otlp_signal_records_by_environment_total", "deployment_environment"), root);
        }

        private static LabeledCounter bind(LabeledCounter lc, MetricGroup root) {
            lc.bind(root);
            return lc;
        }

        void recordResource(long records, Map<String, String> a) {
            if (records <= 0) return;
            total.inc(records);
            byService.inc(records, attr(a, "service.name"));
            byTransport.inc(records, attr(a, "otlp.transport"));
            byLanguage.inc(records, attr(a, "telemetry.sdk.language"));
            bySdk.inc(records, attr(a, "telemetry.sdk.name"), attr(a, "telemetry.sdk.version"));
            byCloud.inc(records, attr(a, "cloud.provider"));
            byK8s.inc(records, attr(a, "k8s.cluster.name"), attr(a, "k8s.namespace.name"));
            byEnvironment.inc(records, attr(a, "deployment.environment"));
        }
    }

    private static void logSnapshot(String signal, InsightsCounters c, LabeledCounter... extras) {
        LOG.info("[insights-{}] total_records={} services={} transports={} languages={} envs={}",
            signal,
            c.total.getCount(),
            c.byService.snapshot().size(),
            c.byTransport.snapshot().size(),
            c.byLanguage.snapshot().size(),
            c.byEnvironment.snapshot().size());
        logTop(signal, c.byService);
        logTop(signal, c.byTransport);
        logTop(signal, c.byLanguage);
        logTop(signal, c.bySdk);
        logTop(signal, c.byCloud);
        logTop(signal, c.byK8s);
        logTop(signal, c.byEnvironment);
        for (LabeledCounter extra : extras) {
            logTop(signal, extra);
        }
    }

    private static void logTop(String signal, LabeledCounter lc) {
        Map<String, Counter> s = lc.snapshot();
        if (s.isEmpty()) {
            LOG.info("[insights-{}] {} = (empty)", signal, lc.metricName());
            return;
        }
        s.entrySet().stream()
            .sorted(Comparator.comparingLong((Map.Entry<String, Counter> e) -> e.getValue().getCount()).reversed())
            .limit(10)
            .forEach(e -> LOG.info(
                "[insights-{}] {} [{}] = {}",
                signal, lc.metricName(),
                e.getKey().replace('\u0001', '|'),
                e.getValue().getCount()));
    }

    /* ---------- traces ---------- */

    public static class TracesInsightsProcess extends ProcessFunction<byte[], Void> {
        private static final String SIGNAL = "traces";

        private transient InsightsCounters c;
        private transient LabeledCounter errorSpans;
        private transient LabeledCounter exceptionSpans;
        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            MetricGroup root = getRuntimeContext().getMetricGroup().addGroup("signal", SIGNAL);
            c = new InsightsCounters(root);
            errorSpans = new LabeledCounter("otlp_spans_errors_total", "service_name");
            errorSpans.bind(root);
            exceptionSpans = new LabeledCounter("otlp_spans_with_exceptions_total", "service_name", "exception_type");
            exceptionSpans.bind(root);
            nextLogAtMs = System.currentTimeMillis() + LOG_SNAPSHOT_INTERVAL_MS;
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

                    long spans = 0L;
                    long errors = 0L;
                    Map<String, Long> exceptionsByType = null;

                    for (ScopeSpans ss : rs.getScopeSpansList()) {
                        for (Span span : ss.getSpansList()) {
                            spans++;
                            if (span.getStatus().getCode() == Status.StatusCode.STATUS_CODE_ERROR) {
                                errors++;
                            }
                            for (Span.Event ev : span.getEventsList()) {
                                if (!"exception".equals(ev.getName())) continue;
                                String type = UNKNOWN;
                                for (KeyValue kv : ev.getAttributesList()) {
                                    if ("exception.type".equals(kv.getKey())) {
                                        String t = stringValue(kv.getValue());
                                        if (t != null && !t.isEmpty()) type = t;
                                        break;
                                    }
                                }
                                if (exceptionsByType == null) exceptionsByType = new HashMap<>();
                                exceptionsByType.merge(type, 1L, Long::sum);
                            }
                        }
                    }

                    if (spans == 0) continue;
                    c.recordResource(spans, attrs);
                    if (errors > 0) errorSpans.inc(errors, svc);
                    if (exceptionsByType != null) {
                        for (Map.Entry<String, Long> e : exceptionsByType.entrySet()) {
                            exceptionSpans.inc(e.getValue(), svc, e.getKey());
                        }
                    }
                }
                maybeLog();
            } catch (Exception e) {
                LOG.warn("[insights-traces] failed to parse OTLP batch: {}", e.toString());
            }
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;
            logSnapshot(SIGNAL, c, errorSpans, exceptionSpans);
        }
    }

    /* ---------- logs ---------- */

    public static class LogsInsightsProcess extends ProcessFunction<byte[], Void> {
        private static final String SIGNAL = "logs";
        private transient InsightsCounters c;
        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            MetricGroup root = getRuntimeContext().getMetricGroup().addGroup("signal", SIGNAL);
            c = new InsightsCounters(root);
            nextLogAtMs = System.currentTimeMillis() + LOG_SNAPSHOT_INTERVAL_MS;
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Void> out) {
            if (value == null) return;
            try {
                ExportLogsServiceRequest req = ExportLogsServiceRequest.parseFrom(value);
                for (ResourceLogs rl : req.getResourceLogsList()) {
                    Map<String, String> attrs = resourceAttrs(rl.getResource().getAttributesList());
                    long records = 0L;
                    for (ScopeLogs sl : rl.getScopeLogsList()) {
                        records += sl.getLogRecordsCount();
                    }
                    c.recordResource(records, attrs);
                }
                maybeLog();
            } catch (Exception e) {
                LOG.warn("[insights-logs] failed to parse OTLP batch: {}", e.toString());
            }
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;
            logSnapshot(SIGNAL, c);
        }
    }

    /* ---------- metrics ---------- */

    public static class MetricsInsightsProcess extends ProcessFunction<byte[], Void> {
        private static final String SIGNAL = "metrics";
        private transient InsightsCounters c;
        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            MetricGroup root = getRuntimeContext().getMetricGroup().addGroup("signal", SIGNAL);
            c = new InsightsCounters(root);
            nextLogAtMs = System.currentTimeMillis() + LOG_SNAPSHOT_INTERVAL_MS;
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Void> out) {
            if (value == null) return;
            try {
                ExportMetricsServiceRequest req = ExportMetricsServiceRequest.parseFrom(value);
                for (ResourceMetrics rm : req.getResourceMetricsList()) {
                    Map<String, String> attrs = resourceAttrs(rm.getResource().getAttributesList());
                    long points = 0L;
                    for (ScopeMetrics sm : rm.getScopeMetricsList()) {
                        for (Metric m : sm.getMetricsList()) {
                            points += dataPointCount(m);
                        }
                    }
                    c.recordResource(points, attrs);
                }
                maybeLog();
            } catch (Exception e) {
                LOG.warn("[insights-metrics] failed to parse OTLP batch: {}", e.toString());
            }
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;
            logSnapshot(SIGNAL, c);
        }
    }

    private static long dataPointCount(Metric m) {
        switch (m.getDataCase()) {
            case GAUGE:                 return m.getGauge().getDataPointsCount();
            case SUM:                   return m.getSum().getDataPointsCount();
            case HISTOGRAM:             return m.getHistogram().getDataPointsCount();
            case EXPONENTIAL_HISTOGRAM: return m.getExponentialHistogram().getDataPointsCount();
            case SUMMARY:               return m.getSummary().getDataPointsCount();
            default:                    return 0L;
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
