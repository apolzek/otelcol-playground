package io.nochaos.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Telemetry cost attribution / FinOps chargeback engine over raw OTLP in Kafka.
 *
 * <h3>The pain</h3>
 * Observability bills are enormous and opaque: ingest-priced backends (Datadog,
 * Grafana Cloud, New Relic, Splunk) charge by GB ingested and/or records, but the
 * invoice never tells you <em>who</em> generated the spend. Platform teams get the
 * bill; product teams generate the volume; nobody can do chargeback or hunt waste.
 *
 * <h3>The chargeback model</h3>
 * This job attributes every ingested byte and record back to the org dimensions the
 * load generator stamps on each resource — {@code team}, {@code vertical},
 * {@code service.name}, plus the {@code signal} (traces / logs / metrics) — so the
 * spend can be split and billed back to its owner. To keep Prometheus cardinality
 * sane it deliberately uses a <em>small</em> label set per metric and never crosses
 * the full team × vertical × service product:
 * <ul>
 *   <li>{@code otlp_cost_bytes_total{signal, team, vertical}} — counter</li>
 *   <li>{@code otlp_cost_records_total{signal, team, vertical}} — counter</li>
 *   <li>{@code otlp_cost_bytes_by_service_total{signal, service_name}} — counter
 *       (service dimension kept on its own metric, not crossed with team/vertical)</li>
 *   <li>{@code otlp_cost_estimated_usd} — Gauge&lt;Double&gt; per {@code team}</li>
 *   <li>{@code otlp_cost_waste_debug_logs_total{service_name}} — log records below
 *       INFO severity (severityNumber &lt; 9), a classic prod-budget waste signal</li>
 * </ul>
 *
 * <h3>Byte attribution method</h3>
 * The OTLP envelope (the {@code ExportXxxServiceRequest}) is the Kafka record, but a
 * single envelope can carry many {@code ResourceXxx} blocks owned by different teams.
 * Rather than splitting the envelope size proportionally by record count, this job
 * uses protobuf's {@link com.google.protobuf.AbstractMessage#getSerializedSize()} on
 * each {@code ResourceSpans / ResourceLogs / ResourceMetrics} as the per-resource byte
 * estimate. This is the wire size of just that resource's sub-message — exact for the
 * resource's own bytes, but it does <em>not</em> include the few envelope-framing bytes
 * (field tags + length prefixes) the parent message spends per resource. The error is
 * single-digit bytes per resource, negligible against KB-to-MB payloads, and it buys
 * exact per-owner attribution without proportional fudging. Documented as the tradeoff
 * in {@code job.md}.
 *
 * <h3>Price model (placeholder — TUNE THIS)</h3>
 * {@link #PRICE_PER_GB_INGESTED} = {@code 0.50} USD/GB is a stand-in for a Datadog /
 * Grafana-style ingest price. The gauge {@code otlp_cost_estimated_usd{team}} =
 * {@code cumulative_team_bytes / 1e9 * PRICE_PER_GB_INGESTED}. Note this is
 * <strong>cumulative since job start</strong>, not a monthly rate. To turn it into a
 * spend rate use the byte counter in PromQL instead, e.g. a projected monthly bill:
 * <pre>{@code
 *   sum by (team)(rate(otlp_cost_bytes_total[1h])) / 1e9 * 0.50 * 730
 * }</pre>
 * See {@code job.md} for the full PromQL cookbook.
 *
 * <p>All metrics are exposed via the Flink PrometheusReporter on TaskManager port 9249
 * (prefixed {@code flink_taskmanager_job_task_operator_}). The job only increments
 * counters and updates per-team gauges; rates and projections are computed in PromQL.
 * Every 60s each subtask logs a Top-10 teams-by-bytes + Top-10 services-by-bytes
 * snapshot so spend can be spot-checked from the Flink UI without Prometheus.
 */
public class OtlpCostAttributionJob {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpCostAttributionJob.class);

    private static final String KAFKA_BOOTSTRAP_SERVERS  = "kafka:29092";
    private static final long   LOG_SNAPSHOT_INTERVAL_MS = 60_000L;
    private static final String UNKNOWN                  = "unknown";
    private static final int    MAX_LABEL_VALUE_LEN      = 120;

    /**
     * Placeholder ingest price in USD per GB. Datadog/Grafana-style ingest pricing
     * lands in this ballpark; replace with your contracted rate. Drives the
     * {@code otlp_cost_estimated_usd} gauge.
     */
    private static final double PRICE_PER_GB_INGESTED = 0.50;
    private static final double BYTES_PER_GB          = 1_000_000_000.0;

    /** OTLP SeverityNumber for INFO. Anything below this (DEBUG/TRACE) is waste-candidate. */
    private static final int SEVERITY_INFO = 9;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromSource(
                kafka("otlp-traces", "flink-otlp-cost-traces"),
                WatermarkStrategy.noWatermarks(),
                "Kafka[traces]")
            .process(new TracesCostProcess())
            .name("cost-traces");

        env.fromSource(
                kafka("otlp-logs", "flink-otlp-cost-logs"),
                WatermarkStrategy.noWatermarks(),
                "Kafka[logs]")
            .process(new LogsCostProcess())
            .name("cost-logs");

        env.fromSource(
                kafka("otlp-metrics", "flink-otlp-cost-metrics"),
                WatermarkStrategy.noWatermarks(),
                "Kafka[metrics]")
            .process(new MetricsCostProcess())
            .name("cost-metrics");

        env.execute("OTLP Cost Attribution");
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
        private static final char LABEL_SEP = '';

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

    /**
     * Lazy factory over per-team {@code Gauge<Double>} cost estimates. Each team gets a
     * gauge under a {@code team=<value>} scope; the gauge reads a live {@link AtomicLong}
     * that holds the team's cumulative attributed bytes, converted to USD on scrape.
     */
    static final class TeamCostGauge {
        private final String metricName;
        private final ConcurrentHashMap<String, AtomicLong> teamBytes = new ConcurrentHashMap<>();
        private MetricGroup root;

        TeamCostGauge(String metricName) {
            this.metricName = metricName;
        }

        void bind(MetricGroup root) {
            this.root = root;
        }

        void addBytes(String team, long bytes) {
            String t = safeLabel(team);
            AtomicLong cell = teamBytes.get(t);
            if (cell == null) {
                cell = teamBytes.computeIfAbsent(t, k -> {
                    AtomicLong created = new AtomicLong(0L);
                    final Gauge<Double> g =
                        () -> created.get() / BYTES_PER_GB * PRICE_PER_GB_INGESTED;
                    root.addGroup("team", k).gauge(metricName, g);
                    return created;
                });
            }
            cell.addAndGet(bytes);
        }

        Map<String, Long> bytesSnapshot() {
            Map<String, Long> m = new HashMap<>();
            for (Map.Entry<String, AtomicLong> e : teamBytes.entrySet()) {
                m.put(e.getKey(), e.getValue().get());
            }
            return m;
        }
    }

    /* ---------- shared cost-counter wiring for all three signals ---------- */

    /**
     * Bundle of the chargeback counters/gauges every signal carries. The waste counter
     * is logs-specific and lives on {@link LogsCostProcess} directly.
     */
    static final class CostMeters {
        final LabeledCounter bytesByTeam;       // {signal, team, vertical}
        final LabeledCounter recordsByTeam;     // {signal, team, vertical}
        final LabeledCounter bytesByService;    // {signal, service_name}
        final TeamCostGauge  estimatedUsd;      // {team}

        CostMeters(MetricGroup root, MetricGroup globalRoot, String signal) {
            bytesByTeam    = bind(new LabeledCounter("otlp_cost_bytes_total", "team", "vertical"), root);
            recordsByTeam  = bind(new LabeledCounter("otlp_cost_records_total", "team", "vertical"), root);
            bytesByService = bind(new LabeledCounter("otlp_cost_bytes_by_service_total", "service_name"), root);
            // USD gauge is keyed by team only and shared in intent across signals; each
            // signal subtask maintains its own per-team accumulation (the global root has
            // no signal scope so PromQL can sum across signals per team).
            estimatedUsd = new TeamCostGauge("otlp_cost_estimated_usd");
            estimatedUsd.bind(globalRoot);
        }

        private static LabeledCounter bind(LabeledCounter lc, MetricGroup root) {
            lc.bind(root);
            return lc;
        }

        void recordResource(long records, long bytes, Map<String, String> a) {
            if (records <= 0 && bytes <= 0) return;
            String team     = attr(a, "team");
            String vertical  = attr(a, "vertical");
            String service  = attr(a, "service.name");
            bytesByTeam.inc(bytes, team, vertical);
            recordsByTeam.inc(records, team, vertical);
            bytesByService.inc(bytes, service);
            estimatedUsd.addBytes(team, bytes);
        }
    }

    private static void logSnapshot(String signal, CostMeters m) {
        long totalBytes = sumCounts(m.bytesByTeam);
        long totalRecords = sumCounts(m.recordsByTeam);
        LOG.info("[cost-{}] total_bytes={} total_records={} teams={} services={}",
            signal, totalBytes, totalRecords,
            m.bytesByTeam.snapshot().size(), m.bytesByService.snapshot().size());
        logTopTeamsByDollars(signal, m.estimatedUsd);
        logTopServices(signal, m.bytesByService);
    }

    private static long sumCounts(LabeledCounter lc) {
        long sum = 0L;
        for (Counter c : lc.snapshot().values()) sum += c.getCount();
        return sum;
    }

    private static void logTopTeamsByDollars(String signal, TeamCostGauge g) {
        Map<String, Long> s = g.bytesSnapshot();
        if (s.isEmpty()) {
            LOG.info("[cost-{}] otlp_cost_estimated_usd = (empty)", signal);
            return;
        }
        s.entrySet().stream()
            .sorted(Comparator.comparingLong((Map.Entry<String, Long> e) -> e.getValue()).reversed())
            .limit(10)
            .forEach(e -> LOG.info(
                "[cost-{}] top-team [{}] bytes={} est_usd={}",
                signal, e.getKey(), e.getValue(),
                String.format("%.4f", e.getValue() / BYTES_PER_GB * PRICE_PER_GB_INGESTED)));
    }

    private static void logTopServices(String signal, LabeledCounter lc) {
        Map<String, Counter> s = lc.snapshot();
        if (s.isEmpty()) {
            LOG.info("[cost-{}] {} = (empty)", signal, lc.metricName());
            return;
        }
        s.entrySet().stream()
            .sorted(Comparator.comparingLong((Map.Entry<String, Counter> e) -> e.getValue().getCount()).reversed())
            .limit(10)
            .forEach(e -> LOG.info(
                "[cost-{}] top-service {} [{}] bytes={}",
                signal, lc.metricName(),
                e.getKey().replace('', '|'),
                e.getValue().getCount()));
    }

    /* ---------- traces ---------- */

    public static class TracesCostProcess extends ProcessFunction<byte[], Void> {
        private static final String SIGNAL = "traces";
        private transient CostMeters m;
        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            MetricGroup global = getRuntimeContext().getMetricGroup();
            MetricGroup root = global.addGroup("signal", SIGNAL);
            m = new CostMeters(root, global, SIGNAL);
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
                    long spans = 0L;
                    for (ScopeSpans ss : rs.getScopeSpansList()) {
                        spans += ss.getSpansCount();
                    }
                    long bytes = rs.getSerializedSize();
                    m.recordResource(spans, bytes, attrs);
                }
                maybeLog();
            } catch (Exception e) {
                LOG.warn("[cost-traces] failed to parse OTLP batch: {}", e.toString());
            }
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;
            logSnapshot(SIGNAL, m);
        }
    }

    /* ---------- logs ---------- */

    public static class LogsCostProcess extends ProcessFunction<byte[], Void> {
        private static final String SIGNAL = "logs";
        private transient CostMeters m;
        private transient LabeledCounter wasteDebugLogs;   // {service_name}
        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            MetricGroup global = getRuntimeContext().getMetricGroup();
            MetricGroup root = global.addGroup("signal", SIGNAL);
            m = new CostMeters(root, global, SIGNAL);
            wasteDebugLogs = new LabeledCounter("otlp_cost_waste_debug_logs_total", "service_name");
            wasteDebugLogs.bind(root);
            nextLogAtMs = System.currentTimeMillis() + LOG_SNAPSHOT_INTERVAL_MS;
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Void> out) {
            if (value == null) return;
            try {
                ExportLogsServiceRequest req = ExportLogsServiceRequest.parseFrom(value);
                for (ResourceLogs rl : req.getResourceLogsList()) {
                    Map<String, String> attrs = resourceAttrs(rl.getResource().getAttributesList());
                    String svc = attr(attrs, "service.name");
                    long records = 0L;
                    long debugRecords = 0L;
                    for (ScopeLogs sl : rl.getScopeLogsList()) {
                        for (LogRecord lr : sl.getLogRecordsList()) {
                            records++;
                            int sev = lr.getSeverityNumberValue();
                            if (sev > 0 && sev < SEVERITY_INFO) {
                                debugRecords++;
                            }
                        }
                    }
                    long bytes = rl.getSerializedSize();
                    m.recordResource(records, bytes, attrs);
                    if (debugRecords > 0) wasteDebugLogs.inc(debugRecords, svc);
                }
                maybeLog();
            } catch (Exception e) {
                LOG.warn("[cost-logs] failed to parse OTLP batch: {}", e.toString());
            }
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;
            logSnapshot(SIGNAL, m);
            logTopServices(SIGNAL, wasteDebugLogs);
        }
    }

    /* ---------- metrics ---------- */

    public static class MetricsCostProcess extends ProcessFunction<byte[], Void> {
        private static final String SIGNAL = "metrics";
        private transient CostMeters m;
        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            MetricGroup global = getRuntimeContext().getMetricGroup();
            MetricGroup root = global.addGroup("signal", SIGNAL);
            m = new CostMeters(root, global, SIGNAL);
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
                        for (Metric mt : sm.getMetricsList()) {
                            points += dataPointCount(mt);
                        }
                    }
                    long bytes = rm.getSerializedSize();
                    m.recordResource(points, bytes, attrs);
                }
                maybeLog();
            } catch (Exception e) {
                LOG.warn("[cost-metrics] failed to parse OTLP batch: {}", e.toString());
            }
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;
            logSnapshot(SIGNAL, m);
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
