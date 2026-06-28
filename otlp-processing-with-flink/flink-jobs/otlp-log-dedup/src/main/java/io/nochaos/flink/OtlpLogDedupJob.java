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
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Log storm / flood detector and deduplication engine over the {@code otlp-logs} Kafka topic.
 *
 * <h3>The high-volume pain</h3>
 * A single bad deploy can emit the <em>same</em> ERROR log line millions of times — a "log
 * storm". The bodies differ only in volatile tokens (timestamps, request IDs, IPs, counters)
 * so to a human they are one message repeated, but to the logging backend they are millions
 * of distinct events: ingestion cost explodes, indices bloat, and the signal drowns in noise.
 *
 * <h3>The technique: Drain-style fingerprinting</h3>
 * Each log {@code body} string is normalized into a <em>template</em> by masking the parts
 * that vary between otherwise-identical messages:
 * <ul>
 *   <li>runs of digits        → {@code <NUM>}</li>
 *   <li>hex / UUID-ish tokens → {@code <HEX>}</li>
 *   <li>dotted IPv4 tokens    → {@code <IP>}</li>
 *   <li>quoted substrings     → {@code <STR>}</li>
 *   <li>whitespace            → collapsed to single spaces</li>
 * </ul>
 * The template's {@code String.hashCode()} (rendered with {@link Integer#toHexString}) gives a
 * stable, compact {@code template_hash}. This is the same masking idea behind the IBM "Drain"
 * online log parser and the OpenTelemetry Collector community's
 * <em>log deduplication processor</em> (the {@code logdedupprocessor}), which collapses
 * consecutive identical log records into one record bearing an occurrence count. Here we do not
 * drop records — we only <em>measure</em> the repetition so teams can quantify the waste and
 * tune their emitters or enable dedup at the collector.
 *
 * <h3>Metrics exposed</h3>
 * Through the Flink PrometheusReporter on TaskManager port 9249
 * (prefixed {@code flink_taskmanager_job_task_operator_}):
 * <ul>
 *   <li>{@code otlp_logdedup_records_total} — counter, every log record seen</li>
 *   <li>{@code otlp_logdedup_unique_templates} — gauge, distinct template count</li>
 *   <li>{@code otlp_logdedup_dedup_ratio} — gauge, {@code 1 - unique/total} (fraction that is repetition)</li>
 *   <li>{@code otlp_logdedup_template_occurrences_total{service_name, severity, template_hash}}
 *       — counter per template, <b>cardinality-guarded</b> (see below)</li>
 *   <li>{@code otlp_logdedup_flood_events_total{service_name}} — counter, fired when a template
 *       exceeds {@code FLOOD_THRESHOLD} occurrences inside a rolling {@code FLOOD_WINDOW_MS}</li>
 *   <li>{@code otlp_logdedup_bytes_saved_total} — counter, estimated bytes that perfect dedup
 *       would have saved = sum over templates of {@code (occurrences-1) * avg_body_bytes}</li>
 * </ul>
 *
 * <h3>Cardinality guard (the key tradeoff)</h3>
 * A naive {@code template_occurrences_total{template_hash}} would create one Prometheus series
 * per distinct template — and floods/garbage frequently produce a long tail of near-unique
 * templates, which is exactly the cardinality blow-up we are trying to detect, not cause. So a
 * labeled series is created <b>only once a template's cumulative occurrence count crosses
 * {@code MIN_OCCURRENCES_FOR_SERIES} (=100)</b>. Rare templates are still counted in the global
 * {@code records_total} / {@code unique_templates} / {@code bytes_saved} aggregates; they simply
 * never earn their own labeled time series. This bounds emitted cardinality to "templates that
 * are actually noisy", which is the population an operator cares about, at the cost of not being
 * able to chart low-volume templates individually. The in-memory template map itself is capped
 * at {@code MAX_TEMPLATES} (=20 000); templates beyond the cap are tallied in a single
 * {@code overflow} bucket so the JVM heap stays bounded under a pathological flood.
 *
 * <p>Every 60s each subtask writes a Top-10 noisiest-templates snapshot to the task log so the
 * storm can be eyeballed from the Flink UI without hitting Prometheus.
 *
 * <p>Note on time: the rolling flood window uses {@link System#currentTimeMillis()}. This is
 * standard and correct for a real streaming job — wall-clock time-bucketing is exactly how you
 * detect "N events in the last minute".
 */
public class OtlpLogDedupJob {

    private static final Logger LOG = LoggerFactory.getLogger(OtlpLogDedupJob.class);

    private static final String   KAFKA_BOOTSTRAP_SERVERS    = "kafka:29092";
    private static final long     LOG_SNAPSHOT_INTERVAL_MS   = 60_000L;
    private static final String   UNKNOWN                    = "unknown";
    private static final int      MAX_LABEL_VALUE_LEN        = 120;

    /** Templates beyond this cap fall into the single "overflow" bucket (bounds heap). */
    private static final int      MAX_TEMPLATES              = 20_000;
    /** A template earns its own labeled occurrences series only past this cumulative count. */
    private static final long     MIN_OCCURRENCES_FOR_SERIES = 100L;
    /** Rolling window over which a flood is measured. */
    private static final long     FLOOD_WINDOW_MS            = 60_000L;
    /** Occurrences of a single template within FLOOD_WINDOW_MS that trip a flood event. */
    private static final long     FLOOD_THRESHOLD            = 1_000L;
    /** Max template-text length kept as the human-readable sample / logged. */
    private static final int      MAX_TEMPLATE_SAMPLE_LEN    = 200;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromSource(
                kafka("otlp-logs", "flink-otlp-log-dedup-logs"),
                WatermarkStrategy.noWatermarks(),
                "Kafka[logs]")
            .process(new LogDedupProcess())
            .name("log-dedup");

        env.execute("OTLP Log Dedup");
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

    /* ---------- fingerprinting (Drain-style normalization) ---------- */

    /** quoted strings (single or double) → <STR>  (run first, before token masks) */
    private static final Pattern QUOTED  = Pattern.compile("\"[^\"]*\"|'[^']*'");
    /** dotted IPv4 (optionally :port) → <IP> */
    private static final Pattern IP      = Pattern.compile("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}(:\\d+)?\\b");
    /** uuid → <HEX> */
    private static final Pattern UUID    = Pattern.compile("\\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\b");
    /** long hex-ish tokens (>=8 chars, must contain a-f letter so we don't eat pure decimals) → <HEX> */
    private static final Pattern HEX      = Pattern.compile("\\b(0x)?[0-9a-fA-F]*[a-fA-F][0-9a-fA-F]*\\b");
    /** runs of digits → <NUM> */
    private static final Pattern NUM      = Pattern.compile("\\d+");
    /** any whitespace run → single space */
    private static final Pattern WS       = Pattern.compile("\\s+");

    /**
     * Normalize a raw log body into a stable template by masking volatile tokens.
     * Order matters: quotes and IP/UUID (which contain digits) are masked before the broad
     * NUM rule, and HEX is restricted to tokens containing a hex letter so we only flag tokens
     * that look like hashes/ids rather than ordinary numbers.
     */
    static String fingerprint(String body) {
        String t = body;
        t = QUOTED.matcher(t).replaceAll("<STR>");
        t = IP.matcher(t).replaceAll("<IP>");
        t = UUID.matcher(t).replaceAll("<HEX>");
        // require >=8 chars to avoid masking short words like "deadbeef"? keep >=8 to target ids
        t = maskLongHex(t);
        t = NUM.matcher(t).replaceAll("<NUM>");
        t = WS.matcher(t).replaceAll(" ").trim();
        return t;
    }

    /** Mask hex-looking tokens of length >= 8 that contain at least one a-f letter. */
    private static String maskLongHex(String s) {
        java.util.regex.Matcher m = HEX.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String tok = m.group();
            String core = tok.startsWith("0x") ? tok.substring(2) : tok;
            m.appendReplacement(sb, core.length() >= 8 ? "<HEX>" : java.util.regex.Matcher.quoteReplacement(tok));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    static String templateHash(String template) {
        return Integer.toHexString(template.hashCode());
    }

    /* ---------- per-template aggregate state ---------- */

    /** Bounded, all-Java per-template accumulator. Not Flink-checkpointed (reset on restart). */
    static final class TemplateStat {
        final String sample;       // truncated template text
        final String hash;
        long occurrences;          // cumulative
        long totalBodyBytes;       // cumulative, to derive avg body size
        boolean seriesEmitted;     // has the labeled occurrences series been created yet
        long lastBytesSavedAccounted; // occurrences-1 already pushed to bytes_saved counter

        // rolling flood window: bucket start + count within current FLOOD_WINDOW_MS
        long windowStartMs;
        long windowCount;
        String lastService;        // most recent service emitting this template
        String lastSeverity;

        TemplateStat(String sample, String hash, long nowMs) {
            this.sample = sample;
            this.hash = hash;
            this.windowStartMs = nowMs;
        }

        long avgBodyBytes() {
            return occurrences == 0 ? 0 : totalBodyBytes / occurrences;
        }
    }

    /* ---------- the dedup process function ---------- */

    public static class LogDedupProcess extends ProcessFunction<byte[], Void> {

        private transient Counter recordsTotal;
        private transient Counter bytesSavedTotal;
        private transient LabeledCounter templateOccurrences; // {service_name, severity, template_hash}
        private transient LabeledCounter floodEvents;         // {service_name}

        private transient Map<String, TemplateStat> templates; // template_hash -> stat
        private transient AtomicLong overflowOccurrences;      // beyond MAX_TEMPLATES

        private transient long nextLogAtMs;

        @Override
        public void open(Configuration parameters) {
            MetricGroup root = getRuntimeContext().getMetricGroup().addGroup("signal", "logs");

            recordsTotal    = root.counter("otlp_logdedup_records_total");
            bytesSavedTotal = root.counter("otlp_logdedup_bytes_saved_total");

            templateOccurrences = new LabeledCounter(
                "otlp_logdedup_template_occurrences_total", "service_name", "severity", "template_hash");
            templateOccurrences.bind(root);

            floodEvents = new LabeledCounter("otlp_logdedup_flood_events_total", "service_name");
            floodEvents.bind(root);

            templates = new HashMap<>();
            overflowOccurrences = new AtomicLong(0L);

            // unique_templates gauge: distinct template count held in the bounded map
            root.gauge("otlp_logdedup_unique_templates",
                (Gauge<Integer>) () -> templates.size());

            // dedup_ratio gauge: 1 - unique/total  (fraction of volume that is repetition)
            root.gauge("otlp_logdedup_dedup_ratio", (Gauge<Double>) () -> {
                long total = recordsTotal.getCount();
                if (total <= 0) return 0.0d;
                int unique = templates.size();
                double ratio = 1.0d - ((double) unique / (double) total);
                return ratio < 0 ? 0.0d : ratio;
            });

            nextLogAtMs = System.currentTimeMillis() + LOG_SNAPSHOT_INTERVAL_MS;
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Void> out) {
            if (value == null) return;
            try {
                long now = System.currentTimeMillis();
                ExportLogsServiceRequest req = ExportLogsServiceRequest.parseFrom(value);
                for (ResourceLogs rl : req.getResourceLogsList()) {
                    Map<String, String> attrs = resourceAttrs(rl.getResource().getAttributesList());
                    String service = attr(attrs, "service.name");
                    for (ScopeLogs sl : rl.getScopeLogsList()) {
                        for (LogRecord lr : sl.getLogRecordsList()) {
                            handleRecord(lr, service, now);
                        }
                    }
                }
                maybeLog();
            } catch (Exception e) {
                LOG.warn("[log-dedup] failed to parse OTLP logs batch: {}", e.toString());
            }
        }

        private void handleRecord(LogRecord lr, String service, long now) {
            recordsTotal.inc();

            String body = bodyString(lr.getBody());
            String severity = severityLabel(lr);
            int bodyBytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;

            String template = fingerprint(body);
            String hash = templateHash(template);

            TemplateStat st = templates.get(hash);
            if (st == null) {
                if (templates.size() >= MAX_TEMPLATES) {
                    // bounded map full: tally into the overflow bucket and stop tracking detail
                    overflowOccurrences.incrementAndGet();
                    return;
                }
                String sample = template.length() > MAX_TEMPLATE_SAMPLE_LEN
                    ? template.substring(0, MAX_TEMPLATE_SAMPLE_LEN) : template;
                st = new TemplateStat(sample, hash, now);
                templates.put(hash, st);
            }

            st.occurrences++;
            st.totalBodyBytes += bodyBytes;
            st.lastService = service;
            st.lastSeverity = severity;

            // ---- bytes saved: (occurrences-1)*avg_body_bytes, pushed incrementally ----
            // newly-saved = (occurrences-1) - already accounted, scaled by current body size.
            long savable = st.occurrences - 1L;
            long delta = savable - st.lastBytesSavedAccounted;
            if (delta > 0) {
                bytesSavedTotal.inc(delta * (long) bodyBytes);
                st.lastBytesSavedAccounted = savable;
            }

            // ---- cardinality-guarded labeled occurrences series ----
            if (st.occurrences >= MIN_OCCURRENCES_FOR_SERIES) {
                if (!st.seriesEmitted) {
                    // backfill the whole cumulative count the first time the series is created
                    templateOccurrences.inc(st.occurrences, service, severity, hash);
                    st.seriesEmitted = true;
                } else {
                    templateOccurrences.inc(1L, service, severity, hash);
                }
            }

            // ---- rolling flood window ----
            if (now - st.windowStartMs >= FLOOD_WINDOW_MS) {
                st.windowStartMs = now;
                st.windowCount = 0L;
            }
            st.windowCount++;
            if (st.windowCount == FLOOD_THRESHOLD) {
                floodEvents.inc(1L, service);
                LOG.warn("[log-dedup] FLOOD service={} template_hash={} count={} in {}ms window: {}",
                    service, hash, st.windowCount, FLOOD_WINDOW_MS, st.sample);
            }
        }

        private void maybeLog() {
            long now = System.currentTimeMillis();
            if (now < nextLogAtMs) return;
            nextLogAtMs = now + LOG_SNAPSHOT_INTERVAL_MS;
            logSnapshot();
        }

        private void logSnapshot() {
            long total = recordsTotal.getCount();
            int unique = templates.size();
            double ratio = total <= 0 ? 0.0 : Math.max(0.0, 1.0 - ((double) unique / (double) total));
            LOG.info("[log-dedup] records_total={} unique_templates={} dedup_ratio={} bytes_saved_total={} overflow_occurrences={}",
                total, unique, String.format("%.4f", ratio), bytesSavedTotal.getCount(), overflowOccurrences.get());

            templates.values().stream()
                .sorted(Comparator.comparingLong((TemplateStat s) -> s.occurrences).reversed())
                .limit(10)
                .forEach(s -> LOG.info(
                    "[log-dedup] noisy hash={} occ={} avg_body_bytes={} svc={} sev={} template=\"{}\"",
                    s.hash, s.occurrences, s.avgBodyBytes(),
                    s.lastService, s.lastSeverity, s.sample));
        }
    }

    /* ---------- body / severity extraction ---------- */

    private static String bodyString(AnyValue body) {
        switch (body.getValueCase()) {
            case STRING_VALUE: return body.getStringValue();
            case INT_VALUE:    return Long.toString(body.getIntValue());
            case BOOL_VALUE:   return Boolean.toString(body.getBoolValue());
            case DOUBLE_VALUE: return Double.toString(body.getDoubleValue());
            case BYTES_VALUE:  return "<bytes>";
            case KVLIST_VALUE: return body.getKvlistValue().toString();
            case ARRAY_VALUE:  return body.getArrayValue().toString();
            default:           return "";
        }
    }

    private static String severityLabel(LogRecord lr) {
        String txt = lr.getSeverityText();
        if (txt != null && !txt.isEmpty()) return txt;
        int num = lr.getSeverityNumberValue();
        return num > 0 ? Integer.toString(num) : UNKNOWN;
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
