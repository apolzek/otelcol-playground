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
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;

import java.io.IOException;

/**
 * Reads OTLP traces / logs / metrics from Kafka and emits Flink-native counters
 * that the PrometheusReporter plugin exposes on port 9249. Prometheus scrapes
 * that endpoint on its normal scrape_interval — same pull-based temporality as
 * the L1 and L2 OTel collectors. No remote_write.
 *
 * <p>Three counters per signal type, labelled with {@code telemetry_type}:
 * <ul>
 *   <li><b>otlp_batches_total</b> — one increment per Kafka record (OTLP envelope).</li>
 *   <li><b>otlp_records_total</b> — individual records inside the batches:
 *       spans for {@code traces}, log records for {@code logs}, metric data
 *       points (across Gauge / Sum / Histogram / ExponentialHistogram / Summary)
 *       for {@code metrics}.</li>
 *   <li><b>otlp_bytes_total</b> — serialized bytes consumed from Kafka.</li>
 * </ul>
 *
 * <p>These line up 1:1 with the L1 / L2 collector metrics for end-to-end parity:
 * <pre>
 *   L1 accepted  →  Kafka  →  L2 accepted  →  Flink processed
 *
 *   otelcol_receiver_accepted_spans_total{job="otel-collector-l1"}
 *   otelcol_receiver_accepted_spans_total{job="otel-collector-l2"}
 *   sum(flink_taskmanager_job_task_operator_otlp_records_total{telemetry_type="traces"})
 * </pre>
 */
public class OtlpStreamProcessorJob {

    private static final String   KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String[] SIGNAL_TYPES            = {"traces", "logs", "metrics"};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        for (String type : SIGNAL_TYPES) {
            String topic   = "otlp-" + type;
            String groupId = "flink-otlp-stream-processor-" + type;

            env.fromSource(
                    createKafkaSource(topic, groupId),
                    WatermarkStrategy.noWatermarks(),
                    "Kafka[" + type + "]")
                .process(new CountProcess(type))
                .name("count-" + type);
        }

        env.execute("OTLP Stream Processor");
    }

    private static KafkaSource<byte[]> createKafkaSource(String topic, String groupId) {
        return KafkaSource.<byte[]>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
            .build();
    }

    /**
     * Per-batch counting operator. Increments three Flink-native counters
     * (batches / records / bytes) scoped by {@code telemetry_type}. No output —
     * metrics leave the job through Flink's metric system, not a DataStream sink.
     */
    public static class CountProcess extends ProcessFunction<byte[], Void> {
        private final String type;
        private transient Counter batches;
        private transient Counter records;
        private transient Counter bytes;

        public CountProcess(String type) {
            this.type = type;
        }

        @Override
        public void open(Configuration parameters) {
            MetricGroup typeGroup = getRuntimeContext().getMetricGroup()
                .addGroup("telemetry_type", type);
            batches = typeGroup.counter("otlp_batches_total");
            records = typeGroup.counter("otlp_records_total");
            bytes   = typeGroup.counter("otlp_bytes_total");
        }

        @Override
        public void processElement(byte[] value, Context ctx, Collector<Void> out) {
            if (value == null) {
                return;
            }
            batches.inc();
            bytes.inc(value.length);
            try {
                records.inc(countRecords(type, value));
            } catch (Exception ignored) {
                // malformed payload — batch is still counted, record count stays at 0
            }
        }
    }

    private static long countRecords(String type, byte[] value) throws Exception {
        switch (type) {
            case "traces":
                return ExportTraceServiceRequest.parseFrom(value)
                    .getResourceSpansList().stream()
                    .flatMap(rs -> rs.getScopeSpansList().stream())
                    .mapToLong(ss -> ss.getSpansList().size())
                    .sum();
            case "logs":
                return ExportLogsServiceRequest.parseFrom(value)
                    .getResourceLogsList().stream()
                    .flatMap(rl -> rl.getScopeLogsList().stream())
                    .mapToLong(sl -> sl.getLogRecordsList().size())
                    .sum();
            case "metrics":
                ExportMetricsServiceRequest req = ExportMetricsServiceRequest.parseFrom(value);
                long total = 0L;
                for (ResourceMetrics rm : req.getResourceMetricsList()) {
                    for (ScopeMetrics sm : rm.getScopeMetricsList()) {
                        for (Metric m : sm.getMetricsList()) {
                            total += dataPointCount(m);
                        }
                    }
                }
                return total;
            default:
                return 0L;
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
