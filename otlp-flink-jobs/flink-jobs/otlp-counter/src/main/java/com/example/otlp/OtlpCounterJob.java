package com.example.otlp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.prometheus.sink.PrometheusSink;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OtlpCounterJob {

    private static final String DEFAULT_BOOTSTRAP = "kafka:29092";
    private static final String DEFAULT_PROM_URL = "http://prometheus:9090/api/v1/write";
    private static final String GROUP_PREFIX = "flink-otlp-counter-";

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP);
        String promUrl = env("PROMETHEUS_REMOTE_WRITE_URL", DEFAULT_PROM_URL);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);

        DataStream<PrometheusTimeSeries> spans = readKafka(env, bootstrap, "otlp-traces", GROUP_PREFIX + "traces")
                .map(new TraceCounter())
                .name("count-spans")
                .uid("count-spans");

        DataStream<PrometheusTimeSeries> points = readKafka(env, bootstrap, "otlp-metrics", GROUP_PREFIX + "metrics")
                .map(new MetricCounter())
                .name("count-metric-points")
                .uid("count-metric-points");

        DataStream<PrometheusTimeSeries> logs = readKafka(env, bootstrap, "otlp-logs", GROUP_PREFIX + "logs")
                .map(new LogCounter())
                .name("count-log-records")
                .uid("count-log-records");

        DataStream<PrometheusTimeSeries> all = spans.union(points, logs);

        all.sinkTo(
                PrometheusSink.builder()
                        .setPrometheusRemoteWriteUrl(promUrl)
                        .setMaxBatchSizeInSamples(500)
                        .build())
                .name("prometheus-remote-write")
                .uid("prometheus-remote-write");

        env.execute("OTLP Counter Job");
    }

    private static DataStream<byte[]> readKafka(
            StreamExecutionEnvironment env,
            String bootstrap,
            String topic,
            String groupId) {
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new BytesDeserializer())
                .build();
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka:" + topic)
                .uid("source-" + topic);
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? fallback : v;
    }
}
