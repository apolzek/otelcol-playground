package com.example.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;

public class MetricCounter extends RichMapFunction<byte[], PrometheusTimeSeries> {

    private long total;
    private long lastEmitMs;
    private String subtask;

    @Override
    public void open(Configuration parameters) {
        this.total = 0L;
        this.lastEmitMs = 0L;
        this.subtask = String.valueOf(getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public PrometheusTimeSeries map(byte[] payload) throws Exception {
        ExportMetricsServiceRequest req = ExportMetricsServiceRequest.parseFrom(payload);
        long inBatch = 0L;
        for (ResourceMetrics rm : req.getResourceMetricsList()) {
            for (ScopeMetrics sm : rm.getScopeMetricsList()) {
                for (Metric m : sm.getMetricsList()) {
                    inBatch += datapointCount(m);
                }
            }
        }
        total += inBatch;

        long ts = System.currentTimeMillis();
        if (ts <= lastEmitMs) ts = lastEmitMs + 1;
        lastEmitMs = ts;

        return PrometheusTimeSeries.builder()
                .withMetricName("flink_otlp_metric_points_total")
                .addLabel("signal", "metrics")
                .addLabel("job", "flink-otlp-counter")
                .addLabel("subtask", subtask)
                .addSample(total, ts)
                .build();
    }

    private static long datapointCount(Metric m) {
        switch (m.getDataCase()) {
            case GAUGE:
                return m.getGauge().getDataPointsCount();
            case SUM:
                return m.getSum().getDataPointsCount();
            case HISTOGRAM:
                return m.getHistogram().getDataPointsCount();
            case EXPONENTIAL_HISTOGRAM:
                return m.getExponentialHistogram().getDataPointsCount();
            case SUMMARY:
                return m.getSummary().getDataPointsCount();
            case DATA_NOT_SET:
            default:
                return 0L;
        }
    }
}
