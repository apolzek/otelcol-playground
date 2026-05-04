package com.example.otlp;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;

public class TraceCounter extends RichMapFunction<byte[], PrometheusTimeSeries> {

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
        ExportTraceServiceRequest req = ExportTraceServiceRequest.parseFrom(payload);
        long inBatch = 0L;
        for (ResourceSpans rs : req.getResourceSpansList()) {
            for (ScopeSpans ss : rs.getScopeSpansList()) {
                inBatch += ss.getSpansCount();
            }
        }
        total += inBatch;

        // Prometheus remote-write rejects duplicate timestamps on the same series.
        // Subtasks already have distinct labels; here we keep timestamps monotonic
        // within a single subtask in case multiple batches land in the same ms.
        long ts = System.currentTimeMillis();
        if (ts <= lastEmitMs) ts = lastEmitMs + 1;
        lastEmitMs = ts;

        return PrometheusTimeSeries.builder()
                .withMetricName("flink_otlp_spans_total")
                .addLabel("signal", "traces")
                .addLabel("job", "flink-otlp-counter")
                .addLabel("subtask", subtask)
                .addSample(total, ts)
                .build();
    }
}
