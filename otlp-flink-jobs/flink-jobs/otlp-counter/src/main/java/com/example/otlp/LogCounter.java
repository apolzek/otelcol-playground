package com.example.otlp;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.prometheus.sink.PrometheusTimeSeries;

public class LogCounter extends RichMapFunction<byte[], PrometheusTimeSeries> {

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
        ExportLogsServiceRequest req = ExportLogsServiceRequest.parseFrom(payload);
        long inBatch = 0L;
        for (ResourceLogs rl : req.getResourceLogsList()) {
            for (ScopeLogs sl : rl.getScopeLogsList()) {
                inBatch += sl.getLogRecordsCount();
            }
        }
        total += inBatch;

        long ts = System.currentTimeMillis();
        if (ts <= lastEmitMs) ts = lastEmitMs + 1;
        lastEmitMs = ts;

        return PrometheusTimeSeries.builder()
                .withMetricName("flink_otlp_log_records_total")
                .addLabel("signal", "logs")
                .addLabel("job", "flink-otlp-counter")
                .addLabel("subtask", subtask)
                .addSample(total, ts)
                .build();
    }
}
