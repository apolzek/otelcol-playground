# HELP otelcol_exporter_queue_capacity Fixed capacity of the retry queue (in batches). [Alpha]
# TYPE otelcol_exporter_queue_capacity gauge
otelcol_exporter_queue_capacity{data_type="logs",exporter="kafka/logs",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 1000
otelcol_exporter_queue_capacity{data_type="metrics",exporter="kafka/metrics",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 1000
otelcol_exporter_queue_capacity{data_type="traces",exporter="kafka/traces",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 1000
# HELP otelcol_exporter_queue_size Current size of the retry queue (in batches). [Alpha]
# TYPE otelcol_exporter_queue_size gauge
otelcol_exporter_queue_size{data_type="logs",exporter="kafka/logs",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 0
otelcol_exporter_queue_size{data_type="metrics",exporter="kafka/metrics",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 0
otelcol_exporter_queue_size{data_type="traces",exporter="kafka/traces",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 0
# HELP otelcol_exporter_sent_log_records_total Number of log record successfully sent to destination. [Alpha]
# TYPE otelcol_exporter_sent_log_records_total counter
otelcol_exporter_sent_log_records_total{exporter="kafka/logs",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 167200
# HELP otelcol_exporter_sent_metric_points_total Number of metric points successfully sent to destination. [Alpha]
# TYPE otelcol_exporter_sent_metric_points_total counter
otelcol_exporter_sent_metric_points_total{exporter="kafka/metrics",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 167200
# HELP otelcol_exporter_sent_spans_total Number of spans successfully sent to destination. [Alpha]
# TYPE otelcol_exporter_sent_spans_total counter
otelcol_exporter_sent_spans_total{exporter="kafka/traces",otel_scope_name="go.opentelemetry.io/collector/exporter/exporterhelper",otel_scope_schema_url="",otel_scope_version=""} 167200
# HELP otelcol_kafka_broker_connects_total The total number of connections opened. [Development]
# TYPE otelcol_kafka_broker_connects_total counter
otelcol_kafka_broker_connects_total{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost"} 5
otelcol_kafka_broker_connects_total{node_id="seed_0",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost"} 3
# HELP otelcol_kafka_exporter_bytes_total The size in bytes of exported records seen by the broker. [Development]
# TYPE otelcol_kafka_exporter_bytes_total counter
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-logs"} 229208
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-metrics"} 262496
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-traces"} 1.478138e+06
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-logs"} 228424
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-metrics"} 260199
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-traces"} 1.692457e+06
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-logs"} 234252
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-metrics"} 254909
otelcol_kafka_exporter_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-traces"} 1.675443e+06
# HELP otelcol_kafka_exporter_bytes_uncompressed_bytes_total The uncompressed size in bytes of exported messages seen by the client. [Development]
# TYPE otelcol_kafka_exporter_bytes_uncompressed_bytes_total counter
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-logs"} 3.096029e+06
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-metrics"} 1.90773e+06
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-traces"} 7.982749e+06
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-logs"} 3.084852e+06
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-metrics"} 1.8942e+06
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-traces"} 9.14106e+06
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-logs"} 3.163091e+06
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-metrics"} 1.85361e+06
otelcol_kafka_exporter_bytes_uncompressed_bytes_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-traces"} 9.047171e+06
# HELP otelcol_kafka_exporter_latency_milliseconds The time it took in ms to export a batch of messages. [Deprecated]
# TYPE otelcol_kafka_exporter_latency_milliseconds histogram
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0"} 2468
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="5"} 2483
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="10"} 2491
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="25"} 2493
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="50"} 2504
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="75"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="100"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="250"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="500"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="750"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="1000"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="2500"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="5000"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="7500"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="10000"} 2505
otelcol_kafka_exporter_latency_milliseconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="+Inf"} 2505
otelcol_kafka_exporter_latency_milliseconds_sum{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost"} 629
otelcol_kafka_exporter_latency_milliseconds_count{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost"} 2505
# HELP otelcol_kafka_exporter_messages_total The number of exported messages. [Deprecated]
# TYPE otelcol_kafka_exporter_messages_total counter
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-logs"} 277
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-metrics"} 282
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-traces"} 255
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-logs"} 276
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-metrics"} 280
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-traces"} 292
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-logs"} 283
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-metrics"} 274
otelcol_kafka_exporter_messages_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-traces"} 289
# HELP otelcol_kafka_exporter_records_total The number of exported records. [Development]
# TYPE otelcol_kafka_exporter_records_total counter
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-logs"} 277
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-metrics"} 282
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="0",server_address="localhost",topic="otlp-traces"} 255
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-logs"} 276
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-metrics"} 280
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="1",server_address="localhost",topic="otlp-traces"} 292
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-logs"} 283
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-metrics"} 274
otelcol_kafka_exporter_records_total{compression_codec="zstd",node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",partition="2",server_address="localhost",topic="otlp-traces"} 289
# HELP otelcol_kafka_exporter_write_latency_seconds The time it took in seconds to export a batch of records. [Development]
# TYPE otelcol_kafka_exporter_write_latency_seconds histogram
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0"} 0
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.005"} 2476
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.01"} 2491
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.025"} 2493
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.05"} 2504
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.075"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.1"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.25"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.5"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="0.75"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="1"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="2.5"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="5"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="7.5"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="10"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="25"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="50"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="75"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="100"} 2505
otelcol_kafka_exporter_write_latency_seconds_bucket{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost",le="+Inf"} 2505
otelcol_kafka_exporter_write_latency_seconds_sum{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost"} 1.7528615869999984
otelcol_kafka_exporter_write_latency_seconds_count{node_id="1",otel_scope_name="github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter",otel_scope_schema_url="",otel_scope_version="",outcome="success",server_address="localhost"} 2505
# HELP otelcol_process_cpu_seconds_total Total CPU user and system time in seconds [Alpha]
# TYPE otelcol_process_cpu_seconds_total counter
otelcol_process_cpu_seconds_total{otel_scope_name="go.opentelemetry.io/collector/service",otel_scope_schema_url="",otel_scope_version=""} 1.7200000000000002
# HELP otelcol_process_memory_rss_bytes Total physical memory (resident set size) [Alpha]
# TYPE otelcol_process_memory_rss_bytes gauge
otelcol_process_memory_rss_bytes{otel_scope_name="go.opentelemetry.io/collector/service",otel_scope_schema_url="",otel_scope_version=""} 2.32247296e+08
# HELP otelcol_process_runtime_heap_alloc_bytes Bytes of allocated heap objects (see 'go doc runtime.MemStats.HeapAlloc') [Alpha]
# TYPE otelcol_process_runtime_heap_alloc_bytes gauge
otelcol_process_runtime_heap_alloc_bytes{otel_scope_name="go.opentelemetry.io/collector/service",otel_scope_schema_url="",otel_scope_version=""} 3.9821408e+07
# HELP otelcol_process_runtime_total_alloc_bytes_total Cumulative bytes allocated for heap objects (see 'go doc runtime.MemStats.TotalAlloc') [Alpha]
# TYPE otelcol_process_runtime_total_alloc_bytes_total counter
otelcol_process_runtime_total_alloc_bytes_total{otel_scope_name="go.opentelemetry.io/collector/service",otel_scope_schema_url="",otel_scope_version=""} 3.72623336e+08
# HELP otelcol_process_runtime_total_sys_memory_bytes Total bytes of memory obtained from the OS (see 'go doc runtime.MemStats.Sys') [Alpha]
# TYPE otelcol_process_runtime_total_sys_memory_bytes gauge
otelcol_process_runtime_total_sys_memory_bytes{otel_scope_name="go.opentelemetry.io/collector/service",otel_scope_schema_url="",otel_scope_version=""} 8.7533848e+07
# HELP otelcol_process_uptime_seconds_total Uptime of the process [Alpha]
# TYPE otelcol_process_uptime_seconds_total counter
otelcol_process_uptime_seconds_total{otel_scope_name="go.opentelemetry.io/collector/service",otel_scope_schema_url="",otel_scope_version=""} 53.009870938
# HELP otelcol_processor_accepted_log_records_total Number of log records successfully pushed into the next component in the pipeline. [Deprecated]
# TYPE otelcol_processor_accepted_log_records_total counter
otelcol_processor_accepted_log_records_total{otel_scope_name="go.opentelemetry.io/collector/processor/memorylimiterprocessor",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167200
# HELP otelcol_processor_accepted_metric_points_total Number of metric points successfully pushed into the next component in the pipeline. [Deprecated]
# TYPE otelcol_processor_accepted_metric_points_total counter
otelcol_processor_accepted_metric_points_total{otel_scope_name="go.opentelemetry.io/collector/processor/memorylimiterprocessor",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167200
# HELP otelcol_processor_accepted_spans_total Number of spans successfully pushed into the next component in the pipeline. [Deprecated]
# TYPE otelcol_processor_accepted_spans_total counter
otelcol_processor_accepted_spans_total{otel_scope_name="go.opentelemetry.io/collector/processor/memorylimiterprocessor",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167300
# HELP otelcol_processor_batch_batch_send_size Number of units in the batch [Development]
# TYPE otelcol_processor_batch_batch_send_size histogram
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="10"} 0
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="25"} 0
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="50"} 0
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="75"} 0
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="100"} 0
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="250"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="500"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="750"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="1000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="2000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="3000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="4000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="5000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="6000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="7000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="8000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="9000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="10000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="20000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="30000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="50000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="100000"} 2508
otelcol_processor_batch_batch_send_size_bucket{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch",le="+Inf"} 2508
otelcol_processor_batch_batch_send_size_sum{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch"} 501600
otelcol_processor_batch_batch_send_size_count{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch"} 2508
# HELP otelcol_processor_batch_batch_size_trigger_send_total Number of times the batch was sent due to a size trigger [Development]
# TYPE otelcol_processor_batch_batch_size_trigger_send_total counter
otelcol_processor_batch_batch_size_trigger_send_total{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch"} 2508
# HELP otelcol_processor_batch_metadata_cardinality Number of distinct metadata value combinations being processed [Development]
# TYPE otelcol_processor_batch_metadata_cardinality gauge
otelcol_processor_batch_metadata_cardinality{otel_scope_name="go.opentelemetry.io/collector/processor/batchprocessor",otel_scope_schema_url="",otel_scope_version="",processor="batch"} 3
# HELP otelcol_processor_incoming_items_total Number of items passed to the processor. [Alpha]
# TYPE otelcol_processor_incoming_items_total counter
otelcol_processor_incoming_items_total{otel_signal="logs",otel_scope_name="go.opentelemetry.io/collector/processor/processorhelper",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167200
otelcol_processor_incoming_items_total{otel_signal="metrics",otel_scope_name="go.opentelemetry.io/collector/processor/processorhelper",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167200
otelcol_processor_incoming_items_total{otel_signal="traces",otel_scope_name="go.opentelemetry.io/collector/processor/processorhelper",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167300
# HELP otelcol_processor_outgoing_items_total Number of items emitted from the processor. [Alpha]
# TYPE otelcol_processor_outgoing_items_total counter
otelcol_processor_outgoing_items_total{otel_signal="logs",otel_scope_name="go.opentelemetry.io/collector/processor/processorhelper",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167200
otelcol_processor_outgoing_items_total{otel_signal="metrics",otel_scope_name="go.opentelemetry.io/collector/processor/processorhelper",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167200
otelcol_processor_outgoing_items_total{otel_signal="traces",otel_scope_name="go.opentelemetry.io/collector/processor/processorhelper",otel_scope_schema_url="",otel_scope_version="",processor="memory_limiter"} 167300
# HELP otelcol_receiver_accepted_log_records_total Number of log records successfully pushed into the pipeline. [Alpha]
# TYPE otelcol_receiver_accepted_log_records_total counter
otelcol_receiver_accepted_log_records_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 167200
# HELP otelcol_receiver_accepted_metric_points_total Number of metric points successfully pushed into the pipeline. [Alpha]
# TYPE otelcol_receiver_accepted_metric_points_total counter
otelcol_receiver_accepted_metric_points_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 167200
# HELP otelcol_receiver_accepted_spans_total Number of spans successfully pushed into the pipeline. [Alpha]
# TYPE otelcol_receiver_accepted_spans_total counter
otelcol_receiver_accepted_spans_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 167300
# HELP otelcol_receiver_failed_log_records_total The number of log records that failed to be processed by the receiver due to internal errors. [Alpha]
# TYPE otelcol_receiver_failed_log_records_total counter
otelcol_receiver_failed_log_records_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 0
# HELP otelcol_receiver_failed_metric_points_total The number of metric points that failed to be processed by the receiver due to internal errors. [Alpha]
# TYPE otelcol_receiver_failed_metric_points_total counter
otelcol_receiver_failed_metric_points_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 0
# HELP otelcol_receiver_failed_spans_total The number of spans that failed to be processed by the receiver due to internal errors. [Alpha]
# TYPE otelcol_receiver_failed_spans_total counter
otelcol_receiver_failed_spans_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 0
# HELP otelcol_receiver_refused_log_records_total Number of log records that could not be pushed into the pipeline. [Alpha]
# TYPE otelcol_receiver_refused_log_records_total counter
otelcol_receiver_refused_log_records_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 0
# HELP otelcol_receiver_refused_metric_points_total Number of metric points that could not be pushed into the pipeline. [Alpha]
# TYPE otelcol_receiver_refused_metric_points_total counter
otelcol_receiver_refused_metric_points_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 0
# HELP otelcol_receiver_refused_spans_total Number of spans that could not be pushed into the pipeline. [Alpha]
# TYPE otelcol_receiver_refused_spans_total counter
otelcol_receiver_refused_spans_total{otel_scope_name="go.opentelemetry.io/collector/receiver/receiverhelper",otel_scope_schema_url="",otel_scope_version="",receiver="otlp",transport="grpc"} 0
# HELP promhttp_metric_handler_errors_total Total number of internal errors encountered by the promhttp metric handler.
# TYPE promhttp_metric_handler_errors_total counter
promhttp_metric_handler_errors_total{cause="encoding"} 0
promhttp_metric_handler_errors_total{cause="gathering"} 0
# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_instance_id="f5577f97-93e2-49f1-9318-38a7738931e8",service_name="otelcol-contrib",service_version="0.147.0"} 1