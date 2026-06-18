# Phase 3 — OTLP Insights (Flink job + Grafana)

Builds on the phase-2 stack (Kafka + L1/L2 collectors + Flink + Prometheus + Grafana) but
swaps the Flink job. The new job derives **business-level insights** from the OTLP
traffic flowing through Kafka and publishes them as Prometheus counters.

## What changed vs phase-2

- **New Flink job**: `flink-jobs/otlp-insights-processor` (replaces `otlp-stream-processor`).
- **L1 collector** (`config/collector-l1-config.yaml`) is split into three receivers so
  every resource is tagged with `otlp.transport`:
  - `4317` gRPC → `otlp.transport=grpc`
  - `4318` HTTP (protobuf-by-convention) → `otlp.transport=http/protobuf`
  - `4319` HTTP (JSON-by-convention) → `otlp.transport=http/json`
- **New Grafana dashboard**: `OTLP Insights (Flink)` (uid `otlp-insights`).

## Insights produced

Shaped by the real Pagmon OTLP data in `phase-1/samples-otlp/otlp-json/`. Every counter
follows Prometheus conventions (lowercase, snake_case, `_total` suffix, counters only —
no gauges, no rates computed in Flink).

All counters are labelled with `signal in {traces, logs, metrics}`, where one record is:

| signal    | what counts as a "record"          |
|-----------|-------------------------------------|
| `traces`  | one span                           |
| `logs`    | one log record                     |
| `metrics` | one metric data point (across Gauge / Sum / Histogram / ExponentialHistogram / Summary) |

### Shared counters (all three signals)

| Metric                                                  | Labels                                         |
|---------------------------------------------------------|------------------------------------------------|
| `otlp_signal_records_total`                             | `signal`                                       |
| `otlp_signal_records_by_service_total`                  | `signal`, `service_name`                       |
| `otlp_signal_records_by_transport_total`                | `signal`, `transport`                          |
| `otlp_signal_records_by_sdk_language_total`             | `signal`, `sdk_language`                       |
| `otlp_signal_records_by_sdk_total`                      | `signal`, `sdk_name`, `sdk_version`            |
| `otlp_signal_records_by_cloud_total`                    | `signal`, `cloud_provider`                     |
| `otlp_signal_records_by_k8s_total`                      | `signal`, `k8s_cluster_name`, `k8s_namespace_name` |
| `otlp_signal_records_by_environment_total`              | `signal`, `deployment_environment`             |

### Traces-only

| Metric                                                  | Labels                                         |
|---------------------------------------------------------|------------------------------------------------|
| `otlp_spans_errors_total`                               | `service_name` (spans with `status.code = STATUS_CODE_ERROR`) |
| `otlp_spans_with_exceptions_total`                      | `service_name`, `exception_type` (spans carrying an event named `exception`; type from `exception.type`) |

### Exposed names in Prometheus

Flink's `PrometheusReporterFactory` prefixes user metrics with the default scope *and*
interleaves each label-key name into the metric name (one quirk of the reporter — the
label *values* still arrive as proper Prometheus labels, which is what queries join on).
The full name of each counter observed on the TaskManager `/metrics` endpoint:

| Metric                                                                                                      | Labels                                         |
|-------------------------------------------------------------------------------------------------------------|------------------------------------------------|
| `flink_taskmanager_job_task_operator_signal_otlp_signal_records_total`                                      | `signal`                                       |
| `flink_taskmanager_job_task_operator_signal_service_name_otlp_signal_records_by_service_total`              | `signal`, `service_name`                       |
| `flink_taskmanager_job_task_operator_signal_transport_otlp_signal_records_by_transport_total`               | `signal`, `transport`                          |
| `flink_taskmanager_job_task_operator_signal_sdk_language_otlp_signal_records_by_sdk_language_total`         | `signal`, `sdk_language`                       |
| `flink_taskmanager_job_task_operator_signal_sdk_name_sdk_version_otlp_signal_records_by_sdk_total`          | `signal`, `sdk_name`, `sdk_version`            |
| `flink_taskmanager_job_task_operator_signal_cloud_provider_otlp_signal_records_by_cloud_total`              | `signal`, `cloud_provider`                     |
| `flink_taskmanager_job_task_operator_signal_k8s_cluster_name_k8s_namespace_name_otlp_signal_records_by_k8s_total` | `signal`, `k8s_cluster_name`, `k8s_namespace_name` |
| `flink_taskmanager_job_task_operator_signal_deployment_environment_otlp_signal_records_by_environment_total` | `signal`, `deployment_environment`             |
| `flink_taskmanager_job_task_operator_signal_service_name_otlp_spans_errors_total`                            | `signal`, `service_name`                       |
| `flink_taskmanager_job_task_operator_signal_service_name_exception_type_otlp_spans_with_exceptions_total`    | `signal`, `service_name`, `exception_type`     |

Example queries (also wired into the dashboard):

```promql
# Spans per second
sum(rate(flink_taskmanager_job_task_operator_signal_otlp_signal_records_total{signal="traces"}[1m]))

# Metric data points per second
sum(rate(flink_taskmanager_job_task_operator_signal_otlp_signal_records_total{signal="metrics"}[1m]))

# Log records per second
sum(rate(flink_taskmanager_job_task_operator_signal_otlp_signal_records_total{signal="logs"}[1m]))

# Top 10 services by spans/sec
topk(10, sum by (service_name) (
  rate(flink_taskmanager_job_task_operator_signal_service_name_otlp_signal_records_by_service_total{signal="traces"}[1m])))

# Transport mix (gRPC / HTTP/protobuf / HTTP/JSON)
sum by (transport) (
  rate(flink_taskmanager_job_task_operator_signal_transport_otlp_signal_records_by_transport_total{signal="traces"}[1m]))

# Error ratio over spans
sum(rate(flink_taskmanager_job_task_operator_signal_service_name_otlp_spans_errors_total[1m]))
  /
clamp_min(sum(rate(flink_taskmanager_job_task_operator_signal_otlp_signal_records_total{signal="traces"}[1m])), 0.001)
```

> Note: Flink's Prometheus reporter exposes all counters with `TYPE gauge`. `rate()` still
> works on monotonically increasing gauges, but counters will dip on task restarts — use
> `increase()`/`rate()` with a rate window long enough to ride over checkpoint interruptions.

### Log snapshots

Every 60s each subtask writes a top-10 snapshot of each counter to the Flink TaskManager
log, so insights can be spot-checked from the Flink UI (port 8082) without hitting
Prometheus. Example line:

```
[insights-traces] otlp_signal_records_by_service_total [agatha] = 12473
```

Multi-label counters are joined with `|`:

```
[insights-traces] otlp_signal_records_by_sdk_total [opentelemetry|1.34.1] = 12473
```

## Building the jar

Java and Maven are not required locally — build inside the official Maven image:

```bash
cd flink-jobs/otlp-insights-processor
docker run --rm -v "$(pwd)":/app -w /app \
  maven:3.9-eclipse-temurin-11 \
  mvn -q clean package

# Copy the fat jar where the submitter looks for it
cp target/otlp-insights-processor-1.0.0.jar ../
```

The committed `flink-jobs/otlp-insights-processor-1.0.0.jar` is what
`flink-job-submitter` uploads to the JobManager on `docker compose up`.

## Running

```bash
docker compose up -d
```

Teardown:

```bash
docker compose down -v
```

## Dashboards

Grafana provisions everything phase-2 shipped **plus** the new insights dashboard:

| Dashboard | UID | What it shows |
|-----------|-----|---------------|
| OTLP Insights (Flink) | `otlp-insights` | Spans vs metrics vs logs, per-service volume, transport split (gRPC vs HTTP/protobuf vs HTTP/JSON), telemetry.sdk.*, cloud.provider, k8s cluster × namespace, deployment.environment, error spans, exception spans |
| Kafka Exporter Overview | `jwPKIsniz` | broker/topic rates, consumer-group lag |
| Apache Flink (2021) | `wKbnD5Gnk` | JM/TM JVM, task slots, checkpoints, backpressure |
| Data Overview | `collectors-overview` | Per-collector (L1 / L2) rate + count split by signal |
| cadvisor | `cadvisor-main` | per-container CPU, memory, network, blkio, fs |

## Port Binding

| Service          | Port(s)                                                 | Notes                              |
|------------------|---------------------------------------------------------|------------------------------------|
| Grafana          | 3000                                                    | UI                                 |
| Flink UI         | 8082                                                    | job status, task slots             |
| Flink RPC        | 6123                                                    | internal RPC                       |
| Prometheus       | 9090                                                    | remote write receiver enabled      |
| Kafka UI         | 8083                                                    | topic inspection, lag              |
| Kafka            | 9092                                                    | external listener (host)           |
| Kafka Exporter   | 9308                                                    | Prometheus metrics for Kafka       |
| Zookeeper        | 2181                                                    | Kafka coordination                 |
| cAdvisor         | 8080                                                    | per-container resource usage       |
| Collector L1     | 4317 (gRPC), 4318 (HTTP/proto), 4319 (HTTP/JSON), 8888, 13133 | OTLP ingest; `network_mode: host` |
| Collector L2     | 24133                                                   | health check                       |

## Load test — `telemetrygen`

Same as phase-2. `tests/run-telemetrygen.py` targets `4317` for gRPC and `4318` for HTTP.
Traffic on `4318` will be tagged `otlp.transport=http/protobuf` (telemetrygen emits
protobuf). For `otlp.transport=http/json` samples, point a JSON-emitting client at
`localhost:4319`.

```bash
# defaults (gRPC + HTTP/protobuf, 20 variations, 10 min)
tests/.venv/bin/python tests/run-telemetrygen.py

# only gRPC
tests/.venv/bin/python tests/run-telemetrygen.py --http-ratio 0

# only HTTP/protobuf
tests/.venv/bin/python tests/run-telemetrygen.py --http-ratio 1
```

## References

- https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/metric_reporters/#prometheus
- https://opentelemetry.io/docs/specs/otlp/
- https://prometheus.io/docs/practices/naming/
- https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/otlpreceiver
