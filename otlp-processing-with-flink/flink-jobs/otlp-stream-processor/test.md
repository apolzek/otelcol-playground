# Testing `otlp-stream-processor`

How to generate telemetry, verify counters, and exercise the two analytics operators (top-100 by `service.name`, top-10 high-cardinality metric attributes).

## Prerequisites

Stack is up with at least `kafka`, `collector-l1`, `flink-jobmanager`, `flink-taskmanager`, `prometheus`:

```bash
docker compose up -d \
  zookeeper kafka kafka-init kafka-ui \
  collector-l1 collector-l2 \
  flink-jobmanager flink-taskmanager flink-job-submitter \
  kafka-exporter prometheus grafana
```

Wait for all containers to report `healthy`, then confirm the job is `RUNNING`:

```bash
curl -s http://localhost:8082/jobs | python3 -m json.tool
```

Baseline health queries:

```bash
# Job alive
curl -s 'http://localhost:9090/api/v1/query?query=flink_job_alive' \
  | python3 -c "import sys,json;[print(r['metric'],r['value'][1]) for r in json.load(sys.stdin)['data']['result']]"

# Kafka consumer lag (should be 0 under light load)
curl -s 'http://localhost:9090/api/v1/query?query=kafka_consumergroup_lag_sum{consumergroup=~%22flink-otlp-stream-processor.%2A%22}'
```

---

## Test 1 — Basic counters (`flink_otlp_*_total`)

Send light telemetry via OTLP HTTP (port 4318, no auth):

```bash
for i in 1 2 3; do
  curl -s -X POST http://localhost:4318/v1/traces \
    -H 'Content-Type: application/json' \
    -d "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"smoke\"}}]},\"scopeSpans\":[{\"spans\":[{\"traceId\":\"010203040506070809$(printf %06d $i)abcdef\",\"spanId\":\"0102030405060708\",\"name\":\"s$i\",\"kind\":1,\"startTimeUnixNano\":\"1700000000000000000\",\"endTimeUnixNano\":\"1700000001000000000\",\"status\":{\"code\":1}}]}]}]}" > /dev/null
done
sleep 15

curl -s 'http://localhost:9090/api/v1/query' --data-urlencode 'query=flink_otlp_spans_total{telemetry_type="traces"}'
```

Expected: `flink_otlp_spans_total{telemetry_type="traces"}` equals the number of spans sent. The `messages_total` often shows a smaller number — the L1 batch processor coalesces rapid HTTP posts into fewer Kafka messages.

---

## Test 2 — Load test with `telemetrygen` (counter correctness under throughput)

```bash
TG="docker run --rm --network host ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest"

$TG traces  --otlp-insecure --otlp-endpoint localhost:4317 --workers 4 --rate 500 --duration 30s &
$TG logs    --otlp-insecure --otlp-endpoint localhost:4317 --workers 4 --rate 500 --duration 30s &
$TG metrics --otlp-insecure --otlp-endpoint localhost:4317 --workers 4 --rate 500 --duration 30s &
wait
sleep 15
```

Verify counter parity against the collector's own outbound counter:

```bash
# Flink view
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode 'query=flink_otlp_spans_total{telemetry_type="traces"}'

# Collector outbound view (host network)
curl -s http://localhost:8888/metrics | grep '^otelcol_exporter_sent_spans_total'
```

Under the test run documented in `job.md`, both reported 60021 spans — exact parity.

Rate queries:

```bash
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode 'query=sum(rate(flink_otlp_spans_total{telemetry_type="traces"}[1m]))'
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode 'query=sum(rate(flink_otlp_bytes_total[1m]))'
```

---

## Test 3 — Top-100 services by `service.name`

Goal: generate traces from many distinct services (some heavy, some tiny) and confirm the job's `otlp_records_by_service_total` metric ranks them correctly.

`telemetrygen` sets the resource `service.name` via `--service`. Do **not** use `--telemetry-attributes service.name=...` — that puts it on the span, not the resource, so the job will see `unknown`.

```bash
TG="docker run --rm --network host ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:latest"

# Phase 1 — 12 heavy services, each ~400 spans
for i in $(seq 1 12); do
  $TG traces --otlp-insecure --otlp-endpoint localhost:4317 \
    --service "heavy-svc-$i" --traces 200 --workers 1 2>/dev/null &
done
wait

# Phase 2 — 150 tiny services, each 10 spans
for i in $(seq 1 150); do
  $TG traces --otlp-insecure --otlp-endpoint localhost:4317 \
    --service "tiny-svc-$i" --traces 5 --workers 1 2>/dev/null &
  (( i % 20 == 0 )) && wait
done
wait
sleep 15
```

Note: `--traces N` emits N root spans; each has `--child-spans 1` by default, so each `--traces` = 2 spans.

### Expected top-15

```bash
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode \
  'query=topk(15, otlp_records_by_service_total{telemetry_type="traces"})' \
  | python3 -c "
import sys, json
rows = sorted(json.load(sys.stdin)['data']['result'], key=lambda x: -int(float(x['value'][1])))
for r in rows:
    print(f\"{r['metric']['service_name']:30s} {r['value'][1]}\")"
```

Expected shape (values are cumulative):

```
telemetrygen                   <baseline from prior tests>
heavy-svc-1                    400
heavy-svc-2                    400
...
heavy-svc-12                   400
tiny-svc-X                     10
tiny-svc-Y                     10
...
```

### Verify truncation at 100

```bash
# Distinct (service, type) series that have been emitted at any tick
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode \
  'query=count(otlp_records_by_service_total{telemetry_type="traces"})'
```

**Caveat**: this count can exceed 100 because Prometheus keeps every series that was *ever* written within retention. In our test, 143 of 163 services appeared at some tick — the 12 heavies and `telemetrygen` are always in the top; the remaining 87 slots rotate through the 150 tinies between ticks (all tied at 10 records, HashMap iteration order picks a different subset each tick).

### Per-service rate (works because the metric is a counter)

```bash
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode \
  'query=topk(10, rate(otlp_records_by_service_total{telemetry_type="traces"}[1m]))'
```

---

## Test 4 — High-cardinality metric attributes (top-10)

Goal: send metrics where one attribute has many distinct values (CPFs, UUIDs) and another has few (region, status). Confirm the job ranks the high-cardinality pairs at the top of `otlp_metric_attr_cardinality`.

`telemetrygen metrics` does not inject per-data-point attributes, so use `curl` + shell to craft OTLP payloads:

```bash
for i in $(seq 1 200); do
  CPF=$(printf '%011d' $((RANDOM*RANDOM % 99999999999)))
  USER_UUID=$(cat /proc/sys/kernel/random/uuid)
  STATUS=$(( (i % 3) ))
  REGION=$(( (i % 5) ))

  # checkout.latency : cpf (high-card) + status (3 values)
  curl -s -X POST http://localhost:4318/v1/metrics -H 'Content-Type: application/json' -d "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"checkout-svc\"}}]},\"scopeMetrics\":[{\"metrics\":[{\"name\":\"checkout.latency\",\"sum\":{\"dataPoints\":[{\"attributes\":[{\"key\":\"cpf\",\"value\":{\"stringValue\":\"$CPF\"}},{\"key\":\"status\",\"value\":{\"stringValue\":\"s$STATUS\"}}],\"startTimeUnixNano\":\"1700000000000000000\",\"timeUnixNano\":\"1700000001000000000\",\"asInt\":\"1\"}],\"aggregationTemporality\":2,\"isMonotonic\":true}}]}]}]}" > /dev/null

  # login.attempts : user_id (high-card) + region (5 values)
  curl -s -X POST http://localhost:4318/v1/metrics -H 'Content-Type: application/json' -d "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"auth-svc\"}}]},\"scopeMetrics\":[{\"metrics\":[{\"name\":\"login.attempts\",\"sum\":{\"dataPoints\":[{\"attributes\":[{\"key\":\"user_id\",\"value\":{\"stringValue\":\"$USER_UUID\"}},{\"key\":\"region\",\"value\":{\"stringValue\":\"r$REGION\"}}],\"startTimeUnixNano\":\"1700000000000000000\",\"timeUnixNano\":\"1700000001000000000\",\"asInt\":\"1\"}],\"aggregationTemporality\":2,\"isMonotonic\":true}}]}]}]}" > /dev/null

  # orders.total : region only (5 values)
  curl -s -X POST http://localhost:4318/v1/metrics -H 'Content-Type: application/json' -d "{\"resourceMetrics\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"orders-svc\"}}]},\"scopeMetrics\":[{\"metrics\":[{\"name\":\"orders.total\",\"sum\":{\"dataPoints\":[{\"attributes\":[{\"key\":\"region\",\"value\":{\"stringValue\":\"r$REGION\"}}],\"startTimeUnixNano\":\"1700000000000000000\",\"timeUnixNano\":\"1700000001000000000\",\"asInt\":\"1\"}],\"aggregationTemporality\":2,\"isMonotonic\":true}}]}]}]}" > /dev/null
done
sleep 15
```

> `UID` is a readonly shell variable in bash — use `USER_UUID` or similar.

### Query: top-10 cardinality

```bash
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode 'query=otlp_metric_attr_cardinality' | python3 -c "
import sys, json
rows = sorted(json.load(sys.stdin)['data']['result'], key=lambda x: -int(float(x['value'][1])))
for r in rows:
    m = r['metric']
    print(f\"{m['metric']:25s} attr={m['attribute_key']:12s} distinct={r['value'][1]}\")"
```

Expected:

```
checkout.latency          attr=cpf          distinct=200
login.attempts            attr=user_id      distinct=200
login.attempts            attr=region       distinct=5
orders.total              attr=region       distinct=5
checkout.latency          attr=status       distinct=3
```

CPF and user_id are the cardinality offenders. In a real TSDB this is the pattern that blows up your series count — the job surfaces it without you needing to grep through the collector pipeline.

### Companion counter (observations)

Each observation = one data-point attribute occurrence. Useful to normalize cardinality against volume:

```bash
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode 'query=otlp_metric_attr_observations_total'

# cardinality / observations → ratio of distinct values per data point
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode \
  'query=otlp_metric_attr_cardinality / on(metric,attribute_key) otlp_metric_attr_observations_total'
```

A ratio near 1.0 means "every data point has a unique value" — textbook high cardinality.

### Saturation signal

Distinct sets are capped at 10,000 per `(metric, attribute_key)` to bound memory. If the count plateaus at 10000 the cardinality is at least that — treat it as a lower bound:

```bash
curl -s 'http://localhost:9090/api/v1/query' --data-urlencode \
  'query=otlp_metric_attr_cardinality >= 10000'
```

---

## Test 5 — Service pollution via logs

Repeat Test 3 for logs to confirm the top-N works on all signal types:

```bash
for i in $(seq 1 12); do
  $TG logs --otlp-insecure --otlp-endpoint localhost:4317 --service "heavy-log-$i" --logs 200 --workers 1 2>/dev/null &
done
wait
sleep 15

curl -s 'http://localhost:9090/api/v1/query' --data-urlencode \
  'query=topk(15, otlp_records_by_service_total{telemetry_type="logs"})'
```

Same PromQL pattern, just swap `telemetry_type="traces"` → `"logs"` or `"metrics"`.

---

## Reset between tests

The analytics operator state (`perServiceRecords`, `attrDistinctValues`, `attrObservations`) is **not checkpointed**. To zero it out, cancel and resubmit the job:

```bash
JOBID=$(curl -s http://localhost:8082/jobs | python3 -c "import sys,json;print(json.load(sys.stdin)['jobs'][0]['id'])")
curl -sf -X PATCH "http://localhost:8082/jobs/$JOBID?mode=cancel"

# Resubmit (flink-job-submitter is a one-shot container; easier to upload manually)
resp=$(curl -sf -X POST http://localhost:8082/jars/upload -H 'Expect:' \
  -F "jarfile=@flink-jobs/otlp-stream-processor-1.0.0.jar")
jar_id=$(echo "$resp" | python3 -c "import sys,json;print(json.load(sys.stdin)['filename'].split('/')[-1])")
curl -sf -X POST "http://localhost:8082/jars/$jar_id/run" \
  -H 'Content-Type: application/json' -d '{"parallelism": 2}'
```

Prometheus itself keeps the previously emitted series for the retention window (1 h in this compose). The new job will start writing fresh values against the same series — the resulting graphs may look like a drop to zero followed by a climb.

The cumulative `flink_otlp_*_total` series **are** checkpointed, so they survive TaskManager restart but not a fresh job submission (ValueState is keyed by operator UID + key, and the new submission generates new operator UIDs unless you pin them with `.uid()`).

---

## Dashboard

Grafana at <http://localhost:3000> has the `otlp-stream-processor` dashboard auto-provisioned. The new analytics panels you can build on top:

- **Top-N services (table)**: `topk(20, otlp_records_by_service_total{telemetry_type="$signal"})`
- **Per-service rate (timeseries)**: `topk(10, rate(otlp_records_by_service_total{telemetry_type="traces"}[1m]))`
- **High-cardinality offenders (bar gauge)**: `topk(10, otlp_metric_attr_cardinality)`
- **Saturation banner**: `count(otlp_metric_attr_cardinality >= 10000)` with a red threshold at 1.
