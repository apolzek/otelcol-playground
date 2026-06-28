# `otlp-stream-processor` — como o job funciona

Job Flink que consome OTLP (traces/logs/metrics) do Kafka e expõe contadores nativos via a porta `9249` do TaskManager, scrapeada pelo Prometheus do stack.

Arquivo fonte: [`src/main/java/io/nochaos/flink/OtlpStreamProcessorJob.java`](src/main/java/io/nochaos/flink/OtlpStreamProcessorJob.java)

---

## 1. O que ele faz

Para cada um dos 3 tópicos Kafka (`otlp-traces`, `otlp-logs`, `otlp-metrics`):

1. Lê os `byte[]` brutos (payload OTLP protobuf).
2. Incrementa `otlp_batches_total` em 1 (um record Kafka = um `ExportXxxServiceRequest`).
3. Desserializa o protobuf e conta os registros individuais dentro do envelope (spans / log records / metric data points), incrementando `otlp_records_total` pela quantidade.
4. Incrementa `otlp_bytes_total` em `value.length`.

Sem side effects (não escreve em Kafka, não faz remote_write, não toca em arquivo). A saída do job é puramente **métricas Flink nativas**.

---

## 2. Arquitetura

```
┌──────────────────┐        ┌──────────────────────────┐
│ Kafka            │        │ Flink TaskManager         │
│ otlp-traces   ───┼───▶── KafkaSource[traces]           │
│ otlp-logs     ───┼───▶── KafkaSource[logs]             │
│ otlp-metrics  ───┼───▶── KafkaSource[metrics]          │
└──────────────────┘        │            │              │
                            │            ▼              │
                            │   ProcessFunction          │
                            │   CountProcess(type)       │
                            │   ├─ batches.inc()         │
                            │   ├─ records.inc(n)        │
                            │   └─ bytes.inc(len)        │
                            │            │              │
                            │            ▼              │
                            │   Flink MetricRegistry     │
                            │            │              │
                            │            ▼              │
                            │   PrometheusReporter       │
                            │   → 0.0.0.0:9249/metrics   │
                            └──────────┬───────────────┘
                                       │ HTTP scrape (15s)
                                       ▼
                            ┌──────────────────────┐
                            │ Prometheus            │
                            └──────────────────────┘
```

### Pipeline por signal

```java
env.fromSource(kafkaSource(topic, groupId), WatermarkStrategy.noWatermarks(), "Kafka[traces]")
   .process(new CountProcess("traces"))
   .name("count-traces");
```

- `fromSource` + `KafkaSource`: connector novo do Flink (substitui `FlinkKafkaConsumer`).
- `setStartingOffsets(OffsetsInitializer.earliest())`: na primeira execução do consumer group, começa do offset 0. Depois disso, o offset fica salvo no Kafka e o job retoma de onde parou.
- `setValueOnlyDeserializer(new ByteArrayDeserializationSchema())`: não tenta parsear — trata o payload como bytes opacos. O parsing é feito dentro de `CountProcess` para poder extrair o record count.
- `process(...)`: operator stateless; apenas incrementa counters.

### Paralelismo

- `env.setParallelism(2)` — cada operator roda com 2 subtasks.
- Cada tópico tem 3 partições (`kafka-init` cria assim). Kafka distribui as partições entre as 2 subtasks do `KafkaSource`.
- O consumer group `flink-otlp-stream-processor-<type>` é compartilhado pelas subtasks da mesma source. Kafka faz o particionamento automaticamente.
- **Importante**: este consumer group é diferente do L2 (`collector-l2-<type>`), então L2 e Flink recebem **todo** o stream de forma independente.

---

## 3. Counters expostos

Três counters por `telemetry_type`, escopados via `MetricGroup.addGroup(key, value)`:

```java
MetricGroup typeGroup = getRuntimeContext().getMetricGroup().addGroup("telemetry_type", type);
batches = typeGroup.counter("otlp_batches_total");
records = typeGroup.counter("otlp_records_total");
bytes   = typeGroup.counter("otlp_bytes_total");
```

Como o `PrometheusReporter` do Flink serializa `addGroup(key, value)`:
- a `key` ("telemetry_type") vai pra URL de escopo **e** pra um label Prometheus
- a `value` ("traces"/"logs"/"metrics") vai pro label

Resultado no endpoint `:9249/metrics`:

```
flink_taskmanager_job_task_operator_telemetry_type_otlp_batches_total{
  job_name="OTLP_Stream_Processor",
  task_name="Source:_Kafka_traces______count_traces",
  operator_name="count_traces",
  subtask_index="0",
  tm_id="172_19_0_6:36577_e23e51",
  host="...",
  telemetry_type="traces"
} 92
```

A parte `flink_taskmanager_job_task_operator_` é o scope default do Flink para métricas de operator (configurável via `metrics.scope.operator`). O `telemetry_type_` no meio do nome é a `key` do `addGroup`. Não pode ser removido sem afetar o pattern geral.

### Semântica exata

| Métrica | Incremento | Significado |
|---|---|---|
| `otlp_batches_total` | `+1` por record Kafka | Número de `ExportTraceServiceRequest` / `ExportLogsServiceRequest` / `ExportMetricsServiceRequest` consumidos |
| `otlp_records_total` | `+n` onde `n` = registros individuais dentro do batch | Spans (traces), log records (logs), ou metric data points (metrics) |
| `otlp_bytes_total` | `+value.length` | Bytes do payload protobuf serializado |

Se o parse do protobuf falhar (payload corrompido), `batches` e `bytes` incrementam normalmente, mas `records` fica em `0` para aquela mensagem — o counter não reseta, só não soma nada. Erros são silenciados (`catch (Exception ignored)`).

---

## 4. Contagem de `records` — como o parse funciona

Função única: `countRecords(type, value)` no fonte. Cada tipo tem sua lógica própria porque os protobufs são distintos.

### Traces

```java
ExportTraceServiceRequest.parseFrom(value)
    .getResourceSpansList().stream()
    .flatMap(rs -> rs.getScopeSpansList().stream())
    .mapToLong(ss -> ss.getSpansList().size())
    .sum();
```

Estrutura: `ExportTraceServiceRequest → ResourceSpans → ScopeSpans → Span`.
Conta spans individuais somando `scopeSpans.spans.size()` pra todos os `(resource, scope)`.

### Logs

```java
ExportLogsServiceRequest.parseFrom(value)
    .getResourceLogsList().stream()
    .flatMap(rl -> rl.getScopeLogsList().stream())
    .mapToLong(sl -> sl.getLogRecordsList().size())
    .sum();
```

Estrutura idêntica à de traces mas com `ResourceLogs → ScopeLogs → LogRecord`.

### Metrics

```java
long total = 0;
for (ResourceMetrics rm : req.getResourceMetricsList())
    for (ScopeMetrics sm : rm.getScopeMetricsList())
        for (Metric m : sm.getMetricsList())
            total += dataPointCount(m);
```

Metrics é mais complexo porque cada `Metric` tem um de cinco tipos de dado, cada um com sua lista de data points:

```java
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
```

O que o Flink conta como "metric point" bate com o que o OTel Collector chama de `metric_points` — por isso `otelcol_receiver_accepted_metric_points_total` e `sum(flink_..._otlp_records_total{telemetry_type="metrics"})` produzem o mesmo número.

---

## 5. Periodicidade — como as métricas chegam ao Prometheus

### Pull, não push

O job **não faz** remote_write. O Flink `PrometheusReporter` (configurado em `docker-compose.yaml` via `metrics.reporter.prom.factory.class`) expõe um endpoint HTTP `/metrics` na porta `9249` do TaskManager. O Prometheus scrapeia esse endpoint.

### Cadeia de atualizações

1. **Counter.inc()**: atualização em memória, imediata, lock-free (AtomicLong interno).
2. **MetricRegistry snapshot**: Flink faz snapshot periódico para os reporters. O `PrometheusReporter` usa o valor mais recente sob demanda, então não há delay configurável do lado Flink.
3. **Prometheus scrape**: definido em `config/prometheus.yml`:
   ```yaml
   global:
     scrape_interval: 15s
   ```
   A cada 15s o Prometheus faz GET em `flink-taskmanager:9249/metrics`, lê todos os counters, e persiste na TSDB local.

### Isso é a "temporalidade igual aos collectors L1/L2"

- L1 (`collector-l1:8888`) expõe `otelcol_*` via OTel SDK.
- L2 (`collector-l2:8888`) também expõe `otelcol_*` via OTel SDK.
- Flink TaskManager (`flink-taskmanager:9249`) expõe `flink_*` via PrometheusReporter.

Os três são **pull-based** na mesma cadência (15s). Nenhum faz push.

---

## 6. Consumer groups e fan-out do Kafka

Dois consumer groups independentes leem os mesmos tópicos:

| Consumer | Group ID | Config |
|---|---|---|
| L2 (OTel Collector) | `collector-l2-{traces,logs,metrics}` | `config/collector-l2-config.yaml` |
| Flink | `flink-otlp-stream-processor-{traces,logs,metrics}` | Hardcoded no job |

Como são groups diferentes, cada um recebe **a cópia inteira** do stream (fan-out). Por isso:

- `otelcol_receiver_accepted_spans_total{job="otel-collector-l2"}` tende a coincidir com `sum(flink_..._otlp_records_total{telemetry_type="traces"})`.
- Diferenças instantâneas refletem o que está em voo em cada consumer (poll pendente do lado L2, ou operator buffer no lado Flink). Quando o produtor para, os dois convergem.

Não há nenhum mecanismo de sincronização entre L2 e Flink. A paridade é consequência natural de Kafka entregar o mesmo stream para os dois groups.

---

## 7. Restart e reset de counters

- **Counter do Flink não é checkpointed**. Ao restart do job, cada `Counter` reseta pra `0`.
- Prometheus detecta `counter_reset` e `rate()` / `increase()` lidam corretamente. Apenas `sum(foo_total)` cumulativo baixa pro total pós-restart.
- Isso é aceitável e equivalente ao L1/L2: quando o container do collector reinicia, `otelcol_receiver_accepted_spans_total` também volta pra 0.

Se precisar persistência de counters (sobreviver a restart), teria que usar `ValueState<Long>` dentro de um `KeyedProcessFunction` **+** `enableCheckpointing(...)`. Não foi feito aqui porque não vale o custo para o objetivo atual.

---

## 8. Como consultar no Prometheus / Grafana

### Total de spans processados pelo Flink
```promql
sum(flink_taskmanager_job_task_operator_telemetry_type_otlp_records_total{telemetry_type="traces"})
```

### Taxa de records por signal (1min)
```promql
sum by (telemetry_type) (
  rate(flink_taskmanager_job_task_operator_telemetry_type_otlp_records_total[1m])
)
```

### Tamanho médio de batch (records por batch) para traces
```promql
sum(rate(flink_taskmanager_job_task_operator_telemetry_type_otlp_records_total{telemetry_type="traces"}[1m]))
/
sum(rate(flink_taskmanager_job_task_operator_telemetry_type_otlp_batches_total{telemetry_type="traces"}[1m]))
```

### Comparação L1 vs L2 vs Flink
Usadas no dashboard "Data Overview" (`uid: collectors-overview`).

---

## 9. Rebuilding o JAR

Não tem `mvn` instalado no host; use Maven via Docker:

```bash
cd flink-jobs/otlp-stream-processor
docker run --rm -v "$PWD":/project -w /project \
  maven:3.9-eclipse-temurin-11 mvn -q -B clean package -DskipTests

# Copiar o fat-jar pro diretório que o flink-job-submitter monta:
cp target/otlp-stream-processor-1.0.0.jar ../otlp-stream-processor-1.0.0.jar
```

Para recarregar no cluster:

```bash
docker compose restart flink-jobmanager flink-taskmanager
docker compose up flink-job-submitter   # resubmete os jars de ./flink-jobs
```

Ou mais limpo (zera counters, Kafka, e tudo):
```bash
docker compose down -v && docker compose up -d
```

---

## 10. Dependências

`pom.xml` enxuto:

- `flink-streaming-java` (provided — está no container do Flink)
- `flink-clients` (provided)
- `flink-connector-kafka` 3.3.0-1.20 — Kafka source
- `opentelemetry-proto` 1.3.2-alpha — classes protobuf OTLP geradas
- `slf4j-api`, `log4j-slf4j-impl` (provided)

Não usa mais `flink-connector-prometheus` nem `flink-connector-base` (foram removidos junto com o `PrometheusSink` — agora o PrometheusReporter nativo do Flink cobre tudo).

O `maven-shade-plugin` empacota só o que **não** é `provided` num fat-jar (~22 MB).

---

## 11. Limitações / pontos de atenção

| Limitação | Impacto | Workaround |
|---|---|---|
| Counters resetam no restart | `sum(...)` baixa pra 0 | Usar `rate()`/`increase()` nas dashboards, ou adicionar ValueState checkpointed |
| Sem tratamento de payloads malformados | Record não é contado, sem alerta | Adicionar counter separado `otlp_parse_errors_total` se virar problema |
| Nome da métrica tem `telemetry_type_` no meio | Queries precisam do nome completo | Seria evitado com `metrics.scope.operator` customizado, mas afetaria todas as métricas |
| `OffsetsInitializer.earliest()` | Se você rodar o job depois que dados já estão no Kafka, ele replay tudo | Trocar pra `.latest()` no código se não quiser backfill |
| `setParallelism(2)` fixo | Escala horizontal não é dinâmica | Parâmetro ou variável de ambiente, se precisar |
| Sem `enableCheckpointing` | Não há garantia exactly-once — no modo do job atual, é fine, porque contadores são tolerantes a at-least-once (duplicação inflaria mas é detectável comparando com L1) | Adicionar `enableCheckpointing(10_000L)` + `JobManagerCheckpointStorage` se precisar durabilidade |

---

## 12. Histórico rápido da refatoração

Antes desta versão o job usava:

- `PrometheusSink` do connector `flink-connector-prometheus` → **remote_write** para `http://prometheus:9090/api/v1/write`
- `CumulativeCounterFunction` com `ValueState<Long>` checkpointed → emitindo `PrometheusTimeSeries` a cada 10s
- `AnalyticsFunction` — top-100 serviços, top-10 cardinality de atributos

Mudou pra:

- Counters nativos do Flink expostos via PrometheusReporter (pull-based, igual L1/L2)
- Sem remote_write, sem timer, sem state
- Analytics removido (objetivo atual é paridade, não análise)

Motivo: simplicidade + temporalidade idêntica ao resto do stack + redução de dependências.
