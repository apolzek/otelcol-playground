# OTLP Stream Processor — anatomia do job Flink

Código-fonte: `src/main/java/io/nochaos/flink/OtlpStreamProcessorJob.java`

O job tem um único objetivo: **ler OTLP de tópicos Kafka, contar o que passa, e expor esses contadores para o Prometheus coletar**. Não escreve de volta em Kafka, não tem sink externo — a saída sai pelo sistema de métricas do próprio Flink.

```
  ┌────────────┐   bytes    ┌──────────┐   Counter.inc()   ┌─────────────────┐
  │ Kafka      │──────────▶│  Flink   │──────────────────▶│ PrometheusReporter │──▶ :9249/metrics
  │ otlp-*     │  (OTLP    │  job     │                    │ (built-in do Flink)│
  └────────────┘   proto)  └──────────┘                    └─────────────────┘
```

---

## 1. O que pluga no Kafka

Função: `createKafkaSource(topic, groupId)` — linhas 74-82.

```java
KafkaSource.<byte[]>builder()
    .setBootstrapServers("kafka:29092")
    .setTopics(topic)                              // otlp-traces | otlp-logs | otlp-metrics
    .setGroupId(groupId)                           // um consumer group por signal
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
    .build();
```

Pontos-chave:

- **Bootstrap `kafka:29092`** — nome DNS interno da rede docker-compose (`otel-flink-network`). O Flink fala com o broker internamente, não via `localhost:9092`.
- **Três fontes, uma por signal** — o `main` (linhas 59-69) faz um loop sobre `{"traces", "logs", "metrics"}` e cria uma `DataStream` independente para cada tópico. Isso mantém as métricas separadas por `telemetry_type` sem precisar de `keyBy`.
- **Consumer group por signal**: `flink-otlp-stream-processor-traces`, `-logs`, `-metrics`. Isso garante que cada signal comita offsets próprios.
- **`OffsetsInitializer.latest()`** — só consome mensagens que chegarem *depois* do job iniciar. Evita inflar contadores com replay de testes antigos que ainda estão na retenção do Kafka. Em troca, se o job cair e subir de novo sem checkpoint válido, as mensagens que chegaram durante o downtime são perdidas para esse consumer group.
- **Deserializer `byte[]` cru** (`ByteArrayDeserializationSchema`, linhas 164-179) — o Flink não parseia o payload na entrada. O parsing OTLP acontece depois, dentro do `CountProcess`, para que uma mensagem malformada não derrube o pipeline.

---

## 2. O que lê OTLP

Classe: `CountProcess extends ProcessFunction<byte[], Void>` — linhas 89-121.

Cada record do Kafka chega como `byte[]` e é um envelope OTLP serializado em protobuf (`otlp_proto`, configurado no `collector-l1-config.yaml`). O `CountProcess`:

1. **Incrementa `otlp_batches_total`** (linha 113) — um por Kafka record, independente de conseguir parsear.
2. **Incrementa `otlp_bytes_total`** pelo tamanho do payload bruto (linha 114).
3. **Chama `countRecords(type, value)`** (linha 116) que desserializa o protobuf e conta registros individuais dentro do envelope:

```java
// traces — linhas 125-130
ExportTraceServiceRequest.parseFrom(value)
    .getResourceSpansList().stream()
    .flatMap(rs -> rs.getScopeSpansList().stream())
    .mapToLong(ss -> ss.getSpansList().size())
    .sum();
```

Estrutura equivalente para `logs` (linhas 131-136) e `metrics` (linhas 137-147). Para **metrics** há um passo a mais: cada `Metric` pode ser Gauge / Sum / Histogram / ExponentialHistogram / Summary, e cada tipo tem seus próprios `dataPoints`. A função `dataPointCount(m)` (linhas 153-162) faz `switch` no `getDataCase()` e retorna o count correto.

**Por que isso importa**: `otlp_records_total` do Flink deve bater 1:1 com `otelcol_receiver_accepted_spans_total` / `_log_records_total` / `_metric_points_total` dos collectors L1 e L2 — são a mesma unidade de contagem (span, log record, data point), não envelopes.

Erros de parse são silenciosamente ignorados (`catch (Exception ignored)`, linha 117) — o batch é contado, o record count fica em 0. Decisão consciente: uma mensagem lixo não deve quebrar o streaming.

---

## 3. O que manda para o Prometheus

**Nada no código-fonte "manda" nada ativamente.** A exposição Prometheus é 100% configuração do Flink runtime, não do job.

### Lado do job

`CountProcess.open(...)` — linhas 100-106 — registra três `Counter` via API nativa de métricas do Flink:

```java
MetricGroup typeGroup = getRuntimeContext().getMetricGroup()
    .addGroup("telemetry_type", type);
batches = typeGroup.counter("otlp_batches_total");
records = typeGroup.counter("otlp_records_total");
bytes   = typeGroup.counter("otlp_bytes_total");
```

O `addGroup("telemetry_type", type)` vira uma **label** no Prometheus (`telemetry_type="traces|logs|metrics"`). Os nomes finais das séries ficam tipo:

```
flink_taskmanager_job_task_operator_otlp_records_total{telemetry_type="traces", ...}
```

### Lado do runtime

Configurado no `docker-compose.yaml` via `FLINK_PROPERTIES` do `flink-jobmanager` e `flink-taskmanager`:

```yaml
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249
```

Isso ativa o **PrometheusReporter** (plugin nativo do Flink) que:

- Sobe um HTTP server em `:9249/metrics` dentro de cada processo Flink (JobManager e TaskManager).
- Traduz todo `Counter`/`Gauge`/`Histogram` registrado via `MetricGroup` para o text-format do Prometheus.
- É **pull** — o Prometheus scrapeia no intervalo normal dele (configurado em `config/prometheus.yml`). Não há remote_write.

No `docker-compose.yaml` os ports são:
- `jobmanager`: `9249:9249`
- `taskmanager`: `9250:9249` (offset no host para evitar conflito; dentro do container é 9249)

Na prática, o `CountProcess` roda nos TaskManagers, então os contadores `otlp_*_total` aparecem no scrape do `9250` (ou do nome DNS `flink-taskmanager:9249` se o Prometheus scrapeia pela rede docker).

---

## 4. Por que o design é assim

- **Sem sink de saída** (`.process(...)` retorna `Collector<Void>`): o único produto do job são métricas Flink-nativas. Evita um segundo hop de dados quando o objetivo é só medir volume.
- **Parallelism = 2** (linha 57): dá pra um TaskManager de 6 slots rodar os três pipelines (2 × 3 = 6 slots) sem contenção.
- **Paridade de unidades com collectors**: `batches_total` alinha com `otelcol_exporter_sent_*` e `records_total` alinha com `otelcol_receiver_accepted_*_points/spans/log_records`. Essa simetria é o que permite comparações ponta-a-ponta:

```
  L1 accepted  →  Kafka produced  →  L2 accepted  →  Flink processed
```

Se os quatro números baterem em janelas equivalentes, o pipeline está sem perda.
