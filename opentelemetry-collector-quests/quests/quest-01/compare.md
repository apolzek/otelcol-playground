# quest-01 — comparação dos cenários

Carga **idêntica** em todos: `telemetrygen traces`, `--duration=5m`,
`--workers=2`, 20 atributos por span, taxa default (sem `--rate`).
Versão do collector e do telemetrygen: `0.151.0`.

## Resultados crus

| #  | proto | batch | gzip | bytes L1→L2 (B) | bytes L1→L2 (KiB) | spans | bytes/span | duração |
| -- | ----- | ----- | ---- | --------------: | ----------------: | ----: | ---------: | ------: |
| 01 | gRPC  | —     | —    |         170 754 |              ~167 |   604 |     282.7  |   301 s |
| 02 | HTTP  | —     | —    |         170 060 |              ~166 |   604 |     281.6  |   301 s |
| 03 | HTTP  | ✓     | —    |         164 185 |              ~160 |   604 |     271.8  |   301 s |
| 04 | gRPC  | ✓     | —    |         177 543 |              ~173 |   604 |     294.0  |   301 s |
| 05 | HTTP  | ✓     | gzip |         164 857 |              ~161 |   604 |     273.0  |   301 s |
| 06 | gRPC  | ✓     | gzip |         179 666 |              ~175 |   604 |     297.5  |   301 s |

> "bytes L1→L2" = delta de RX em `eth0` do container `otel-l2` durante o teste,
> medido lendo `/proc/<pid>/net/dev` no host antes/depois. Ruído de scrape do
> Prometheus em `:8888` é ~30 KB por 5 min — desprezível na ordem de grandeza.
>
> Em **scenario-01** o counter `otelcol_receiver_accepted_spans_total` ficou
> contaminado por um pull/restart anterior; o telemetrygen gerou 604 spans
> nesta rodada também (consistente com 170 754 B ≈ 282 B/span).

## Cruzamentos

### gRPC vs HTTP, sem batch (01 vs 02)

| | gRPC (01) | HTTP (02) |
|---|---:|---:|
| bytes L1→L2 | 170 754 | 170 060 |
| Δ vs HTTP   | +0,4 %  | baseline |

Praticamente empatados. Cada batch tem 1 span (taxa baixa do telemetrygen
default), então a diferença de framing entre HTTP/1.1 e HTTP/2+gRPC se
cancela.

### Efeito do batch (01→04 gRPC, 02→03 HTTP)

| | gRPC s/batch (01) | gRPC c/batch (04) | HTTP s/batch (02) | HTTP c/batch (03) |
|---|---:|---:|---:|---:|
| bytes L1→L2 | 170 754 | 177 543 | 170 060 | 164 185 |
| Δ vs sem batch | — | **+4,0 %** | — | **−3,5 %** |

Em HTTP, batch reduziu bytes (menos overhead de connection/header por
request). Em gRPC, batch **aumentou** marginalmente — porque cada chamada
gRPC já tem um único stream e o batch só introduz mais um header de
mensagem agrupada. Na taxa default, batch só ajuda HTTP, e mesmo assim só
~3 %.

### Efeito do gzip (03→05 HTTP, 04→06 gRPC)

| | HTTP+batch (03) | HTTP+batch+gzip (05) | gRPC+batch (04) | gRPC+batch+gzip (06) |
|---|---:|---:|---:|---:|
| bytes L1→L2 | 164 185 | 164 857 | 177 543 | 179 666 |
| Δ vs sem gzip | — | **+0,4 %** | — | **+1,2 %** |

`compression: gzip` **não reduziu nada** — pelo contrário, adicionou bytes.
Motivo: na taxa default do telemetrygen (≈ 2 traces/s/worker), com
`batch:` no default (`timeout: 200ms`, `send_batch_size: 8192`), cada
batch sai com **1 span só**. Um payload de ~280 B é menor que o overhead
do header gzip (10 B mínimo + dicionário). Compressão só compensa quando
há volume agrupado de telemetria parecida.

## Conclusões pragmáticas

1. **Batch sem volume não vale.** Em carga baixa com `batch` default, os
   batches saem com 1 span. O processor só agrega valor se a vazão de entrada
   encher batches de centenas/milhares de spans.
2. **gzip sem batch cheio é nocivo.** O ganho de compressão depende do
   tamanho do payload. Em batches minúsculos o header do gzip custa mais
   do que economiza.
3. **gRPC vs HTTP empatam em volume baixo.** A diferença de framing some
   quando cada request carrega 1 span. Em volume alto a tendência é gRPC
   abrir vantagem (multiplexing HTTP/2).
4. **A baseline 01/02 (sem nada) é a referência honesta.** Para esse
   tráfego (~1 KB/s), qualquer otimização vai mexer no 4º decimal.

Para ver compressão de fato funcionar, basta repetir os cenários 05 e 06
com `--rate` alto no telemetrygen — ex: `--rate 5000`. Esperado: gzip
chega a ~70 % de redução em payloads de OTLP traces com atributos
repetitivos.

## Reproduzir

```bash
for d in scenario-0{1,2,3,4,5,6}; do
  ( cd "$d" && rm -f results.log && ./run.sh && docker compose down ) || break
done
```

Cada cenário grava seu próprio `results.log`. Este `compare.md` foi
montado a partir desses arquivos.
