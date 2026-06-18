#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

DURATION="${DURATION:-5m}"
WORKERS="${WORKERS:-2}"
RESULTS_FILE="${RESULTS_FILE:-results.log}"

export DURATION WORKERS

echo "==> Subindo stack (l1, l2, prometheus, grafana, cadvisor)"
docker compose up -d otel-l2 otel-l1 prometheus grafana cadvisor

echo "==> Aguardando collectors responderem em :8888 / :8889 (host)"
for url in http://localhost:8888/metrics http://localhost:8889/metrics; do
  for _ in {1..30}; do
    if curl -fs "$url" >/dev/null 2>&1; then break; fi
    sleep 1
  done
done

# Lê rx_bytes/tx_bytes do eth0 do container, via /proc/<pid>/net/dev no host.
# Funciona com imagens distroless (sem sh/wget dentro do container).
read_bytes() {
  local pid
  pid=$(docker inspect -f '{{.State.Pid}}' "$1")
  awk '/eth0:/ {print $2, $10}' "/proc/$pid/net/dev"
}

L1_BEFORE=$(read_bytes otel-l1)
L2_BEFORE=$(read_bytes otel-l2)

START_EPOCH=$(date +%s)
START_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "==> Disparando carga via loadgen (HTTP)"
NET="$(basename "$(pwd)")_default" \
PROTO=http TARGET_HOST=otel-l1 TARGET_PORT=4318 \
DURATION="$DURATION" \
../loadgen/run-load.sh

END_EPOCH=$(date +%s)
ELAPSED=$((END_EPOCH - START_EPOCH))

L1_AFTER=$(read_bytes otel-l1)
L2_AFTER=$(read_bytes otel-l2)

read L1_RX_B L1_TX_B <<<"$L1_BEFORE"
read L1_RX_A L1_TX_A <<<"$L1_AFTER"
read L2_RX_B L2_TX_B <<<"$L2_BEFORE"
read L2_RX_A L2_TX_A <<<"$L2_AFTER"

L1_RX=$((L1_RX_A - L1_RX_B)); L1_TX=$((L1_TX_A - L1_TX_B))
L2_RX=$((L2_RX_A - L2_RX_B)); L2_TX=$((L2_TX_A - L2_TX_B))

# Spans (via métricas internas do collector, expostas no host)
spans_metric() {
  curl -fs "$1" \
    | awk -v p="$2" '$1 ~ "^"p"({|$)" {sum+=$2} END {print sum+0}'
}
L1_RECEIVED=$(spans_metric http://localhost:8888/metrics otelcol_receiver_accepted_spans_total)
L1_SENT=$(spans_metric     http://localhost:8888/metrics otelcol_exporter_sent_spans_total)
L2_RECEIVED=$(spans_metric http://localhost:8889/metrics otelcol_receiver_accepted_spans_total)

human() { numfmt --to=iec-i --suffix=B "$1"; }

{
  echo "===================================================="
  echo "scenario-08 — HTTP l1 -> HTTP l2 (macro-batch: send_batch_size=8192)"
  echo "  start:      $START_ISO"
  echo "  duration:   ${ELAPSED}s (alvo $DURATION)"
  echo "  workers:    $WORKERS"
  echo "  attributes: 20"
  echo
  printf "  %-9s %15s %15s\n" "container" "rx"               "tx"
  printf "  %-9s %15s %15s\n" "otel-l1"   "$(human $L1_RX)"  "$(human $L1_TX)"
  printf "  %-9s %15s %15s\n" "otel-l2"   "$(human $L2_RX)"  "$(human $L2_TX)"
  echo
  echo "  Bytes l1 -> l2 (HTTP) ≈ otel-l2.rx = $(human $L2_RX)  (= ${L2_RX} B)"
  echo
  echo "  spans recebidos no l1:  $L1_RECEIVED"
  echo "  spans enviados pelo l1: $L1_SENT"
  echo "  spans recebidos no l2:  $L2_RECEIVED"
  echo "===================================================="
  echo
} | tee -a "$RESULTS_FILE"

echo "Resultado anexado em: $(pwd)/$RESULTS_FILE"
