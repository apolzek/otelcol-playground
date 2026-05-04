#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Carga parametrizável: dispara N instâncias de telemetrygen em paralelo,
# cada uma representando um "service" diferente, com atributos sorteados de
# um pool de M valores aleatórios por chave.
#
# Tudo configurável via env. Pode ser chamado direto ou pelo run.sh dos
# cenários (que cuida da medição de bytes/spans antes/depois).
# ============================================================================

# ---- Parâmetros (sobrescreva via env) -------------------------------------
NUM_SERVICES="${NUM_SERVICES:-100}"           # quantos "services" simulados
ATTR_VALUES_PER_KEY="${ATTR_VALUES_PER_KEY:-3}" # quantos valores por chave
RANDOM_STRING_LEN="${RANDOM_STRING_LEN:-64}"  # tamanho de cada valor aleatório
DURATION="${DURATION:-5m}"
WORKERS="${WORKERS:-1}"                       # workers POR service
RATE_PER_SERVICE="${RATE_PER_SERVICE:-10}"    # traces/s POR worker POR service
SEED="${SEED:-42}"

# Lista de chaves de atributo de span (separadas por vírgula).
# Para cada chave geramos ATTR_VALUES_PER_KEY valores aleatórios, e cada
# service sorteia 1 dos valores e usa em TODOS os seus spans.
ATTR_KEYS="${ATTR_KEYS:-http.method,http.url,http.status_code,db.system,db.statement,user.id,host.name,k8s.pod.name,k8s.namespace,deployment.environment,cloud.region,exception.message}"

# Onde mandar os traces. Default: a network/hostname dos cenários.
NET="${NET:-bridge}"
TARGET_HOST="${TARGET_HOST:-otel-l1}"
TARGET_PORT="${TARGET_PORT:-4317}"
PROTO="${PROTO:-grpc}"                        # grpc | http

TG_IMAGE="${TG_IMAGE:-ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.151.0}"
NAME_PREFIX="${NAME_PREFIX:-tg-loadgen}"

# Quantos containers podem ficar Up simultaneamente (rate limit do daemon).
PARALLELISM="${PARALLELISM:-100}"

# ---- Sanidade --------------------------------------------------------------
case "$PROTO" in
  grpc) PROTO_FLAG="" ;;
  http) PROTO_FLAG="--otlp-http" ;;
  *) echo "PROTO inválido: $PROTO (use grpc ou http)" >&2; exit 1 ;;
esac

if ! docker network inspect "$NET" >/dev/null 2>&1; then
  echo "Network '$NET' não existe. Suba a stack do cenário antes." >&2
  exit 1
fi

# ---- Helpers ---------------------------------------------------------------
# bash RANDOM é sequencial e seedável — bom pra reproduzir runs idênticos.
RANDOM=$SEED

random_string() {
  local n="${1:-$RANDOM_STRING_LEN}"
  # subshell isola o `set +o pipefail` (head fecha o pipe e o tr morre com SIGPIPE).
  ( set +o pipefail; LC_ALL=C tr -dc 'A-Za-z0-9_-' </dev/urandom | head -c "$n" )
}

# ---- Pool de valores por chave --------------------------------------------
IFS=',' read -ra KEYS <<<"$ATTR_KEYS"
declare -A POOL
for k in "${KEYS[@]}"; do
  for i in $(seq 1 "$ATTR_VALUES_PER_KEY"); do
    POOL["$k|$i"]="$(random_string)"
  done
done

# ---- Resumo da carga ------------------------------------------------------
echo "=========================================="
echo "loadgen: $NUM_SERVICES services × $WORKERS workers × $RATE_PER_SERVICE/s"
echo "         duração $DURATION, alvo ${PROTO}://${TARGET_HOST}:${TARGET_PORT}"
echo "         ${#KEYS[@]} chaves × $ATTR_VALUES_PER_KEY valores ($RANDOM_STRING_LEN bytes cada)"
TOTAL_RPS=$(( NUM_SERVICES * WORKERS * RATE_PER_SERVICE ))
echo "         total alvo: ~${TOTAL_RPS} traces/s"
echo "=========================================="

# ---- Cleanup -----------------------------------------------------------
RUN_ID="$(date +%s)"
NAMES=()
trap 'echo "Limpando containers..."; for n in "${NAMES[@]}"; do docker rm -f "$n" >/dev/null 2>&1 || true; done' EXIT

# ---- Spawn ----------------------------------------------------------------
spawn_one() {
  local s="$1"
  local svc_name; svc_name="svc-$(printf '%03d' "$s")"
  local cname="${NAME_PREFIX}-${RUN_ID}-$(printf '%03d' "$s")"

  local -a cmd=(
    docker run -d --rm
    --name "$cname"
    --network "$NET"
    "$TG_IMAGE"
    traces
    "--otlp-endpoint=${TARGET_HOST}:${TARGET_PORT}"
    "--otlp-insecure"
    "--duration=$DURATION"
    "--workers=$WORKERS"
    "--rate=$RATE_PER_SERVICE"
    "--otlp-attributes=service.name=\"$svc_name\""
  )
  [[ -n "$PROTO_FLAG" ]] && cmd+=("$PROTO_FLAG")

  for k in "${KEYS[@]}"; do
    local pick=$(( (RANDOM % ATTR_VALUES_PER_KEY) + 1 ))
    cmd+=("--telemetry-attributes=$k=\"${POOL[$k|$pick]}\"")
  done

  "${cmd[@]}" >/dev/null
  echo "$cname"
}

INFLIGHT=0
for s in $(seq 1 "$NUM_SERVICES"); do
  cname=$(spawn_one "$s")
  NAMES+=("$cname")
  INFLIGHT=$(( INFLIGHT + 1 ))

  # rate limit no daemon
  if (( INFLIGHT >= PARALLELISM )); then
    sleep 0.05
    INFLIGHT=$(( INFLIGHT - 1 ))
  fi
done

echo "Spawn ok: ${#NAMES[@]} containers. Aguardando finalização..."

# ---- Wait -----------------------------------------------------------------
# `docker wait` retorna o exit code; ficamos aqui até todos saírem.
docker wait "${NAMES[@]}" >/dev/null

echo "Carga concluída."
