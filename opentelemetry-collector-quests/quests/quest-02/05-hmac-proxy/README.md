# 05 — Reverse proxy HMAC com proteção contra replay

## Ideia

O collector **não fica exposto**. Na frente dele, um reverse-proxy escrito em Go (`hmac-proxy/`) recebe o tráfego público e valida cada requisição assinada com **HMAC-SHA256**.

Cada cliente tem um par `(key_id, secret)`. Em cada request OTLP/HTTP, o cliente envia:

```
X-Sig-KeyId:    abc123
X-Sig-Timestamp: 1714400000           # epoch seconds, deve estar dentro de skew (±300s)
X-Sig-Nonce:    <16 bytes hex>        # único — anti-replay
X-Sig:          base64(HMAC_SHA256(secret, "{ts}\n{nonce}\n{method}\n{path}\n{body_sha256}"))
```

O proxy:
1. Verifica skew (timestamp atual ±300s).
2. Verifica que o nonce não foi usado nas últimas N janelas (cache em memória + opcional Redis).
3. Recalcula HMAC e compara com `subtle.ConstantTimeCompare`.
4. Se válido, encaminha para `localhost:4318` (collector OTLP/HTTP) com header `X-Tenant-Id` injetado.
5. O collector escuta apenas em `127.0.0.1` — nunca em interface pública.

Existe API admin no proxy para gerenciar keys (criar, revogar, rotacionar).

## Por que essa abordagem

- **Sem segredo trafegado**: o `secret` nunca vai na rede. Mesmo TLS comprometido em algum ponto não revela credencial reusável.
- **Anti-replay nativo** via nonce + timestamp — defesa contra captura+reenvio.
- **Custo determinístico**: HMAC-SHA256 é ~1µs por request. Comparado a bcrypt/JWT/RSA, é o mais rápido com auth real.
- **Body integrity**: assinatura cobre `sha256(body)` — qualquer mutação no caminho invalida.

## Trade-offs

- Cliente precisa implementar lógica de assinatura — não há SDK OpenTelemetry pronto. Você precisa fornecer biblioteca ou middleware. Para clientes não controlados, prefira abordagens 1/2/4.
- gRPC é mais complicado: o proxy precisa reescrever streams, e o body de gRPC é binário. Implementação aqui só faz **OTLP/HTTP**. Para gRPC, prefira mTLS (abordagem 3) ou bearer (1).
- Estado de nonces precisa ser compartilhado entre réplicas do proxy — neste PoC, cache em memória local; em produção, Redis com TTL = janela do skew.

## Layout

```
05-hmac-proxy/
├── docker-compose.yml
├── otel-collector-config.yaml
└── hmac-proxy/
    ├── go.mod
    ├── main.go
    ├── client_example.go      (helper para os SDKs)
    └── Dockerfile
```

## Como rodar

```bash
docker compose up --build

# Cria key:
curl -X POST http://localhost:8082/admin/keys \
  -H 'X-Admin-Key: change-me-admin-key' \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"tenant-a"}'
# Resposta: { "key_id":"...", "secret":"...", "tenant_id":"tenant-a" }

# Manda telemetria (helper Python):
python <<'PY'
import os, time, hmac, hashlib, base64, secrets, requests, json
KEY_ID = os.environ["KEY_ID"]; SECRET = os.environ["SECRET"]
body = json.dumps({"resourceSpans":[]}).encode()
ts = str(int(time.time())); nonce = secrets.token_hex(16); method = "POST"; path = "/v1/traces"
body_hash = hashlib.sha256(body).hexdigest()
msg = f"{ts}\n{nonce}\n{method}\n{path}\n{body_hash}".encode()
sig = base64.b64encode(hmac.new(SECRET.encode(), msg, hashlib.sha256).digest()).decode()
r = requests.post("http://localhost:4318/v1/traces", data=body, headers={
  "Content-Type":"application/json",
  "X-Sig-KeyId":KEY_ID, "X-Sig-Timestamp":ts, "X-Sig-Nonce":nonce, "X-Sig":sig,
})
print(r.status_code, r.text)
PY
```

## Segurança

- Skew window: 300s. Diminua se relógios forem confiáveis (NTP estrito → 30s).
- Nonce store: 30s TTL > 2x skew. Cache LRU 1M entradas (~64MB) por proxy.
- Assinatura cobre method/path/body — protege contra request smuggling e tampering.
- Secret armazenado com bcrypt no DB, **mas o proxy precisa do plaintext para verificar HMAC**. Solução: cache em memória após primeira leitura, e DB com KMS-wrap. Neste PoC, secret em texto na DB com permissão restritiva (use sempre disco cifrado).
- Constant-time comparison obrigatória — `subtle.ConstantTimeCompare`.
- Rate-limit por `key_id` na frente da verificação HMAC para mitigar DoS por requests inválidos.

## Performance

- HMAC-SHA256 verify: ~500ns. Lookup do secret no cache: ~50ns. Total: ~1µs.
- Throughput: limite é a network e o collector downstream, não a auth.
- Footprint do proxy: ~30MB RSS, single binary Go.
