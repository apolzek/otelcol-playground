# 03 — mTLS com CA própria em Go

## Ideia

O collector exige certificado de cliente assinado por uma **CA privada** sob seu controle. No `tls` do receiver OTLP:

```yaml
tls:
  cert_file: /etc/otelcol/tls/server.crt
  key_file:  /etc/otelcol/tls/server.key
  client_ca_file: /etc/otelcol/tls/ca.crt   # <- exige client cert assinado por essa CA
```

Toda conexão TCP precisa apresentar um certificado válido **antes** de qualquer byte HTTP/2 ou gRPC ser trocado. Quem não tem cert é rejeitado no handshake — **a auth acontece no kernel/TLS, não no aplicativo**, então o overhead é zero por requisição (paga só no handshake e na keep-alive estabelecida).

A pasta inclui:
- `gen-certs.sh` — script para gerar CA + cert do servidor (one-shot, dev).
- `ca-service/` — serviço HTTP Go que **emite certificados de cliente** sob demanda. Recebe um CSR, valida via API key admin, e devolve o cert assinado.

## Por que essa abordagem

- **Mais forte cripto-identidade**: cliente prova posse de chave privada — não dá para "vazar um token" porque o segredo é a chave privada que nunca trafega.
- **Custo por requisição zero** depois do handshake — ideal para alta vazão e long-lived gRPC connections (caso típico de SDKs OpenTelemetry).
- **Revogação via CRL ou short-lived certs**: emita certs com TTL de horas e renove via API. Sem CRL distribuída, o "revoke" é "não emitir novo cert".

## Trade-offs

- Distribuição inicial de certificados é dor: cada cliente novo precisa gerar par de chaves, mandar CSR, instalar cert. Mais complicado que copiar um token.
- Rotação automática exige cliente capaz de renovar — em Kubernetes, [`cert-manager`](https://cert-manager.io) com `CertificateRequest` resolve. Fora do K8s, o cliente precisa do daemon de rotação.
- Preservar IP/identidade do cliente atrás de proxies L7 (Cloudflare) é difícil; ver `public.md` — para mTLS, prefira L4 passthrough (MetalLB / NLB).
- Não há header com identidade do cliente para o pipeline do collector — você precisa extrair do `Common Name` ou `SAN` no cert via processor custom (ou o próprio collector com `tls.client_auth_type: RequireAndVerifyClientCert` expõe o subject ao auth context, dependendo da versão).

## Layout

```
03-mtls/
├── docker-compose.yml
├── otel-collector-config.yaml
├── gen-certs.sh
├── tls/                       (gerada pelo script — gitignore)
└── ca-service/
    ├── go.mod
    ├── main.go
    └── Dockerfile
```

## Como rodar

```bash
# 1. Gera CA + server cert
./gen-certs.sh

# 2. Sobe collector + ca-service
docker compose up --build

# 3. Cliente: gera chave + CSR
openssl req -newkey rsa:2048 -nodes -keyout client.key \
  -subj "/CN=app-a/O=tenant-a" -out client.csr

# 4. Pega cert assinado
curl -X POST http://localhost:9100/sign \
  -H 'X-Admin-Key: change-me-admin-key' \
  -H 'Content-Type: application/x-pem-file' \
  --data-binary @client.csr \
  -o client.crt

# 5. Manda telemetria com mTLS
curl --cert client.crt --key client.key --cacert tls/ca.crt \
  https://localhost:4318/v1/traces \
  -H 'Content-Type: application/json' \
  -d '{"resourceSpans":[]}'
```

## Segurança

- **Chave da CA é o segredo mais crítico do sistema**. Em produção, KMS / HSM ou Vault PKI.
- TTL curto nos certs de cliente (default 24h no `ca-service`). Revogação por expiração + recusa de novo signing.
- O `ca-service` autentica o solicitante via `X-Admin-Key` — em produção, OIDC ou mTLS na própria API de signing.
- `client_auth_type` deve ser explicitamente `RequireAndVerifyClientCert` (default da extension TLS quando `client_ca_file` está setado).
- Considere `trusted_ca_file` separada para diferentes "tiers" de cliente, e use SANs para distinguir.

## Performance

- Handshake mTLS adiciona ~5–15ms na primeira conexão. Imperceptível para SDKs OpenTelemetry, que mantêm conexão gRPC aberta (HTTP/2 multiplexada).
- Throughput sustentado: igual ao TLS puro (auth feita no handshake, payload é stream cifrado padrão).
- Em volumes altos, prefira ECDSA P-256 (~3x mais rápido que RSA-2048 no handshake).
