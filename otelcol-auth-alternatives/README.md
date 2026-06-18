# OTel Collector público — 5 estratégias de auth

Cinco abordagens distintas para expor um **OpenTelemetry Collector v0.151.0** publicamente na internet rejeitando fontes desconhecidas. Cada pasta é independente e roda com `docker compose up`.

| # | Pasta | Mecanismo | Identifica cliente? | Custo por req | Rotação | Quando usar |
|---|-------|-----------|:-------------------:|---------------|---------|-------------|
| 1 | [`01-bearer-token`](01-bearer-token/)   | Bearer estático + API Go + hot-reload  | parcial (token→tenant na API) | <1µs   | API + SIGHUP   | volume alto, integração simples |
| 2 | [`02-oidc-jwt`](02-oidc-jwt/)           | OIDC + JWT (oidcauth)                  | sim (claims)                  | ~30µs  | TTL curto      | SaaS multi-tenant, defense in depth |
| 3 | [`03-mtls`](03-mtls/)                   | mTLS com CA própria                    | sim (Subject/SAN)             | 0*     | reissue + TTL  | máxima segurança, gRPC long-lived |
| 4 | [`04-basic-auth`](04-basic-auth/)       | Basic Auth (htpasswd) + API Go         | sim (username)                | 50–100ms** | API + SIGHUP   | legado, baixa cardinalidade |
| 5 | [`05-hmac-proxy`](05-hmac-proxy/)       | Reverse-proxy HMAC + replay protection | sim (key_id→tenant)           | ~1µs   | rotate via API | integridade de body, anti-replay |

\* Custo no handshake apenas; conexões long-lived amortizam.
\*\* Primeira requisição. Subsequentes ficam <100ns no cache da extension.

## Comparação rápida

```
                                 simplicidade ─→ complexidade
   bearer token  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ HMAC proxy
                                                                ↑
   ╔═════ identificação fraca ═════╗   ╔═════ identificação forte ═════╗
   bearer token        basic auth      OIDC/JWT       mTLS        HMAC
```

## Arquivos top-level

- [`public.md`](public.md) — exposição em Kubernetes: MetalLB, Cloudflare Tunnel, Ingress, Cloud LB, Istio. Comparação por requisito.

## Pré-requisitos

- Docker + Docker Compose v2
- Para a abordagem 3 (mTLS): `openssl`
- Versão fixa do collector: `otel/opentelemetry-collector-contrib:0.151.0` (não usar `latest`)

## Convenções

- Todos os Go services rodam como UID `65532` (nonroot do distroless), bind em portas não privilegiadas.
- Admin APIs usam header `X-Admin-Key` (env `ADMIN_KEY`) — em produção, troque por OIDC ou mTLS na própria API admin.
- Memory limiter sempre é o **primeiro** processor do pipeline.
- TLS é responsabilidade da camada de exposição (ver `public.md`) — exceto na abordagem 3, onde o collector termina TLS direto.
