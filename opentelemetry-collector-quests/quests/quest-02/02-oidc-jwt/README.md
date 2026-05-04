# 02 — OIDC + JWT (oidcauth extension)

## Ideia

A extensão [`oidcauth`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/oidcauthextension) valida JWTs assinados por um issuer OIDC. O collector busca o `jwks_uri` do issuer e, em cada request, valida:
- assinatura RSA/ECDSA contra a JWK ativa,
- `iss` (issuer match),
- `aud` (audience match — `otel-collector`),
- `exp` (expiração),
- claims customizadas obrigatórias (ex.: `tenant_id`).

A pasta inclui um **provider OIDC mínimo em Go** (`oidc-provider/`) que:
- expõe `/.well-known/openid-configuration` e `/jwks` (chaves públicas).
- expõe `POST /token` (client credentials grant) — cliente autentica com `client_id` + `client_secret` e recebe JWT (RS256) com TTL curto (15min default).
- expõe API admin para registrar/revogar `client_id`s.

## Por que essa abordagem

- **Padrão de mercado**: OIDC/OAuth2 é o que SaaS sério usa. SDKs do OpenTelemetry têm helpers (`oauth2clientauth` extension no lado do cliente).
- **Tokens curtos + revogação por chave**: JWTs expiram em minutos; revogação imediata via rotação da JWK signing key.
- **Defense in depth**: o mesmo JWT pode ser validado por um gateway (Istio, Cloudflare Access) **antes** do collector — duplo check sem custo de redes adicionais.
- **Multi-tenant nativo**: claim `tenant_id` é propagada para o pipeline via processor `transform` para enriquecer spans/metrics com a origem.

## Trade-offs

- Validação de assinatura RSA é mais cara que comparação de string (~10–50µs por request). Insignificante para volumes normais; relevante a 100k req/s — nesse caso, prefira ECDSA P-256 ou Ed25519.
- Cliente precisa de mecanismo de refresh — adiciona complexidade no SDK. A extension `oauth2clientauth` cuida disso no lado client.
- Operar um issuer OIDC tem responsabilidades: rotação de chaves, JWKS cache, monitoring. Em produção real, considere usar **Keycloak / Dex / Auth0 / AWS Cognito** em vez do PoC desta pasta. O Go provider aqui é didático.

## Layout

```
02-oidc-jwt/
├── docker-compose.yml
├── otel-collector-config.yaml
└── oidc-provider/
    ├── go.mod
    ├── main.go
    └── Dockerfile
```

## Como rodar

```bash
docker compose up --build

# Registrar um cliente:
curl -X POST http://localhost:9000/admin/clients \
  -H 'X-Admin-Key: change-me-admin-key' \
  -H 'Content-Type: application/json' \
  -d '{"client_id":"app-a","tenant_id":"tenant-a"}'
# Resposta: { "client_id": "app-a", "client_secret": "..." }

# Pegar token:
curl -X POST http://localhost:9000/token \
  -d 'grant_type=client_credentials&client_id=app-a&client_secret=...'
# Resposta: { "access_token": "<jwt>", "token_type": "Bearer", "expires_in": 900 }

# Mandar telemetria:
curl http://localhost:4318/v1/traces \
  -H "Authorization: Bearer $JWT" \
  -H 'Content-Type: application/json' \
  -d '{"resourceSpans":[]}'
```

## Segurança

- Signing key RSA-2048 gerada no startup e persistida em volume com permissão `0600`. Em produção: KMS ou HSM.
- `kid` (key ID) na JWK + JWS header — permite rotação sem invalidar tokens em flight (chave antiga fica em JWKS por `grace_period`).
- Claims auditadas: `iat`, `exp`, `nbf`, `iss`, `aud`, `sub`, `tenant_id`. `aud` é validada estritamente pela extension.
- `client_secret` armazenado com bcrypt (mesmo flow da abordagem 1).
- TLS obrigatório no path do issuer — JWKS vai ser buscado pelo collector e qualquer MITM compromete tudo.

## Performance

- Validação JWT: ~30µs (cache de JWK + verificação RSA-PKCS1-v1.5).
- JWKS é cacheado pela `oidcauth` (default 5min). Rotação de chave: novos tokens só validam após o cache expirar — para rotação imediata, ajuste `cache_refresh_interval` baixo.
- Throughput: ~50k req/s por core para validação. Para >100k req/s, troque para ES256 / EdDSA.
