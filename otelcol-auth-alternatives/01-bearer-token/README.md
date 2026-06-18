# 01 — Bearer Token com API Go e hot-reload

## Ideia

O collector usa a extensão [`bearertokenauth`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/bearertokenauthextension) para validar o header `Authorization: Bearer <token>` em **toda** requisição OTLP. A lista de tokens válidos vive em `tokens.yaml`, montado como volume no container.

A **API Go** (`token-api/`) gerencia o ciclo de vida desses tokens:
- `POST /tenants` — registra um cliente (nome, email, descrição) e devolve um token novo (UUID v7 + bytes aleatórios, 256 bits).
- `DELETE /tenants/{id}` — revoga.
- `GET /tenants` — lista (sem expor o token em plain — só hash).
- A cada mutação, regrava `tokens.yaml` e envia `SIGHUP` para o processo do collector, que recarrega config sem dropar conexões in-flight.

Tokens são armazenados em SQLite com **hash bcrypt** — o token plaintext só é mostrado uma vez no momento da criação. O arquivo `tokens.yaml` lido pelo collector contém os tokens em texto plano (necessário para validação O(1)), mas mora em volume privado lido só pelo collector.

## Por que essa abordagem

- **Simples de operar**: um endpoint HTTP cria token, cliente cola no SDK do OpenTelemetry como `OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer xxx`. Sem CA, sem OIDC, sem proxy.
- **Performance**: validação é uma comparação de string em memória (set lookup O(1) na extensão `bearertokenauth`). Sem round-trip para banco, sem verificação de assinatura.
- **Revogação imediata** após o reload (poucos segundos).

## Trade-offs

- Tokens são **bearer** — quem tem o token pode usar. Trate como senha. Use TLS no ingress (obrigatório).
- O `tokens.yaml` precisa estar em sync com o collector. Se a API Go cair durante a regravação, o collector pode ficar com tokens desatualizados — mitigação: escrita atômica (`os.Rename`) e reload best-effort.
- Não há expiração nativa na extensão. A API expira tokens em `expires_at`, e a rotina de write filtra os expirados antes de gerar o `tokens.yaml`. Um cron (a cada 60s) regrava o arquivo para garantir que tokens expirem mesmo sem mutação na API.

## Layout

```
01-bearer-token/
├── docker-compose.yml
├── otel-collector-config.yaml
├── tokens.yaml             (gerado pela API; exemplo inicial vazio)
└── token-api/
    ├── go.mod
    ├── main.go
    └── Dockerfile
```

## Como rodar

```bash
docker compose up --build
# Em outro terminal:
curl -X POST http://localhost:8080/tenants \
  -H 'X-Admin-Key: change-me-admin-key' \
  -H 'Content-Type: application/json' \
  -d '{"name":"meu-app","email":"dev@example.com","ttl_hours":720}'
# Resposta: { "id": "...", "token": "otel_..." }   <-- guarde, não aparece de novo

# Manda um span:
curl -v http://localhost:4318/v1/traces \
  -H "Authorization: Bearer otel_..." \
  -H 'Content-Type: application/json' \
  -d '{"resourceSpans":[]}'
# Sem o header: 401. Com header inválido: 401. Com header válido: 200.
```

## Segurança

- A API exige `X-Admin-Key` (env `ADMIN_KEY`) para criar/revogar — sem isso, qualquer um geraria tokens. Em produção, autentique a API com SSO.
- TLS é responsabilidade da camada de exposição (ver `public.md`). Bearer tokens **não** podem trafegar em clear-text.
- O processo da API Go envia `SIGHUP` ao PID do collector via signal compartilhado — em K8s, faça via shared process namespace no pod ou via sidecar exec.
- O DB SQLite tem permissões `0600`. Backup criptografado.
- Rotação: o cliente chama `POST /tenants/{id}/rotate` para receber novo token; o antigo continua válido por `grace_period` (default 1h) para evitar downtime.

## Performance

- Validação por requisição: ~100ns (lookup em `map[string]struct{}`).
- Reload de config: o collector continua aceitando tráfego; só a config nova passa a vigorar após o `SIGHUP`. Sem dropping.
- Throughput esperado: igual ao collector sem auth (a auth não é o gargalo). Limitado por CPU do collector e network.
