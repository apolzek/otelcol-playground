# 04 — Basic Auth (htpasswd) com API Go de provisionamento

## Ideia

A extensão [`basicauth`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/basicauthextension) do collector-contrib valida `Authorization: Basic <base64(user:pass)>` contra um arquivo `htpasswd` (formato Apache, hashes bcrypt/sha/crypt). Múltiplos usuários no mesmo arquivo, nativamente.

A pasta inclui uma **API Go** (`user-api/`) que gerencia o `htpasswd`:
- `POST /users` — cria usuário (gera senha aleatória, devolve uma vez).
- `DELETE /users/{name}` — remove.
- `POST /users/{name}/rotate` — gera nova senha.
- A cada mutação, regrava `htpasswd` atomicamente e envia `SIGHUP` ao collector.

Bcrypt é usado por padrão (cost 12). O arquivo segue o formato esperado pela extension.

## Por que essa abordagem

- **Zero dependência exótica**: clientes HTTP da terra inteira sabem fazer Basic Auth. Excelente para integrar sistemas legados que não falam JWT/OIDC.
- **Multi-usuário nativo**: um único arquivo, várias identidades. Diferente da abordagem 1 (bearer com `tokens` list), aqui cada credencial tem **identidade legível** (`user`).
- **Mais simples que OIDC, mais identificável que bearer puro**: ideal quando você precisa saber "quem mandou" sem montar OIDC.

## Trade-offs

- Cada request paga **um bcrypt verify** — esse é caro de propósito (~50–100ms a cost 12). Resultado: throughput por core fica em ~10–20 req/s **se não houver cache**. **A `basicauth` faz cache em memória dos pares user/pass aprovados**, então só a primeira requisição de cada par paga o custo. Para SDKs OpenTelemetry com long-lived connections, isso é ótimo. Para clientes que abrem conexão nova a cada request, é fatal — diminua o cost para 8 ou troque para `argon2id` se a extension permitir, ou mude para abordagem 1/2.
- Basic Auth manda credencial em todo request (mesmo cifrado por TLS) — vazamento de TLS = vazamento de senhas reusáveis. Mitigação: TTL curto, rotação frequente.
- Não há expiração nativa por entrada — a API Go enforça TTL via remoção programada.

## Layout

```
04-basic-auth/
├── docker-compose.yml
├── otel-collector-config.yaml
├── htpasswd               (vazio inicialmente; gerenciado pela API)
└── user-api/
    ├── go.mod
    ├── main.go
    └── Dockerfile
```

## Como rodar

```bash
docker compose up --build

# Cria usuário:
curl -X POST http://localhost:8081/users \
  -H 'X-Admin-Key: change-me-admin-key' \
  -H 'Content-Type: application/json' \
  -d '{"name":"app-a","ttl_hours":720}'
# Resposta: { "name":"app-a", "password":"...", "expires_at":"..." }

# Manda telemetria:
curl http://localhost:4318/v1/traces \
  -u "app-a:<password>" \
  -H 'Content-Type: application/json' \
  -d '{"resourceSpans":[]}'
```

## Segurança

- bcrypt cost 12 (default da `bcrypt` do Go). Suficiente para 2026.
- Senhas geradas com 32 bytes random hex — não dá para brute-force.
- API Go exige `X-Admin-Key` (TLS obrigatório no fronting).
- O `htpasswd` mora em volume compartilhado **read-only** para o collector e read-write para a API. Permissão `0600`.
- Em K8s: secret separado para o htpasswd, mounted no collector. Use `reloader` ou a extension `file_storage` com watch.

## Performance

- Primeira requisição de um par user/pass: ~50ms (bcrypt). Subsequentes (cache da extension): <100ns.
- Cache da `basicauth` é por-processo — múltiplas réplicas re-pagam o custo da primeira request cada uma. Use `consistent hash` no LB (anotação no Service / NLB target group attributes) para grudar cliente em pod.
- Para volumes muito altos, considere mover do bcrypt para SHA-256 com salt longo (menos seguro contra brute-force offline mas igual contra brute-force online — TLS + rate-limit cobre o resto).
