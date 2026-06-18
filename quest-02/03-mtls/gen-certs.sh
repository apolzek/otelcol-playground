#!/usr/bin/env bash
# Gera CA + server cert para o PoC.
# Em produção, use Vault PKI ou cert-manager. Esta CA é apenas para dev/local.
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p tls

[ -f tls/ca.key ] || openssl genrsa -out tls/ca.key 4096
[ -f tls/ca.crt ] || openssl req -x509 -new -nodes -key tls/ca.key -days 3650 \
  -subj "/CN=otel-collector-ca/O=acme/OU=observability" -out tls/ca.crt

[ -f tls/server.key ] || openssl genrsa -out tls/server.key 2048
openssl req -new -key tls/server.key \
  -subj "/CN=otel-collector/O=acme" \
  -addext "subjectAltName=DNS:localhost,DNS:otel-collector,IP:127.0.0.1" \
  -out tls/server.csr

cat > tls/server.ext <<EOF
subjectAltName=DNS:localhost,DNS:otel-collector,IP:127.0.0.1
extendedKeyUsage=serverAuth
EOF

openssl x509 -req -in tls/server.csr -CA tls/ca.crt -CAkey tls/ca.key \
  -CAcreateserial -days 825 -extfile tls/server.ext -out tls/server.crt

chmod 600 tls/*.key
echo "tls/ contém ca.crt, ca.key, server.crt, server.key"
