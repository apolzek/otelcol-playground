# ---- build stage ----
FROM golang:1.26 AS build

ARG TELEMETRYGEN_VERSION=v0.154.0
ENV CGO_ENABLED=0 GOOS=linux

# Build telemetrygen (the actual load generator) pinned to the collector version.
RUN go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@${TELEMETRYGEN_VERSION}

WORKDIR /src
COPY go.mod ./
# (no external deps — go.sum is empty by design)
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/loadgen .

# ---- runtime stage ----
FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /go/bin/telemetrygen /usr/local/bin/telemetrygen
COPY --from=build /out/loadgen /usr/local/bin/loadgen
USER app

ENV TELEMETRYGEN_BIN=/usr/local/bin/telemetrygen \
    COLLECTOR_METRICS_URL=http://otelcol:8888/metrics \
    LISTEN_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/loadgen"]
