FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/token-api .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates procps   # procps fornece pkill
COPY --from=build /out/token-api /usr/local/bin/token-api
RUN mkdir -p /data /etc/otelcol/tokens && chown 65532:65532 /data /etc/otelcol/tokens
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/token-api"]
