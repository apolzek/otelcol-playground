FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/user-api .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates procps
COPY --from=build /out/user-api /usr/local/bin/user-api
RUN mkdir -p /data /etc/otelcol/auth && chown 65532:65532 /data /etc/otelcol/auth
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/user-api"]
