FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/hmac-proxy .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/hmac-proxy /usr/local/bin/hmac-proxy
USER 65532:65532
EXPOSE 4318 8082
ENTRYPOINT ["/usr/local/bin/hmac-proxy"]
