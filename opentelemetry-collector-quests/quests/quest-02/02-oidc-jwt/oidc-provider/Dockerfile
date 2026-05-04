FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/oidc-provider .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/oidc-provider /usr/local/bin/oidc-provider
USER 65532:65532
EXPOSE 9000
ENTRYPOINT ["/usr/local/bin/oidc-provider"]
