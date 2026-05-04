FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ca-service .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/ca-service /usr/local/bin/ca-service
USER 65532:65532
EXPOSE 9100
ENTRYPOINT ["/usr/local/bin/ca-service"]
