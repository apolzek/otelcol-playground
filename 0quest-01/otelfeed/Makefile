BIN := bin/otelfeed
PKG := ./cmd/otelfeed
SAMPLES ?= ../samples/otlp-json

.PHONY: all build vet run-http run-grpc collector collector-down tidy clean

all: build

tidy:
	go mod tidy

vet:
	go vet ./...

build:
	@mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BIN) $(PKG)

run-http: build
	$(BIN) --dir $(SAMPLES) --http http://localhost:4318 --workers 32

run-grpc: build
	$(BIN) --dir $(SAMPLES) --http "" --grpc localhost:4317 --grpc-insecure --workers 32

collector:
	docker compose -f deploy/docker-compose.yml up -d

collector-down:
	docker compose -f deploy/docker-compose.yml down

clean:
	rm -rf bin
