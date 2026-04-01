.PHONY: build test bench lint clean run update docker-build docker-push

# Docker Hub (override tag or full ref as needed)
IMAGE ?= kperreau/postgres-cdc:latest

BINARY := cdc
PKG := ./...
LDFLAGS := -s -w

build:
	@go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/cdc

run: build
	@./bin/$(BINARY) -config config.yaml

test:
	@go test -race -count=1 $(PKG)

bench:
	@go test -bench=. -benchmem -count=3 $(PKG)

lint:
	@golangci-lint run $(PKG)

clean:
	@rm -rf bin/

tidy:
	@go mod tidy
	@gofumpt -l -w .

update:
	@go get -u ./...

docker-build:
	@docker build -t $(IMAGE) .

docker-push: docker-build
	@docker push $(IMAGE)

up:
	@docker compose -f docker-compose.yaml -p postgres-cdc up -d

down:
	@docker compose -f docker-compose.yaml -p postgres-cdc down

# Integration tests require Docker
test-integration:
	@go test -race -tags=integration -count=1 $(PKG)
