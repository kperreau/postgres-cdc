.PHONY: build test bench lint clean run

BINARY := cdc
PKG := ./...
LDFLAGS := -s -w

build:
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/cdc

run: build
	./bin/$(BINARY) -config config.yaml

test:
	go test -race -count=1 $(PKG)

bench:
	go test -bench=. -benchmem -count=3 $(PKG)

lint:
	golangci-lint run $(PKG)

clean:
	rm -rf bin/

tidy:
	go mod tidy

# Integration tests require Docker
test-integration:
	go test -race -tags=integration -count=1 $(PKG)
