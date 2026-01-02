.PHONY: build test test-short lint fmt up down loadtest reconcile

build:
	go build -o bin/kinesis ./cmd/kinesis

# Full suite; integration tests start Postgres/Redis via testcontainers (needs Docker).
test:
	go test -race -count=1 ./...

# Unit tests only.
test-short:
	go test -short -count=1 ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .

up:
	docker compose up --build -d

down:
	docker compose down -v

reconcile:
	docker compose exec api /kinesis reconcile

loadtest:
	k6 run loadtest/transfers.js
