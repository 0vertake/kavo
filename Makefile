.PHONY: build test lint up down etcd clean

build:
	go build ./...

# Tests that touch metadata need a real etcd; faking it would test the fake.
# Starting it here is idempotent and keeps `make test` a single command.
test: etcd
	go test -race ./...

etcd:
	@docker compose -f deploy/compose.yaml up -d --wait etcd

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:" && gofmt -l . && exit 1)

up:
	docker compose -f deploy/compose.yaml up -d

down:
	docker compose -f deploy/compose.yaml down -v

clean:
	rm -rf bin/
