.PHONY: build test bench lint up down etcd clean

build:
	go build ./...

# Tests that touch metadata need a real etcd; faking it would test the fake.
# Starting it here is idempotent and keeps `make test` a single command.
test: etcd
	go test -race ./...

# Fixed iteration counts, not a duration: a 64 MB write takes 160 ms, so letting
# Go pick would spend minutes proving what ten passes already show. Results and
# what they mean: docs/benchmarks.md.
bench: etcd
	go test ./internal/... -run XXX -bench . -benchtime 10x -timeout 1800s

etcd:
	@docker compose -f deploy/compose.yaml up -d --wait etcd

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:" && gofmt -l . && exit 1)

up:
	docker compose -f deploy/compose.yaml up -d --build --wait

down:
	docker compose -f deploy/compose.yaml down -v

clean:
	rm -rf bin/
