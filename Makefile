.PHONY: build test bench bench-remote measure measure-remote demo lint up down etcd clean

build:
	go build ./...

# Tests that touch metadata need a real etcd; faking it would test the fake.
# Starting it here is idempotent and keeps `make test` a single command.
test: etcd
	go test -race ./...

# Go's own benchtime, not a fixed ten passes. Ten was cheap and wrong: the first
# request through the AWS SDK pays for credential resolution and a connection, and
# at ten iterations that fixed cost made a 2 ms GET look like an 8 ms one. Results
# and what they mean: docs/benchmarks.md.
bench: etcd
	go test ./internal/... -run XXX -bench . -timeout 1800s

# Same S3 gateway benchmarks as `make bench`, against a cluster reachable over the
# network. Set KAVO_BENCH_ENDPOINT (and optionally KAVO_BENCH_KEY/SECRET).
# Runbook: deploy/README.md
bench-remote:
	@test -n "$$KAVO_BENCH_ENDPOINT" || (echo "set KAVO_BENCH_ENDPOINT; see deploy/README.md" && exit 1)
	go test ./internal/s3 -run XXX -bench . -timeout 1800s

# The cluster-level numbers a benchmark harness cannot express: how long a heal
# takes, how much a join moves, and what a node's memory does under a
# multi-gigabyte object. They print rather than assert, so `make test` skips them
# and this runs them. Writes several GB and takes a few minutes.
measure: etcd
	go test ./test -run TestMeasure -measure -v -timeout 3600s

# Heal time over a real network cluster. Set KAVO_N1_HOST … KAVO_N6_* and
# KAVO_WIPE_CMD (see deploy/README.md). Runbook: ./scripts/measure-remote.sh
measure-remote:
	@test -n "$$KAVO_N1_HOST" || (echo "set KAVO_N1_HOST or source deploy/cluster.env; see deploy/README.md" && exit 1)
	@test -n "$$KAVO_WIPE_CMD" || (echo "set KAVO_WIPE_CMD; see deploy/README.md" && exit 1)
	go test ./test -run TestMeasureRemote -measure -measure.remote -v -timeout 3600s

# Six nodes on this host, an object, one of its owners killed with SIGKILL, and
# redundancy coming back on its own — every step checked rather than narrated. Real
# processes rather than containers, because a demo about fsync should not run on top
# of a virtual machine that absorbs it (docs/benchmarks.md). Needs the aws CLI.
demo: etcd
	@./scripts/demo.sh

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
