#!/usr/bin/env bash
#
# Run the S3 benchmarks against a remote cluster (milestone 11: numbers over a
# real network). Same tests as `make bench`, but only the gateway path — internal
# API benchmarks still start a coordinator in-process and stay local.
#
# Usage:
#   cp deploy/cluster.env.example deploy/cluster.env   # edit addresses
#   ./scripts/cluster-check.sh deploy/cluster.env
#   ./scripts/bench-remote.sh deploy/cluster.env
#
# Optional: install MinIO warp and set WARP=1 to run the outside-client numbers too.
set -euo pipefail

envfile=${1:-deploy/cluster.env}
[[ -f "$envfile" ]] || { echo "missing config: $envfile"; exit 1; }

# shellcheck disable=SC1090
set -a
source "$envfile"
set +a

endpoint=${KAVO_BENCH_ENDPOINT:?set KAVO_BENCH_ENDPOINT in $envfile}
export KAVO_BENCH_ENDPOINT
export KAVO_BENCH_KEY=${KAVO_ACCESS_KEY:-kavo}
export KAVO_BENCH_SECRET=${KAVO_SECRET_KEY:-kavosecret}

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

echo "→ checking cluster"
./scripts/cluster-check.sh "$envfile"

echo "→ S3 benchmarks against $endpoint"
go test ./internal/s3 -run XXX -bench . -timeout 1800s "$@"

if [[ "${WARP:-}" == 1 ]] && command -v warp >/dev/null; then
	host=${endpoint#http://}
	host=${host#https://}
	echo "→ warp put/get (host $host)"
	warp put --host "$host" --access-key "$KAVO_BENCH_KEY" --secret-key "$KAVO_BENCH_SECRET" \
		--obj.size 4KiB --concurrent 8 --duration 30s
	warp get --host "$host" --access-key "$KAVO_BENCH_KEY" --secret-key "$KAVO_BENCH_SECRET" \
		--obj.size 4KiB --concurrent 8 --duration 30s
	warp put --host "$host" --access-key "$KAVO_BENCH_KEY" --secret-key "$KAVO_BENCH_SECRET" \
		--obj.size 64MiB --concurrent 4 --duration 30s
	warp get --host "$host" --access-key "$KAVO_BENCH_KEY" --secret-key "$KAVO_BENCH_SECRET" \
		--obj.size 64MiB --concurrent 4 --duration 30s
else
	echo "(set WARP=1 and install warp to run the outside-client suite too)"
fi
