#!/usr/bin/env bash
#
# Run the heal-time measurement against a remote cluster (milestone 11). Same
# scenario as `make measure`'s TestMeasureHealTime, but the cluster is already
# running and the victim's disk is wiped through KAVO_WIPE_CMD.
#
# Usage:
#   cp deploy/cluster.env.example deploy/cluster.env   # edit addresses + wipe
#   ./scripts/measure-remote.sh deploy/cluster.env
#
# Docker dev cluster smoke test (after `make up`):
#   KAVO_WIPE_CMD='docker exec deploy-{id}-1 sh -c "rm -rf /data/chunks/*/*"' \
#     ./scripts/measure-remote.sh deploy/cluster.env
set -euo pipefail

envfile=${1:-deploy/cluster.env}
[[ -f "$envfile" ]] || { echo "missing config: $envfile"; exit 1; }

# shellcheck disable=SC1090
set -a
source "$envfile"
set +a

: "${KAVO_WIPE_CMD:?set KAVO_WIPE_CMD in $envfile (see deploy/README.md)}"
export KAVO_WIPE_CMD
export KAVO_MEASURE_VICTIM=${KAVO_MEASURE_VICTIM:-n6}

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

echo "→ checking cluster"
./scripts/cluster-check.sh "$envfile"

echo "→ remote heal measurement (victim ${KAVO_MEASURE_VICTIM})"
go test ./test -run TestMeasureRemoteHealTime -measure -measure.remote -v -timeout 3600s "${@:2}"
