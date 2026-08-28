#!/usr/bin/env bash
#
# Start one kavod node for a multi-host cluster.
#
# Usage:
#   ./scripts/kavod-node.sh n1 deploy/cluster.env
#   KAVO_NODE=n1 ./scripts/kavod-node.sh    # uses deploy/cluster.env
#
# Build the binary first: go build -o kavod ./cmd/kavod
# Pass KAVOD=/path/to/kavod to use a built binary outside PATH.
set -euo pipefail

id=${1:-${KAVO_NODE:?pass node id (n1..n6) as the first argument or set KAVO_NODE}}
envfile=${2:-deploy/cluster.env}
[[ -f "$envfile" ]] || { echo "missing config: $envfile (copy deploy/cluster.env.example)"; exit 1; }

# shellcheck disable=SC1090
source "$envfile"

upper=$(echo "$id" | tr '[:lower:]' '[:upper:]')
host_var="KAVO_${upper}_HOST"
port_var="KAVO_${upper}_INTERNAL_PORT"
s3_var="KAVO_${upper}_S3_PORT"
data_var="KAVO_${upper}_DATA"

host=${!host_var:?set $host_var in $envfile}
internal_port=${!port_var:?set $port_var in $envfile}
s3_port=${!s3_var:?set $s3_var in $envfile}
data=${!data_var:?set $data_var in $envfile}

advertise="${host}:${internal_port}"
bin=${KAVOD:-kavod}

mkdir -p "$data"

exec "$bin" \
	-id "$id" \
	-addr "0.0.0.0:${internal_port}" \
	-advertise "$advertise" \
	-s3 "0.0.0.0:${s3_port}" \
	-data "$data" \
	-etcd "${KAVO_ETCD:?set KAVO_ETCD in $envfile}" \
	-cluster "${KAVO_CLUSTER:-/kavo}" \
	-access-key "${KAVO_ACCESS_KEY:-kavo}" \
	-secret-key "${KAVO_SECRET_KEY:-kavosecret}"
