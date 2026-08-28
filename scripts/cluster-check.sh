#!/usr/bin/env bash
#
# Check that every node in cluster.env is up and sees the full membership.
#
# Usage: ./scripts/cluster-check.sh [deploy/cluster.env]
set -euo pipefail

envfile=${1:-deploy/cluster.env}
[[ -f "$envfile" ]] || { echo "missing config: $envfile"; exit 1; }

# shellcheck disable=SC1090
source "$envfile"

want=0
ok=0
for n in 1 2 3 4 5 6; do
	id="n$n"
	host_var="KAVO_N${n}_HOST"
	port_var="KAVO_N${n}_INTERNAL_PORT"
	host=${!host_var:?}
	port=${!port_var:?}
	want=$((want + 1))

	url="http://${host}:${port}/cluster/members"
	if ! resp=$(curl -sf "$url" 2>/dev/null); then
		echo "$id: unreachable at $url"
		continue
	fi
	count=$(python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' <<<"$resp")
	if [[ "$count" -ne 6 ]]; then
		echo "$id: sees $count members, want 6"
		continue
	fi
	echo "$id: ok ($host:$port, 6 members)"
	ok=$((ok + 1))
done

if [[ "$ok" -ne "$want" ]]; then
	echo "cluster not ready: $ok/$want nodes healthy"
	exit 1
fi
echo "cluster ready"
