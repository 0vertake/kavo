#!/usr/bin/env bash
#
# The demo: a six-node cluster, an object, a node killed with SIGKILL while the
# object is being read, and redundancy coming back on its own.
#
# Real processes on this host rather than containers, for two reasons. A demo about
# durability should send a real SIGKILL to a real process, and Docker Desktop's
# virtual machine absorbs F_FULLFSYNC into a page cache nobody has promised anything
# about — measured in docs/benchmarks.md, where a containerised small write came out
# five times "faster" than the honest one.
#
# Every claim here is checked rather than narrated: the object is read back and its
# digest compared at each step, and redundancy is confirmed by asking each node
# whether it holds the chunk rather than by waiting a plausible number of seconds.
#
# Usage: make demo   (or ./scripts/demo.sh)
set -euo pipefail

nodes=6
size=${SIZE:-32}                 # MB, big enough to span several chunks
etcd=${KAVO_ETCD:-127.0.0.1:2379}
prefix=/kavo-demo
bucket=demo
key=holiday.bin
root=$(mktemp -d)
pids=()

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
step() { printf '\033[36m→\033[0m %s\n' "$*"; }
ok()   { printf '\033[32m✓\033[0m %s\n' "$*"; }

cleanup() {
	for pid in ${pids[@]+"${pids[@]}"}; do kill -9 "$pid" 2>/dev/null || true; done
	docker exec kavo-etcd etcdctl del --prefix "$prefix" >/dev/null 2>&1 || true
	rm -rf "$root"
}
trap cleanup EXIT

for tool in aws docker python3; do
	command -v "$tool" >/dev/null || { echo "demo needs $tool on PATH"; exit 1; }
done

export AWS_ACCESS_KEY_ID=kavo AWS_SECRET_ACCESS_KEY=kavosecret AWS_DEFAULT_REGION=us-east-1
s3() { aws --endpoint-url "http://127.0.0.1:9101" --no-progress "$@"; }
internal() { curl -sf "http://127.0.0.1:$((8100 + $1))$2"; }

# members is how many nodes node $1 can see, or 0 if it cannot be asked at all —
# which happens to any node this demo kills, and is a wait rather than an error.
members() {
	internal "$1" /cluster/members |
		python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null || echo 0
}

# holders prints the ids of the nodes that actually have the chunk, asking each one
# directly. This is the measurement the whole demo turns on: a manifest naming three
# nodes is a promise, and only the nodes can say whether it is kept.
holders() {
	local chunk=$1 held=()
	for i in $(seq 1 $nodes); do
		if curl -sf -I "http://127.0.0.1:$((8100 + i))/peer/chunks/$chunk" >/dev/null 2>&1; then
			held+=("n$i")
		fi
	done
	echo "${held[*]-}"
}

manifest() {
	docker exec kavo-etcd etcdctl get "$prefix/objects/$bucket/$key" --print-value-only
}

# placement is the nodes the committed manifest names, which is the object's
# promised redundancy as opposed to holders' measurement of the kept one.
placement() {
	manifest | python3 -c 'import json,sys; print(" ".join(json.load(sys.stdin)["Nodes"]))'
}

bold "Starting $nodes nodes"
go build -o "$root/kavod" ./cmd/kavod
for i in $(seq 1 $nodes); do
	# Intervals a person can watch. Everything else is the default, including W=2
	# of N=3 and the repair rate, so what the demo shows is the shipped behaviour
	# on a shorter clock.
	"$root/kavod" -id "n$i" -addr "127.0.0.1:$((8100 + i))" -s3 "127.0.0.1:$((9100 + i))" \
		-data "$root/n$i" -etcd "$etcd" -cluster "$prefix" \
		-repair-interval 2s -rebalance-interval 2s -lease-ttl 2s \
		>"$root/n$i.log" 2>&1 &
	pids+=($!)
	# Off the job table, so the shell does not narrate the kills at the end.
	disown
done

for i in $(seq 1 $nodes); do
	for _ in $(seq 100); do
		internal "$i" /cluster/members >/dev/null 2>&1 && break
		sleep 0.1
	done
done
until [ "$(members 1)" = "$nodes" ]; do sleep 0.2; done
ok "$nodes nodes, each seeing all $nodes"

bold "Storing a ${size}MB object"
dd if=/dev/urandom of="$root/$key" bs=1m count="$size" 2>/dev/null
want=$(python3 -c "import hashlib,sys; print(hashlib.md5(open(sys.argv[1],'rb').read()).hexdigest())" "$root/$key")
s3 s3 mb "s3://$bucket" >/dev/null 2>&1 || true
s3 s3 cp "$root/$key" "s3://$bucket/$key" >/dev/null
ok "stored, md5 $want"

chunk=$(manifest | python3 -c 'import json,sys; print(json.load(sys.stdin)["Chunks"][0]["ID"])')
owners=$(placement)
step "the manifest places it on: $owners"
step "the nodes holding its first chunk: $(holders "$chunk")"

read_back() {
	s3 s3 cp "s3://$bucket/$key" "$root/back.bin" >/dev/null
	python3 -c "import hashlib,sys; print(hashlib.md5(open(sys.argv[1],'rb').read()).hexdigest())" "$root/back.bin"
}
[ "$(read_back)" = "$want" ] && ok "reads back byte-identical"

bold "Killing an owner with SIGKILL, mid-read"
victim=${owners%% *}
index=${victim#n}
s3 s3 cp "s3://$bucket/$key" "$root/during.bin" >/dev/null 2>&1 &
racing=$!
kill -9 "${pids[$((index - 1))]}"
wait "$racing" 2>/dev/null && ok "the read that was in flight finished anyway" \
	|| step "the read in flight failed, which is allowed: it was not an acknowledged write"
ok "$victim is gone (no shutdown, no flush, no goodbye to etcd)"

# Asked of a node that is still alive, which is not a detail: the one being killed
# is an owner, and half the point is that any surviving node answers for the cluster.
witness=$([ "$index" = 1 ] && echo 2 || echo 1)
until [ "$(members "$witness")" = "$((nodes - 1))" ]; do sleep 0.2; done
ok "the cluster noticed: $((nodes - 1)) members, by lease expiry alone"

[ "$(read_back)" = "$want" ] && ok "still reads back byte-identical, from the copies that remain"
step "the nodes holding its first chunk now: $(holders "$chunk")"

bold "Waiting for redundancy to come back"
start=$SECONDS
while :; do
	held=$(holders "$chunk")
	count=$(wc -w <<<"$held" | tr -d ' ')
	if [ "$count" -ge 3 ]; then
		ok "back to $count copies after $((SECONDS - start))s, on $held"
		break
	fi
	if [ $((SECONDS - start)) -gt 120 ]; then
		echo "redundancy did not return within 120s; last seen on: $held"
		exit 1
	fi
	sleep 1
done

# The copies come back before the placement does, and that order is deliberate: a
# move copies to the new owner first and commits the manifest afterwards, because
# every reader in between has to find the object where the current manifest says it
# is. So the demo waits for the second half too — redundancy is not back until what
# the object promises is three nodes that are alive.
step "the copies are back; the placement follows, since a move commits last"
start=$SECONDS
until [[ " $(placement) " != *" $victim "* ]]; do
	if [ $((SECONDS - start)) -gt 120 ]; then
		echo "the placement still names the dead $victim after 120s: $(placement)"
		exit 1
	fi
	sleep 1
done
ok "the manifest now places it on $(placement), every one of them alive ($((SECONDS - start))s later)"
[ "$(read_back)" = "$want" ] && ok "and it is still the object that was stored, md5 $want"

bold "Nothing was lost, nothing was corrupted, and nobody was asked to intervene."
