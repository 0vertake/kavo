# Multi-host cluster

Six kavod nodes on separate machines, one etcd they all reach, and benchmarks run from a
driver that talks to the cluster over the network. That is milestone 11: the same
`make bench` / `warp` numbers as [`docs/benchmarks.md`](../docs/benchmarks.md), but with chunk
replication crossing real links instead of loopback.

Nothing here replaces `make up` or `make demo` on one laptop — those stay the dev loop.
Docker Compose on one host is explicitly **not** a substitute: it distorts fsync and bandwidth
(see the container table in `docs/benchmarks.md`).

## Layout

| role | count | notes |
| --- | --- | --- |
| etcd | 1 | client URL reachable from every node |
| kavod | 6 | one process per host, own disk for `-data` |
| driver | 1 | runs `bench-remote`; can be any host that reaches S3 |

Each node listens on `0.0.0.0` and **advertises** the address peers dial (`host:8080` on the
cluster LAN). The S3 API (`host:9000`) is what clients and benchmarks use; any node coordinates
any request.

## 1. Configure

```sh
cp deploy/cluster.env.example deploy/cluster.env
# Edit: KAVO_ETCD, KAVO_N1_HOST … KAVO_N6_HOST, KAVO_BENCH_ENDPOINT
```

`KAVO_BENCH_ENDPOINT` is any node's S3 URL (e.g. `http://10.0.0.11:9000`).

## 2. Start etcd

On the etcd host (or use Docker on one machine only for etcd — nodes still run on separate hosts):

**Docker (dev):**

```sh
docker run -d --name kavo-etcd --restart unless-stopped \
  -p 2379:2379 \
  -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
  -e ETCD_ADVERTISE_CLIENT_URLS=http://10.0.0.10:2379 \
  quay.io/coreos/etcd:v3.5.17 \
  etcd --name kavo-etcd --data-dir /etcd-data
```

**systemd:** copy [`etcd.service`](etcd.service), set `ETCD_ADVERTISE_CLIENT_URLS`, `systemctl enable --now kavo-etcd`.

## 3. Build and install kavod

On each node (or build once and copy the static binary):

```sh
go build -o kavod ./cmd/kavod
sudo install -m755 kavod /usr/local/bin/kavod
sudo install -m755 scripts/kavod-node.sh /usr/local/bin/kavod-node.sh
sudo mkdir -p /etc/kavo
sudo cp deploy/cluster.env /etc/kavo/cluster.env
```

Create each node's data directory (paths from `cluster.env`):

```sh
sudo mkdir -p /var/lib/kavo/n1
sudo chown "$(whoami)" /var/lib/kavo/n1   # or a dedicated kavo user
```

## 4. Start nodes

**Foreground (smoke test on host 1):**

```sh
./scripts/kavod-node.sh n1 deploy/cluster.env
```

**systemd (production-style):**

```sh
sudo cp deploy/kavod.service /etc/systemd/system/kavo@.service
sudo systemctl daemon-reload
sudo systemctl enable --now kavo@n1   # on host 1; n2 on host 2; …
```

Repeat on all six hosts with the matching id.

## 5. Verify membership

From the driver (or anywhere that can reach internal APIs):

```sh
./scripts/cluster-check.sh deploy/cluster.env
```

Every node should report six members.

## 6. Benchmark

From the driver, with this repo checked out:

```sh
./scripts/bench-remote.sh deploy/cluster.env
# or:
KAVO_BENCH_ENDPOINT=http://10.0.0.11:9000 make bench-remote
```

Optional outside-client numbers (MinIO `warp`):

```sh
WARP=1 ./scripts/bench-remote.sh deploy/cluster.env
```

Record results in the **Multi-host results** section at the end of `docs/benchmarks.md`.

## 7. Heal measurement (remote)

The heal-time number in `docs/benchmarks.md` (`make measure`) starts six local processes and
wipes a data directory on disk. Over a real network the cluster is already running and the wipe
must happen on the victim host:

```sh
# In cluster.env — shell command run on the driver; {id} becomes n1 … n6
KAVO_WIPE_CMD='rm -rf /var/lib/kavo/{id}/chunks/*/*'
KAVO_MEASURE_VICTIM=n6

./scripts/measure-remote.sh deploy/cluster.env
# or, with cluster.env sourced:
make measure-remote
```

The measurement writes objects under the `measure-remote/` prefix through the internal API on
`KAVO_N1_HOST`, counts how many chunks the victim holds, wipes them, and polls repair through
`POST /peer/chunks/check` until redundancy is back. Repair rate is whatever the nodes were started
with; for numbers comparable to the unthrottled local run, start every node with `-repair-rate=0`.

Docker dev cluster smoke test (after `make up`):

```sh
cat > deploy/cluster.env <<'EOF'
KAVO_ETCD=127.0.0.1:2379
KAVO_CLUSTER=/kavo
KAVO_N1_HOST=127.0.0.1
KAVO_N1_INTERNAL_PORT=8081
KAVO_N2_HOST=127.0.0.1
KAVO_N2_INTERNAL_PORT=8082
KAVO_N3_HOST=127.0.0.1
KAVO_N3_INTERNAL_PORT=8083
KAVO_N4_HOST=127.0.0.1
KAVO_N4_INTERNAL_PORT=8084
KAVO_N5_HOST=127.0.0.1
KAVO_N5_INTERNAL_PORT=8085
KAVO_N6_HOST=127.0.0.1
KAVO_N6_INTERNAL_PORT=8086
KAVO_WIPE_CMD='docker exec deploy-{id}-1 sh -c "rm -rf /data/chunks/*/*"'
KAVO_MEASURE_VICTIM=n6
EOF
./scripts/measure-remote.sh deploy/cluster.env
```

## What stays local

Join rebalance and streaming RSS still need local process control (`make measure`). The internal-API
benchmarks in `make bench` also start coordinators in the test process. Remote heal and S3 gateway
benchmarks (`make measure-remote`, `make bench-remote`) run against a cluster reachable over the
network.

## Firewall

Open between all nodes:

- **2379** — etcd (nodes → etcd)
- **8080** — internal API (nodes ↔ nodes; driver for `/cluster/members`)
- **9000** — S3 API (clients → any node)

Peer chunk transfer uses the internal port; it is unauthenticated and must not be exposed to
clients.
