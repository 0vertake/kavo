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

## What stays local

`make measure` (heal time, join rebalance, streaming RSS) and the internal-API benchmarks in
`make bench` still start coordinators in the test process. Only the S3 gateway benchmarks — and
`warp` — can run remote. Heal/join over a real network needs a follow-up harness that drives
faults on named hosts; that is not wired yet.

## Firewall

Open between all nodes:

- **2379** — etcd (nodes → etcd)
- **8080** — internal API (nodes ↔ nodes; driver for `/cluster/members`)
- **9000** — S3 API (clients → any node)

Peer chunk transfer uses the internal port; it is unauthenticated and must not be exposed to
clients.
