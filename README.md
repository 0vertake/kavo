# kavo

A distributed, S3-compatible object store in Go. Symmetric nodes, consistent-hash placement,
quorum replication or Reed–Solomon erasure coding, etcd as the commit point, automatic repair — and
a chaos suite whose job is to break the guarantees below rather than to demonstrate them.

The point of this project is not API surface. It is four durability guarantees that hold while nodes
are being killed, and numbers honest enough to be worth reading.

## The guarantees

Four invariants. Each one has a test that injects the failure it is about, because a durability
claim without a failure injected is a wish.

| | proven by |
| --- | --- |
| **No acknowledged write is ever lost.** | The chaos suite (`TestChaos`) records every ack and re-reads all of them after killing, freezing and wiping nodes mid-flight. `TestCrashDuringConcurrentUploads` SIGKILLs a node under concurrent uploads and re-reads every ack after it restarts. |
| **No partially written object is ever readable.** | A write is acknowledged only once W chunk replicas are fsynced on distinct nodes *and* the manifest is committed to etcd. Readers resolve objects only through committed manifests, so a torn object has nothing to be read through. |
| **Every read returns checksum-valid data or an explicit error.** | Every chunk is verified against the manifest's CRC32C on the way out, and the last byte of each chunk is withheld until that check passes — so a corrupt chunk becomes a transfer that stops short, never a successful wrong answer. The chaos suite flips bits on disk to check it. |
| **After healing, redundancy is back to the configured level.** | Repair rebuilds missing copies against the ring; the chaos suite wipes a node's disk and then asserts every chunk is back on every owner it should be on. |

The fsync discipline behind the first two: write to `tmp/` → fsync the file → rename → fsync the
directory. An fsync error is treated as data loss and never retried, because a failed fsync leaves
the kernel with clean pages and no data (fsyncgate).

## Numbers

Six nodes, real HTTP, real etcd, **all six nodes and all three replicas on one laptop NVMe** — so
every write number is a floor set by one disk's fsync queue, not a ceiling. Full methodology, the
profiles behind each one, and the optimisations that were measured and rejected:
[`docs/benchmarks.md`](docs/benchmarks.md).

| through the S3 API | one client | 8 clients |
| --- | --- | --- |
| PUT 4 KB | 25 ms | 18 ms |
| PUT 64 MB | 191 ms / 352 MB/s | 100 ms / 668 MB/s |
| GET 4 KB | 0.64 ms | 0.14 ms |
| GET 64 MB | 48 ms / 1.4 GB/s | 21 ms / 3.1 GB/s |
| `ListObjectsV2`, a page of 1000 keys | 13 ms | |

Three things those numbers say, none of them about the code being fast:

- **A small write is disk barriers.** ~22 ms of that 25 ms is waiting for `F_FULLFSYNC` on three
  replicas sharing one drive. On server NVMe with `fdatasync` it is an order of magnitude cheaper.
- **A small read is one etcd round trip.** 0.29 ms to resolve the manifest against 0.015 ms to read
  the chunk off local disk. That is the price of the commit point living somewhere else, and it is
  paid deliberately.
- **The S3 gateway is free.** Signing, parsing and XML are within noise of the internal API.
  Compatibility is not where a store's time goes.

Erasure coding (`-ec=6+3`) tolerates three losses at 1.5x the bytes where three copies need 3x. It
costs ~30% on a large write and ~45% on a large read, measured at 4+2 so that replication and coding
are compared at the same two-node tolerance on a six-node cluster. A degraded read is no slower than
a healthy one: moving the shards is the expensive part, not the arithmetic.

## Run it

```sh
make up   # six kavod nodes and etcd, via Docker Compose
```

Every node speaks the full S3 API and coordinates any request, so it does not matter which one a
client talks to. Nodes 1–6 publish S3 on ports 9001–9006.

```sh
export AWS_ACCESS_KEY_ID=kavo AWS_SECRET_ACCESS_KEY=kavosecret AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:9001 s3 cp ./file.bin s3://demo/hello.bin
aws --endpoint-url http://127.0.0.1:9003 s3 ls s3://demo/
aws --endpoint-url http://127.0.0.1:9005 s3 cp s3://demo/hello.bin ./back.bin
```

Buckets are key prefixes, so there is nothing to create first. `make down` removes the cluster and
its volumes.

### Kill two of six

```sh
docker kill deploy-n2-1 deploy-n4-1
aws --endpoint-url http://127.0.0.1:9001 s3 cp s3://demo/hello.bin ./still-here.bin   # works
```

Reads keep working, because three copies means two can be gone and a read needs one. Writes are the
more interesting half, and worth being precise about: for up to one lease TTL (5 s by default) the
dead nodes are still members, so a chunk whose owners include both of them cannot reach W=2 and the
write is refused. Once their leases expire the ring shrinks to the four survivors and the same write
succeeds. Failure detection is a window, and inside it kavo would rather refuse a write than
acknowledge one it cannot make durable.

## What it does not do

Deliberate anti-goals, not a roadmap: no IAM, no ACLs, no versioning, no lifecycle rules, no
bucket policies, no `ListObjects` v1. The S3 subset is PUT, GET (including ranges), HEAD, DELETE,
`ListObjectsV2` and multipart upload, with SigV4 verification.

Real limitations — leaked chunks with no GC pass yet, an etcd-bound object count, rot that can sit
until the next scrub — are listed with their consequences in
[`docs/design.md`](docs/design.md#known-limitations-publish-these). They are published rather than
fixed because a known limit is cheaper than a surprise.

## Layout

```
cmd/kavod        the single binary: S3 gateway + chunk store + repair participant
internal/s3      the S3 API: objects, ranges, listing, multipart
internal/sigv4   SigV4 verification, checked against the AWS SDK's own signer
internal/cluster placement, quorum, erasure coding, repair, scrub, rebalance
internal/ring    the consistent-hash ring: partitions, vnodes, owners
internal/object  chunking and streaming reassembly
internal/store   the local chunk store and its commit discipline
internal/peer    chunk transfer between nodes, verified end to end
internal/meta    manifests, membership leases and cursors in etcd
internal/api     the internal HTTP API nodes use on each other
internal/ec      Reed–Solomon encode and reconstruct
test             crash-safety harness, aws CLI tests, and the chaos suite
```

`make test` runs everything with `-race` (it starts etcd itself). `make bench` reproduces the
numbers above. Architecture and milestones: [`docs/design.md`](docs/design.md). Research notes with
sources: [`docs/research.md`](docs/research.md).
