# kavo

[![ci](https://github.com/0vertake/kavo/actions/workflows/ci.yml/badge.svg)](https://github.com/0vertake/kavo/actions/workflows/ci.yml)

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

Those are kavo timing itself. MinIO's `warp`, which is not, driving the same cluster through
minio-go: 731 `PUT`/s at 4 KiB (median 9.1 ms) and 4,235 `GET`/s (median 1.5 ms) at 8 concurrent
clients, 118 MiB/s writing 64 MiB objects and 130 MiB/s reading them at 4. Same shape, measured by
something this repo did not choose — and at N=3 that 118 MiB/s is ~354 MiB/s of fsynced writes to one
consumer drive.

Erasure coding (`-ec=6+3`) tolerates three losses at 1.5x the bytes where three copies need 3x. It
costs ~30% on a large write and ~45% on a large read, measured at 4+2 so that replication and coding
are compared at the same two-node tolerance on a six-node cluster. A degraded read is no slower than
a healthy one: moving the shards is the expensive part, not the arithmetic.

Three more that are about the cluster rather than a call (`make measure`):

- **A node loses its entire disk: full redundancy is back in 9.2 s** at the default 32 MB/s repair
  cap, 1.1 s uncapped, rebuilding 1.09 GB of copies. Nobody asks for the repair. The cap is per
  node, so heal bandwidth grows with the cluster while the disturbance to any one node's clients
  does not.
- **A seventh node joins and converges in 4.6 s**, and the 34 copies of 192 that move onto it are
  exactly the 34 the seven-node ring owes it — placement after a join is the ring's placement, key
  for key, with 192 copies before and after.
- **A 4 GB object streams through a node whose RSS peaks at 89 MB** — 68 MB above idle, against
  33 MB for a 64 MB object. Sixty-four times the object for twice the memory, because what memory
  scales with is the 32 MB chunk, not the object. This is measured on the node process with `ps`,
  through the signed API, with an unsigned payload — because hashing the body to sign it would mean
  reading a 4 GB object twice.

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

## How compatible is compatible

Ceph's `s3-tests` is the suite S3 implementations are measured against, and nobody here chose what it
asserts. It has 886 tests and kavo does not implement most of what they cover, on purpose. Of the 641
that fail: **429 are explicit anti-goals** — ACLs, versioning, server-side encryption, object lock,
bucket policy, lifecycle, logging, CORS, tagging, SigV2, browser form uploads — **48 are v1
`ListObjects`**, which kavo answers only at v2, and **28 follow from buckets being prefixes** rather
than records. **135 are named gaps**, half of them `CopyObject` and conditional requests. One is the
suite asserting Ceph's own configured region name.

With that framing: **151 pass, 641 fail, 94 the suite skips, and nothing errors** — every test
reaches a verdict rather than dying in setup, and every failure is accounted for in
[`docs/s3-compatibility.md`](docs/s3-compatibility.md), which generates its breakdown from the
suite's own output so it can be checked rather than believed. Of the tests covering `ListObjectsV2`,
the operation kavo does claim, 37 of 40 pass.

Running it was worth more than the number, twice over. It found four real defects, three of which
kavo's own tests could not see — including a listing that reported itself truncated when it had ended
exactly on a page boundary, which the in-repo test missed because it used three keys and a page size
of two. And it started at 169: eighteen of those passes came from answering `PUT ?lifecycle`,
`?policy` and `?encryption` with a 200, so kavo was passing by claiming to have configured things
that exist nowhere in the code. Refusing them cost eighteen tests and is the right answer.

## What it does not do

Deliberate anti-goals, not a roadmap: no IAM, no ACLs, no versioning, no lifecycle rules, no
bucket policies, no `ListObjects` v1. The S3 subset is PUT, GET (including ranges), HEAD, DELETE,
`ListObjectsV2` and multipart upload, with SigV4 verification — plus the handful of calls clients
make without being asked, which answer for records that do not exist: `CreateBucket` succeeds
because a bucket is a prefix, `ListBuckets` is a root listing, `DeleteBucket` refuses while objects
remain, and `ListObjectVersions` reports every object once as version `null`.

The gaps a client might actually notice are named in
[`docs/s3-compatibility.md`](docs/s3-compatibility.md) rather than buried: no `CopyObject` (so no
server-side `aws s3 mv`), no conditional requests, no `Content-MD5` verification, no
`x-amz-meta-*` passthrough.

Real limitations — an etcd-bound object count, rot that can sit until the next scrub, deleted space
that comes back within a collection cycle rather than at once — are listed with their consequences in
[`docs/design.md`](docs/design.md#known-limitations-publish-these). They are published rather than
fixed because a known limit is cheaper than a surprise.

## Layout

```
cmd/kavod        the single binary: S3 gateway + chunk store + repair participant
internal/s3      the S3 API: objects, ranges, listing, multipart
internal/sigv4   SigV4 verification, checked against the AWS SDK's own signer
internal/cluster placement, quorum, erasure coding, repair, scrub, rebalance, collection
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
