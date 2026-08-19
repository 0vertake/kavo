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
| **No acknowledged write is ever lost.** | The chaos suite (`TestChaos`) records every ack and re-reads all of them after killing, freezing and wiping nodes mid-flight — including objects made by a server-side copy whose source was then deleted, and copies assembled out of ranges of the source while that source's owners were being killed, with the garbage collector turned up so that it is deleting chunks throughout. A 45-minute run acknowledged 51,623 writes, took 40 faults — the last wiping 40,274 chunks off a node — and re-read all 43,329 survivors byte-identical. `TestCrashDuringConcurrentUploads` SIGKILLs a node under concurrent uploads and re-reads every ack after it restarts. |
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
asserts. It has 886 tests and kavo does not implement most of what they cover, on purpose. Of the 616
that fail: **488 are explicit anti-goals** — ACLs, versioning, server-side encryption, object lock,
bucket policy, lifecycle, logging, CORS, tagging, SigV2, browser form uploads — **47 are v1
`ListObjects`**, which kavo answers only at v2, and **28 follow from buckets being prefixes** rather
than records. **24 are conditional writes**, which would make the commit a compare-and-set and so
need arguing for rather than adding. **27 are named gaps**, led by the checksum algorithms CRC32C
does not cover (SHA-256, CRC64NVME, COMPOSITE). The last measured run also filed reads of a
single part there; those are implemented now, and the count stays until the suite is re-run. Two
are artifacts of the suite's own environment.

With that framing: **176 pass, 616 fail, 94 the suite skips, and nothing errors** — every test
reaches a verdict rather than dying in setup, and every failure is accounted for in
[`docs/s3-compatibility.md`](docs/s3-compatibility.md), which generates its breakdown from the
suite's own output so it can be checked rather than believed. Of the tests covering the operations
kavo does claim, 37 of 40 `ListObjectsV2` tests pass, 15 of 23 single-object copy, 6 of 7
server-side copies of a multipart-sized object, 14 of 25 multipart, 12 of 12 conditional reads, and
6 of 7 user metadata.

The pass count has gone down three times on purpose, and those moves are the most useful thing the
suite produced. It started at 169, of which eighteen came from answering `PUT ?lifecycle`, `?policy`
and `?encryption` with a 200 — kavo was passing by claiming to have configured things that exist
nowhere in the code. Later it reached 196, and twenty-two of those were requests for server-side
encryption that kavo *ignored*: a client sending a customer key was told its object was stored, which
it was, in plaintext that anyone could read back without the key. Both sets are refused now. The third move is the one to read: an object's
subresources are a query on the object's own path, so `PUT /key?tagging` reached the handler that
writes an object and **replaced the object with the tagging XML**, `PUT /key?acl` truncated it to
nothing, and `DELETE /key?tagging` deleted it — each answered 200, so a client tagging an object
destroyed it and was told the tag was set. Eight passes were tests doing precisely that, and 169 was
the honest number — landing exactly where the measurement had started, which is a coincidence worth
distrusting, since the same count covered `CopyObject`, conditional reads, `Content-MD5`, user
metadata and three multipart calls that did not exist at the outset. Implementing `UploadPartCopy`
then took it to 176. A pass count rewards a store for answering; only reading the failures tells you what
it answered with.

That last one the suite did not find, and neither did kavo's own tests. It is also what led to the
copy path being finished: `UploadPartCopy` now works, so a server-side copy of an object above the
CLI's 8 MB threshold streams inside the cluster instead of failing, and a read of an object's tags
is answered with none — true here — while asking for tags to exist is refused, which is the pair
that keeps either answer honest. Ten tagging tests were
failing already, filed under an anti-goal, which is the easiest kind of failure to stop reading.
What found it was `aws s3 cp` of a 20 MB object between two keys: past 8 MB the CLI copies by
multipart, and the first call it makes is `GetObjectTagging`. Every guarantee here had been tested
with the real CLI on the near side of that threshold.

Running the suite found four real defects too, three of which kavo's own tests could not see —
including a listing that reported itself truncated when it had ended exactly on a page boundary, and
metadata keys replayed as `X-Amz-Meta-Colour` where S3 sends `x-amz-meta-colour`. That one was
invisible in-repo because kavo's tests use the AWS Go SDK, which lowercases those keys before handing
them over; botocore does not, and seven tests died on it.

## What it does not do

Deliberate anti-goals, not a roadmap: no IAM, no ACLs, no versioning, no lifecycle rules, no
bucket policies, no `ListObjects` v1. The S3 subset is PUT, GET (including ranges), HEAD, DELETE,
`ListObjectsV2`, multipart upload (including `UploadPartCopy`, which is how a client copies an object
too large to copy in one call, and GET/HEAD `?partNumber` of a completed object) and `CopyObject`, with SigV4 verification, conditional reads,
`Content-MD5` verification, CRC32C on a whole-object PUT or a multipart upload (header or aws-chunked trailer), and
`x-amz-meta-*` passthrough — plus the handful of calls clients make
without being asked, which answer for records that do not exist: `CreateBucket` succeeds because a
bucket is a prefix, `ListBuckets` is a root listing, `DeleteBucket` refuses while objects remain, and
`ListObjectVersions` reports every object once as version `null`.

A request kavo cannot honour is refused rather than ignored, which is a rule and not a habit: asking
for server-side encryption gets a 501 saying so, because storing the object in plaintext and
answering 200 tells a client its data is encrypted when it is readable by anyone. Asking for tags
gets the same treatment, while *reading* an object's tags is answered with none — which is true, and
only stays true because the write is refused rather than dropped.

The gaps a client might actually notice are named in
[`docs/s3-compatibility.md`](docs/s3-compatibility.md) rather than buried: SHA-256, CRC64NVME and
COMPOSITE checksums, and the wrong error code for a malformed authorization header. CRC32C on a
whole-object PUT or a multipart upload is not among them — it is checked, stored, and returned on a
HEAD/GET that asks. A GET of one completed part (`?partNumber`) is not among them either.

Real limitations — an etcd-bound object count, rot that can sit until the next scrub, deleted space
that takes about half an hour to come back because collection is the only thing that deletes a
chunk — are listed with their consequences in
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
numbers above. `make demo` is the shortest way to see the point of it: six nodes on this host, an
object, `SIGKILL` to one of the nodes holding it, and three copies again a second or two later —
with every step checked rather than narrated. Architecture and milestones: [`docs/design.md`](docs/design.md). Research notes with
sources: [`docs/research.md`](docs/research.md).
