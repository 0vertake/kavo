# AGENTS.md

## Project

kavo — a distributed, S3-compatible object store in Go. Multi-node cluster with
consistent-hash placement, quorum replication and Reed–Solomon erasure coding, etcd for
metadata/membership, automatic repair, and a chaos suite that proves the guarantees.

The deliverable is **measured guarantees** (durability, heal time, flat-memory streaming),
not API surface. Full design and milestones: `docs/design.md`. Research notes with sources:
`docs/research.md`.

## Commands

- `make build` — build all binaries
- `make test` — unit + integration tests (always runs with `-race`)
- `make bench` — benchmarks against a real six-node cluster; results and the decisions they
 settle are in `docs/benchmarks.md`; starts etcd first, so
 Docker must be running. Tests that touch metadata use a real etcd, never a fake.
- `make measure` — the numbers a per-operation benchmark cannot express: heal time after a node
 loses its disk, what a join moves, and a node's peak RSS under a multi-gigabyte object. They print
 rather than assert, so `make test` skips them; writes several GB and takes a few minutes. Results
 in `docs/benchmarks.md`.
- `make lint` — `go vet` + `gofmt` check
- `go test ./test -run TestChaos` — the chaos suite: a concurrent S3 workload against four real
 processes while faults arrive at random, then the four invariants checked against the recorded
 history. `-chaos.duration` to run it longer, `-chaos.seed` to replay one, `-chaos.ec=4+2` to store
 the workload erasure-coded instead of replicated — CI runs both, because the two modes fail
 differently and running one proved the invariants for half the store. It runs at a short
 default as part of `make test`; run it long before believing a durability change. `-chaos.duration`
 buys workload only: the verification afterwards reads the whole history back and waits for
 redundancy to settle, which cost 16 minutes after a 45-minute run. Give `-timeout` at least twice
 the duration, or a run dies mid-verdict having proved nothing.
- `make demo` — six real processes on this host, an object, `SIGKILL` to one of its owners, and
 redundancy returning on its own. Every step is checked (digests compared, each node asked whether
 it holds the chunk) rather than narrated, so it doubles as a smoke test of the deployment path.
 Processes rather than containers on purpose: `docs/benchmarks.md` measures Docker Desktop
 absorbing `F_FULLFSYNC`, and a demo about durability should not run on top of that. Needs `aws`.
- `make etcd` — start just etcd (idempotent)
- `make up` / `make down` — 6-node dev cluster plus etcd via Docker Compose
 (`deploy/compose.yaml`). Each node publishes two ports: the S3 API on `localhost:9001`–`9006`
 and the internal API on `8081`–`8086`. One image serves every node: nodes are symmetric, so
 only the flags differ.

Ceph's `s3-tests` is external and run by hand, not from the Makefile. The pass count and the
classification of every failure live in `docs/s3-compatibility.md`; update both if the S3 surface
changes.

Run `make test` before declaring any task done. CI (`.github/workflows/ci.yml`) runs `make lint`,
`make test` and a five-minute chaos run on every push and pull request — the same commands a
developer runs, because CI that drifts from them eventually only proves things about CI.

## Invariants — never violate, never weaken to make a test pass

1. No acknowledged write is ever lost.
2. No partially written object is ever readable.
3. Every read returns checksum-valid data or an explicit error — never silent corruption.
4. After healing completes, redundancy is back to the configured level.

Rules that make these structural:

- **A write in flight is recorded, not inferred**: a write that can store more than one chunk puts
  one key in etcd naming itself before it stores any of them, and clears it after the commit — in
  that order. Every chunk of a write shares an id prefix, so one key protects a 5 GB upload, and a
  write with a single short chunk needs none. Collection reads those records before it reads any
  manifest, and drops the ones belonging to nodes that are no longer members; the commit of a
  recorded write is conditional on its record still being there, so a writer that lost etcd fails
  its write rather than acknowledging an object whose early chunks were collected. Never make the
  grace period the argument again: it is a backstop for the tail of a single-chunk write.
- **Commit point**: a write is acknowledged only when W chunk replicas (or the required EC
  shards) are fsynced on distinct nodes AND the object manifest is committed to etcd. W never
  narrows to however many nodes happen to be visible: a coordinator that can see fewer than W
  refuses the write. A node keeps itself in its own ring, so "the cluster is small" and "I am
  cut off" look identical from inside, and the forgiving reading of that cost an acknowledged
  object in chaos (`-chaos.seed=1786500476032706000`). Readers
 resolve objects only through committed manifests. A plain etcd `Put` is enough for this: it
 is atomic and serialized, so a concurrent overwrite yields one manifest or the other, never a
 mix. It stays a plain `Put`: garbage collection is mark-and-sweep and so does not need the
 revision it superseded, and a chunk that is live if *any* manifest names it is what lets an
 object be copied without copying its data. Compare-and-swap exists where a background pass
 rewrites what it read minutes ago, which is rebalancing.
- **Deleting a chunk**: the collection pass is the only thing that deletes one, and there is no
 code left that can delete a chunk on another node. Deletes, aborted uploads and rebalances all
 leave their garbage for it. That is because `CopyObject` copies a manifest rather than data, so
 the chunks under one key are the chunks under another, and any pass that deleted on the strength
 of the single manifest in front of it would take a second object's data with it. A chunk goes
 only when every manifest has been read and none names it *and* the ring does not make this node
 an owner of the object it belongs to. The ring clause is not belt and braces: a move copies to
 the new owners before committing the manifest that names them, so mid-move the destination holds
 chunks only the ring accounts for, and a sweep that went by the manifests alone deleted them and
 lost an object. A pass that could not read all the metadata deletes nothing — a partial answer is
 indistinguishable from an empty one, and acting on it loses acknowledged writes. See
 `internal/cluster/collect.go`.
- **fsync discipline**: chunk commit is write temp → fsync file → rename → fsync directory.
  An fsync error means the data is gone — fail the write or quarantine the disk. Never
  retry-and-trust fsync (fsyncgate: the kernel marks pages clean after a failed fsync).
- **Streaming**: never buffer a whole object in memory. Process in chunks (default 32 MB);
  memory must stay flat regardless of object size.

## Architecture (short)

- Symmetric nodes: one binary `kavod` = S3 gateway + chunk store + repair participant.
  Any node coordinates any request.
- Two listeners per node: `-s3` is the signed client API, `-addr` the internal one (peer chunk
  transfer, cluster state). The internal port is unauthenticated and can delete chunks, so it
  must never be the one clients reach.
- Placement: object key → one of 256 partitions → nodes, via a consistent-hash ring with
  ~128 vnodes per node. Rebalance/repair bookkeeping is per-partition, never per-object.
- Defaults: N=3, W=2, R=2 replication; EC mode (`-ec=6+3`) is 6 data + 3 parity, encoded per
 chunk, acknowledged at k+1 shards. The code is recorded per object, so both modes coexist. W is
 declared with `-w` and never inferred from how many nodes are reachable; `-w 1` is how a
 single-node store (the crash harness) asks for one copy.
- Metadata: etcd only (manifests, membership leases, partition layout). Chunks are immutable —
  no vector clocks, no sloppy quorums, no read repair for correctness.
- Four background passes, all resumable by cursor and all rate-limited or sliced so they never
  grow with the disk: repair (missing copies), scrub (rot), rebalance (misplaced copies),
  collect (chunks no manifest references). Repair restores copies only on the nodes a manifest
  already names; changing *where* copies belong is rebalancing's alone, which is why rebalancing
  is also what widens a placement narrower than N — an object written while part of the cluster
  was unreachable, which nothing widened until it was made to. Both passes take the width from
  the object's own recorded code, never from the placement in front of them and never from this
  node's mode.
- Inter-node chunk transfer: plain HTTP with streaming bodies. No gRPC.

## Conventions

- Layout: thin `main.go` in `cmd/<binary>/`, all logic in `internal/`. No `pkg/`.
- Errors: wrap with `%w` and context; sentinel errors for programmatic checks.
- Tests: table-driven; integrity/durability claims need a test that injects the failure.
- Scope: build only what the current milestone needs (see `docs/design.md`). Do not add
  S3 API surface beyond the locked subset (no IAM, ACLs, versioning, lifecycle) — that is
  an explicit anti-goal. The subset is object PUT/GET/HEAD/DELETE, ListObjectsV2, multipart
  upload, SigV4, `CopyObject`, and the six calls clients make unprompted: `CreateBucket`,
  `ListBuckets`, `DeleteBucket`, `DeleteObjects`, `ListObjectVersions`, `GetBucketLocation`.
  Conditional *reads* (`If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`, and
  the same four as `x-amz-copy-source-if-*` on a copy), `Content-MD5` verification, CRC32C on a
  whole-object PUT (header or aws-chunked trailer, and a HEAD/GET that asks for it with
  `x-amz-checksum-mode: ENABLED`), and user metadata
  (`x-amz-meta-*`, `Cache-Control`, `Content-Disposition`, `Content-Encoding`, `Content-Language`,
  `Expires`, and `x-amz-metadata-directive` on a copy) are in too, being headers on calls that already
  exist rather than new surface. `ListParts` and `ListMultipartUploads` are in because they are part of
  multipart upload, and because both are a `GET` that was being answered by the object handler — a
  client asking which parts had arrived was told `NoSuchKey`. `UploadPartCopy` is in for the same
  reason: above 8 MB the `aws` CLI performs every server-side copy that way, so without it
  `CopyObject` was a copy only for small objects. It re-chunks through the ordinary write path rather
  than handing the part the source's chunk references, because a client's part size does not land on
  chunk boundaries — so a copied part costs a read and a write inside the cluster where `CopyObject`
  costs neither. A copied range that runs past the end of the source is refused, not shortened: the
  client sees only the etag of whatever was copied, so a short copy assembles an object nobody
  described. **Reading** an object's tags is answered, with none, because the CLI reads the source's
  tags before a multipart copy — and because "this object has no tags" is true. Setting them is
  refused, `x-amz-tagging` included: both halves are needed for either to be honest, since a store
  that dropped the header and then reported no tags would have said the tags were gone by way of two
  successes. Conditional *writes* are not: `If-None-Match: *` on a PUT needs the commit to become a
  compare-and-set, which is a change to the commit point and has to be argued for.
  A request for something outside the subset is **refused, not ignored**: a request carrying
  `x-amz-server-side-encryption*` or a customer key is answered 501, because storing the object in
  plaintext and answering 200 tells a client its data is encrypted while anyone can read it. Ignoring
  those headers was worth twenty-two `s3-tests` passes, which is the clearest argument on record that
  a pass count is not a measure of a store. SHA-256, CRC32, CRC64NVME, a trailing checksum other than CRC32C, and a
  checksum on a part or a copy are refused for the same reason: the header asks to record a number
  this write would not look at. CRC32C on a whole-object PUT is the exception that is actually
  checked, because the write already hashes the body to make the ETag — whether the client names
  it in a header or in an aws-chunked trailer.
  The same rule is structural for **subresources**, because S3 addresses them as a query on the same
  path an object or bucket already has, so ignoring the query does not drop the request — it runs a
  different one. Both paths take an allowlist of the queries they understand (`knownObjectQuery`,
  `bucketOnly`) and answer 501 to anything else. This is not stylistic: before the object-side
  allowlist existed, `PUT /key?tagging` reached the object PUT and replaced the object with the
  tagging XML, `PUT /key?acl` truncated it to nothing, `DELETE /key?tagging` deleted it, and
  `UploadPartCopy` stored an empty part — each answered with a success. Never widen either allowlist
  to make a client happy without asking what the handler on the other side of it does.
  Buckets are still prefixes and nothing is versioned — those last two answer for records that
  do not exist, because a client that cannot list or empty a bucket cannot use the store.
  `CopyObject` is in because `aws s3 mv` is a copy and a delete, and because a copy that made the
  client download and re-upload the object would be a copy in name only.

## Git conventions

- Modular branches and PRs, one logical component each.
- Branches: `feat/<technical-description>` (also `fix/`, `chore/`, `refactor/`).
- Commits: conventional — `feat: <description>` (also `fix:`, `chore:`, `refactor:`).
- After finishing a piece of work, end the response with ready-to-run `git add` +
  `git commit` commands naming the exact files. The user runs them.

## Boundaries

- Never create git commits, push, or open PRs unless explicitly asked.
- Dependencies: prefer the standard library. Pre-approved: `klauspost/reedsolomon`,
 `go.etcd.io/etcd/client/v3`, and `aws/aws-sdk-go-v2` **in tests only** — its signer and its S3
 client are the independent oracles the SigV4 and S3 API tests check against. Anything else:
 propose it first with a one-line reason.
- The `aws` CLI is a test oracle too (`test/awscli_test.go`), skipped when it is not installed.
 It is the only thing that proves the S3 subset works for a client kavo did not choose.
- Do not "fix" a failing chaos/integration test by loosening its assertions.
