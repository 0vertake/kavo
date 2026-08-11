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
- `make lint` — `go vet` + `gofmt` check
- `go test ./test -run TestChaos` — the chaos suite: a concurrent S3 workload against four real
 processes while faults arrive at random, then the four invariants checked against the recorded
 history. `-chaos.duration` to run it longer, `-chaos.seed` to replay one. It runs at a short
 default as part of `make test`; run it long before believing a durability change.
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

- **Commit point**: a write is acknowledged only when W chunk replicas (or the required EC
 shards) are fsynced on distinct nodes AND the object manifest is committed to etcd. Readers
 resolve objects only through committed manifests. A plain etcd `Put` is enough for this: it
 is atomic and serialized, so a concurrent overwrite yields one manifest or the other, never a
 mix. Compare-and-swap arrives with chunk garbage collection, which needs to know which
 revision it superseded.
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
 chunk, acknowledged at k+1 shards. The code is recorded per object, so both modes coexist.
- Metadata: etcd only (manifests, membership leases, partition layout). Chunks are immutable —
  no vector clocks, no sloppy quorums, no read repair for correctness.
- Inter-node chunk transfer: plain HTTP with streaming bodies. No gRPC.

## Conventions

- Layout: thin `main.go` in `cmd/<binary>/`, all logic in `internal/`. No `pkg/`.
- Errors: wrap with `%w` and context; sentinel errors for programmatic checks.
- Tests: table-driven; integrity/durability claims need a test that injects the failure.
- Scope: build only what the current milestone needs (see `docs/design.md`). Do not add
  S3 API surface beyond the locked subset (no IAM, ACLs, versioning, lifecycle) — that is
  an explicit anti-goal. The subset is object PUT/GET/HEAD/DELETE, ListObjectsV2, multipart
  upload, SigV4, and the six calls clients make unprompted: `CreateBucket`, `ListBuckets`,
  `DeleteBucket`, `DeleteObjects`, `ListObjectVersions`, `GetBucketLocation`. Buckets are
  still prefixes and nothing is versioned — those last two answer for records that do not
  exist, because a client that cannot list or empty a bucket cannot use the store.

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
