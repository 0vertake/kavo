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
- `make etcd` — start just etcd (idempotent)
- `make up` / `make down` — 6-node dev cluster plus etcd via Docker Compose
 (`deploy/compose.yaml`), published on `localhost:8081`–`8086`. One image serves every node:
 nodes are symmetric, so only the flags differ.

Run `make test` before declaring any task done.

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
- Placement: object key → one of 256 partitions → nodes, via a consistent-hash ring with
  ~128 vnodes per node. Rebalance/repair bookkeeping is per-partition, never per-object.
- Defaults: N=3, W=2, R=2 replication; EC mode is 6 data + 3 parity, encoded per chunk.
- Metadata: etcd only (manifests, membership leases, partition layout). Chunks are immutable —
  no vector clocks, no sloppy quorums, no read repair for correctness.
- Inter-node chunk transfer: plain HTTP with streaming bodies. No gRPC.

## Conventions

- Layout: thin `main.go` in `cmd/<binary>/`, all logic in `internal/`. No `pkg/`.
- Errors: wrap with `%w` and context; sentinel errors for programmatic checks.
- Tests: table-driven; integrity/durability claims need a test that injects the failure.
- Scope: build only what the current milestone needs (see `docs/design.md`). Do not add
  S3 API surface beyond the locked subset (no IAM, ACLs, versioning, lifecycle) — that is
  an explicit anti-goal.

## Git conventions

- Modular branches and PRs, one logical component each.
- Branches: `feat/<technical-description>` (also `fix/`, `chore/`, `refactor/`).
- Commits: conventional — `feat: <description>` (also `fix:`, `chore:`, `refactor:`).
- After finishing a piece of work, end the response with ready-to-run `git add` +
  `git commit` commands naming the exact files. The user runs them.

## Boundaries

- Never create git commits, push, or open PRs unless explicitly asked.
- Dependencies: prefer the standard library. Pre-approved: `klauspost/reedsolomon`,
  `go.etcd.io/etcd/client/v3`. Anything else: propose it first with a one-line reason.
- Do not "fix" a failing chaos/integration test by loosening its assertions.
