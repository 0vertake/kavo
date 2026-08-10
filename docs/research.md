# Research notes (2026-08-10)

Condensed findings that informed the design; keep for reference when implementing.

## Prior art — what the reference systems actually do

- **MinIO** ([distributed design](https://github.com/minio/minio/blob/master/docs/distributed/DESIGN.md)):
  objects map to one *erasure set* (2–16 drives) via deterministic hash; quorum/heal/rebalance
  scoped per set. `klauspost/reedsolomon` for EC, HighwayHash for bitrot on read. No external
  metadata DB. Recovery is object-scoped, incremental, resumable, throttled — no rebuild storms.
- **Garage** ([FOSDEM talk](https://archive.fosdem.org/2024/events/attachments/fosdem-2024-3009-advances-in-garage-the-low-tech-storage-platform-for-geo-distributed-clusters/slides/22432/talk_uSWKzTL.pdf)):
  256 partitions; centrally computed layout table minimizing data movement; CRDT metadata with
  Merkle anti-entropy (deliberate contrast to kavo's etcd choice); separate meta/data volumes;
  persistent block-resync queue.
- **SeaweedFS / Haystack**: packs small files into 32 GB append-only volumes because
  per-object central metadata doesn't scale to billions of objects — the at-scale answer to
  kavo's documented metadata ceiling.
- **Ceph CRUSH** ([paper](https://ceph.io/assets/pdfs/weil-crush-sc06.pdf)): placement is a
  pure function (object → PG → nodes over weighted hierarchy); no per-object location table;
  replicas in distinct failure domains; minimal movement on change.

Common lesson: **object → partition/group → nodes indirection everywhere**; repair is always
object/partition-scoped, throttled, resumable.

## Crash safety

- **fsyncgate** ([PostgreSQL wiki](https://wiki.postgresql.org/wiki/Fsync_Errors),
  [ATC'20 CuttleFS paper](https://research.cs.wisc.edu/wind/Publications/atc20-cuttlefs.pdf)):
  Linux marks pages clean after fsync failure; retry falsely succeeds. PostgreSQL/MySQL panic
  on fsync error. Policy: fsync error = data loss; fail or quarantine, never retry-and-trust.
- Durable commit pattern: write temp → fsync file → rename → **fsync directory**.

## Replication / quorums

- W + R > N gives read/write overlap ([Dynamo literature](https://www.unskewdata.com/blog/leaderless-replication)).
  Sloppy quorums + LWW lose acknowledged writes (Jepsen measured 28% loss on Cassandra QUORUM
  under partition). kavo avoids the whole class: immutable chunks + etcd CAS commit point.

## Erasure coding

- [`klauspost/reedsolomon`](https://github.com/klauspost/reedsolomon): in-memory API
  recommended for shards <10 MB; streaming API (4 MB blocks) above. Max 256 shards.
  `ReconstructData` cheaper than full `Reconstruct`. Must persist shard order, size, per-shard
  hash — cannot recover from swapped/corrupted shards otherwise.

## Placement

- Ring + vnodes: ~100–150 vnodes per node is the sweet spot (diminishing returns above).
  Jump hash rejected: no arbitrary node removal. Rendezvous viable at 6 nodes but ring+vnodes
  is the locked, more instructive choice.

## etcd

- Leases: TTL must exceed etcd election timeout (use ≥3–5 s); client keepalive fires at TTL/3;
  keepalive response channel buffers 16 and drops when full. Watch the membership prefix for
  bounded-time failure detection. Practical storage cap ~2–8 GB → metadata ceiling.

## S3 semantics

- Parts of a multipart upload are invisible until `CompleteMultipartUpload` atomically creates
  the object — the model for kavo's etcd commit point.
- `aws s3 cp` uses `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`: per-chunk chained signatures the
  server must verify. References: [`amwolff/awsig`](https://github.com/amwolff/awsig)
  (server-side SigV4 for S3 clones), MinIO `cmd/auth-handler.go`,
  [LabStore SigV4 write-up](https://datalabtechtv.com/posts/labstore-part-2-sigv4-auth/).
- Single-part PUT ETag must be the MD5 for client compatibility (checked by s3-tests).

## Testing / benchmarking tools

- **Chaos**: etcd reimplemented Jepsen as a native Go runner — the model for `kavo-chaos`.
  [toxiproxy](https://github.com/Shopify/toxiproxy) for network faults (HTTP API, CI-friendly);
  Docker SIGKILL for node death; direct byte-flips for corruption.
  [porcupine](https://github.com/anishathalye/porcupine) considered, scoped out (kavo's
  invariants are durability properties, not register linearizability).
- **S3 conformance**: [Ceph s3-tests](https://github.com/ceph/s3-tests) — config file with
  host/port/two credentials, `S3TEST_CONF=your.conf tox -- s3tests/functional`; individual
  tests selectable, so CI can run a curated passing subset.
- **Benchmarks**: [MinIO warp](https://github.com/minio/warp) — put/get/mixed/multipart,
  p50/p90/p99/p99.9 + first-byte latency, segment analysis, distributed mode. Custom harness
  needed for heal time, rebalance fraction, peak RSS.
