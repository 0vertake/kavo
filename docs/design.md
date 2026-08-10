# kavo — design

A multi-node object store: clients PUT/GET large objects, the cluster places each object
across nodes, keeps redundant copies (replication or Reed–Solomon erasure-coded shards), and
survives node/disk loss by repairing missing data in the background. The pitch is the
guarantees: acknowledged writes are never lost, partial objects are never readable, a dead
node is healed automatically — with published numbers behind each claim.

## Components

- **`kavod`** — the node daemon. Symmetric (MinIO-style): every node runs the S3 gateway,
  the local chunk store, and the repair participant. Any node can coordinate any request.
- **`kavoctl`** — admin CLI: cluster status, layout changes, heal/scrub triggers.
- **`kavo-chaos`** — chaos runner + invariant checker (milestone 10).
- **etcd** — object manifests, membership (leases), partition layout. Single instance in dev.

## Data path

### Write (replicated mode)

1. Gateway streams the body in fixed chunks (default 32 MB), computing CRC32C per chunk and
   MD5 over the object (S3 ETag compatibility).
2. Each chunk fans out to the N=3 nodes owning the object's partition; each node persists via
   write temp → fsync file → rename → fsync directory, then acks.
3. After W=2 acks per chunk, and all chunks done, the gateway commits the object manifest
   (chunk list, locations, checksums, size, version) to etcd in one compare-and-swap txn.
4. Only then is the client acked. **The etcd commit is the commit point** — the same model as
   S3's `CompleteMultipartUpload`: parts are invisible until atomically assembled.

### Read

Resolve the committed manifest from etcd, stream chunks from any live replica (R=2 for
freshness checks), verify CRC32C per chunk, return data or an explicit error. Readers never
see uncommitted state, so torn objects are structurally impossible.

### Erasure-coded mode (milestone 7)

Per-chunk encoding: each 32–64 MB chunk is encoded in memory into 6 data + 3 parity shards
(~5–10 MB each — `klauspost/reedsolomon` recommends the in-memory API below ~10 MB shards).
The manifest persists shard order, size, and per-shard hash; the library cannot recover from
swapped or corrupted shards without that. Same fault tolerance as RF=3 at ~1.5× disk instead
of 3×; the rebuild-cost penalty is measured and reported honestly.

## Placement

Object key → hash → one of **256 partitions** → nodes via a consistent-hash ring with
**~128 vnodes per node** (the ~100–150 sweet spot; the milestone-3 distribution test verifies
the choice). The partition indirection is what Ceph (placement groups), MinIO (erasure sets),
and Garage (partitions) all do: rebalance tracking, repair queues, and "% of data moved" are
per-partition, not per-object.

## Consistency model

kavo is **not** Dynamo. Chunks are immutable and etcd serializes metadata, so there are no
vector clocks, no sloppy quorums, and no read repair needed for correctness. Quorums answer
exactly one question: are enough copies durably on disk before the manifest commits?

## Membership and repair

- Nodes register in etcd with a lease (TTL ~5–10 s, keepalive at TTL/3); every node watches
  the membership prefix. Lease expiry = failure detection within bounded time.
- The repair coordinator (own code, the interesting part) scans for under-replicated
  partitions, queues rebuilds, and is **rate-limited and resumable** — heal time is measured
  at several rate limits while serving client load (heal-time vs client-latency chart).
- A background scrubber re-reads chunks, verifies checksums, and repairs bit rot from peers.

## Crash safety

fsync errors are data loss: fail the write or quarantine the disk, never retry-and-trust
(fsyncgate — the kernel marks pages clean after a failed fsync). Directory fsync after rename
is mandatory. "Acknowledged" is precisely defined in the commit-point rule above.

## S3 subset (milestone 9)

PUT, GET, DELETE, LIST, multipart upload, SigV4 — nothing else (no IAM/ACLs/versioning/
lifecycle; anti-goal). SigV4 verification is in-house, including streaming chunked payloads
(`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, per-chunk chained signatures — this is what
`aws s3 cp` actually sends). References: `amwolff/awsig`, MinIO `cmd/auth-handler.go`.
External validation: Ceph `s3-tests` via config file + tox; report the pass count.

## Testing and benchmarks

- **Chaos** (milestone 10): Go-native runner (etcd's Jepsen-in-Go precedent) drives
  concurrent workloads, records every acknowledged PUT, injects faults — Docker SIGKILL,
  toxiproxy partitions/latency/bandwidth, direct byte-flips — and asserts the four
  invariants (see `AGENTS.md`) from recorded history.
- **Benchmarks** (milestone 11): MinIO `warp` for throughput/latency (put/get/mixed/
  multipart, p50–p99.9); custom harness for heal time, rebalance fraction, and peak RSS
  while streaming. Final numbers on 3–4 separate cloud VMs over a real network.

## Known limitations (publish these)

- **Small objects**: at 1 KB, etcd round-trips and manifest overhead dominate. MinIO inlines
  small objects into metadata for this reason; kavo documents the trade-off.
- **Metadata ceiling**: per-object manifests in etcd cap object count (etcd practical limit
  ~2–8 GB). Fine for the ~100 GB working set; the at-scale answer is volume/needle packing
  (SeaweedFS/Haystack).

## Milestones

1. Single-node store: chunked, streaming, checksummed (flat RSS on 1 GB; corrupt chunk fails read)
2. Crash safety on one node (SIGKILL under 100 concurrent uploads; zero acked loss)
3. Multi-node placement (partitions, vnode ring, distribution test)
4. Replication + quorum (read with node down; overhead measured)
5. Membership + failure detection (etcd leases; bounded detection time)
6. Automatic repair (rate-limited, resumable; heal-time vs latency chart)
7. Erasure coding as second mode (both modes measured side by side)
8. Rebalance on join/leave (% moved, convergence time)
9. S3 subset + SigV4 (`aws s3 cp` end to end; s3-tests pass count)
10. Chaos suite in CI (invariants asserted under randomized faults)
11. Benchmarks + README + demo video (real machines, real network)

Milestones 6, 7, 10 are where the project stops being a tutorial — never skip them for API
surface.
