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
   write temp → fsync file → rename → fsync directory, then acks. One chunk is held in memory
   so it can be sent to all owners from a single pass over the body — memory stays flat in
   object size, which is the claim that matters, at a cost of one chunk per upload in flight.
   The coordinator writes its own copy through the local store instead of over HTTP.
   The fan-out waits for all N: the extra copy is the entire point of N > W, and a push
   abandoned when the request ends would leave the object under-replicated until repair
   noticed. Returning at W would cut tail latency but needs a background copy that outlives
   the request, so it waits for a benchmark to justify it.
3. After W=2 acks per chunk, and all chunks done, the gateway commits the object manifest
   (chunk list, locations, checksums, size, version) to etcd in one atomic `Put`. That is
   enough: etcd serializes it, so a concurrent overwrite of the same key resolves to one
   manifest or the other and never a mix of both. Compare-and-swap is only needed to reclaim
   the chunks of the manifest being replaced, so it lands with garbage collection.
4. Only then is the client acked. **The etcd commit is the commit point** — the same model as
   S3's `CompleteMultipartUpload`: parts are invisible until atomically assembled.

### Read

Resolve the committed manifest from etcd, stream chunks from any live replica, verify CRC32C
per chunk, return data or an explicit error. Readers never see uncommitted state, so torn
objects are structurally impossible.

**One replica is read, not R=2.** A quorum read exists to resolve disagreement between
replicas, and there is nothing here to disagree about: chunks are immutable, and the manifest
that names them is already the serialized truth in etcd. The chunk's checksum proves the copy
is the right bytes, so reading a second copy would double the network cost to confirm what the
first one already proved. Owners are tried in turn — a chunk that only reached W of N is
genuinely absent from the rest, so a miss is expected rather than a fault — and this node's own
copy is tried first, since it costs no round trip.

The failover window closes once the first byte is on the wire. A replica that turns out to be
corrupt mid-stream aborts the transfer instead of silently falling back, because the earlier
bytes are already sent. That is the honest failure: an error, never a short body presented as
complete.

### Inter-node chunk transfer

Plain HTTP with streaming bodies, no gRPC: a chunk transfer is one request with a body.

- `PUT /peer/chunks/{id}` — `X-Kavo-Crc32c` and `Content-Length` are both mandatory. The
  receiver verifies the staged data against them **before** committing, so a 200 means a
  checked, fsynced replica exists and the coordinator may count it towards W. Committing an
  unverified chunk would make the quorum a promise about data nobody validated.
- `GET /peer/chunks/{id}` — the same header declares what the caller expects. Both ends
  verify: the sender checks its disk copy as it streams, the receiver checks what came off the
  wire. TCP's 16-bit checksum is not strong enough to be the only thing between a flipped bit
  and a committed replica.

**Fault attribution is part of the protocol.** Only a node's own failures may be reported as
5xx, because a coordinator reading 5xx may take that node out of rotation. A bad checksum, a
missing header, an unknown length, or a body that ends early are all the sender's fault and
answered with 4xx.

### Erasure-coded mode (milestone 7)

Per-chunk encoding: each 32–64 MB chunk is encoded in memory into 6 data + 3 parity shards
(~5–10 MB each — `klauspost/reedsolomon` recommends the in-memory API below ~10 MB shards).
The manifest persists shard order, size, and per-shard hash; the library cannot recover from
swapped or corrupted shards without that. Same fault tolerance as RF=3 at ~1.5× disk instead
of 3×; the rebuild-cost penalty is measured and reported honestly.

## Placement

Object key → hash → one of **256 partitions** → nodes via a consistent-hash ring with
**128 vnodes per node**. The partition indirection is what Ceph (placement groups), MinIO
(erasure sets), and Garage (partitions) all do: rebalance tracking, repair queues, and "% of
data moved" are per-partition, not per-object.

A partition is the top 8 bits of the key hash, so it is also a contiguous span of the ring:
one hash gives both the partition and where to start walking for its owners. Owners are the
next N distinct nodes clockwise from the start of the span.

The milestone-3 distribution test (`internal/ring/ring_test.go`) measures what 128 vnodes buys
— worst per-node deviation from an even share of all 256×N ownership slots:

| vnodes/node | 1 | 8 | 32 | 128 | 512 |
|---|---|---|---|---|---|
| 6 nodes | 53% | 35% | 15% | **8%** | 12% |
| 12 nodes | 34% | 28% | 20% | **6%** | 22% |

The curve flattens past roughly one vnode per partition: with only 256 partitions to hand out,
the residual imbalance is set by partition count, not vnode count, and more vnodes merely
reshuffle the same variance. 128 sits at the knee. At exactly N nodes the split is trivially
perfect (every node owns every partition), so that case cannot judge the ring.

## Consistency model

kavo is **not** Dynamo. Chunks are immutable and etcd serializes metadata, so there are no
vector clocks, no sloppy quorums, and no read repair needed for correctness. Quorums answer
exactly one question: are enough copies durably on disk before the manifest commits?

## Membership and repair

Nodes register in etcd under a lease (default TTL 5 s, keepalive at TTL/3) and every node
watches the membership prefix. The lease **is** the failure detector: a node that stops
renewing — crashed, partitioned, or too wedged to heartbeat — has its key dropped by etcd, and
every other node learns it left. No separate heartbeat protocol and no argument about who is
up, because the same etcd that decides which manifests are committed decides who is a member.

Two consequences that fall out of it:

- **Detection is bounded, not merely eventual.** Measured with a 1-second lease, every
  surviving node dropped a SIGKILLed node within 2.3 s — the lease plus etcd's expiry sweep.
- **The ring holds only live nodes**, so writes for a dead node's keys continue on a smaller
  ring instead of being refused until an operator intervenes. In the window before detection
  they are refused with 503, since the coordinator still counts the dead owners and cannot
  reach W. Existing objects are unaffected: their manifests name the nodes their chunks were
  written to, so reads do not depend on the current ring.

A node is always a member of its own ring, even before its registration lands, and a node that
loses its lease re-registers itself. Serving starts only after the first membership arrives from
etcd: a node that placed data while believing it was alone would acknowledge writes with fewer
copies than the cluster could actually make.

Repair:
- The repair coordinator (own code, the interesting part) scans for under-replicated
  partitions, queues rebuilds, and is **rate-limited and resumable** — heal time is measured
  at several rate limits while serving client load (heal-time vs client-latency chart).
- A background scrubber re-reads chunks, verifies checksums, and repairs bit rot from peers.

## Crash safety

fsync errors are data loss: fail the write or quarantine the disk, never retry-and-trust
(fsyncgate — the kernel marks pages clean after a failed fsync). Directory fsync after rename
is mandatory. "Acknowledged" is precisely defined in the commit-point rule above.

The harness (`test/crash_test.go`) SIGKILLs a real `kavod` process mid-flight under 100
concurrent uploads, restarts it on the same data directory, and asserts that every
acknowledged write is byte-identical and that nothing is readable in a partial state.

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
- **Corruption is detected at chunk end, not before**: a streamed GET verifies each chunk's
  checksum as it finishes, so bytes from a corrupt chunk can already be on the wire when the
  mismatch is found. The transfer then aborts short of the promised `Content-Length`, so the
  client always sees a failed transfer — but it may have received corrupt bytes. Verifying
  before sending would mean buffering a whole chunk per request. Same trade-off MinIO makes.
- **A degraded write stays degraded until repair runs**: acknowledging at W=2 of N=3 means the
  third copy is genuinely missing, and nothing yet notices. Reads still succeed because owners
  are tried in turn, but redundancy is below the configured level until milestone 6 restores it.
- **A node that cannot reach etcd is out, even if it is healthy**: it cannot renew its lease, so
  the cluster drops it, and it cannot commit manifests anyway. This makes etcd a hard dependency
  for availability, which is the deliberate trade for having one place that decides what is true.
- **Placement follows the live ring, so a flapping node moves where new objects go**: objects
  written while it was out live on a different owner set. Reads are unaffected because manifests
  record placement, but a node that flaps repeatedly spreads a key's objects over several sets,
  which makes the eventual rebalance (milestone 8) more work.
- **Capacity is balanced to ~±8%, and vnodes cannot fix it**: 256 partitions over 6 nodes
  leaves a worst-case per-node deviation around 8% (measured above). The lever is more
  partitions, not more vnodes — but partition count is fixed for the life of a cluster, so
  kavo publishes the number instead of pretending the split is even.
- **Killing a process is not killing a machine**: SIGKILL proves commit ordering and rename
  atomicity, but the page cache survives it, so the harness cannot prove fsync actually
  reached the platter. That needs power-loss or filesystem fault injection (milestone 10).
- **Interrupted and refused uploads leak chunks**: chunks committed before a crash, or before a
  write was refused for missing quorum, are unreachable and never reclaimed. Harmless but
  unbounded until a GC pass exists (manifests are the only roots, so a mark-and-sweep is
  straightforward).
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
