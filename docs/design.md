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

### Erasure-coded mode

Per-chunk encoding: each chunk is encoded in memory into `k` data + `m` parity shards, 6+3 by
default (~5–10 MB each — `klauspost/reedsolomon` recommends the in-memory API below ~10 MB
shards). Any `k` shards reconstruct the chunk, so 6+3 tolerates three losses at 1.5× the bytes
where three copies cost 3×. Measured on disk: 3.00× against 1.50× for the same object.

The mode is per object, not per cluster. `-ec=6+3` changes what new writes do; every manifest
records the code it was written with, so switching modes — or misconfiguring one node — cannot
strand data that already exists. Both kinds of object are readable from the same cluster at the
same time.

**Shard position is structural.** Shard *i* lives on `Nodes[i]` and nowhere else, because
Reed–Solomon identifies a shard by which equation it belongs to. Two consequences the tests pin
down:

- The manifest carries the chunk length, the shard order, and a checksum per shard. Without them
  the library cannot tell a swapped or corrupted shard from a valid one — it solves whatever
  equations it is handed, and returns garbage with no error. `ec.TestSwappedShardsAreNotDetected`
  exists to demonstrate exactly that.
- A `k+m` code needs `k+m` nodes. Replication can narrow to a smaller cluster and let repair catch
  up; erasure coding cannot, so a write that has nowhere to put shard `k+1` is refused instead of
  storing something unrebuildable.

**Acknowledged at `k+1` shards, not `k`.** Acking at `k` would mean one later loss makes the
chunk unreadable — an acknowledged write lost to a single failure. One spare, mirroring how W=2 of
N=3 leaves a spare copy; repair rebuilds the rest.

**Repair and scrub rebuild rather than copy.** There is no second copy of shard 4 anywhere, only
the arithmetic that produces it, so a missing or rotted shard costs a decode of its chunk. One
decode produces every missing shard of that chunk, which is why the survey is per chunk. That
rebuild cost is the read-side price of storing 1.5× instead of 3×, and the numbers are in
`docs/benchmarks.md`: writes 15% slower for half the bytes, reads ~23% slower, and a degraded read
— with the code's full tolerance missing — no slower than a healthy one.

Still costlier in memory than replication: a decode holds a chunk's shards plus the chunk. Flat in
object size, which is the invariant, but ~2.6× a chunk rather than 1×.

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

**Reads do not depend on the current membership either**, which is a stronger claim and one that
had to be fixed rather than assumed. A node's address is remembered after it leaves, and a read
falls back to the last one it was known at. A lease lapses because a node was too busy to renew
it — which is exactly when reads are arriving — and it does not move a chunk off the disk it is
sitting on. The failure this prevents is a GET returning nothing while every copy is present and
every process is answering; under load it is not hypothetical, it is what the `aws` CLI test hit.

Reading from a node the cluster has given up on is safe because chunks are immutable and
checksum-verified: a stale address either serves the bytes the manifest names or fails. Placement
deliberately does **not** get the same fallback — acknowledging a write to a node the cluster has
written off would promise durability nobody is maintaining.

A node is always a member of its own ring, even before its registration lands, and a node that
loses its lease re-registers itself. Serving starts only after the first membership arrives from
etcd: a node that placed data while believing it was alone would acknowledge writes with fewer
copies than the cluster could actually make.

### Repair

Under-replication is a state to keep converging out of, not an event to react to. Two things
cause it and neither announces itself: a write acknowledged at W=2 of N=3 leaves a copy that
never existed, and a disk that comes back empty leaves copies that did. So every node runs a
repair loop, forever.

A pass walks the cluster's manifests in key order and restores every copy each manifest promises:

- **One node repairs each partition** — the first owner in the current ring. Without that rule
  every node would survey every object, N times the work, and several would race to push the
  same chunk to the same place. If that node dies, membership changes, the ring's first owner
  changes with it, and the new one takes over: no election and no repair leader. Two nodes
  briefly disagreeing about membership is harmless, because a push is idempotent — both would
  write and verify the same bytes.
- **Rate-limited** to a byte-per-second cap (default 32 MB/s per node). Repair competes with
  clients for the same disks and the same network; an unthrottled heal turns one dead node into
  a cluster-wide latency spike.
- **Resumable**, with a cursor in etcd per node. A heal that started over every time a process
  restarted would never reach the last object.
- **Idempotent**: a pass over a healthy cluster moves nothing. Missing copies are found with
  `HEAD /peer/chunks/{id}`, because surveying by fetching would move the cluster's data to
  answer a yes-or-no question.
- **Verified**: a rebuilt copy is streamed from a source that checks its own disk into a receiver
  that checks the wire and refuses to commit anything not matching the manifest's checksum.
  Repair cannot spread corruption.
- **Loud about data loss**: a pass that finds copies missing everywhere still finishes, because
  the rest of the cluster needs repairing, but reports `ErrUnrepairable` rather than success.

### Scrubbing

Repair asks whether a copy is *there*. Rot is the other question: a disk that quietly rewrites a
sector answers every survey correctly and fails only at the moment a client reads it, which is
far too late for that to be the first time anyone looked.

So every node also re-reads its own chunks and checksums them against their manifests. Each node
scrubs its own disk, because it is the only node that can read it — a different rule from repair,
where one node surveys a partition on everyone's behalf. A rotted copy is replaced from a peer
whose copy still verifies, staged and renamed so the bad copy is never half-overwritten. Rot in
every copy reports `ErrRotUnrecovered`: the last good copy of that data is gone, and that must be
said out loud.

Passes are paced by the same byte rate as repair and default to once a day, because a pass reads
every byte the node stores. Rot is rare and disks are large, so scrubbing is a slow sweep rather
than a reaction.

### Rebalancing

Repair restores the copies a manifest promises and refuses to put them anywhere else, which
leaves one hole on purpose: a node that leaves for good takes its copy's *place* with it. The
manifest still names it, repair still declines to substitute another node, and the object sits at
N-1 copies forever. Rebalancing is the part that changes where copies belong — and the same
mechanism handles a join, where the ring hands a partition to a new node that holds none of its
data.

A pass compares each manifest's owners against the ring and moves the objects that disagree, at
the same byte rate as repair and behind the same first-owner rule. The interval is longer
(5 minutes against 1) because misplaced redundancy is a weaker problem than missing redundancy.

**Copy, commit, delete — in that order.** Every reader in between resolves the old manifest and
finds the old copies exactly where it expects them, so at no point is the object readable only
from nodes that no manifest names. Dropping first would delete the only copies a live manifest
points at; committing first would promise copies that do not exist yet. A move that cannot place
every copy is abandoned before the commit and retried next pass, which is why the pass counts
failures instead of returning them: one unreachable node must not stop the sweep, and the object
is still readable where it was.

**The commit is a compare-and-swap** against the revision the manifest was read at. A client may
overwrite an object while the pass is copying its chunks; that manifest is the newer truth and was
written to the current owners anyway. Committing the moved placement on top of it would resurrect
the old object and lose an acknowledged write, so the pass abandons the move and leaves the copies
it made for the next pass to collect.

Two rules keep a move from making redundancy worse:

- **A shrinking cluster is left alone.** If the ring cannot name as many owners as the manifest
  has copies, moving now would commit a manifest promising fewer copies than the object was
  acknowledged with. Waiting costs nothing — the copies that exist are still where the manifest
  says.
- **Coded shards move by position.** Shard *i* belongs to owner *i* and nowhere else, so both the
  copy and the delete are decided per position rather than per node. A position-blind move leaves
  every shard present and the chunk undecodable.

Deletions on the old owners are logged rather than returned: the object is correctly placed and
committed by then, so a copy that could not be deleted is wasted disk, not a durability problem.
A node that has already left is never asked — its disk is gone or will be reclaimed when it
rejoins and finds no manifest pointing at what it holds.

## Crash safety

fsync errors are data loss: fail the write or quarantine the disk, never retry-and-trust
(fsyncgate — the kernel marks pages clean after a failed fsync). Directory fsync after rename
is mandatory. "Acknowledged" is precisely defined in the commit-point rule above.

The harness (`test/crash_test.go`) SIGKILLs a real `kavod` process mid-flight under 100
concurrent uploads, restarts it on the same data directory, and asserts that every
acknowledged write is byte-identical and that nothing is readable in a partial state.

## S3 subset (milestone 9)

PUT, GET, DELETE, LIST, multipart upload, SigV4 — plus the six operations a client calls without
being asked to: `CreateBucket`, `ListBuckets`, `DeleteBucket`, `DeleteObjects`,
`ListObjectVersions` and `GetBucketLocation`. Nothing else (no IAM/ACLs/versioning/lifecycle;
anti-goal).

Those six are not a widening of the subset so much as the cost of being reachable. An SDK creates
the bucket before its first upload, `aws s3 ls` with no argument lists buckets, and every client
empties a bucket by listing versions and bulk-deleting what it finds. None of them store anything
new: a bucket is still a prefix, and the version listing reports every object once with the id
`null`, which is S3's own answer for a bucket that was never versioned.

External validation: Ceph `s3-tests`. **151 of 886 pass, nothing errors**, and every failure is
classified in `docs/s3-compatibility.md` — as an anti-goal, a consequence of buckets being prefixes,
or a named gap. The suite found four real defects, three of which kavo's own tests could not see;
they are listed there too. It also found eighteen passes that were not real: a `PUT` to any bucket
subresource reached the create-bucket handler and answered 200, so kavo was claiming to have
configured lifecycle rules, policies and encryption it has no code for. Refusing them is what took
the count from 169 to 151.

### Object API

**Two listeners.** `-s3` is the client port; `-addr` is the internal one carrying peer chunk
transfer and cluster state. The internal port needs no signature and can delete a chunk, so a
client that could reach it could empty the store one chunk at a time. Splitting them is one line
of wiring and removes the whole class of problem.

**Buckets are prefixes.** An object's key in etcd is `bucket/key`, so a bucket exists as soon as
it is named: nothing to create, nothing to delete, and a listing is a prefix scan of the manifest
keyspace. `HEAD /bucket` therefore always succeeds — clients check before uploading, and a
truthful "no such bucket" would stop a client that is about to create it in the same breath.

**ETag is the object's MD5**, computed as the body streams past on the way to the chunkers, not
by reading the object back. MD5 because that is the value clients compare their upload against;
integrity is still CRC32C per chunk, verified on every read. Two hashes with two jobs.

**Ranged GETs are not optional.** `aws s3 cp` downloads anything over 8 MB as concurrent ranged
GETs — measured, not assumed — so without ranges the standard client cannot fetch a large object
at all. Open-ended (`bytes=N-`) and suffix (`bytes=-N`) forms both appear in practice. A range is
served by reading **every chunk the window touches in full** and discarding the surplus: a
chunk's checksum covers the chunk, so serving a slice without reading the rest would hand back
bytes nothing verified, and rot just outside the window would go unreported. The cost is at most
two extra chunks of I/O; the alternative is invariant 3 with a hole in it.

Anything before the response body is decided before a byte is written, because a status already
sent cannot be withdrawn. A read that fails **after** the body started is reported by stopping
short of the promised `Content-Length` — a truncated transfer every client treats as an error,
which is the only remaining way to say "do not trust these bytes".

**Delete removes the manifest first**, which is the instant the object stops existing, and only
then reclaims chunks. The reverse order would drop chunks a live manifest still names. Deleting a
key that is not there succeeds, because S3 promises an idempotent delete and clients build
cleanup loops on it.

### Listing

`ListObjectsV2` only. Every current client uses it, and v1 differs in how it carries the resume
point, so answering a v1 request with a v2 body would be worse than refusing it.

Keys are stored as `bucket/key`, so a listing is a prefix scan and etcd returns it already
sorted — which is the whole reason a listing is cheap. Paging reads etcd in fixed batches
independent of the page the client asked for, because a page of entries and a page of keys are
not the same thing once a delimiter is involved.

**The delimiter is where listings go wrong.** A grouped prefix must be reported once, and the
keys inside it must not be looked at again — including by the *next* page, which is the part that
is easy to miss: a resume point that names the last key seen restarts inside the group and reports
it a second time, so a client paging through a bucket sees the same directory over and over. The
continuation token is therefore a *position*, not a key: for a page that ended inside a group it
is the point just past everything the group covers. Within a page already fetched, the group's
remaining keys are skipped in memory; across pages, they are never read at all.

**`encoding-type=url` is correctness, not cosmetics.** Encoding down to the unreserved set, not
`url.QueryEscape` and not `url.PathEscape`: the first turns a space into `+`, which a client
unescaping paths hands back as a literal plus, and the second leaves `+` alone, which a client
unescaping queries hands back as a space. Either mistake renames the key. A key whose raw form is
itself a valid escape sequence — `100%25.txt` — is the one that proves it: sent unencoded, the
`aws` CLI asks for `100%.txt` and gets a 404 for a key the listing had just named. That is a test.

### Multipart upload

Not optional either: the `aws` CLI switches to it above 8 MB, so without it the standard client
cannot upload a large file at all.

**A part is written exactly like a small object, but placed by the final object's key.** That one
choice is what makes completion cheap: the part's chunks are already on the nodes the object's
manifest will name, so completing an upload is a single manifest commit rather than a copy of
everything. Part manifests live under the upload's own prefix in etcd, not among the objects — a
part must never appear in a listing or resolve as an object, because a part *is* a partially
written object and invariant 2 forbids reading one.

The commit point is unchanged. Before the completion there is no object; the completion is one
manifest naming every part's chunks in order, all of them already fsynced on a quorum. The
consequence worth stating: an upload interrupted at any point leaves no object at all, and the
object that does appear appears whole.

**The completion is validated before anything is committed.** A part that was never uploaded, an
ETag that is not the one this server stored, a part named twice, parts listed descending — each is
refused with nothing changed. Committing a manifest over a client's mistake would produce an
object that is readable, checksum-valid, and not what anybody uploaded: the worst available
outcome, since nothing downstream can detect it. A refused completion leaves the upload intact so
the client can correct its request; only an abort discards parts.

Parts are **not** sorted for the client. The client believes it knows what order its bytes go in,
and quietly reordering them to whatever we prefer would turn a client bug into a corrupt object.

The one failure a client cannot cause is the ring moving between two parts. A manifest names one
node set for all its chunks, so parts placed by two different memberships cannot be described by
any single manifest, and the completion is refused rather than committed against nodes that never
held the chunks. Tested by changing membership mid-upload.

The ETag is the MD5 of the parts' MD5s with the part count after a dash, because that is the value
clients recompute to check the upload. `ListParts` and `ListMultipartUploads` are not implemented:
nothing in the locked subset needs them, and no client kavo is tested against asks.

Error bodies are S3's XML with S3's codes, because the code is the part clients act on: an SDK
retries `SlowDown`, refuses to retry `SignatureDoesNotMatch`, and turns `NoSuchKey` into a typed
error applications branch on. A quorum failure is `SlowDown` — it says the write may succeed
later, where `InternalError` says nothing.

Tested against the AWS SDK's S3 client and against the real `aws` CLI driving a cluster of real
processes. The CLI is the only oracle that proves the subset is usable rather than merely
self-consistent: it is what found the ranged-download requirement, and it is what will fail the
day a header is quietly wrong.

### SigV4 verification

In-house, and verification only: kavo is the server, so the signature is recomputed from the
request as it arrived. The awkward parts are all in rebuilding the string the client hashed —
`host` and `content-length` are not in Go's header map, header values have their internal
whitespace collapsed, the query is sorted and re-encoded with `%20` rather than `+`, and S3
signs the request path **as sent** where every other AWS service re-escapes it. That last one
only breaks keys containing a space, a plus, or anything non-ASCII, which is exactly the kind
of bug that ships.

Four payload modes, because a client that gets a 400 for the mode it chose cannot upload at all:

| mode | what the signature covers |
| --- | --- |
| hex SHA-256 | the whole body, verified as it streams |
| `UNSIGNED-PAYLOAD` | headers only — the client's choice, not the server's |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | each `aws-chunked` chunk, chained to the one before |
| `STREAMING-UNSIGNED-PAYLOAD-TRAILER` | headers only, `aws-chunked` framing |

Both signed modes are verified **as the body streams**, never after buffering it: a body may be
larger than memory, so the hash runs alongside the read and a mismatch is reported in place of
the final EOF. A caller that reads to the end therefore cannot mistake unverified bytes for
verified ones — the same discipline the chunk store uses for its checksums.

Measured, not assumed: `aws-cli` 2.35 and `boto3` 1.43 were pointed at a request-dumping server,
and **neither sends `aws-chunked` at all** — both hash the payload up front, even for 8 MB
multipart parts read from a pipe. The streaming modes are implemented for the clients that do
(the Java SDK, `mc`), and because streaming is the only way to send an object of unknown length
without buffering it. The design note that `aws s3 cp` requires it is out of date.

The chain is what makes streaming safe: each chunk's signature includes the previous chunk's, so
chunks cannot be reordered, duplicated, or dropped, and a body that stops before its
zero-length terminator is rejected rather than stored as a short object. Trailers are parsed and
ignored — the only ones S3 clients send are whole-object checksums, and every byte has already
been verified by the time they arrive.

Credentials are one static key pair (`-access-key`/`-secret-key`). There is no user directory
because IAM is an anti-goal: authentication is proof of holding the one secret, and
authorization does not exist. The credential scope's region is not pinned — it is covered by the
signature, so it cannot be forged, it just is not required to be any particular value.

Tested against the AWS SDK's own signer, used as a test-only dependency. Hand-written vectors
would only prove the implementation agrees with what its author believed the spec said; a second
implementation disagreeing is the only thing that catches a misreading.

## Testing and benchmarks

- **Chaos** (milestone 10): `go test ./test -run TestChaos`, tunable with `-chaos.duration` and
  replayable with `-chaos.seed`. Eight workers drive a signed S3 workload — single PUTs and
  multipart uploads, reads through a different node than the write went to, and deletes — against
  four real `kavod` processes while faults arrive on a seeded schedule: SIGKILL and restart,
  SIGSTOP (a frozen process: ports open, nothing answered, no lease renewed), a wiped disk, and a
  flipped bit under a running node. Then it asserts the four invariants (see `AGENTS.md`) from the
  recorded history.

  Three things make it more than a smoke test.

  **The history distinguishes three outcomes, not two.** Acknowledged, refused, and *unknown* — a
  request whose response was lost. An acknowledged write must be readable byte for byte; an
  acknowledged delete must be gone; an operation nobody got an answer to may have happened or not,
  and both are correct. Conflating the last two is how a chaos suite invents a lost write: a crash
  between "manifest removed" and "204 sent" is two correct steps and a dropped reply, and the first
  version of this suite reported it as data loss.

  **Faults stay inside the redundancy they were configured with.** N=3 tolerates two copies of a
  chunk being lost, not three. The first version wiped three disks in five seconds, lost an
  object, and was right to: nothing promises to survive that. So faults are strictly one at a time,
  and the next one waits at a barrier — every node a member again, and every chunk of every
  manifest present on every owner that manifest names. That barrier *is* invariant 4, asserted
  after every single fault rather than once at the end, and it is what turns "healing works" from
  a claim into a measurement.

  **Redundancy is checked against the manifests, not the ring.** An object is where its manifest
  says it is, so the checker reads manifests out of etcd and stats files on each node's disk —
  the same question repair and rebalance answer, asked from outside the code that answers it.

  A read that fails during a fault window is allowed: invariant 3 promises checksum-valid data
  **or an explicit error**. The one unacceptable outcome is a complete response whose bytes are not
  what was written, which the suite separates from every other failure and reports on its own.

  What it does not cover: a partition that isolates nodes from each other while leaving them
  reachable by clients (SIGSTOP freezes a node from everyone at once), erasure-coded mode, and
  anything that needs the page cache to disappear — see the fsync limitation below.
- **Benchmarks** (milestone 11): in-repo benchmarks cover both APIs, both redundancy modes, repair,
  scrub and listing — what they measured, what it changed, and what it deliberately did not, is in
  `docs/benchmarks.md`. Still outstanding: MinIO `warp` for latency distributions (p50–p99.9) and
  peak RSS while streaming, on 3–4 separate cloud VMs over a real network. Nothing measured on one
  laptop with one disk is a headline number.

## Known limitations (publish these)

- **Small objects**: at 1 KB, etcd round-trips and manifest overhead dominate. MinIO inlines
  small objects into metadata for this reason; kavo documents the trade-off.
- **Corruption is detected at chunk end, not before**: a streamed GET verifies each chunk's
  checksum as it finishes, so bytes from a corrupt chunk can already be on the wire when the
  mismatch is found. The transfer then aborts short of the promised `Content-Length`, so the
  client always sees a failed transfer — but it may have received corrupt bytes. Verifying
  before sending would mean buffering a whole chunk per request. Same trade-off MinIO makes.

  "Aborts short" had a hole, and the chaos suite found it: for the **last** chunk there is
  nothing left to withhold once the mismatch is known, so a corrupt final chunk was delivered as
  a complete, successful, wrong response — worst of all on a single-chunk object, where the last
  chunk is the only one. Fixed by holding back the final byte of every chunk until that chunk's
  checksum verifies. One byte of memory, and the client either gets the length it was promised
  from chunks that verified, or a transfer that stops short.
- **Redundancy returns at two different speeds**: repair rebuilds a missing copy on an owner the
  manifest already names, so a crash or a lost disk heals on the repair interval. A node that is
  gone for good needs its copy's place moved, which is a rebalance pass — five times less frequent
  by default, and it moves data rather than restoring it.
- **Repair sees presence, not integrity**: a chunk that has rotted still answers repair's survey
  as present. The scrubber is what finds it, on a much slower cycle, so rot can sit undetected for
  up to one scrub interval.
- **Surveying costs a round trip per copy**: one `HEAD` per chunk per owner, so a pass over a
  large cluster is many small requests. Batching the question ("which of these ids do you have?")
  is the obvious fix, and waits for a benchmark that shows it matters.
- **A node that cannot reach etcd is out, even if it is healthy**: it cannot renew its lease, so
  the cluster drops it, and it cannot commit manifests anyway. This makes etcd a hard dependency
  for availability, which is the deliberate trade for having one place that decides what is true.
- **Placement follows the live ring, so a flapping node moves where new objects go**: objects
  written while it was out live on a different owner set. Reads are unaffected because manifests
  record placement, but a node that flaps repeatedly spreads a key's objects over several sets, and
  every rebalance pass then moves them back — real disk and network spent on a node that is not
  staying. Nothing damps this: the ring follows membership immediately, and a node is a member the
  moment its lease exists.
- **Capacity is balanced to ~±8%, and vnodes cannot fix it**: 256 partitions over 6 nodes
  leaves a worst-case per-node deviation around 8% (measured above). The lever is more
  partitions, not more vnodes — but partition count is fixed for the life of a cluster, so
  kavo publishes the number instead of pretending the split is even.
- **Killing a process is not killing a machine**: SIGKILL proves commit ordering and rename
  atomicity, but the page cache survives it, so the harness cannot prove fsync actually
  reached the platter. That needs power-loss or filesystem fault injection (milestone 10).
- **Interrupted uploads, overwrites and abandoned moves leak chunks**: chunks committed before a
  crash, before a write was refused for missing quorum, replaced by an overwrite, or copied by a
  rebalance whose commit lost to a client are unreachable and never reclaimed. A `DELETE` and an
  aborted multipart upload do reclaim their own chunks; nothing collects the rest. Harmless but
  unbounded until a GC pass exists (manifests are the only roots, so a mark-and-sweep is
  straightforward).
- **A multipart upload nobody finishes is never cleaned up**: an upload whose client walks away
  without aborting keeps its parts' chunks and its etcd records forever. S3 solves this with a
  lifecycle rule, which is an anti-goal here; the same GC pass is the fix, and an upload's record
  carries its creation time so the pass has something to age against. Parts uploaded twice, or
  uploaded and left out of the completion, leak the same way.
- **A delete can leave copies behind**: the manifest goes first, so the object is gone the moment
  it returns, but an owner that is down when the chunks are dropped keeps its copy forever — the
  delete does not come back for it. Same GC pass fixes it.
- **Multi-range requests are answered with the whole object**: `bytes=0-9,20-29` returns everything
  rather than a `multipart/byteranges` body. Allowed by HTTP, matches S3, and no S3 client asks.
- **`ListObjects` v1 is refused**: only v2 is served. Every current client uses v2; a v1 client gets
  `NotImplemented` rather than a body in the wrong shape.
- **A listing is not a snapshot**: it pages through etcd, so an object written or deleted mid-listing
  may or may not appear. S3 makes the same non-promise.
- **Metadata ceiling**: per-object manifests in etcd cap object count (etcd practical limit
  ~2–8 GB). Fine for the ~100 GB working set; the at-scale answer is volume/needle packing
  (SeaweedFS/Haystack).

## Milestones

1. Single-node store: chunked, streaming, checksummed (corrupt chunk fails read; flat memory proven
   twice — allocation counters at 64 MB in a unit test, and the node process's own RSS at 4 GB in
   `make measure`, where 64x the object costs 2x the memory above idle)
2. Crash safety on one node (SIGKILL under 100 concurrent uploads; zero acked loss)
3. Multi-node placement (partitions, vnode ring, distribution test)
4. Replication + quorum (read with node down; overhead measured)
5. Membership + failure detection (etcd leases; bounded detection time)
6. Automatic repair (rate-limited, resumable). Heal time measured: a node that loses its whole disk
   is back to full redundancy in 9.2 s at the default 32 MB/s cap, 1.1 s uncapped. The cap is per
   node, so cluster heal bandwidth grows with the cluster while the disturbance per node does not.
7. Erasure coding as second mode (both modes measured side by side)
8. Rebalance on join/leave. Measured: a seventh node is seen in 40 ms and converged in 4.6 s, and
   the 34 copies of 192 that move onto it are exactly the 34 the seven-node ring owes it — placement
   after a join is the placement the ring specifies, key for key.
9. S3 subset + SigV4 (`aws s3 cp` end to end; s3-tests pass count — `docs/s3-compatibility.md`)
10. Chaos suite in CI (invariants asserted under randomized faults; GitHub Actions runs it long
    on every push, which is where a fixed heal deadline was caught measuring the keyspace)
11. Benchmarks + README (`docs/benchmarks.md`, including `warp` as an outside client and `make
    measure` for the cluster-level numbers). Still outstanding: separate machines over a real
    network, and a demo recording

Milestones 6, 7, 10 are where the project stops being a tutorial — never skip them for API
surface.
