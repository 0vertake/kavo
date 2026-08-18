# kavo — design

A multi-node object store: clients PUT/GET large objects, the cluster places each object
across nodes, keeps redundant copies (replication or Reed–Solomon erasure-coded shards), and
survives node/disk loss by repairing missing data in the background. The pitch is the
guarantees: acknowledged writes are never lost, partial objects are never readable, a dead
node is healed automatically — with published numbers behind each claim.

## Components

- **`kavod`** — the node daemon. Symmetric (MinIO-style): every node runs the S3 gateway,
  the local chunk store, and the repair participant. Any node can coordinate any request.
- **etcd** — object manifests, membership (leases), partition layout. Single instance in dev.

That is the whole of it, and two things planned here were deliberately not built. The chaos runner
became a test (`test/chaos_test.go`) rather than a binary, because it has to start and kill real
`kavod` processes and assert against a recorded history, which is what a test harness already does.
And `kavoctl` has nothing left to do: cluster status is one HTTP GET on the internal port, and repair,
scrub, rebalancing and collection are background passes with no button to press — a CLI that could
trigger them would be a remote-control surface on an unauthenticated port, which is the one surface
this design is otherwise careful to keep small.
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
   manifest or the other and never a mix of both. It stayed a plain `Put` after garbage
   collection landed too — reclaiming an overwritten version's chunks turned out not to need
   the superseded revision, and the reason is under *Collecting unreferenced chunks* below.
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
- A `k+m` code needs `k+m` nodes, so a write that has nowhere to put shard `k+1` is refused instead
  of storing something unrebuildable. Replication refuses too, below W nodes, for the reason in the
  next section.

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

**A placement narrower than N is widened, not lived with.** The other half of the same mistake, and
a longer-lived one. A write acknowledged at W while some nodes were unreachable names fewer nodes
than the configuration asks for — correctly, at the time — and the pass that fixes placement was
asking the ring for as many owners as the manifest already had, so it moved such an object sideways
forever and never added the owner it was missing. Repair could not help: it refuses to put a copy
anywhere the manifest does not already name, because that is rewriting placement, which is
rebalancing's job. So the object stayed one copy short of its configuration for life.

Nothing could see it, either, which is the part worth keeping in mind. Every check in the codebase
and in the chaos suite asked whether the copies a manifest names are present, and an object
promising two copies and holding two is a full house by that measure. So the barrier now checks the
promise as well: the cluster is whole when every manifest names its own code's full width. With
that check and the fix reverted, `-chaos.seed=1786500476032706000` fails on
`chaos/w0/obj0446: placed on n1,n4, want 3 owners` — a real narrow write from a freeze storm, which
is what invariant 4 was quietly missing.

Rebalancing now targets the redundancy the object was *written* with, read from the object's own
recorded code rather than from this node's configuration, since a cluster can hold replicated and
erasure-coded objects at once. It still never commits fewer copies than a write was acknowledged
with, so a shrinking cluster waits rather than narrowing anything.

**W copies on distinct nodes or none at all, even when the cluster looks smaller than W.** The
code used to require "every owner there is", so a coordinator that could see fewer than W nodes
would place on the nodes it had and acknowledge that. It reads like graceful degradation and it
behaves like data loss, which the chaos suite proved by doing it: with three of four nodes frozen,
the survivor's view collapsed to itself — a node keeps itself in its own ring, because a node is
alive by definition — so it placed a two-chunk object on its own disk, acknowledged it, and the
disk wipe that arrived next destroyed it. Repair had nothing to rebuild from, the read returned
`unexpected EOF`, and the object's server-side copy went with it, since a copy shares its source's
chunks. Replay it with `-chaos.seed=1786500476032706000`; the four missing copies for two
two-chunk objects are the tell, because a manifest naming three nodes would have left twelve.

A narrow ring and a small cluster are indistinguishable from inside, so a write that cannot reach
W distinct nodes is refused with `SlowDown` before a byte is stored. The cost is availability, in
the only case where availability and durability actually conflict: a node cut off from its peers
becomes read-only rather than accepting writes it cannot keep. That is the trade a store whose
first invariant is "no acknowledged write is ever lost" has already made.

Which makes W something an operator declares (`-w`, default 2) rather than something the code
infers from the moment. One node running kavo can only be asking for one copy, and it says so with
`-w 1`; what it must not do is have that decision made for it by a lapsed lease. The crash-safety
harness is exactly that deployment — its claim is about what one disk holds after SIGKILL, and a
second node would answer its reads from a copy and prove nothing — so it passes `-w 1` and gives up
replication explicitly.

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

"A peer" means any node the manifest names that the cluster has ever known, not only a current
member — the same rule the read path uses, and for the same reason: a chunk is immutable and
verified against the manifest's checksum at both ends, so a node whose lease has lapsed either
hands over the bytes the manifest names or fails to answer. Asking only current members, which is
what this did, made rot unrecoverable in the case where recovery matters most: the copies that
verify sitting on nodes the cluster has dropped while the broken one is here. Reads kept working
through those very addresses, so nothing reported a problem, and the rot waited for the good copies
to go too. It divides every peer call in the cluster package: whatever wants bytes accepts a
last-known address, whatever hands bytes over or counts them as present requires a member.

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

### Collecting unreferenced chunks

Every write leaves chunks nothing points at. An overwrite supersedes the chunks of the manifest
it replaces. A write that fails after storing data leaves what it stored. A delete and an aborted
upload leave everything they named. A rebalance leaves behind the copies it moved away from. None of
it is reachable — readers resolve objects only through committed manifests — so none of it threatens
correctness. It is storage that is paid for and not counted, and it only ever grows.

The obvious design is to record what each write superseded: commit the new manifest and a list
of the old one's chunks in one transaction, then have a worker delete them. That needs the
revision the commit replaced, which is the compare-and-swap the write path was always going to
grow. **It is the wrong design, and the reason is `CopyObject`.** Chunk ids are a random prefix
per write plus an index, so no two *writes* ever name the same chunk — but copying an object
server-side means copying its manifest and not its data, which makes two keys name one set of
chunks. Under sharing, deleting the chunks of the manifest an overwrite replaced would delete
chunks a copy still references, which is an acknowledged write lost from the collection path. A
per-write record cannot express sharing without reference counts in the commit path.

So the pass is mark-and-sweep instead. A node reads every manifest in the cluster, builds the
set of chunk ids some manifest says *this node* should hold, and deletes the local chunks that
set does not contain. A chunk is live if any manifest names it, so sharing costs nothing, and
the write path is untouched: the commit point keeps the proof it already had. It also reclaims
garbage no record would ever have been written for, which is every category above except the
first.

**And it is the only thing that deletes a chunk.** That is the same argument reaching every other
pass, once sharing exists. A delete used to drop its object's chunks, an abort its parts', a
rebalance the copies it had just moved away from — each on the strength of the one manifest in front
of it, which is exactly the judgement sharing invalidates. Deleting a copied object would have taken
its source's data; and the worse one, because nobody asked for it: a copy keeps its source's
placement, so the ring makes it misplaced from the moment it exists, and the rebalance that tidied
it up would drop the source's copies on the way past. `TestMovingACopyLeavesTheSourceReadable`
covers that. So none of them delete anything now, and there is no longer any code that can delete a
chunk on another node — the peer API's delete route is gone, which also takes a capability off the
unauthenticated internal port. The cost is paid at the disk: space returns within a collection cycle
rather than at once, which is why the interval defaults to a minute and a full cycle to about half
an hour.

Three things make it safe:

- **A partial read of the manifests deletes nothing.** A scan that failed halfway is
  indistinguishable from a cluster where the objects it did not reach do not exist. Any error
  ends the pass before it touches the disk.
- **The cutoff is when the scan started, not now.** An object written during the scan, under a
  key the scan had already passed, is invisible to it. Nothing written after the scan began may
  be deleted on the strength of it, however long the scan took.
- **A write in flight says so, and the sweep reads it.** A chunk is durable before the manifest
  naming it is committed, so for that window good data is referenced by nothing at all. A write
  that can have more than one chunk therefore records itself in etcd before it stores any of
  them, and the sweep treats every chunk of a recorded write as live. One key covers a whole
  upload, because every chunk of one write shares an id prefix. In-flight multipart uploads are
  covered the same way and additionally by their part manifests — S3 lets a client take days —
  and the pass reads the in-flight records first, then the parts, then the objects, which is
  what makes each handover safe whichever side of the scan it falls on.
- **A grace period covers the rest, which is now a tail rather than a window.** A write with a
  single chunk commits a moment after storing it and never bothers with a record; age is what
  covers that moment, and the default is a minute.

All of these are checked against real processes rather than argued, at a grace of zero so that age
protects nothing and the records are all that is holding the objects up: six clients writing
continuously against a sweep every 10ms, every acknowledged write read back byte for byte
(`TestWritesArrivingDuringSweepsAreAllReadable`); one client handing over eight chunks with a pause
between each, so the pass sweeps the object's slice several times while the upload is in flight
(`TestASlowUploadIsNotSweptWhileItArrives`); and an upload left as nothing but parts through fifteen
full cycles of the id space before it completes (`TestAnUploadOutlivingManySweepsStillCompletes`).
Ignoring the in-flight records, or recording a write after its first chunk is stored instead of
before, or reading part manifests not at all: every one of those mutations turns a read into
`unexpected EOF`.

What the manifest says, not what exists: a copy on a node the manifest does not name is
unreachable, because a reader tries the nodes it names and stops. That is what lets this pass
reclaim the copies a move leaves behind — which is now every move, since nothing but this pass
deletes a chunk.

**And what the ring says, which is the seam where this pass first lost data.** A rebalance
copies every chunk to the new owners before committing the manifest that names them — it has to,
because until that commit a reader must still find the object where the old manifest says it is
— and the copying is rate-limited. So for the length of a move, the object's size over the
repair rate, the destination holds chunks no manifest mentions. Judged by the manifests alone
they are garbage, and a sweep deletes them as fast as the move makes them; then the move commits,
promising copies that are no longer there. At the time the move also dropped the old copies on the
strength of that promise, which is what turned a missing copy into a missing object. Both passes
were doing exactly what they were written to do.

Measured, in a three-node cluster grown to six with the sweep set to a short grace: for keys the
join reassigns to an entirely new set of owners, one chunk ended up on no node at all, the object
returned `unexpected EOF`, and the cluster sat at 21 of the 24 copies its manifests promised.
Smaller membership changes hid it — a move that replaces one owner of three leaves two good
copies, so repair restores the third and nothing is lost. That is the failure mode worth naming:
not a wrong line of code, but two correct passes whose windows overlap, showing up as a race
repair usually wins.

The fix is to make a chunk live if the manifest names this node **or the ring makes this node an
owner of the object it belongs to**. The ring is what says a move here is in flight or coming, so
the destination's copies are referenced for the whole move. It spares nothing this pass was
written for: a copy a move left behind sits on a node that lost the partition, so neither the
manifest nor the ring names it, and it is still collected. `TestAMoveThatReplacesEveryOwnerKeepsItsData`
holds the cluster in that state on purpose: it doubles a three-node cluster and picks keys whose
owners all change, so no copy of them stays put. It asserts that the objects read *and* that repair
never restored anything — because before the fix the objects sometimes still read, and only because
repair replaced what the sweep had taken. A guarantee that depends on one background pass outrunning
another is not a guarantee. With the ring clause there is nothing to restore: before the commit every
copy is where the manifest says, and after it every copy is where the move put it.

Each pass sweeps one thirty-second of the id space, chosen by cursor, because the alternative is
holding a set of every chunk id on the node — tens of megabytes at a million chunks, and a
background pass whose memory grows with the disk is one that eventually cannot run. A chunk id
starts with a random base32 character and a shard id starts with its chunk's id, so one slice
covers a chunk and all of its shards. At the default one-minute interval a full cycle is about half
an hour, and nothing is reclaimed before the grace period either, so a delete shows up as free
space within a grace period plus a cycle — about half an hour at the shipped defaults.

**The grace period used to be an hour, and shrinking it was a correctness change rather than a
tuning one.** Age was the only thing protecting a write in flight, so the default had to exceed
the longest a write could take — and S3 allows a single PUT of 5 GB, which over a 10 Mbit link
is more than an hour. An hour of grace was therefore not generosity but an estimate of the
outside world, and a client slower than about 1.2 MB/s beat it: the sweep took the upload's first
chunks, the manifest committed naming them, and the client was told its object was stored. That
is invariant 1 broken by a slow link, and `TestASweepInsideAWriteLeavesItAlone` reproduces it
deterministically by running a whole collection cycle, at zero grace, between two chunks of a
write. Before the fix the PUT succeeds and the read fails with `unexpected EOF`.

So a write that can have another chunk now writes one small key naming itself before it stores
any of them, and clears it after its manifest is committed — in that order, because a chunk
protected by neither the record nor a manifest, even for an instant, is a chunk a sweep may take.
Only a write that *can* have another chunk: a chunk shorter than the chunk size is the last one,
so its manifest follows a moment later, and the ordinary small object pays nothing at all. That
is what keeps this off the hot path, and the 1 KB PUT numbers with it.

**Two things then have to agree about a writer that vanishes.** The record's value is the node
coordinating the write, and the sweep drops records belonging to nodes that are no longer members
— otherwise a crash mid-upload would protect its garbage forever, since nothing else would ever
remove the record. But a node can lose etcd for longer than its lease and come back: it would
find its record gone and its early chunks collected, and committing then would acknowledge an
object with a hole. So the commit of a recorded write is conditional on the record, in one etcd
transaction (`meta.CommitWhileWriting`). Such a write fails instead, which is the outcome that
keeps the invariant: nothing was acknowledged. The membership lease, already the cluster's answer
to who is still working, decides which of the two happens.

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

Four things clients send as *headers* on those calls rather than as calls of their own, and all were
added for the same reason the six were:

- **Conditional reads.** `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since`
  on GET and HEAD, answered from the committed manifest — so a client that already holds the object
  pays one etcd read instead of the object's bytes, which is what `aws s3 sync` asks of every file it
  considers. The precedence is HTTP's, which S3 follows: an entity tag is a better answer than a
  date, so `If-Match` outranks `If-Unmodified-Since` and `If-None-Match` outranks
  `If-Modified-Since`. A failed `If-Match` is 412 because the client's belief about what it is
  reading is wrong; a matched `If-None-Match` is 304 because its copy is current, and carries the
  same validators a 200 would.

  The same four arrive on a copy as `x-amz-copy-source-if-*`, where they are conditions on the
  *source* — still a question about a manifest, still nothing to do with the commit. They are
  answered by translating them into the plain headers and running the rules above, rather than by a
  second implementation that drifts from the first, with one difference in the answer: a copy has no
  "you already have it" outcome to report, so an unmet condition is 412 whichever kind it was, where
  a read would have said 304. Ceph's suite disagreed with the first version of this, which had
  returned 304 for a copy nobody asked to be told about.
- **`Content-MD5`.** A client declaring what it sent is asking to be contradicted, and the only
  useful answer is a refusal — an object stored under a digest it does not have is corruption the
  client has been told is fine. Checked before the manifest commits, so the write is refused
  *entirely*: the digest covers the whole body, so the chunks are already on disk when the answer is
  known, and they are left unreferenced for collection rather than committed and left for the client
  to delete. A malformed digest is `InvalidDigest` and a mismatch is `BadDigest`, because one is the
  client's bug and the other may be the network's. An *empty* `Content-MD5` is malformed rather than
  absent: the client said it was declaring a digest and then declared none, and reading that as "no
  digest was sent" would store the object under a promise nobody made.

- **CRC32C.** The write already hashes the body to make the ETag; CRC32C (Castagnoli) is hashed
  next to it. A client that names it — `x-amz-checksum-crc32c` on a PUT or a part, or an aws-chunked
  trailer — is compared before commit, and a mismatch stores nothing. Completing a multipart upload
  combines the parts' CRC32Cs into the object's (FULL_OBJECT) rather than re-reading the bytes, and
  a declared value that does not match is refused the same way. A HEAD or GET that asks with
  `x-amz-checksum-mode: ENABLED` gets the stored value back. SHA-256, CRC32, CRC64NVME, COMPOSITE,
  and a checksum on CopyObject are 501 rather than stored unread.

- **User metadata and the headers that describe the bytes.** `x-amz-meta-*`, plus `Cache-Control`,
  `Content-Disposition`, `Content-Encoding`, `Content-Language` and `Expires`: stored with the
  manifest and replayed on every read, never interpreted. The line between these and the headers kavo
  drops is whether they describe the *object* or the *exchange that delivered it* — `Content-Length`
  and `Content-MD5` are facts about one HTTP request, and answering a later reader with them would be
  describing a conversation that is over. `aws-chunked` is dropped from `Content-Encoding` for the
  same reason: it is the framing of the body that arrived, which kavo decoded, so keeping it would
  tell every future reader the stored bytes are chunk-framed when they are not.

  A multipart object takes its metadata from the call that began the upload, because that is the only
  call a client can attach it to, so the upload record carries it until the completion commits. On a
  copy, `x-amz-metadata-directive` selects `COPY` (the source's, which is the default) or `REPLACE`
  (the request's, and *only* the request's — replacing with nothing is how a client strips metadata).
  `REPLACE` is also what makes a copy onto itself meaningful, and so what makes it legal: without it
  a copy onto itself is a request to overwrite an object with itself.

  Metadata is capped at 2 KB per object, as S3 caps it. Unbounded metadata is unbounded manifests, and
  a manifest is read by every request for the object and by every background pass over it.

Conditional *writes* are deliberately not here. `If-None-Match: *` on a PUT means "create only if
absent", which needs the manifest commit to be a compare-and-set rather than a `Put`, and that is a
change to the commit point — the one place in this store where an extra condition has to be argued
for rather than added. Nothing kavo is tested against asks for it.

Those six are not a widening of the subset so much as the cost of being reachable. An SDK creates
the bucket before its first upload, `aws s3 ls` with no argument lists buckets, and every client
empties a bucket by listing versions and bulk-deleting what it finds. None of them store anything
new: a bucket is still a prefix, and the version listing reports every object once with the id
`null`, which is S3's own answer for a bucket that was never versioned.

A request for something kavo does not do is refused, not ignored. Server-side encryption is the case
that matters: any request carrying `x-amz-server-side-encryption*` or a customer key is answered 501,
because the alternative — storing the object in plaintext and answering 200 — tells a client its data
is encrypted while anyone can read it back without the key. That is the one failure mode a client
cannot detect for itself, and it is worth being explicit that this rule *cost* pass count rather than
earning it.

External validation: Ceph `s3-tests`. **176 of 886 pass, nothing errors**, and every failure is
classified in `docs/s3-compatibility.md` — as an anti-goal, a consequence of buckets being prefixes,
or a named gap. The suite found four real defects, three of which kavo's own tests could not see;
they are listed there too. It also found two sets of passes that were not real. Eighteen came from a
`PUT` to any bucket subresource reaching the create-bucket handler and answering 200, so kavo was
claiming to have configured lifecycle rules, policies and encryption it has no code for. Twenty-two
more came from ignoring the encryption headers above. Refusing the first set took the count from 169
to 151; `CopyObject`, conditional reads, `Content-MD5`, metadata and the missing multipart calls took
it to 196; refusing the second set brought it back to 177, and refusing an object's subresources —
which had been answered by overwriting the object — brought it to 169. That this is the number the
measurement opened with is a coincidence: the same count covered `CopyObject`, conditional reads,
`Content-MD5`, user metadata and three multipart calls that did not exist at the outset.
`UploadPartCopy` then took it to **176**, which is the honest figure.

The third set was the serious one, and the suite did not find it — it was found by copying a 20 MB
object with the `aws` CLI, which above 8 MB copies by multipart and begins by reading the source's
tags. Because S3 addresses an object's subresources as a query on the object's own path, ignoring
the query does not drop the request, it performs a different one: `PUT /key?tagging` wrote the
tagging XML *as the object*, `PUT /key?acl` truncated the object to nothing, `DELETE /key?tagging`
deleted it, and `UploadPartCopy` — whose source is named in a header rather than the query — stored
an empty part. Every one answered success. An object request now carries an allowlist of the
queries it can mean (`knownObjectQuery`), matching the one buckets have had since the same shape of
bug was found there, and the tests that cover it assert that the object is unchanged afterwards
rather than that the call was refused.

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

**Delete removes the manifest and nothing else**, which is the instant the object stops existing,
since a reader can only reach chunks through one. The chunks stay until the collection pass has read
every manifest in the cluster and found no name for them — a copy shares its source's chunks, so this
manifest alone does not speak for them. Deleting a key that is not there succeeds, because S3
promises an idempotent delete and clients build cleanup loops on it.

**`CopyObject` copies the manifest, not the data.** A server-side copy of a terabyte is one etcd
write, which is the only reason to have the operation at all: `aws s3 mv` is a copy and a delete, and
a copy that made the client download and re-upload would be slower than the client doing it itself.
The copy keeps the source's placement rather than its own key's, so the ring considers it misplaced
from the moment it exists and rebalancing moves it in the background; until then it is readable
exactly where the source is. `UploadPartCopy` is the range form of this, and is under Multipart
upload below: a client's part size does not land on chunk boundaries, so that path re-chunks
through the ordinary write rather than handing over the source's references.

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
clients recompute to check the upload.

**The API answers all of its own calls.** `ListParts` and `ListMultipartUploads` used to be missing,
which is worse than it sounds: both are a `GET`, so a client asking which parts had arrived was
answered by the object read handler with `NoSuchKey` — telling a client whose parts are all safely
stored that there is nothing there — and a client asking what uploads were in flight got an object
listing, which parses cleanly as "none". A listing that is empty for the wrong reason is the failure
mode this subset is meant to avoid, and it is the reason both exist now.

`ListMultipartUploads` pages in upload-id order rather than S3's key order, and says so here because
a client cannot tell from the response. Ordering by key means either reading every in-flight upload in
the store into memory to sort it, which is the one thing no request here is allowed to do, or keeping
a second index by key that can disagree with the first. The markers work, so a client that pages sees
every upload exactly once; only the order differs. The scan is bounded in the same breath: an upload's
parts share its prefix, so a listing reads a fixed number of etcd keys and reports itself truncated
rather than reading however many there are.

**A part can come from another object** (`UploadPartCopy`), which is not an extra: above 8 MB the aws
CLI performs *every* server-side copy that way, so without it `CopyObject` was a copy for small
objects and an error for large ones — and before it was refused, a lie for large ones, since the
header naming the source was ignored and each request stored a part with an empty body.

It is implemented as a stream: the source's range is read through the ordinary read path and written
through the ordinary write path, one chunk in flight, so a copy of a 5 GB object holds no more memory
than a copy of a small one. That costs a read and a write inside the cluster where `CopyObject` costs
neither, and the cheap alternative was rejected rather than missed. Handing the part the source's own
chunk references only works when the copied range begins and ends exactly on chunk boundaries, and a
client choosing 8 MB parts against 32 MB chunks never lands there — so the fast path would apply to
almost nothing while making a part different in kind from an uploaded one, which is what would
complicate the commit. Re-chunking keeps completion a single manifest commit over parts that are all
the same thing.

A copied range that runs past the end of the source is refused rather than satisfied by what exists,
which is the opposite of what a `Range` header on a read does. The difference is who can tell: a
reader sees the bytes it got, while a copying client sees only an etag for whatever was copied, so a
silently shortened range assembles an object nobody described. Both ends of the range are required
for the same reason.

The CLI reads the source's tags before a multipart copy, so **a read of an object's tags is answered
with none** — true here, since nothing stores any — while **asking for tags to exist is refused**,
`x-amz-tagging` on a PUT or an upload creation included. Both halves are load-bearing: dropping the
header and then reporting no tags would tell a client its tags had vanished by way of two successful
responses, which is the same failure as ignoring an encryption header. Tagging as a feature remains
an anti-goal; what is answered is a question about an object, not a place to put tags.

**A completion is idempotent for as long as a retry could take.** Every SDK retries a request whose
connection died, and a completion whose response was lost used to be answered `NoSuchUpload` — telling
the client its upload failed while the object was sitting there committed. The upload id is now
remembered with what it produced for an hour (`meta.CompletionMemory`), written *before* the upload
record is deleted so there is no instant in which the id is neither in flight nor remembered. It is an
etcd lease rather than a background pass, so the record expires even if every node is down. What a
retry gets back is what *that upload* produced, which is not necessarily what is at the key now: the
question a retry is asking is about its own request.

**Aborting an upload that does not exist is an error**, which reverses an earlier decision here. It
had been idempotent on the grounds that a cleanup loop aborts what it has already aborted, but S3
answers `NoSuchUpload` and Ceph's suite checks it: a client told it successfully aborted an id that
never existed cannot distinguish that from having aborted the wrong one, so a cleanup loop with a
typo in it reports success.

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
zero-length terminator is rejected rather than stored as a short object. A CRC32C trailer is
compared to the hash of the body before commit — the same check a header CRC32C gets, just later,
because that is when the trailer exists, and the same on a part as on a whole-object PUT. Any other
trailer is refused rather than stored unread.
A signed trailer's own signature is not checked: the chunks already were.

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
  multipart uploads, reads through a different node than the write went to, server-side copies both
  as a manifest copy and assembled out of ranges of the source, and deletes — against four real
  `kavod` processes while faults arrive on a seeded schedule: SIGKILL and
  restart, SIGSTOP (a frozen process: ports open, nothing answered, no lease renewed), a wiped disk,
  and a flipped bit under a running node. Then it asserts the four invariants (see `AGENTS.md`) from
  the recorded history.

  The copies are there because they are what makes two keys name one set of chunks, and roughly one
  in five of their sources is then deleted. That combination is the reason nothing but the collection
  pass may delete a chunk, so the run turns the collector up — a one-second interval and a
  ten-second grace, still seven times the longest stall a fault here imposes — and counts what it
  reclaimed. A long run that reclaimed nothing fails: it would mean the copies had been tested
  against an idle collector, which is the version of this test that proves nothing.

  Half of those copies are assembled out of two ranges of the source with `UploadPartCopy` instead,
  and a run that acknowledged none of them fails for the same reason. It is the only write path whose
  bytes are read out of the store while they are written back into it, so it is the only one a fault
  can interrupt from the *read* side: the source's owners can be killed, frozen or wiped while the
  copy is streaming. What has to hold is that a part is all or nothing, because a client's only
  evidence is the etag it gets back and an etag for the first half of a range is one it will complete
  an upload with — assembling an object that is not the copy it asked for, and being told it is.
  `object.Write` fails on a read error rather than committing what it got, and
  `TestACopiedPartIsAllOrNothingWhenItsSourceStopsReading` pins that by taking the source's second
  chunk away from every node: closing the pipe cleanly instead of with the error is a one-word change
  that stores the short part and passes everything else.

  A 45-minute replicated run acknowledged 51,623 writes — 10,344 of them server-side copies, 5,146 of
  those assembled from ranges — deleted 8,295 again, served 82,560 reads during fault windows, and
  took 40 faults, the last two of which wiped 20,121 and then 40,274 chunks off a node. It verified
  43,329 objects byte-identical and 8,301 absent, none lost and none torn, having reclaimed on 3,373
  occasions along the way. A 45-minute coded run of the same workload (`-chaos.ec=4+2`, seed
  `1786659083719981000`) acknowledged 26,836 writes — 5,309 of them copies, 2,620 assembled from
  ranges — took 61 faults of which one wiped 40,306 shards, and verified 22,585 objects byte-identical
  with none lost and none torn.

  Note the shape of the cost: the workload is what `-chaos.duration` names, but the verification
  afterwards reads the whole history back and waits for redundancy to settle, so a long run needs
  `-timeout` well above its duration — that 45-minute replicated run took 61 minutes and would have
  been killed mid-verdict by the 70-minute timeout that looked generous.

  A 60-minute replicated run reached the scale where the heal barrier's 30 s window was no longer a
  bound on progress. It acknowledged 57,418 writes, 5,783 of them assembled from ranges, took 50
  faults, and verified every survivor byte-identical with none lost and none torn — including after
  the final barrier, which found the cluster whole. The barrier *during* the workload declared
  repair stuck after a kill of n1 left six copies unrestored for 30 s
  (`-chaos.seed=1786654359933316000`). Repair walks every object in key order before returning to
  any of them, so at a quarter of a million copies a single pass is ~50 s of survey and a 30 s
  window measures where the cursor happens to be. The window now grows with the copies the barrier
  itself surveys (`surveyPerCopy` in `test/chaos_test.go`); the same seed then ran for 60 minutes
  without the barrier firing, verified 52,770 objects byte-identical, and lost none.

  It runs once per storage mode. `-chaos.ec=4+2` stores the same workload erasure-coded instead of
  replicated, on a cluster one node wider than the code, and CI runs both. That gap was open for a
  while and it was the wrong one to leave: the two modes fail differently — k+m shards at fixed
  positions against N interchangeable copies, acknowledgement at k+1 shards against W copies, a lost
  shard rebuilt from arithmetic over its siblings against a copy fetched from a peer — so a suite
  that only ever ran replication was proving the four invariants for half the store. Every durability
  bug found here has lived in a seam of exactly that shape. The coded run survives the same schedule,
  including six wiped disks (up to 3,751 shards from one node in a five-minute run) and six flipped
  bits, which means shards rebuilt by decode rather than copies fetched.

  A run also checks that it stored what it was asked to store: if `-chaos.ec` is set and no manifest
  came out coded, it fails rather than passing. Otherwise a mistyped flag would leave a green job
  asserting nothing about the mode in its own name.

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
  reachable by clients (SIGSTOP freezes a node from everyone at once), and anything that needs the
  page cache to disappear — see the fsync limitation below. Erasure-coded mode is covered: CI runs
  the same suite with `-chaos.ec=4+2` on every push, because the two modes fail differently.
- **Benchmarks** (milestone 11): in-repo benchmarks cover both APIs, both redundancy modes, repair,
  scrub and listing — what they measured, what it changed, and what it deliberately did not, is in
  `docs/benchmarks.md`. MinIO `warp` is there too, as an outside client. Still outstanding: the
  same numbers on separate machines over a real network. Nothing measured on one laptop with one
  disk is a headline number.

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
  by default, and it moves data rather than restoring it. The same is true of an object written
  while part of the cluster was unreachable: it is acknowledged at W and names fewer nodes than N,
  and the rebalance pass is what widens it, so it is a copy short until that pass reaches it.
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
- **Deleted space comes back in about half an hour, not at once**: nothing deletes a chunk
  except the collection pass, so a delete, an overwrite, an aborted upload, a write refused for
  missing quorum and a rebalance all leave chunks for it, and it frees them a slice of the id space
  at a time — no sooner than the one-minute grace period, and within a further half-hour cycle after
  that. Measured on a join: a seventh node's arrival left 32 copies of 192 that no manifest named,
  and with the collector turned up to a 1-second interval and a 10-second grace they were gone 37
  seconds after the move. Bounded rather than unbounded, but a store that has just deleted a
  terabyte does not see the space back immediately. Sharing chunks between keys is what costs this:
  a delete cannot know whether a copy it never heard of still needs what it is about to free, and
  the pass that reads every manifest can.
- **A node cut off from its peers cannot accept writes**: placement needs W distinct nodes, and a
  coordinator that can see fewer refuses with `SlowDown` rather than acknowledging copies one disk
  could take with it. A minority partition therefore serves reads and refuses writes — availability
  traded for the first invariant, in the one case where the two genuinely conflict. A deliberately
  small cluster says so with `-w`, which is the difference between choosing one copy and being left
  with one.
- **A recorded write that loses etcd for longer than its lease fails, rather than finishing**: the
  cost of the record being what protects a slow upload is that its absence has to be fatal. A node
  partitioned from etcd for longer than the five-second membership lease has its record dropped and
  its chunks made collectable, so its commit is refused and the client retries a 5 GB upload it had
  nearly finished. Wasteful, and the alternative is acknowledging an object with a hole in it.
- **A multipart upload nobody finishes is cleaned up after seven days**: the upload
  record carries its creation time, and the collection loop drops records older than
  that. Until then the sweep treats the parts as live, because it cannot tell a client
  that will come back from one that will not. S3 solves this with a lifecycle rule,
  which is an anti-goal here, so the age is a constant rather than per-bucket
  configuration. Parts uploaded twice, or uploaded and left out of the completion,
  are reclaimed once the upload is completed, aborted, or expired.
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
    measure` for the cluster-level numbers). `make demo` is the recorded kill-and-heal on this
    host. Still outstanding: separate machines over a real network.

Milestones 6, 7, 10 are where the project stops being a tutorial — never skip them for API
surface.
