# Benchmarks

These are the in-repo benchmarks, run with `make bench`. They exist to decide where the time
goes and which optimisations are worth their complexity — not to produce a headline number. For a
number nothing in this repo chose, see [what an outside client measures](#what-an-outside-client-measures)
at the end: MinIO's `warp` against the S3 API.

Three numbers cannot be expressed as a per-operation benchmark, because they are about a cluster
rather than a call: how long a heal takes, what a join moves, and what a node's memory does under a
multi-gigabyte object. `make measure` produces those against six real processes; they print rather
than assert, so the normal suite skips them.

Everything below runs against a six-node cluster over real HTTP, real etcd and real disks, at the
production 32 MB chunk size. Nothing is mocked, because a mocked store would measure the mock.

Every number in the tables below comes from one run of `make bench` on the machine described below,
so they can be compared with each other. The before-and-after numbers further down are pairs, each
measured by alternating the two versions in one sitting, because comparing against a table from
yesterday measures the disk's mood. Two caveats on reading them: anything with 64 MB writes or eight
concurrent clients drifts ~20–30% run to run, because three replicas fsyncing on one drive is a
queue whose length depends on what the OS was doing, so read those as a shape and not a score; and
the benchmarks are timed with Go's own benchtime rather than a fixed ten passes, because ten was not
enough to get past a cold connection — a 2 ms GET measured 8 ms that way, and chasing that phantom
is how this section got written.

## The machine these numbers came from

Apple M1 Pro, 8 GOMAXPROCS, APFS on internal NVMe, etcd in Docker, all six nodes and all three
replicas of every chunk on **one physical disk**. That last part matters more than anything else
here: three replicas sharing one drive turn a cluster's parallel fsyncs into a queue. Read every
write number as a floor, not a ceiling.

## Where a write's time goes

| what | cost |
| --- | --- |
| one durable 4 KB chunk (`WriteChunk`) | 8.2 ms |
| ...of which fsync of the file | ~3.7 ms |
| ...of which fsync of the parent directory | ~3.6 ms |
| manifest commit to etcd (`meta.Commit`) | 0.54 ms |
| a 4 KB PUT end to end, 3 replicas | 25 ms |

A small write is **disk barriers, not code**. Two barriers per chunk, three chunk copies, one etcd
commit: on this machine that is ~22 ms of the 25 ms, and it is spent waiting for a drive to
confirm. `F_FULLFSYNC` on APFS forces a full cache flush; Linux `fdatasync` on server NVMe is an
order of magnitude cheaper, so the fixed cost of a small object on real hardware is closer to
2–3 ms than 25.

Measured by removing each barrier in turn and re-running — both are load-bearing and both stay.

## Where a read's time goes

| what | cost |
| --- | --- |
| resolve the manifest in etcd (`meta.Get`) | 0.29 ms |
| read a 4 KB chunk from local disk (`ReadChunk`) | 0.015 ms |
| a 4 KB GET end to end | 0.74 ms |

A small read is **one etcd round trip and a rounding error**. The disk is 2% of it; resolving the
object through its committed manifest is ~40%, and that round trip is the price of the commit point
being somewhere other than the node answering.

etcd can answer a read from the local member without a quorum round, which would cut most of this.
kavo does not ask it to: a serialisable read can return a manifest that is already superseded, so a
client could write, get an ack, read, and be told the old object — and every guarantee in this
project is written on the assumption that a committed manifest is what a reader sees. A 4 KB object
is the one shape where this dominates, and it is the shape a cache would fix if it ever mattered.

## Throughput

The internal API, which is the data path with no S3 compatibility on top of it.

| operation | one client | 8 clients |
| --- | --- | --- |
| PUT 4 KB | 25 ms | 17 ms |
| PUT 1 MB | 28 ms / 37 MB/s | 20 ms / 54 MB/s |
| PUT 64 MB | 143 ms / 469 MB/s | 92 ms / 727 MB/s |
| GET 4 KB | 0.74 ms | 0.15 ms |
| GET 1 MB | 1.6 ms / 642 MB/s | 0.53 ms / 2.0 GB/s |
| GET 64 MB | 46 ms / 1.5 GB/s | 23 ms / 2.9 GB/s |

Every PUT byte is written three times, so 727 MB/s of client throughput is 2.2 GB/s of durable
writes on one disk. Reads scale to 2.9 GB/s and the local store alone reads at 4.0 GB/s, so reads
are bound by the disk and the loopback, not by the code.

Concurrency helps a large write (143 ms → 92 ms across 8 clients) and barely helps a small one
(25 ms → 17 ms), because a small write is almost entirely disk barriers and they serialise on the
shared drive. On separate disks this is where the cluster should scale.

## What the S3 gateway costs

The same cluster through the S3 API, driven by the AWS SDK so that requests are signed the way a
real client signs them. Against the table above, the difference is the price of S3 compatibility.

| operation | one client | 8 clients |
| --- | --- | --- |
| PUT 4 KB | 25 ms | 18 ms |
| PUT 1 MB | 29 ms / 36 MB/s | 19 ms / 55 MB/s |
| PUT 64 MB | 191 ms / 352 MB/s | 100 ms / 668 MB/s |
| GET 4 KB | 0.64 ms | 0.14 ms |
| GET 1 MB | 1.8 ms / 595 MB/s | 0.59 ms / 1.8 GB/s |
| GET 64 MB | 48 ms / 1.4 GB/s | 21 ms / 3.1 GB/s |
| GET an 8 MB range of a 64 MB object | 22 ms / 379 MB/s | |
| HEAD | 0.66 ms | |
| PUT 64 MB as 8 multipart parts | 342 ms / 196 MB/s | |
| `ListObjectsV2`, a page of 1000 keys | 13 ms | |
| `ListObjectsV2`, the same keyspace under a delimiter | 4.8 ms | |

**The gateway is free.** Every row matches the internal API within the noise, which is the answer
to the question this table exists to ask: parsing S3, verifying a signature, and producing XML are
not where a store's time goes. The one thing S3 compatibility does cost is on the client side of a
signed upload — a client that signs its payload hashes every byte with SHA-256 before sending it,
and the benchmark pays for that in the same process.

Multipart is 2x a single PUT of the same size here because this benchmark uploads its parts one
after another: eight parts means eight round trips and eight part manifests where a plain PUT has
one. Real clients upload parts concurrently, which is what the concurrency column of the plain PUT
row is measuring.

A ranged read runs at 388 MB/s against 1.5 GB/s for a whole-object read, and that is by design: a
range still verifies every chunk it touches in full, so an 8 MB window into 32 MB chunks reads
32 MB. Invariant 3 does not have a discount for partial reads.

## Background work

| operation | rate |
| --- | --- |
| heal a lost disk, unrated (`RepairHeal`) | 349 MB/s |
| scrub, unrated (`Scrub`) | 2.2 GB/s |
| survey a healthy partition (`RepairSurvey`) | 163 µs per copy |

The default repair cap of 32 MB/s is ~10× below what a heal can actually do, which is the intended
relationship: the cap, not the hardware, decides how much a heal disturbs clients. Scrubbing is
checksum-bound and effectively free at any sane interval.

### How long a heal takes

Rates are not an answer to the question an operator asks, which is: a node died, when is redundancy
back? `make measure` answers it against six real processes — 64 objects of 32 MB, 192 chunk copies,
one node losing its entire disk while the cluster keeps serving, and nobody asking for a repair.

| repair cap | redundancy restored | effective rate |
| --- | --- | --- |
| 32 MB/s (the default) | 9.2 s | 122 MB/s |
| unlimited | 1.1 s | 994 MB/s |

The node came back with nothing and 1.09 GB of copies had to be rebuilt. **122 MB/s under a 32 MB/s
cap is not a broken limiter** — the cap is per node, and each node repairs the objects it is
responsible for, so four nodes rebuilding at once move four times one node's allowance while each
one disturbs its own clients by no more than the cap. That is the property worth having: heal
bandwidth grows with the cluster, and the blast radius per node does not.

The gap between the two rows is the whole argument for the cap. Unthrottled, this cluster heals a
dead disk almost as fast as it can read one — and every byte of that is competing with client
requests on the same disks and the same network.

### What a join moves

The claim consistent hashing exists to make is that adding a node moves that node's share and
nothing else, where hashing keys modulo the node count would move most of the data. A seventh node
joining a six-node cluster holding 2 GB in 64 objects:

| | |
| --- | --- |
| seen by every node | 40 ms |
| converged | 4.4 s |
| copies moved onto it | 34 of 192 (17.7%) |
| copies the seven-node ring owes it | 34 |
| copies once the move had committed | 224 |
| of those, copies no manifest names | 32 |
| until collection had reclaimed every one | 37 s |

Rows three and four are the measurement. 34 moved and 34 owed is not a statistical agreement with a
prediction — it is the exact count the ring assigns to that node for the keys that exist, so
placement after the join is the placement the ring specifies, key for key.

The rows below them are what a move costs while it is settling, and they used to read differently.
A move used to copy, commit, and then delete what it had copied away from, so the count came back to
192 on its own. It cannot any more: a server-side copy shares its source's chunks, so no pass may
delete a chunk on the strength of the one manifest in front of it, and the move now stops at the
commit. For a while the cluster holds two placements of everything that moved — 224 copies of what
needs 192 — and the collection pass is what brings it back down, after reading every manifest in the
cluster and finding no name for them.

So the residue is a measured duration rather than a caveat now. With the collector turned up to a
1-second interval and a 10-second grace it reclaimed all 32 in 37 seconds, which is one full cycle of
the id space plus the grace. At the shipped defaults — a minute of interval and a minute of grace —
the same residue lives about half an hour. That is the price of the copy operation, paid in disk
rather than in durability, and it is why the interval is a minute rather than the ten it was.

Thirty-two, not the thirty-four that moved: a sweep had already taken two by the time the count was
made. The measurement waits for the number to reach zero rather than asserting what it starts at,
because a background pass that has already done some of its work is not a failure.

## A multi-gigabyte object

Memory staying flat regardless of object size is the invariant that separates an object store from a
key-value store with S3 headers on it, so it is worth measuring at a size where no buffer could
hide. `TestStreamingIsConstantMemory` pins the data path at 64 MB with Go's own allocation counters;
this pins the *process* with `ps`, through the signed S3 API, at 64 times that.

| object | PUT | GET | peak RSS | over idle | of the object |
| --- | --- | --- | --- | --- | --- |
| 64 MB | 210 ms / 305 MB/s | 48 ms / 1.3 GB/s | 55 MB | 33 MB | 86% |
| 4 GB | 13.1 s / 312 MB/s | 16.8 s / 243 MB/s | 89 MB | 68 MB | 2.2% |

Idle RSS is 22 MB. The object grew 64x and the memory above idle grew 2x — from 33 MB to 68 MB,
which is chunk-shaped rather than object-shaped: a 32 MB chunk buffer, its replication in flight,
and the hash running beside it. Throughput is flat across the two sizes in both directions, so
nothing is being paid for in the large case that the small case avoids.

The upload is signed with `UNSIGNED-PAYLOAD`, which is the only honest way to stream something larger
than memory to a signed API: a hex payload hash would require reading the object twice, and holding
it in order to hash it is the exact thing this measures the absence of.

## Replication against erasure coding

4+2 rather than the 6+3 default, because a `k+m` code needs `k+m` nodes and the test cluster has
six. Same tolerance as three copies in both cases: two nodes may be lost.

| | replicated (3 copies) | coded (4+2) |
| --- | --- | --- |
| stored per byte of object | 3.00x | 1.50x |
| PUT 4 KB | 25 ms | 41 ms |
| PUT 1 MB | 27 ms / 39 MB/s | 44 ms / 24 MB/s |
| PUT 64 MB | 142 ms / 472 MB/s | 184 ms / 365 MB/s |
| GET 64 MB | 48 ms / 1.4 GB/s | 70 ms / 966 MB/s |
| GET 64 MB, two shards gone | — | 65 ms / 1.0 GB/s |
| allocated per 64 MB GET | 125 KB | 168 MB |

A large coded write costs ~30% more wall time and stores half the bytes, which is the trade the mode
exists to make. Reads are ~45% slower, and **a degraded read is no slower than a healthy one** —
reconstruction arithmetic is not the expensive part, moving the shards is, and a degraded read moves
the same number of shards.

Small writes are where coding hurts: 41 ms against 25 ms, because a 4 KB object still becomes six
shards on six nodes, so it pays six disk barriers instead of three to store 4 KB.

The memory column is the honest cost. A replicated read streams a chunk through; a coded read has to
hold a chunk's shards *and* the reconstructed chunk before any of it is valid. Sizing each shard
read from the manifest instead of growing it took this from 315 MB to 168 MB per 64 MB object.
Still flat in object size — the invariant — but ~2.6x a chunk rather than ~1x.

## Listing, and why the shape of a manifest matters

Listing is the one operation whose cost is per key rather than per byte. A page reports three
scalars per object — size, ETag, last-modified — and has to get past everything else the manifest
holds to find them, which makes the cost depend on the objects' size in a way nothing else about a
listing does: a 1 GB object has 32 chunk references, and a page of a thousand of them carries
32,000.

| a page of 1000 keys | server side | allocated |
| --- | --- | --- |
| one-chunk manifests (small objects) | 6.6 ms | 1.7 MB |
| 32-chunk manifests (1 GB objects) | 47 ms | 5.9 MB |

Still ~7x, after the fixes below took it down from 55 ms and 12.5 MB. What is left is
`encoding/json` walking past chunk references at ~180 MB/s to reach fields it has already found.
Two ways to make that go away, both rejected — see the last section.

## What the numbers changed

**Hashing the ETag was 44% of a large write.** MD5 has no hardware acceleration on arm64 and runs
at ~550 MB/s, and it sat in front of the network: read the bytes, hash the bytes, then store the
chunk. Removing the hash entirely took a 64 MB PUT from 268 ms to 151 ms, which is how much of the
write it was. Hashing each chunk *beside* storing it instead — one goroutine, waited on before the
buffer is reused — gets nearly all of that back: **268 ms → 154 ms**, against 151 ms for not
hashing at all. The two overlap almost perfectly because storing a chunk waits on disks and peers
while hashing waits on nothing.

Both only read the chunk, which is what makes it safe. The erasure-coding path gets the chunk with
its capacity clamped, so that splitting it into shards cannot pad into the buffer's spare room
while the hash is reading it.

**An 8 MB multipart part was allocating 33 MB.** The write buffer started at 1 MB and jumped
straight to a full 32 MB chunk as soon as the source proved bigger, so everything between those two
sizes paid for a chunk it did not use — and a 64 MB upload in 8 parts allocated 280 MB. Every S3
write declares its length, so the buffer now starts at what the client said and is capped at a
chunk: **280 MB → 70 MB** for that upload, and 1.28 MB → 0.21 MB for a 4 KB PUT. The declared
length is a hint and nothing else — too small and the buffer grows as it always did, absurdly large
and it is capped, and `TestTheSizeHintIsOnlyAHint` covers each way a client can be wrong.

**A listing decoded 32,000 chunk references to report 3,000 numbers.** Listings now decode only the
fields they report, and ask etcd for the whole page at once instead of 256 keys at a time when
there is no delimiter to collapse them: **55 ms → 41 ms** per page of 1 GB objects, with half the
garbage, and 9.4 ms → 6.5 ms for small ones.

**A node redialled its peers for every chunk.** Go's default transport keeps two idle connections
per host, and a node talks to the same handful of peers for every chunk it ever writes or reads —
so past two concurrent requests to one peer, every chunk after that paid for a fresh connection.
Measured back to back, keeping 64 warm per peer took a 4 KB read across 8 clients from 146 µs to
131 µs and a 1 MB read from 1.8 GB/s to 2.3 GB/s. The decisive part was not the throughput: on the
default transport, the parallel read benchmark **ran the machine out of ephemeral ports** and failed
with `can't assign requested address`. That is a storage node dying under sustained load, not a
benchmark artifact. `TestConnectionsToAPeerAreReused` fails if the pool is ever left at the default.

**A 32 MB chunk reached the disk in a thousand write syscalls.** `io.Copy`'s buffer is 32 KB, so a
chunk streaming in from a peer was copied to the file 32 KB at a time — and with six nodes doing
that at once, most of the CPU was in the kernel rather than on the data. Sizing that buffer to the
chunk being written, bounded to 256 KB, is worth **~25% on a large concurrent write** (a 64 MB PUT
across 8 clients: 119 ms → 86 ms, and 24 ms → 20 ms for 1 MB) for about 1 MB more allocation per
64 MB written. It was measured by alternating the two versions three times, because the raw numbers
drift ±30% between runs on this disk and a single before-and-after pair proves nothing here; a local
write, whose path does not change, was watched alongside as a control.

1 MB buffers measured no better than 256 KB and hold four times as much per chunk being received,
which on a node with many uploads in flight is memory that buys nothing. A chunk already in memory
is offered as a `*bytes.Reader`, which writes itself in one syscall and gets no buffer at all.

**Every key of a listing rebuilt the same prefix.** The etcd prefix manifests live under was
composed with `path.Join` on every read and trimmed off every key a listing returned, so a page of a
thousand built two thousand copies of one constant: 16,200 allocations per page to 13,200, and 1.9 MB
to 1.7 MB. It buys no time — it is one line and one fewer thing happening per key.

**The held-back byte allocated once per 32 KB.** The reader that withholds a chunk's last byte
until its checksum verifies was building a fresh one-byte slice for each write, which is ~4,000
allocations for a 64 MB read. It keeps the byte in an array now: 5,000 allocations per read to
2,950.

**A 4 KB object was allocating 34 MB.** The write path allocated one full chunk up front, so
every object — however small — paid for 32 MB. The buffer now starts at one small object and grows
to a chunk only once the source proves bigger, which cost 25 lines and took allocation per small
write from 34 MB to 1.3 MB (25×). Chunking a 1 MB object got 4× faster as a side effect, since it
stopped touching 32 MB of fresh pages. Objects over 1 MB are unaffected: they still allocate
exactly one chunk, once, and reuse it.

## What the numbers did not change, and why

Optimisations the benchmarks located and left alone. Each one is real, and each one is left undone
on purpose:

- **Acknowledging at W instead of waiting for all N.** Worth the slowest of three barriers, but
  only with a background copy that outlives the request — a new failure mode for one barrier's
  latency. Not while three replicas share one disk and that barrier is a measurement artifact.
- **Batching the repair survey** into one "which of these do you have?" per node. At 163 µs per
  copy, a pass over 10 M chunks is ~1.3 hours of pure survey. That is a real problem at that
  scale and no problem at all at this one; the fix is a protocol change and can wait for the
  scale that needs it.
- **Coalescing directory fsyncs across concurrent commits** (group commit). The dir barrier is
  ~3.6 ms of an 8.2 ms chunk write, and concurrent writers on one node could share it. But it puts
  shared mutable state in the one function the durability invariants depend on, to win back a cost
  that is 10× smaller on the hardware this will actually run on.
- **Getting chunk references out of the way of listings**, either by splitting a manifest across
  two etcd keys (a header a listing reads, a chunk list a read reads) or by deriving chunk ids from
  the object instead of storing all 32 of them. Both would make a listing of large objects ~7x
  faster. Both also put churn in the commit path — the transaction that makes a write exist, and
  the compare-and-swap that rebalancing depends on — to speed up an operation no user is blocked
  on. 47 ms for a page of a thousand 1 GB objects is not a problem; a subtle bug in the commit
  point is.
- **Bigger copy buffers on the read path**, the mirror of the write-side fix above. It measured
  nothing at all on a read and cost five times the allocation: a 1 MB GET went from 58 KB to 298 KB
  of garbage for 1.8 GB/s either way. Reads already move big pieces — a shard is fetched with one
  sized read — and only the write path was ever counting syscalls. The same change also made every
  read allocate a buffer per chunk, which is the wrong direction for the one path that has to hold
  memory flat.
- **Rebuilding an erasure-coded read in place.** A coded 64 MB read allocates 168 MB: six shard
  buffers plus a joined chunk, per chunk, none reused. It looks like the worst number in this file
  and it is not one — the profile puts 80% of that read in socket syscalls, 0.8% in `memmove` and
  nothing measurable in GC. Reusing the buffers across chunks would cut the garbage and win no time,
  in exchange for reasoning about a buffer's lifetime across the goroutines that fill it.
- **Deferring the resume position of a listing.** A page computes where the next page starts once per
  key and throws away all but the last, which is a thousand small allocations per page. Fixing it
  means restructuring the loop that does both paging and delimiter grouping — the one place a subtle
  bug loses or repeats keys — and the allocations do not show up in the time. Left alone.
- **The payload hash of a signed request.** SigV4 with a hex payload hash requires hashing the body
  to verify the signature, and that is not an optimisation to find but a promise to keep. Clients
  that would rather not pay it already have two ways out that kavo implements: `UNSIGNED-PAYLOAD`
  and `aws-chunked` streaming.

Pipelining chunks — reading chunk N+1 while N replicates — was also considered and rejected on
measurement: chunking runs at 6.2 GB/s against replication's ~470 MB/s, so the read is 8% of a large
write and overlapping it buys nearly nothing for a second chunk buffer.

## What an outside client measures

Everything above is kavo timing itself. MinIO's [`warp`](https://github.com/minio/warp) is not: it is
an S3 benchmark written against S3, driving the same six-node cluster `make up` starts, over the
signed client API, through minio-go rather than through anything this repo chose.

| workload | concurrency | throughput | objects/s | request latency |
| --- | --- | --- | --- | --- |
| `PUT` 4 KiB | 8 | 2.9 MiB/s | 731 | median 9.1 ms, p99 35 ms |
| `GET` 4 KiB | 8 | 16.5 MiB/s | 4,235 | median 1.5 ms, p99 7.4 ms |
| `PUT` 64 MiB | 4 | 118 MiB/s | 1.8 | TTFB median 856 ms |
| `GET` 64 MiB | 4 | 130 MiB/s | 2.0 | TTFB median 32 ms |

```sh
make up
warp put --host 127.0.0.1:9001 --access-key kavo --secret-key kavosecret \
    --obj.size 4KiB --concurrent 8 --duration 30s
```

Read these with the caveat from the top of this file, and one more: `warp` itself is on the same
laptop as all six nodes and etcd, competing for the same CPU and the same drive. The write numbers
are a floor for a further reason too — at N=3 every client byte is three fsynced bytes, so 118 MiB/s
of 64 MiB `PUT` is ~354 MiB/s of durable writes to one consumer NVMe.

What they confirm is that the shape the in-repo benchmarks describe is real and not an artifact of
measuring ourselves. A small write is dominated by disk barriers: 9.1 ms at eight concurrent clients
against 25 ms for one, which is what a queue of fsyncs looks like when there is parallelism to fill
it. A small read is dominated by one etcd round trip: 1.5 ms, matching the manifest read floor. Large
objects run at the disk's speed in both directions.

`warp` also exercised paths kavo's own tests do not reach at all. It signs every upload with
`aws-chunked` streaming (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`) rather than a hex payload hash, so
those 730 objects a second are 730 chained per-chunk signature verifications a second, by a client
that implements the scheme independently. Getting it to run at all also turned up a bulk delete
posted to `/{bucket}/` with a trailing slash, which had been answered with a redirect.

## The network these numbers are not crossing

Every figure above was measured with six nodes on one host, so a chunk reaches its second owner
across a memory copy. The per-chunk round trip that dominates small writes on real hardware is
missing from all of them, and nothing here should be read as a network measurement.

What running it elsewhere needs is an address rather than a new harness. The S3 benchmarks drive
whatever cluster `KAVO_BENCH_ENDPOINT` names:

```sh
# On each host: same etcd, same cluster prefix, its own reachable address.
kavod -id n1 -addr 0.0.0.0:8080 -advertise 10.0.0.11:8080 -s3 0.0.0.0:9000       -data /var/lib/kavo -etcd 10.0.0.10:2379 -cluster /kavo

# From anywhere that can reach them:
KAVO_BENCH_ENDPOINT=http://10.0.0.11:9000 go test ./internal/s3 -run XXX -bench . -timeout 1800s
```

Only the gateway benchmarks can go remote; the internal-API ones drive a coordinator in the test
process, which is what makes them a measurement of the code rather than of a deployment. `warp` takes
`--host` and needs nothing new either.

What to predict when someone runs it: a write pushes to its W owners in parallel, so a PUT should
gain roughly one round trip per chunk rather than per copy — at 0.25 ms RTT that is noise against a
25 ms small write, which is fsync, and it stays noise until the disks are fast enough for the network
to be the slower of the two. Large writes should be bandwidth-bound: 469 MB/s of client throughput is
1.4 GB/s of chunk traffic leaving the coordinator, which is more than a 10 GbE link carries, so a
single-NIC node caps a large PUT near 400 MB/s before anything in kavo does. Heal rate has the same
ceiling and is already rate-limited well below it.

**A containerised cluster on one host is not a substitute, and measuring it is how that became
clear.** Pointing the same benchmarks at the six-node `make up` cluster on macOS moves every number,
in both directions:

| operation | six nodes in process | six containers, same host |
| --- | --- | --- |
| PUT 4 KB | 25 ms | 5.0 ms |
| PUT 1 MB | 28 ms / 37 MB/s | 18 ms / 58 MB/s |
| PUT 64 MB | 143 ms / 469 MB/s | 743 ms / 90 MB/s |
| GET 4 KB | 0.74 ms | 0.76 ms |

The small write got **five times faster** by being containerised, which is not a speedup: it is
Docker Desktop's virtual machine absorbing `F_FULLFSYNC` into a page cache the host has not
promised anything about. The large write got five times *slower*, because the same virtualised
filesystem and network cap bandwidth. Distortion in both directions, from the same layer — so these
numbers are recorded here as a caution and are not the ones published above. An object store
benchmarked in Docker Desktop is measuring Docker Desktop.
