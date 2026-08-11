# Benchmarks

These are the in-repo benchmarks, run with `make bench`. They exist to decide where the time
goes and which optimisations are worth their complexity — not to produce a headline number. The
headline numbers belong to `warp` against the S3 API, on separate cloud VMs over a real network.

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
| one durable 4 KB chunk (`WriteChunk`) | 9.5 ms |
| ...of which fsync of the file | ~3.7 ms |
| ...of which fsync of the parent directory | ~3.6 ms |
| manifest commit to etcd (`meta.Commit`) | 0.54 ms |
| a 4 KB PUT end to end, 3 replicas | 26 ms |

A small write is **disk barriers, not code**. Two barriers per chunk, three chunk copies, one etcd
commit: on this machine that is ~22 ms of the 26 ms, and it is spent waiting for a drive to
confirm. `F_FULLFSYNC` on APFS forces a full cache flush; Linux `fdatasync` on server NVMe is an
order of magnitude cheaper, so the fixed cost of a small object on real hardware is closer to
2–3 ms than 26.

Measured by removing each barrier in turn and re-running — both are load-bearing and both stay.

## Throughput

The internal API, which is the data path with no S3 compatibility on top of it.

| operation | one client | 8 clients |
| --- | --- | --- |
| PUT 4 KB | 26 ms | 20 ms |
| PUT 1 MB | 29 ms / 37 MB/s | 20 ms / 53 MB/s |
| PUT 64 MB | 142 ms / 472 MB/s | 86 ms / 778 MB/s |
| GET 4 KB | 0.62 ms | 0.14 ms |
| GET 1 MB | 1.6 ms / 672 MB/s | 0.53 ms / 2.0 GB/s |
| GET 64 MB | 47 ms / 1.4 GB/s | 22 ms / 3.1 GB/s |

Every PUT byte is written three times, so 778 MB/s of client throughput is 2.3 GB/s of durable
writes on one disk. Reads scale to 3.1 GB/s and the local store alone reads at 4.2 GB/s, so reads
are bound by the disk and the loopback, not by the code.

Concurrency helps a large write (142 ms → 86 ms across 8 clients) and barely helps a small one
(26 ms → 20 ms), because a small write is almost entirely disk barriers and they serialise on the
shared drive. On separate disks this is where the cluster should scale.

## What the S3 gateway costs

The same cluster through the S3 API, driven by the AWS SDK so that requests are signed the way a
real client signs them. Against the table above, the difference is the price of S3 compatibility.

| operation | one client | 8 clients |
| --- | --- | --- |
| PUT 4 KB | 26 ms | 18 ms |
| PUT 1 MB | 30 ms / 35 MB/s | 21 ms / 50 MB/s |
| PUT 64 MB | 196 ms / 342 MB/s | 90 ms / 742 MB/s |
| GET 4 KB | 0.69 ms | 0.16 ms |
| GET 1 MB | 1.9 ms / 562 MB/s | 0.56 ms / 1.9 GB/s |
| GET 64 MB | 45 ms / 1.5 GB/s | 21 ms / 3.2 GB/s |
| GET an 8 MB range of a 64 MB object | 24 ms / 356 MB/s | |
| HEAD | 0.56 ms | |
| PUT 64 MB as 8 multipart parts | 344 ms / 195 MB/s | |
| `ListObjectsV2`, a page of 1000 keys | 13 ms | |
| `ListObjectsV2`, the same keyspace under a delimiter | 4.6 ms | |

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
| heal a lost disk, unrated (`RepairHeal`) | 326 MB/s |
| scrub, unrated (`Scrub`) | 2.1 GB/s |
| survey a healthy partition (`RepairSurvey`) | 157 µs per copy |

The default repair cap of 32 MB/s is ~10× below what a heal can actually do, which is the intended
relationship: the cap, not the hardware, decides how much a heal disturbs clients. Scrubbing is
checksum-bound and effectively free at any sane interval.

## Replication against erasure coding

4+2 rather than the 6+3 default, because a `k+m` code needs `k+m` nodes and the test cluster has
six. Same tolerance as three copies in both cases: two nodes may be lost.

| | replicated (3 copies) | coded (4+2) |
| --- | --- | --- |
| stored per byte of object | 3.00x | 1.50x |
| PUT 4 KB | 25 ms | 44 ms |
| PUT 1 MB | 28 ms / 38 MB/s | 46 ms / 23 MB/s |
| PUT 64 MB | 143 ms / 469 MB/s | 189 ms / 355 MB/s |
| GET 64 MB | 47 ms / 1.4 GB/s | 68 ms / 981 MB/s |
| GET 64 MB, two shards gone | — | 71 ms / 951 MB/s |
| allocated per 64 MB GET | 126 KB | 168 MB |

A large coded write costs ~32% more wall time and stores half the bytes, which is the trade the mode
exists to make. Reads are ~44% slower, and **a degraded read is no slower than a healthy one** —
reconstruction arithmetic is not the expensive part, moving the shards is, and a degraded read moves
the same number of shards.

Small writes are where coding hurts: 44 ms against 25 ms, because a 4 KB object still becomes six
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
| one-chunk manifests (small objects) | 7.2 ms | 1.9 MB |
| 32-chunk manifests (1 GB objects) | 41 ms | 6.2 MB |

Still 6x, after the fixes below took it down from 55 ms and 12.5 MB. What is left is
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
- **Batching the repair survey** into one "which of these do you have?" per node. At 157 µs per
  copy, a pass over 10 M chunks is ~1.3 hours of pure survey. That is a real problem at that
  scale and no problem at all at this one; the fix is a protocol change and can wait for the
  scale that needs it.
- **Coalescing directory fsyncs across concurrent commits** (group commit). The dir barrier is
  ~3.6 ms of a 9.5 ms chunk write, and concurrent writers on one node could share it. But it puts
  shared mutable state in the one function the durability invariants depend on, to win back a cost
  that is 10× smaller on the hardware this will actually run on.
- **Getting chunk references out of the way of listings**, either by splitting a manifest across
  two etcd keys (a header a listing reads, a chunk list a read reads) or by deriving chunk ids from
  the object instead of storing all 32 of them. Both would make a listing of large objects ~6x
  faster. Both also put churn in the commit path — the transaction that makes a write exist, and
  the compare-and-swap that rebalancing depends on — to speed up an operation no user is blocked
  on. 41 ms for a page of a thousand 1 GB objects is not a problem; a subtle bug in the commit
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
- **The payload hash of a signed request.** SigV4 with a hex payload hash requires hashing the body
  to verify the signature, and that is not an optimisation to find but a promise to keep. Clients
  that would rather not pay it already have two ways out that kavo implements: `UNSIGNED-PAYLOAD`
  and `aws-chunked` streaming.

Pipelining chunks — reading chunk N+1 while N replicates — was also considered and rejected on
measurement: chunking runs at 6.1 GB/s against replication's ~470 MB/s, so the read is 8% of a large
write and overlapping it buys nearly nothing for a second chunk buffer.
