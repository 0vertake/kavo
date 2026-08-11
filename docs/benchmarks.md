# Benchmarks

These are the in-repo benchmarks, run with `make bench`. They exist to decide where the time
goes and which optimisations are worth their complexity — not to produce a headline number. The
headline numbers belong to milestone 11: `warp` against the S3 API, on separate cloud VMs over a
real network.

Everything below runs against a six-node cluster over real HTTP, real etcd and real disks, at the
production 32 MB chunk size. Nothing is mocked, because a mocked store would measure the mock.

## The machine these numbers came from

Apple M1 Pro, 8 GOMAXPROCS, APFS on internal NVMe, etcd in Docker, all six nodes and all three
replicas of every chunk on **one physical disk**. That last part matters more than anything else
here: three replicas sharing one drive turn a cluster's parallel fsyncs into a queue. Read every
write number as a floor, not a ceiling.

## Where a write's time goes

| what | cost |
| --- | --- |
| one durable 4 KB chunk (`WriteChunk`) | 9.3 ms |
| ...of which fsync of the file | ~3.7 ms |
| ...of which fsync of the parent directory | ~2.9 ms |
| manifest commit to etcd (`meta.Commit`) | 1.2 ms |
| a 4 KB PUT end to end, 3 replicas | 28 ms |

A small write is **disk barriers, not code**. Two barriers per chunk, three chunk copies, one etcd
commit: on this machine that is ~25 ms of the 28 ms, and it is spent waiting for a drive to
confirm. `F_FULLFSYNC` on APFS forces a full cache flush; Linux `fdatasync` on server NVMe is an
order of magnitude cheaper, so the fixed cost of a small object on real hardware is closer to
2–3 ms than 28.

Measured by removing each barrier in turn and re-running — both are load-bearing and both stay.

## Throughput

| operation | one client | 8 clients |
| --- | --- | --- |
| PUT 4 KB | 28 ms | 22 ms |
| PUT 1 MB | 29 ms / 36 MB/s | 23 ms / 47 MB/s |
| PUT 64 MB | 160 ms / 421 MB/s | 111 ms / 602 MB/s |
| GET 4 KB | 0.75 ms | 0.35 ms |
| GET 1 MB | 1.8 ms / 570 MB/s | 0.70 ms / 1.5 GB/s |
| GET 64 MB | 42 ms / 1.6 GB/s | 21 ms / 3.2 GB/s |

Every PUT byte is written three times, so 602 MB/s of client throughput is 1.8 GB/s of durable
writes on one disk. Reads scale to 3.2 GB/s and the local store alone reads at 4.1 GB/s, so reads
are bound by the disk and the loopback, not by the code.

Writes barely improve with concurrency (28 ms → 22 ms across 8 clients) because the barriers
serialise on the shared drive. On separate disks this is where the cluster should scale.

## Background work

| operation | rate |
| --- | --- |
| heal a lost disk, unrated (`RepairHeal`) | 348 MB/s |
| scrub, unrated (`Scrub`) | 2.2 GB/s |
| survey a healthy partition (`RepairSurvey`) | 182 µs per copy |

The default repair cap of 32 MB/s is ~10× below what a heal can actually do, which is the intended
relationship: the cap, not the hardware, decides how much a heal disturbs clients. Scrubbing is
checksum-bound and effectively free at any sane interval.

## What the numbers changed

**A 4 KB object was allocating 34 MB.** The write path allocated one full chunk up front, so
every object — however small — paid for 32 MB. The buffer now starts at one small object and grows
to a chunk only once the source proves bigger, which cost 25 lines and took allocation per small
write from 34 MB to 1.3 MB (25×). Chunking a 1 MB object got 4× faster as a side effect, since it
stopped touching 32 MB of fresh pages. Objects over 1 MB are unaffected: they still allocate
exactly one chunk, once, and reuse it.

## What the numbers did not change, and why

Three optimisations the design doc explicitly deferred "until a benchmark justifies it". The
benchmark now exists, and it does not justify them yet. Each is left undone on purpose:

- **Acknowledging at W instead of waiting for all N.** Worth the slowest of three barriers, but
  only with a background copy that outlives the request — a new failure mode for one barrier's
  latency. Not while three replicas share one disk and that barrier is a measurement artifact.
- **Batching the repair survey** into one "which of these do you have?" per node. At 182 µs per
  copy, a pass over 10 M chunks is ~1.5 hours of pure survey. That is a real problem at that
  scale and no problem at all at this one; the fix is a protocol change and can wait for the
  scale that needs it.
- **Coalescing directory fsyncs across concurrent commits** (group commit). The dir barrier is
  ~2.9 ms of a 9.3 ms chunk write, and concurrent writers on one node could share it. But it puts
  shared mutable state in the one function the durability invariants depend on, to win back a cost
  that is 10× smaller on the hardware this will actually run on.

Pipelining chunks — reading chunk N+1 while N replicates — was also considered and rejected on
measurement: chunking runs at 6 GB/s against replication's 421 MB/s, so the read is 7% of a large
write and overlapping it buys nearly nothing for a second chunk buffer.
