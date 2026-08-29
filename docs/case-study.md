# kavo — case study

A distributed object store built to answer one question: *what has to be true for an acknowledged
write to survive nodes dying mid-flight?*

Most hobby object stores stop at "PUT and GET work." kavo stops at four invariants, each backed by a
test that injects the failure the invariant is about.

## Problem

Object stores promise durability, but durability is a property of the **commit point** — the moment
a client is told the write succeeded. If that moment comes before data is safe, you have acknowledged
loss. If readers can see data before the commit point, you have torn reads. If repair is best-effort,
you have silent degradation.

The design question is not "how do I implement S3?" It is "where do I draw the line between
acknowledged and visible?"

## Design choices

**etcd as the commit point.** A write is acknowledged only after W chunk replicas are fsynced on
distinct nodes *and* the object manifest is committed to etcd. Readers resolve objects only through
committed manifests. A plain etcd `Put` is atomic — concurrent overwrites yield one manifest or the
other, never a mix.

**Symmetric nodes.** One binary (`kavod`) is S3 gateway, chunk store, and repair participant. Any node
coordinates any request. Placement is consistent hashing over 256 partitions with ~128 vnodes per
node.

**Fsync discipline.** Chunks commit as write temp → fsync file → rename → fsync directory. An fsync
error is treated as data loss and never retried (fsyncgate: the kernel marks pages clean after a
failed fsync).

**Refuse rather than ignore.** Requests for server-side encryption, ACLs, or tagging on write get
501 — storing plaintext and answering 200 would lie to the client. That cost s3-tests pass count;
it bought honesty.

**Streaming by construction.** Objects are processed in 32 MB chunks. A 4 GB object peaks the node
process at 89 MB RSS — sixty-four times the size for twice the memory of a 64 MB object, measured
with `ps` on the running process.

## Proof

**Chaos suite (`TestChaos`).** A concurrent S3 workload runs while faults arrive at random: nodes
killed, frozen, restarted, disks wiped, bits flipped. The suite records every acknowledged write and
re-reads the full history afterward. A 45-minute run: 51,623 acks, 40 faults, 43,329 survivors
byte-identical. CI runs a five-minute variant on every push, with one retry on flake.

**Crash harness.** `SIGKILL` under 100 concurrent uploads; zero acknowledged loss after restart.

**Demo (`make demo`).** Six processes, store an object, kill an owner mid-read, watch three copies
return — with digest checks at each step and chunk presence verified by asking each node directly.

## Numbers (honest scope)

All published benchmarks run six nodes as **real processes on one laptop NVMe**. That makes small
writes fsync-bound (~25 ms PUT 4 KB) and large writes bandwidth-bound — a floor set by one disk, not
a datacenter ceiling. The README states this explicitly; a multi-host harness exists
(`make bench-remote`, `make measure-remote`) for when separate machines are available.

What the numbers are for:

| measurement | result | meaning |
| --- | --- | --- |
| Heal after full disk wipe | 3.36 s capped / 430 ms uncapped | repair is rate-limited and automatic |
| Seventh node joins | 4.6 s, exact copy count | rebalance matches the ring, key for key |
| 4 GB streaming GET | 89 MB peak RSS | memory scales with chunk size, not object size |

## What I would do in production

- Run etcd as a real cluster (three or five nodes), not a single dev instance.
- Separate the internal API from the S3 port at the network layer — the internal port can delete
  chunks and is unauthenticated.
- Monitor repair lag and etcd latency; small reads are manifest-bound.
- For scale beyond etcd's practical object-count ceiling, volume/needle packing (Haystack-style)
  metadata — documented as a known limitation, not hidden.

## Stack

Go · etcd · HTTP chunk transfer · Reed–Solomon (`klauspost/reedsolomon`) · SigV4 · AWS SDK and CLI
as independent test oracles · `-race` on every CI test run.

## Links

- [cv-brief.md](cv-brief.md) — **canonical facts for CV / résumé copy**
- [README](../README.md) — guarantees table and quick start
- [design.md](design.md) — full architecture and invariants
- [benchmarks.md](benchmarks.md) — methodology and what was rejected
- [demo.md](demo.md) — record the kill-and-heal demo
- [s3-compatibility.md](s3-compatibility.md) — 182/886 s3-tests, every failure classified
