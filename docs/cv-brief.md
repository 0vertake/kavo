# kavo — CV brief

**Canonical facts for résumé / portfolio copy.** Prefer this file over older paragraphs elsewhere
if numbers disagree — `docs/s3-compatibility.md` and `docs/benchmarks.md` are the other sources of
truth.

## Identity

| field | value |
| --- | --- |
| **Name** | kavo |
| **URL** | https://github.com/0vertake/kavo |
| **One line** | Distributed, S3-compatible object store in Go with proven durability invariants |
| **Status** | Complete as a portfolio / research project; not a production deployment |

## Stack

Go · etcd (metadata & membership) · consistent hashing · quorum replication (N=3, W=2) · Reed–Solomon
erasure coding (optional) · SigV4 S3 subset · HTTP chunk transfer · chaos testing · `-race` CI

## Four durability invariants (each has a failure-injecting test)

1. No acknowledged write is ever lost.
2. No partially written object is ever readable.
3. Every read returns checksum-valid data or an explicit error — never silent corruption.
4. After healing completes, redundancy is back to the configured level.

## Proof (use these in CV bullets)

| claim | evidence |
| --- | --- |
| Chaos under random faults | 45-min run: **51,623** acked writes, **40** faults (incl. wipe of **40,274** chunks), **43,329** survivors re-read byte-identical |
| CI | Chaos suite (replicated + erasure-coded) on every push; tests run with `-race` |
| Crash safety | `SIGKILL` under 100 concurrent uploads; zero acked loss after restart |
| Live demo | `SIZE=1 make demo` — kill node owner mid-read, redundancy returns in ~1 s ([transcript](demo-transcript.txt)) |

## Measured guarantees (`make measure`, six processes on one laptop NVMe)

| measurement | result | caveat |
| --- | --- | --- |
| Heal after full disk wipe | **3.36 s** at 32 MB/s cap / **430 ms** uncapped | 512 MB test data, six nodes |
| Seventh node joins | **4.6 s** converge; **34** copies moved = exact ring expectation | milestone 8 |
| 4 GB object streaming | **89 MB** peak node RSS (vs 33 MB for 64 MB object) | flat memory claim |

Benchmark methodology and honest limits: [`benchmarks.md`](benchmarks.md). Multi-host harness exists
(`make bench-remote`, `make measure-remote`); **multi-host results table not filled** — needs six
separate machines; Docker on one laptop is explicitly not valid for publication.

## S3 compatibility (Ceph s3-tests)

**182 pass · 610 fail · 94 skip · 886 total · nothing errors**

Most failures are **anti-goals** (ACLs, versioning, SSE, lifecycle, etc.) or **deliberate refusals**
(501 for encryption/tagging rather than lying to clients). Full classification:
[`s3-compatibility.md`](s3-compatibility.md).

Implemented subset includes: PUT/GET/HEAD/DELETE, ListObjectsV2, multipart (+ UploadPartCopy),
CopyObject, SigV4, conditional reads, CRC32/CRC32C/CRC64NVME on upload, user metadata.

## Anti-goals (do not claim on CV)

IAM, ACLs, versioning, bucket policies, lifecycle, SSE, object lock, CORS, tagging writes, SigV2,
conditional writes (commit-point change).

## Suggested CV bullets (pick 1–2)

- Built a distributed S3-compatible object store in Go (consistent hashing, quorum replication,
  Reed–Solomon EC, etcd commit point); proved four durability invariants under randomized faults
  (51k+ acked writes, zero loss in 45-min chaos run).

- Designed commit-point semantics (W fsyncs + etcd manifest before ack); chaos-tested with node
  kills, disk wipes, and bit rot; `-race` CI on every push.

- Measured cluster guarantees: 3.36 s heal after full disk loss, flat memory under 4 GB streaming
  (89 MB RSS), join rebalance exact to the ring — with methodology and caveats documented.

## Best docs for an agent to read

1. **This file** — canonical numbers
2. [`case-study.md`](case-study.md) — narrative for portfolio / cover letter
3. [`../README.md`](../README.md) — guarantees table, demo, benchmark snapshot
4. [`design.md`](design.md) — architecture and invariants (long)
5. [`benchmarks.md`](benchmarks.md) — all performance numbers and what was rejected
6. [`s3-compatibility.md`](s3-compatibility.md) — s3-tests breakdown

## Commands (for “I built and tested it” claims)

```sh
make test      # unit + integration + short chaos, -race
make demo      # kill-and-heal smoke test (needs aws, Docker for etcd)
make measure   # heal / join / RSS numbers (minutes, writes GB)
```

Do **not** claim multi-host network benchmarks unless the table in `benchmarks.md` has been filled
from a real six-host run.
