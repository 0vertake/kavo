# kavo

A distributed, S3-compatible object store in Go — consistent-hash placement, quorum
replication and Reed–Solomon erasure coding, automatic repair, and a chaos suite that
proves the guarantees.

Work in progress. See [`docs/design.md`](docs/design.md) for the architecture and
milestones. This README will carry the measured numbers (throughput, latency, heal time,
storage overhead) once the benchmark milestones land.
