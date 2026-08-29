# Demo

The shortest proof that kavo's durability claims are checked, not narrated: six real processes, one
object, `SIGKILL` to an owner, redundancy returns on its own.

## Run

Requires Go, Docker (etcd), and the AWS CLI v2.

```sh
make etcd
SIZE=1 make demo
```

`SIZE=1` keeps the object at 1 MB so the AWS CLI read-back checks stay reliable on all versions.
The default 32 MB demo is the same story with more chunks.

A recorded transcript: [`demo-transcript.txt`](demo-transcript.txt).

## What each step checks

| step | claim |
| --- | --- |
| Store + read back | acknowledged write is durable |
| Kill owner mid-read | in-flight read may fail; acked data is not lost |
| Read after kill | quorum read from remaining replicas |
| Wait for 3 holders | repair restores redundancy |
| Placement update | dead node removed from manifest |
| Final read + md5 | no silent corruption |

The demo uses real processes rather than Docker for the nodes, because Docker Desktop on macOS
distorts fsync timing (see [`benchmarks.md`](benchmarks.md)). etcd alone runs in Docker.

## Troubleshooting

| symptom | fix |
| --- | --- |
| `demo needs aws on PATH` | AWS CLI v2 required |
| `demo needs docker` | `make etcd` starts etcd via Compose |
| `int too big to convert` on download | use `SIZE=1` (some AWS CLI builds on macOS) |
