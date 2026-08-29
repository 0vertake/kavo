# Demo walkthrough

The shortest proof that kavo's durability claims are checked, not narrated: six real processes, one
object, `SIGKILL` to an owner, redundancy returns on its own.

## Record a screen capture (2–3 minutes)

**What to show:** terminal only, font large enough to read on a phone.

```sh
# Prerequisites: Go, Docker, aws CLI
make etcd
SIZE=1 make demo
```

`SIZE=1` keeps the object at 1 MB so the AWS CLI read-back checks stay reliable on all versions.
The default 32 MB demo is the same story with more chunks.

**Optional — terminal recording with asciinema** (upload to asciinema.org, embed in README):

```sh
brew install asciinema   # or apt install asciinema
asciinema rec kavo-demo.cast -c "cd $(pwd) && SIZE=1 make demo"
```

**What to say while it runs** (if you narrate):

1. Six kavod nodes join through etcd; object stored with three replicas.
2. One owner is killed mid-read — no graceful shutdown.
3. Reads still work from surviving copies.
4. Repair rebuilds the third copy; the manifest is updated to name only live nodes.
5. Every step compares digests and asks each node whether it holds the chunk.

A pre-recorded transcript lives in [`demo-transcript.txt`](demo-transcript.txt).

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

- **`demo needs aws on PATH`** — install the AWS CLI v2.
- **`demo needs docker`** — `make etcd` starts etcd via Compose.
- **Download fails with `int too big to convert`** — use `SIZE=1` (some AWS CLI builds on macOS).
