# S3 compatibility

Ceph's [`s3-tests`](https://github.com/ceph/s3-tests) is the suite S3 implementations are measured
against. It is an independent oracle in the strongest sense available: nobody involved in kavo chose
what it asserts, and it encodes S3's behaviour as observed by people who had to match it.

**151 of 886 pass. 641 fail, 94 the suite skips itself, and nothing errors** — every test reaches a
verdict rather than dying in setup. The pass count is not the interesting number on its own, because
most of what the suite covers is deliberately absent here (see the locked subset in
`docs/design.md`). What is interesting is the classification below: what fails because of an
anti-goal, and what fails because of a gap.

It used to be 169, and the 18 it lost are the most useful thing the suite produced. Every bucket
subresource is a query on the bucket's own path — `?versioning`, `?acl`, `?lifecycle`, `?encryption`,
`?policy`, `?tagging` — so a `PUT` to any of them reached the handler that creates a bucket and was
answered with a 200. Those eighteen tests configure something and check that the call succeeded, and
kavo passed them by claiming to have installed a lifecycle rule, a bucket policy, or server-side
encryption that does not exist anywhere in the code. That is worse than refusing: a client told
versioning is now on will overwrite objects believing the old ones can be recovered. They are
refused now, and the number is 18 lower and honest.

## Running it

```sh
git clone https://github.com/ceph/s3-tests && cd s3-tests
python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt
make -C /path/to/kavo up                       # six nodes on 9001–9006
S3TEST_CONF=kavo.conf ./.venv/bin/python -m pytest \
    s3tests/functional/test_s3.py s3tests/functional/test_headers.py -q --tb=no \
    | rg '^FAILED' | sed 's/^FAILED //' > failed.txt
python3 /path/to/kavo/docs/classify.py failed.txt
```

`kavo.conf` is the sample config with `host = 127.0.0.1`, `port = 9001`, and `kavo`/`kavosecret` as
the credentials in every user section. The `[s3 alt]` and `[s3 tenant]` users point at the same key
pair, because kavo authenticates exactly one — so the multi-user tests fail on kavo's behaviour
rather than on a missing config.

The other five test files are not run: `test_iam.py`, `test_sts.py`, `test_sns.py`,
`test_s3control.py` and `test_s3select.py` target IAM, STS, SNS, S3 Control and S3 Select, which are
whole services rather than features of an object store.

## What the suite found

Four real defects, three of them invisible to kavo's own tests, which is the entire argument for
running someone else's suite:

- **A complete listing claimed to be truncated.** A page that filled exactly as the keyspace ran out
  reported `IsTruncated: true` and handed out a continuation token, because the listing stopped at a
  full page and reported a resume point rather than looking to see whether anything followed. A
  client then spent a round trip to receive nothing. kavo's own paging test used three keys and a
  page size of two, so its last page was short and the bug had nowhere to appear; `s3-tests` lists
  two keys with a page size of two. The listing now looks one entry past the page, and
  `TestAFullPageThatEndsTheListingIsNotTruncated` covers the flat, single-page and grouped shapes.
- **A truncated version listing could not be paged.** `ListObjectVersions` omitted
  `NextVersionIdMarker`, and clients feed each response's markers straight into the next request, so
  the marker arrived back as a null and the paging loop stopped on page two — with objects still in
  the bucket. This is how an SDK empties a bucket, so the failure mode is a cleanup that silently
  does half its work. It surfaced as 14 errors rather than failures.
- **`/` came back percent-encoded.** With `encoding-type=url`, kavo encoded everything outside the
  unreserved set, so a client that asked for `delimiter=/` was told the delimiter was `%2F`. S3
  leaves `/` alone; it means the same thing to either unescaper, so encoding it bought no safety.
- **`fetch-owner=true` returned no owner**, which a client reads as an object that has none.

It found no integrity failure: nothing in the suite got back bytes other than the ones it wrote.

## Why the 641 fail

`docs/classify.py` produces this table from the suite's own failure list. Each test lands in exactly
one family — the first that matches its name, in the order shown — so the counts sum to 641 rather
than counting an SSE copy twice. A test is filed under what it is about, which is not always what it
died on: many of these never reach their assertion because a `ListObjects` v1 call or a
`GetBucketVersioning` in their setup is refused first.

| count | family | verdict |
| --- | --- | --- |
| 89 | server-side encryption (SSE-C, SSE-KMS) | anti-goal |
| 75 | ACLs, grants, and the public/private access matrix | anti-goal |
| 66 | `CopyObject` and multipart copy | gap |
| 47 | versioning: version ids, delete markers, suspend | anti-goal |
| 46 | `ListObjects` v1 and its paging parameters | deliberate: v2 only |
| 39 | bucket policy, public access block, ownership controls | anti-goal |
| 36 | object lock, retention, legal hold, governance | anti-goal |
| 34 | lifecycle and expiration | anti-goal |
| 28 | browser `POST` form uploads | anti-goal |
| 28 | conditional requests (`If-Match`, `If-None-Match`, `If-Modified-Since`) | gap |
| 28 | consequences of a bucket existing as soon as it is named | design, see below |
| 26 | bucket and request logging | anti-goal |
| 21 | SigV2 signing | anti-goal: SigV4 only |
| 13 | multipart upload edge cases | gap, see below |
| 12 | CORS | anti-goal |
| 10 | tagging | anti-goal |
| 9 | non-MD5 checksum algorithms (CRC32, CRC32C, SHA-1) | gap |
| 8 | anonymous and unsigned access | anti-goal: one key pair, everything signed |
| 7 | user metadata (`x-amz-meta-*`) and header passthrough | gap |
| 6 | error codes for malformed authorization and date headers | gap |
| 3 | `Content-MD5` verification of an upload | gap |
| 3 | `100-continue` and `Expect` | gap |
| 2 | bulk delete, both failing in setup on a v1 listing | deliberate: v2 only |
| 2 | request id and usage reporting | anti-goal |
| 1 | `GetObjectAttributes` | anti-goal |
| 1 | website, torrent, select, notification, inventory, analytics, replication | anti-goal |
| 1 | `GetBucketLocation` | suite artifact, see below |

Anti-goals are listed in `docs/design.md` and are not defects: kavo is an object store with a locked
S3 subset, not an S3 clone. The rows marked **gap** are things a client might reasonably expect that
kavo does not do yet, and they are worth naming honestly:

- **`CopyObject`** is the largest one. `aws s3 cp s3://a s3://b` and `aws s3 mv` need it, and it is
  a server-side operation over data kavo already has — a manifest copy under a new key, with no
  chunk movement at all, which is why it would be small to add and why chunk garbage collection has
  to exist first: two keys would then name the same chunks.
- **Conditional requests** (`If-Match`, `If-None-Match`, `If-Modified-Since`) are how clients cache
  and how they implement compare-and-swap on an object. Manifests carry an ETag and etcd carries a
  revision, so the answer is available; nothing consumes it yet.
- **`Content-MD5`** is the one that fits the project's own thesis. A client that sends the header is
  asking the server to verify the bytes it received, kavo already computes that MD5 for the ETag as
  the body streams past, and a mismatch would simply mean never committing the manifest — so the
  object would never exist, which is the guarantee working as designed.
- **Re-completing a finished multipart upload** answers `NoSuchUpload` where S3 answers 200. A client
  whose completion response was lost retries and concludes the upload failed while the object is
  sitting there. Being idempotent means remembering which upload ids completed and what they
  produced, and that record needs the same garbage collection the manifest compare-and-swap work is
  waiting on, so it is deferred rather than half-built.
- **`x-amz-meta-*` user metadata** is stored for nothing today: kavo keeps `Content-Type` and drops
  the rest.

One failure is neither: `test_bucket_get_location` asserts that `GetBucketLocation` returns
`default`, which is the region name in Ceph's own config file rather than anything S3 defines. kavo
returns the empty constraint, which is what AWS returns for `us-east-1` and what botocore surfaces
as `None`. Matching the suite here would mean being wrong for every real client.

### Buckets exist as soon as they are named

Twenty-eight failures follow from one design decision rather than from an oversight. An object's key in
etcd is `bucket/key`, so a bucket is a prefix: `HEAD` of a bucket that was never created succeeds,
deleting a bucket that does not exist succeeds, and `CreateBucket` is idempotent where S3 conflicts.
`docs/design.md` explains why, and the alternative — a bucket record — is a second source of truth
that can disagree with the objects in it.

Three bucket operations exist all the same, because clients call them unprompted: `CreateBucket` as
a no-op that succeeds, `ListBuckets` as a root listing grouped on `/`, and `DeleteBucket`, which
refuses while any object under the prefix is still readable. Along with `DeleteObjects`,
`ListObjectVersions` and `GetBucketLocation`, they are what makes the store reachable by clients
kavo did not choose: without `ListObjectVersions` and `DeleteObjects` this suite could not clean up
after itself, and without `GetBucketLocation` `warp` and `mc` cannot even check that a bucket
exists.

## The number to watch

The pass count moves when the subset changes, and on its own it is easy to game — implementing ACLs
would add 75 without making the store better at storing objects, and answering a subresource write
with a 200 adds 18 for nothing but a lie. The number worth watching is the one for the operations
kavo claims:

| family | passing |
| --- | --- |
| `ListObjectsV2`, all shapes | 37 of 40 |

The three remaining are anonymous access, the `allow-unordered` extension, and an empty
`continuation-token` echoed back as an absent field rather than an empty one.
