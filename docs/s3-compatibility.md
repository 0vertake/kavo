# S3 compatibility

Ceph's [`s3-tests`](https://github.com/ceph/s3-tests) is the suite S3 implementations are measured
against. It is an independent oracle in the strongest sense available: nobody involved in kavo chose
what it asserts, and it encodes S3's behaviour as observed by people who had to match it.

**177 of 886 pass. 615 fail, 94 the suite skips itself, and nothing errors** — every test reaches a
verdict rather than dying in setup. The pass count is not the interesting number on its own, because
most of what the suite covers is deliberately absent here (see the locked subset in
`docs/design.md`). What is interesting is the classification below: what fails because of an
anti-goal, and what fails because of a gap.

The count has moved five times, and twice it moved down on purpose:

| count | what changed |
| --- | --- |
| 169 | where it started being measured |
| 151 | refusing bucket subresource writes that had been answered with a 200 |
| 170 | `CopyObject` |
| 179 | conditional reads and `Content-MD5` |
| 196 | user metadata, header passthrough, and the multipart calls the API was missing |
| 177 | refusing requests for encryption instead of ignoring them |

The last line is the one worth reading. kavo does not encrypt objects, and it used to *ignore* the
headers asking it to: a client that sent a customer key was answered `200`, its object stored in
plaintext that anyone could read back without the key, and the suite scored that arrangement as
twenty-two passes. Three of them arrived in this very round, as a side effect of unrelated multipart
work — which is how the problem surfaced, since an encryption test passing on a store with no
encryption can only be passing for a bad reason. They are refused now, all of them, and the count
is nineteen lower and honest. The 20 real gains in the same round are in the row above it.

The nine that came with conditional reads and `Content-MD5` are worth naming too, because the suite
disagreed with the first version of both in ways that reading the specification had not: an
`If-Match` that fails is a 412 while an `If-None-Match` that matches is a 304, so the same unmet
condition has two answers depending on which way it was asked; the four date and tag headers have a
precedence order rather than being four independent tests; a condition on a copy is 412 either way,
since a copy has no "you already have it" outcome to report; and an *empty* `Content-MD5` is a
malformed digest rather than an absent one. None of them was the feature — they were all the edges
of it.

The 19 `CopyObject` added are the cheapest 19 in the suite: a copy is a manifest written under a
second key, no chunk moves, and four encryption tests that had been failing in their setup got far
enough to pass as well.

The 18 lost before that are the most useful thing the suite produced. Every bucket
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

## Why the 615 fail

`docs/classify.py` produces this table from the suite's own failure list. Each test lands in exactly
one family — the first that matches its name, in the order shown — so the counts sum to 615 rather
than counting an SSE copy twice. A test is filed under what it is about, which is not always what it
died on: many of these never reach their assertion because a `ListObjects` v1 call or a
`GetBucketVersioning` in their setup is refused first.

| count | family | verdict |
| --- | --- | --- |
| 138 | server-side encryption (SSE-C, SSE-KMS) | anti-goal, and refused rather than ignored |
| 75 | ACLs, grants, and the public/private access matrix | anti-goal |
| 47 | versioning: version ids, delete markers, suspend | anti-goal |
| 45 | `ListObjects` v1 and its paging parameters | deliberate: v2 only |
| 39 | bucket policy, public access block, ownership controls | anti-goal |
| 36 | object lock, retention, legal hold, governance | anti-goal |
| 34 | lifecycle and expiration | anti-goal |
| 28 | browser `POST` form uploads | anti-goal |
| 28 | consequences of a bucket existing as soon as it is named | design, see below |
| 26 | bucket and request logging | anti-goal |
| 24 | conditional writes and deletes (`If-Match` on `PUT`, `DELETE`) | deliberate, see below |
| 21 | SigV2 signing | anti-goal: SigV4 only |
| 12 | CORS | anti-goal |
| 10 | tagging | anti-goal |
| 9 | `UploadPartCopy` and cross-account copy | gap and anti-goal, see below |
| 9 | multipart upload edge cases | mixed, see below |
| 9 | non-MD5 checksum algorithms (CRC32, CRC32C, SHA-1) | gap |
| 8 | anonymous and unsigned access | anti-goal: one key pair, everything signed |
| 6 | error codes for malformed authorization and date headers | gap |
| 3 | `100-continue` and `Expect` | gap |
| 2 | bulk delete, both failing in setup on a v1 listing | deliberate: v2 only |
| 2 | request id and usage reporting | anti-goal |
| 1 | `GetObjectAttributes` | anti-goal |
| 1 | website, torrent, select, notification, inventory, analytics, replication | anti-goal |
| 1 | `GetBucketLocation` | suite artifact, see below |
| 1 | non-ASCII metadata | suite artifact, see below |

The encryption row has grown twice without encryption being any nearer. Once by 37, when the rule
that catches encryption tests was found to match `enc_` and not `enc[`, so the parametrised
`test_copy_enc[...]` cases had been counted as copy gaps. Once by 16, when kavo started refusing the
requests it had been ignoring, which moved tests that had been passing into this row. A classifier is
only worth the numbers it produces, so both are filed here rather than quietly fixed.

By verdict: **478 anti-goals, 47 v1 `ListObjects`, 28 consequences of buckets being prefixes, 24
conditional writes, 36 named gaps, and 2 artifacts of the suite's own environment.** The gap column
is the one to read — it is the list of things a client might reasonably expect and not get. It is led
by `UploadPartCopy` (6) and by `?partNumber` reads (3), with the rest in checksums and error codes.

Not one conditional *read* fails. The 24 in the row above are all `If-Match` on a `PUT` or a
`DELETE`, and they are an exclusion rather than an oversight: a conditional write makes the commit a
compare-and-set instead of a `Put`, which is a change to the commit point — the one place in this
store where an argument is required before code. The distinction is in `classify.py` as two rules
rather than in this paragraph, because a single row covering both would report an excluded feature
and a regression in the read path identically.

Anti-goals are listed in `docs/design.md` and are not defects: kavo is an object store with a locked
S3 subset, not an S3 clone. The rows marked **gap** are things a client might reasonably expect that
kavo does not do yet, and they are worth naming honestly:

- **`UploadPartCopy`.** `CopyObject` works — a manifest written under a second key, no chunk
  movement, which is what makes `aws s3 mv` server-side. Assembling a new object out of *ranges* of
  existing ones does not, and that is 6 of the 9 in the copy row. A part is already a manifest of
  chunk references, so copying a whole object into a part would be easy; a range of one is not,
  because it would have to re-chunk at the range boundaries, and re-chunking is the one thing a copy
  never does. The other 3 are cross-account, which needs a second key pair to exist.
- **`?partNumber` on a read**, which returns one part of a multipart object and the `PartsCount` of
  the whole, and the 3 tests that ask for it. The manifest records the object's chunks but not where
  its parts ended, so answering this means recording part boundaries at completion — a change to what
  a manifest is, for a feature whose only user is a client parallelising a download it could do with
  ranges.
- **Non-MD5 checksums** (`x-amz-checksum-crc32`, `-crc32c`, `-sha1`, `-sha256`). This one is
  half-built already and in the right direction: chunks are CRC32C-checksummed on disk and verified
  on every read. What is missing is the S3 surface for a client to declare one and read it back,
  including its trailer form on a streaming upload.
- **Error codes for malformed authorization and date headers**, where kavo answers a plausible
  refusal with the wrong code — a client is told `AccessDenied` where S3 says
  `MissingSecurityHeader`. Both refuse, so nothing is stored on a bad signature; a client
  distinguishing the two programmatically gets the wrong answer.

Three failures are deliberate rather than missing, and each is a case where the suite asks kavo to
be more forgiving than it is willing to be:

- **A part smaller than 5 MiB** is accepted where S3 answers `EntityTooSmall`
  (`test_multipart_upload_size_too_small`). The limit exists to bound the part count, which kavo
  bounds directly at 10,000 parts, and enforcing it would cost more than it buys: every multipart
  test in this repository — including the chaos suite, which writes thousands of objects while
  killing nodes — would have to move 5 MiB per part. Taxing the test that proves durability to gain
  a compatibility test is the wrong trade, and the alternative, a minimum part size that tests can
  turn off, is a protocol constant pretending to be configuration.
- **A completion that names the same part twice**, with two different etags, is refused where S3
  picks one (`test_multipart_resend_first_finishes_last`, which uploads part 1 twice concurrently and
  then lists both etags). The client has sent contradictory instructions; committing either version
  produces an object nobody uploaded, and refusing is the same rule that makes any completion naming
  a part it did not upload change nothing.
- **A conditional write** is refused, as described above.

Two failures are neither defect nor decision, but artifacts of the environment the suite runs in.
`test_bucket_get_location` asserts that `GetBucketLocation` returns
`default`, which is the region name in Ceph's own config file rather than anything S3 defines. kavo
returns the empty constraint, which is what AWS returns for `us-east-1` and what botocore surfaces
as `None`. Matching the suite here would mean being wrong for every real client.

`test_object_set_get_unicode_metadata` is the other. It sends a metadata value as UTF-8 and reads the
response back as latin-1, so passing it requires the server to re-encode between the two — to guess
the charset of a header whose charset HTTP does not carry. kavo stores the bytes it was given and
returns them unchanged, which round-trips for any client that reads the header the way it wrote it.
The suite marks this one as failing on Ceph too, with the comment that the decoding "is not happening
properly for unknown reasons".

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
| multipart upload, end to end | 14 of 25 |
| single-object `CopyObject` | 15 of 23 |
| conditional reads: 8 on `GET`, 4 on a copy source | 12 of 12 |
| user metadata and header passthrough | 6 of 7 |

The three listing failures are anonymous access, the `allow-unordered` extension, and an empty
`continuation-token` echoed back as an absent field rather than an empty one.

The multipart row counts the 25 multipart tests that are not encryption, ACL, versioning, lock,
policy, logging, tagging, attributes or copy variants. It was 9 of 25 before this round. Its 11
remaining failures are the 3 `?partNumber` reads, 3 conditional writes, 2 lifecycle expiry of an
abandoned upload, 1 owner reporting, and the 2 deliberate refusals above.

The copy row counts the 23 copy tests that are not encryption, ACL, versioning, tagging, lock,
policy, logging or cross-tenant variants — those fail on the anti-goal, not on the copy. Its 8
failures are 6 `UploadPartCopy` and 2 cross-account. Two of the 15 passes were accidents until
recently: `test_copy_object_ifmatch_good` and `test_copy_object_ifnonematch_failed` expect a copy to
proceed, and kavo proceeded because it ignored the condition. Their mirror images, where the
condition should refuse the copy, are two of the nine tests the conditional work added — which is the
argument for reading the failures rather than the passes, and for having written down that those two
were hollow while they were.

The metadata row's one failure is the charset artifact above. This row is the reason to keep an
independent client in the loop: kavo's own tests use the AWS Go SDK, which lowercases the metadata
keys of a response before handing them over, so it could not see that kavo was replaying
`X-Amz-Meta-Colour` where S3 sends `x-amz-meta-colour`. Every one of these seven tests failed on that
alone, and every one of kavo's own passed. The claim now has a test that can fail
(`TestStoredMetadata`, which asserts the stored key rather than a client's reading of it).
