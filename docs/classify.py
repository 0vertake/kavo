#!/usr/bin/env python3
"""Classify a pytest failure list from Ceph's s3-tests into families.

Usage:
    pytest ... -q --tb=no | rg '^FAILED' | sed 's/^FAILED //' > failed.txt
    python3 docs/classify.py failed.txt

Every failure lands in exactly one family: the first rule below that matches its
test name wins, so the order is the classification. The counts therefore sum to
the number of failures rather than double-counting a test that touches two
features — an SSE copy is counted as encryption, not as a copy.
"""

import collections
import re
import sys

RULES = [
    ("SigV2 signing", r"aws2$"),
    ("browser POST form uploads", r"^test_post_"),
    # enc\[ catches the parametrised copy_enc/copy_part_enc ids, which are SSE tests
    # wearing a copy's name: they pass a customer key or ask for SSE-S3, so no amount
    # of copy support reaches them.
    ("server-side encryption (SSE-C, SSE-KMS)", r"sse_|_encryption|enc_|enc\[|kms"),
    ("ACLs, grants, and the public/private access matrix", r"acl|grant|^test_access_|anon"),
    ("object lock, retention, legal hold, governance", r"object_lock|legal_hold|retention|governance"),
    ("versioning (version ids, delete markers, suspend)", r"version"),
    ("bucket policy, public access block, ownership controls", r"policy|public_block|public_access|public_buckets|ownership|owner_enforced|owner_preferred|object_writer"),
    ("lifecycle and expiration", r"lifecycle|expiration"),
    ("bucket and request logging", r"logging"),
    ("CopyObject and multipart copy", r"copy"),
    ("conditional requests (If-Match, If-None-Match, If-Modified-Since)", r"ifmatch|ifnonematch|ifnonmatch|ifmodifiedsince|ifunmodifiedsince|if_match|if_none_match|conditional"),
    ("ListObjects v1 and its paging parameters", r"list(?!v2)|_list_|delimiter|prefix|marker|maxkeys"),
    ("bulk delete (DeleteObjects)", r"multi_object_delete|delete_objects"),
    ("anonymous and raw unsigned access", r"object_raw|unreadable"),
    ("GetBucketLocation", r"get_location"),
    ("CORS", r"cors"),
    ("tagging", r"tag"),
    ("non-MD5 checksum algorithms", r"crc|sha1|checksum"),
    ("Content-MD5 verification", r"md5"),
    ("multipart upload edge cases", r"multipart|_mpu|upload_part"),
    ("user metadata and header passthrough", r"metadata|cache_control|expires|content_disposition|content_language|content_encoding|response_headers"),
    ("bucket-as-prefix consequences (create, head, delete, gone)", r"bucket_create|bucket_head|head_bucket|bucket_delete|delete_bucket|bucket_gone|nonexist_bucket|notexist|bucket_recreate|bucketv2|expected_bucket_owner|bucket_list"),
    ("100-continue and Expect", r"100_continue|expect"),
    ("request id and usage reporting", r"requestid|request_id|usage"),
    ("authorization header edge cases", r"authorization|amz_date|_date$|contentlength|chunked_transfer"),
    ("atomic overwrite semantics", r"atomic"),
    ("GetObjectAttributes", r"object_attributes"),
    ("website, torrent, select, notification, inventory, analytics, replication",
     r"website|torrent|select|notification|inventory|analytics|replication"),
    ("IAM, STS, roles", r"iam|sts|assume_role"),
]


def main() -> int:
    names = [line.split("::")[-1].strip() for line in open(sys.argv[1]) if "::" in line]
    counts = collections.Counter()
    residual = []
    for name in names:
        for label, pattern in RULES:
            if re.search(pattern, name):
                counts[label] += 1
                break
        else:
            residual.append(name)

    for label, _ in RULES:
        if counts[label]:
            print(f"| {counts[label]} | {label} |")
    for name in residual:
        print(f"| 1 | UNCLASSIFIED: {name} |")
    print(f"\ntotal {sum(counts.values()) + len(residual)} of {len(names)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
