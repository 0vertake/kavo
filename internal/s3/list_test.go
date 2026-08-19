package s3_test

// Listing, driven through the SDK — including its paginator, since paging is the
// part of a listing that goes wrong. A listing that silently stops early looks
// exactly like a bucket with fewer objects in it.

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// put writes an object and fails the test if it does not stick.
func put(t *testing.T, client *awss3.Client, bucket, key string, body []byte) {
	t.Helper()
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

// listAll pages through a listing with the SDK's paginator and returns the keys
// and grouped prefixes in the order they arrived.
func listAll(t *testing.T, client *awss3.Client, in *awss3.ListObjectsV2Input) (keys, prefixes []string) {
	t.Helper()
	pages := awss3.NewListObjectsV2Paginator(client, in)
	for pages.HasMorePages() {
		page, err := pages.NextPage(t.Context())
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
		for _, p := range page.CommonPrefixes {
			prefixes = append(prefixes, aws.ToString(p.Prefix))
		}
	}
	return keys, prefixes
}

// The base claim: every key in a bucket, in order, and only that bucket's keys.
func TestListingReturnsEveryKeyInOrder(t *testing.T) {
	client := newGateway(t)
	want := []string{"a.txt", "b/c.txt", "b/d.txt", "e.txt", "z/y/x.txt"}
	for _, key := range slices.Backward(want) { // written out of order on purpose
		put(t, client, "bucket", key, []byte(key))
	}
	put(t, client, "other", "not-mine.txt", []byte("x"))

	keys, prefixes := listAll(t, client, &awss3.ListObjectsV2Input{Bucket: aws.String("bucket")})
	if !slices.Equal(keys, want) {
		t.Errorf("listed %v, want %v", keys, want)
	}
	if len(prefixes) != 0 {
		t.Errorf("got common prefixes %v without asking for a delimiter", prefixes)
	}
}

// A prefix narrows the listing, and the keys keep their full names: a client that
// asked for "b/" and got "c.txt" back cannot fetch it.
func TestListingByPrefix(t *testing.T) {
	client := newGateway(t)
	for _, key := range []string{"a.txt", "b/c.txt", "b/d.txt", "bb.txt", "c.txt"} {
		put(t, client, "bucket", key, []byte(key))
	}

	keys, _ := listAll(t, client, &awss3.ListObjectsV2Input{
		Bucket: aws.String("bucket"),
		Prefix: aws.String("b"),
	})
	if want := []string{"b/c.txt", "b/d.txt", "bb.txt"}; !slices.Equal(keys, want) {
		t.Errorf("listed %v, want %v", keys, want)
	}
}

// The delimiter is how a flat keyspace becomes directories, which is what `aws s3
// ls` shows. Keys below the delimiter must appear once as a group and not
// individually, or a bucket with a million objects in one folder lists a million
// entries.
func TestDelimiterGroupsKeysIntoPrefixes(t *testing.T) {
	client := newGateway(t)
	for _, key := range []string{"top.txt", "dir/a.txt", "dir/b.txt", "dir/sub/c.txt", "other/d.txt"} {
		put(t, client, "bucket", key, []byte(key))
	}

	keys, prefixes := listAll(t, client, &awss3.ListObjectsV2Input{
		Bucket:    aws.String("bucket"),
		Delimiter: aws.String("/"),
	})
	if want := []string{"top.txt"}; !slices.Equal(keys, want) {
		t.Errorf("listed keys %v, want %v", keys, want)
	}
	if want := []string{"dir/", "other/"}; !slices.Equal(prefixes, want) {
		t.Errorf("listed prefixes %v, want %v", prefixes, want)
	}

	// One level down: the prefix and delimiter together walk into the directory.
	keys, prefixes = listAll(t, client, &awss3.ListObjectsV2Input{
		Bucket:    aws.String("bucket"),
		Prefix:    aws.String("dir/"),
		Delimiter: aws.String("/"),
	})
	if want := []string{"dir/a.txt", "dir/b.txt"}; !slices.Equal(keys, want) {
		t.Errorf("listed keys %v, want %v", keys, want)
	}
	if want := []string{"dir/sub/"}; !slices.Equal(prefixes, want) {
		t.Errorf("listed prefixes %v, want %v", prefixes, want)
	}
}

// Paging has to return every key exactly once across page boundaries. This is the
// test that matters most: an off-by-one in the resume point either skips a key or
// repeats one forever, and both look like a working listing from one page away.
func TestPagingCoversEveryKeyExactlyOnce(t *testing.T) {
	client := newGateway(t)
	const n = 25
	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("key-%03d", i)
		put(t, client, "bucket", want[i], []byte("x"))
	}

	for _, size := range []int32{1, 2, 7, n - 1, n, n + 1} {
		t.Run(fmt.Sprintf("pages of %d", size), func(t *testing.T) {
			keys, _ := listAll(t, client, &awss3.ListObjectsV2Input{
				Bucket:  aws.String("bucket"),
				MaxKeys: aws.Int32(size),
			})
			if !slices.Equal(keys, want) {
				t.Errorf("listed %d keys in pages of %d, want %d\n got %v", len(keys), size, n, keys)
			}
		})
	}
}

// Paging with a delimiter is the awkward case: a page's worth of entries may be
// spread over many more keys, so the resume point has to be a key rather than a
// count of entries.
func TestPagingWithADelimiter(t *testing.T) {
	client := newGateway(t)
	var want []string
	for d := range 5 {
		want = append(want, fmt.Sprintf("dir-%d/", d))
		for i := range 4 {
			put(t, client, "bucket", fmt.Sprintf("dir-%d/file-%d", d, i), []byte("x"))
		}
	}

	for _, size := range []int32{1, 2, 5} {
		t.Run(fmt.Sprintf("pages of %d", size), func(t *testing.T) {
			keys, prefixes := listAll(t, client, &awss3.ListObjectsV2Input{
				Bucket:    aws.String("bucket"),
				Delimiter: aws.String("/"),
				MaxKeys:   aws.Int32(size),
			})
			if len(keys) != 0 {
				t.Errorf("every key is inside a group, but %v came back individually", keys)
			}
			if !slices.Equal(prefixes, want) {
				t.Errorf("listed %v, want %v", prefixes, want)
			}
		})
	}
}

// The page's own bookkeeping has to be right, not just its contents: clients decide
// whether to ask again from IsTruncated, and count what they got from KeyCount.
func TestPageReportsItsOwnShape(t *testing.T) {
	client := newGateway(t)
	for i := range 3 {
		put(t, client, "bucket", fmt.Sprintf("key-%d", i), []byte("x"))
	}

	page, err := client.ListObjectsV2(t.Context(), &awss3.ListObjectsV2Input{
		Bucket:  aws.String("bucket"),
		MaxKeys: aws.Int32(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(page.IsTruncated) {
		t.Error("a page of 2 out of 3 says it is not truncated, so a client stops here")
	}
	if aws.ToInt32(page.KeyCount) != 2 {
		t.Errorf("KeyCount = %d, want 2", aws.ToInt32(page.KeyCount))
	}
	if aws.ToString(page.NextContinuationToken) == "" {
		t.Error("a truncated page carries no continuation token, so the rest is unreachable")
	}

	rest, err := client.ListObjectsV2(t.Context(), &awss3.ListObjectsV2Input{
		Bucket:            aws.String("bucket"),
		ContinuationToken: page.NextContinuationToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToBool(rest.IsTruncated) {
		t.Error("the last page says it is truncated, so a client pages forever")
	}
	if got := aws.ToString(rest.Contents[0].Key); got != "key-2" {
		t.Errorf("the second page starts at %s, want key-2", got)
	}
}

// An empty bucket is a normal answer, not an error: `aws s3 ls` on one must print
// nothing rather than fail.
func TestListingAnEmptyBucket(t *testing.T) {
	client := newGateway(t)
	page, err := client.ListObjectsV2(t.Context(), &awss3.ListObjectsV2Input{Bucket: aws.String("empty")})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if aws.ToInt32(page.KeyCount) != 0 || len(page.Contents) != 0 {
		t.Errorf("an empty bucket listed %d keys", len(page.Contents))
	}
	if aws.ToBool(page.IsTruncated) {
		t.Error("an empty listing says it is truncated")
	}
}

// A listing carries the metadata a client shows without fetching anything, so it
// has to match what a HEAD of the same object says.
func TestListingCarriesObjectMetadata(t *testing.T) {
	client := newGateway(t)
	data := []byte("some bytes to measure")
	put(t, client, "bucket", "described.txt", data)

	page, err := client.ListObjectsV2(t.Context(), &awss3.ListObjectsV2Input{Bucket: aws.String("bucket")})
	if err != nil {
		t.Fatal(err)
	}
	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("described.txt"),
	})
	if err != nil {
		t.Fatal(err)
	}

	listed := page.Contents[0]
	if aws.ToInt64(listed.Size) != int64(len(data)) {
		t.Errorf("listed size %d, want %d", aws.ToInt64(listed.Size), len(data))
	}
	if aws.ToString(listed.ETag) != aws.ToString(head.ETag) {
		t.Errorf("listed ETag %s, HEAD says %s", aws.ToString(listed.ETag), aws.ToString(head.ETag))
	}
	// Truncated to the millisecond the XML carries, which is the same instant
	// rather than a rounding of it.
	if listed.LastModified == nil || !listed.LastModified.Equal(head.LastModified.Truncate(0)) {
		t.Errorf("listed Last-Modified %v, HEAD says %v", listed.LastModified, head.LastModified)
	}
}

// Keys that need escaping have to survive the XML and the client's unescaping.
// Getting this wrong means a listing that names keys nothing can fetch.
func TestListingKeysThatNeedEscaping(t *testing.T) {
	client := newGateway(t)
	// Each of these is read differently depending on how the client unescapes: a
	// plus is a space to one and a plus to the other. The last two are the ones
	// that force the server to encode rather than hope — their raw form is itself a
	// valid escape sequence, so a client that unescapes what it is given turns
	// "100%25.txt" into "100%.txt" and hands back a key that was never stored.
	want := []string{
		"100%.txt", "a b+c.txt", "a+b.txt", "needs&escaping.txt", "two words.txt", "☕.txt",
		"100%25.txt", "a%2Fb.txt",
	}
	for _, key := range want {
		put(t, client, "bucket", key, []byte("x"))
	}

	slices.Sort(want)

	// Without encoding-type the keys come back as they were stored, and can be
	// used directly. This is the default path, and the one the Go SDK expects:
	// it does not decode, so a server that encoded unasked would break it.
	keys, _ := listAll(t, client, &awss3.ListObjectsV2Input{Bucket: aws.String("bucket")})
	slices.Sort(keys)
	if !slices.Equal(keys, want) {
		t.Errorf("listed %q, want %q", keys, want)
	}
	for _, key := range keys {
		if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String(key),
		}); err != nil {
			t.Errorf("head %q, a key the listing named: %v", key, err)
		}
	}

	// With encoding-type=url — which is what the aws CLI asks for — the keys come
	// back percent-encoded and the client decodes them. Both unescapings have to
	// give the original back: a client is free to use either, and a key that means
	// two things depending on which is a key one of them cannot fetch.
	encoded, _ := listAll(t, client, &awss3.ListObjectsV2Input{
		Bucket:       aws.String("bucket"),
		EncodingType: types.EncodingTypeUrl,
	})
	var viaQuery, viaPath []string
	for _, key := range encoded {
		q, err := url.QueryUnescape(key)
		if err != nil {
			t.Errorf("query-unescape %q: %v", key, err)
			continue
		}
		p, err := url.PathUnescape(key)
		if err != nil {
			t.Errorf("path-unescape %q: %v", key, err)
			continue
		}
		viaQuery, viaPath = append(viaQuery, q), append(viaPath, p)
	}
	slices.Sort(viaQuery)
	slices.Sort(viaPath)
	if !slices.Equal(viaQuery, want) {
		t.Errorf("query-unescaping the encoded listing gives %q, want %q", viaQuery, want)
	}
	if !slices.Equal(viaPath, want) {
		t.Errorf("path-unescaping the encoded listing gives %q, want %q", viaPath, want)
	}
}

// A deleted object leaves the listing, since the manifest is both the object and
// its listing entry.
func TestDeletedObjectsLeaveTheListing(t *testing.T) {
	client := newGateway(t)
	for _, key := range []string{"stays.txt", "goes.txt"} {
		put(t, client, "bucket", key, []byte("x"))
	}
	if _, err := client.DeleteObject(t.Context(), &awss3.DeleteObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("goes.txt"),
	}); err != nil {
		t.Fatal(err)
	}

	keys, _ := listAll(t, client, &awss3.ListObjectsV2Input{Bucket: aws.String("bucket")})
	if want := []string{"stays.txt"}; !slices.Equal(keys, want) {
		t.Errorf("listed %v, want %v", keys, want)
	}
}

// ListObjectVersions against a store that keeps no versions. The claim is narrow:
// every current object appears once, marked as the latest with the version id S3
// uses for an object written before versioning existed. Clients call this to empty
// a bucket, and one that under-reports leaves objects behind.
func TestListingVersionsReportsEachObjectOnce(t *testing.T) {
	client := newGateway(t)
	keys := []string{"a.txt", "b.txt", "dir/c.txt"}
	for _, k := range keys {
		put(t, client, "versioned", k, []byte("payload of "+k))
	}
	// Overwritten, because an object written twice is where a second version
	// would appear if anything here kept one.
	put(t, client, "versioned", "a.txt", []byte("replacement"))

	out, err := client.ListObjectVersions(t.Context(), &awss3.ListObjectVersionsInput{
		Bucket: aws.String("versioned"),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	var got []string
	for _, v := range out.Versions {
		got = append(got, aws.ToString(v.Key))
		if id := aws.ToString(v.VersionId); id != "null" {
			t.Errorf("%s has version id %q, want null", aws.ToString(v.Key), id)
		}
		if !aws.ToBool(v.IsLatest) {
			t.Errorf("%s is not marked latest, and it is the only one there is", aws.ToString(v.Key))
		}
	}
	if !slices.Equal(got, keys) {
		t.Errorf("versions listed %v, want each key once: %v", got, keys)
	}

	// A client that read that listing deletes by the id it was given, and the
	// object has to actually go.
	if _, err := client.DeleteObject(t.Context(), &awss3.DeleteObjectInput{
		Bucket: aws.String("versioned"), Key: aws.String("a.txt"), VersionId: aws.String("null"),
	}); err != nil {
		t.Fatalf("DeleteObject by version id: %v", err)
	}
	after, err := client.ListObjectVersions(t.Context(), &awss3.ListObjectVersionsInput{
		Bucket: aws.String("versioned"),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(after.Versions) != len(keys)-1 {
		t.Errorf("after deleting one of %d keys the listing has %d", len(keys), len(after.Versions))
	}
}

// The case that hides: a page that ends exactly where the keyspace does.
//
// A listing that fills its page has no idea whether more follows unless it looks,
// and reporting "truncated" because it did not look is wrong in the one arrangement
// a test with an awkward key count never produces. Clients page until IsTruncated
// is false, so this is a wasted round trip and an empty page for anything that
// trusts the flag — and Ceph's suite is what caught it.
func TestAFullPageThatEndsTheListingIsNotTruncated(t *testing.T) {
	tests := []struct {
		name      string
		keys      []string
		delimiter string
		maxKeys   int32
	}{
		{name: "keys divide evenly into pages", keys: []string{"a", "b", "c", "d"}, maxKeys: 2},
		{name: "one page holding everything", keys: []string{"a", "b"}, maxKeys: 2},
		{name: "grouped prefixes divide evenly",
			keys: []string{"x/1", "x/2", "y/1", "y/2"}, delimiter: "/", maxKeys: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newGateway(t)
			bucket := "exact"
			for _, k := range tt.keys {
				put(t, client, bucket, k, []byte("x"))
			}

			in := &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), MaxKeys: aws.Int32(tt.maxKeys)}
			if tt.delimiter != "" {
				in.Delimiter = aws.String(tt.delimiter)
			}
			// Page through to the last page, which is the one under test.
			var pages int
			for {
				page, err := client.ListObjectsV2(t.Context(), in)
				if err != nil {
					t.Fatalf("list: %v", err)
				}
				pages++
				if pages > len(tt.keys)+1 {
					t.Fatal("the listing never reported itself complete")
				}
				if got := len(page.Contents) + len(page.CommonPrefixes); got == 0 {
					t.Fatalf("page %d came back empty, so the page before it was "+
						"truncated with nothing left to read", pages)
				}
				if !aws.ToBool(page.IsTruncated) {
					if token := aws.ToString(page.NextContinuationToken); token != "" {
						t.Errorf("a complete listing still handed out the token %q", token)
					}
					return
				}
				in.ContinuationToken = page.NextContinuationToken
			}
		})
	}
}

// Paging a version listing, which is how a client empties a bucket: it feeds each
// response's markers into the next request. A marker the server leaves out arrives
// as a null and the loop stops on the second page with objects still in the bucket,
// which is exactly what Ceph's suite reported as fourteen errors.
func TestPagingAVersionListingReachesEveryObject(t *testing.T) {
	tests := []struct {
		name      string
		keys      []string
		prefix    string
		delimiter string
		encode    bool
		// maxKeys defaults to 1, so that every page but the last is truncated and
		// the markers are exercised on all of them. The reordering case needs 2:
		// the frontier is only ambiguous on a page holding both a key and a group.
		maxKeys int32
		// want is what a client must see across every page, in order.
		want []string
	}{
		{name: "flat keys", keys: []string{"a", "b", "c", "d", "e"},
			want: []string{"a", "b", "c", "d", "e"}},
		{name: "grouped, so a marker names a group",
			keys: []string{"x/1", "x/2", "y/1", "z/1"}, delimiter: "/",
			want: []string{"x/", "y/", "z/"}},
		// An object whose key is the listing's prefix ends in the delimiter without
		// being a group, and treating it as one steps over everything underneath it.
		{name: "a key that is the prefix itself",
			keys: []string{"dir/", "dir/1", "dir/2"}, prefix: "dir/", delimiter: "/",
			want: []string{"dir/", "dir/1", "dir/2"}},
		// Escaping maps reserved bytes to "%", which sorts below the characters it
		// leaves alone, so a frontier chosen from encoded keys can go backwards and
		// report a key twice.
		{name: "keys that reorder when escaped",
			keys: []string{"a0/1", "a:", "b"}, delimiter: "/", encode: true, maxKeys: 2,
			want: []string{"a0/", "a%3A", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newGateway(t)
			for _, k := range tt.keys {
				put(t, client, "paged", k, []byte("x"))
			}

			maxKeys := tt.maxKeys
			if maxKeys == 0 {
				maxKeys = 1
			}
			in := &awss3.ListObjectVersionsInput{
				Bucket:  aws.String("paged"),
				MaxKeys: aws.Int32(maxKeys),
			}
			if tt.delimiter != "" {
				in.Delimiter = aws.String(tt.delimiter)
			}
			if tt.prefix != "" {
				in.Prefix = aws.String(tt.prefix)
			}
			if tt.encode {
				in.EncodingType = types.EncodingTypeUrl
			}
			var seen []string
			for pages := 0; ; pages++ {
				if pages > 2*len(tt.keys) {
					t.Fatalf("the listing never completed; saw %v", seen)
				}
				out, err := client.ListObjectVersions(t.Context(), in)
				if err != nil {
					t.Fatalf("ListObjectVersions: %v", err)
				}
				for _, v := range out.Versions {
					seen = append(seen, aws.ToString(v.Key))
				}
				for _, p := range out.CommonPrefixes {
					seen = append(seen, aws.ToString(p.Prefix))
				}
				if !aws.ToBool(out.IsTruncated) {
					break
				}
				// A truncated page must hand back both markers, or a client
				// cannot ask for the next one at all.
				if aws.ToString(out.NextKeyMarker) == "" {
					t.Fatalf("a truncated page carries no key marker; saw %v", seen)
				}
				if aws.ToString(out.NextVersionIdMarker) == "" {
					t.Fatal("a truncated page carries no version id marker")
				}
				in.KeyMarker = out.NextKeyMarker
				in.VersionIdMarker = out.NextVersionIdMarker
			}

			// Sorted, because keys and grouped prefixes arrive in two lists and
			// which one this loop drains first is not the server's business. What
			// is: every key appears once. A lost key means the paging stopped
			// early, a repeated one means a marker went backwards.
			if got := slices.Sorted(slices.Values(seen)); !slices.Equal(got, slices.Sorted(slices.Values(tt.want))) {
				t.Errorf("paging saw %v, want each of %v exactly once", seen, tt.want)
			}
		})
	}
}

// What an encoded listing echoes back, and who owns what.
//
// Both are answers clients read rather than data they store, and both were wrong in
// a way no round-trip test could see: the delimiter came back as "%2F" because
// escaping did not spare "/", and fetch-owner returned nothing, which a client
// reads as an object with no owner.
func TestAnEncodedListingEchoesWhatTheClientSent(t *testing.T) {
	client := newGateway(t)
	put(t, client, "bucket", "dir/one.txt", []byte("x"))

	page, err := client.ListObjectsV2(t.Context(), &awss3.ListObjectsV2Input{
		Bucket:       aws.String("bucket"),
		Delimiter:    aws.String("/"),
		EncodingType: types.EncodingTypeUrl,
		FetchOwner:   aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := aws.ToString(page.Delimiter); got != "/" {
		t.Errorf("the delimiter came back as %q, want the %q that was sent", got, "/")
	}
	if got := aws.ToString(page.CommonPrefixes[0].Prefix); got != "dir/" {
		t.Errorf("the grouped prefix came back as %q, want %q", got, "dir/")
	}

	flat, err := client.ListObjectsV2(t.Context(), &awss3.ListObjectsV2Input{
		Bucket:     aws.String("bucket"),
		FetchOwner: aws.Bool(true),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if flat.Contents[0].Owner == nil {
		t.Fatal("fetch-owner=true returned an object with no owner")
	}
	if got := aws.ToString(flat.Contents[0].Owner.ID); got != creds.AccessKey {
		t.Errorf("owner is %q, want the one key pair there is (%q)", got, creds.AccessKey)
	}

	// Not asked for, not sent: S3 omits the owner unless fetch-owner says otherwise.
	quiet, err := client.ListObjectsV2(t.Context(), &awss3.ListObjectsV2Input{Bucket: aws.String("bucket")})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if quiet.Contents[0].Owner != nil {
		t.Error("an owner came back for a listing that did not ask for one")
	}
}

// An explicitly empty continuation token is still an explicit value and must be
// echoed as such. Omitting it turns an empty token into "not provided", which is
// what Ceph's test catches as a missing field.
func TestEmptyContinuationTokenIsEchoed(t *testing.T) {
	client := newGateway(t)
	put(t, client, "bucket", "a.txt", []byte("x"))

	page, err := client.ListObjectsV2(t.Context(), &awss3.ListObjectsV2Input{
		Bucket:            aws.String("bucket"),
		ContinuationToken: aws.String(""),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if page.ContinuationToken == nil {
		t.Fatal("an explicit empty continuation token was omitted")
	}
	if got := aws.ToString(page.ContinuationToken); got != "" {
		t.Errorf("ContinuationToken = %q, want empty string", got)
	}
}

// Ceph's allow-unordered extension is a v1 ListObjects parameter. kavo does not
// implement v1, but this invalid parameter combination still has to answer the
// status/code clients see on S3.
func TestListObjectsV1AllowUnorderedWithDelimiterIsInvalidArgument(t *testing.T) {
	_, endpoint := newGatewayURL(t, 3, testChunkSize)
	req, err := http.NewRequest(http.MethodGet, endpoint+"/bucket?allow-unordered=true&delimiter=%2F", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	c, err := credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, "").Retrieve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	if err := signer.SignHTTP(t.Context(), c, req, "UNSIGNED-PAYLOAD", "s3", "us-east-1", time.Now()); err != nil {
		t.Fatalf("sign: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d: %s, want 400", resp.StatusCode, body)
	}
}
