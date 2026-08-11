package s3_test

// Listing, driven through the SDK — including its paginator, since paging is the
// part of a listing that goes wrong. A listing that silently stops early looks
// exactly like a bucket with fewer objects in it.

import (
	"bytes"
	"fmt"
	"net/url"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
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
