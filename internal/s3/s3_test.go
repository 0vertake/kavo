package s3_test

// The client in these tests is the AWS SDK's own S3 client, pointed at a real
// three-node cluster over real HTTP. That is the whole point: a hand-rolled
// request proves the server agrees with this author's reading of S3, where the SDK
// signing, framing and parsing its own way is the thing that catches a misreading.
// Every failure here is a failure a user would have hit with `aws s3`.

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/0vertake/kavo/internal/api"
	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/s3"
	"github.com/0vertake/kavo/internal/sigv4"
	"github.com/0vertake/kavo/internal/store"
)

// Small enough that objects span several chunks, so a range that crosses a chunk
// boundary is the normal case rather than a special one.
const testChunkSize = 64 << 10

var creds = sigv4.Credentials{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}

// newGateway starts a cluster of three nodes and returns an SDK client pointed at
// the S3 port of one of them. Three because a write is acknowledged at W=2 of
// N=3: a smaller cluster cannot accept an object at all.
func newGateway(t testing.TB) *awss3.Client {
	t.Helper()
	client, _ := newGatewayURL(t, 3, testChunkSize)
	return client
}

// newGatewaySized is newGateway with the cluster size and chunk size the caller
// needs. Benchmarks want six nodes at the production chunk size, so that their
// numbers can be put next to the ones the internal API produces.
func newGatewaySized(t testing.TB, n int, chunkSize int64) *awss3.Client {
	t.Helper()
	client, _ := newGatewayURL(t, n, chunkSize)
	return client
}

func newGatewayURL(t testing.TB, n int, chunkSize int64) (*awss3.Client, string) {
	t.Helper()
	prefix := "/kavo-test/" + rand.Text()

	srvs := make([]*httptest.Server, n)
	peers := make(map[string]string, n)
	ids := make([]string, n)
	for i := range srvs {
		srvs[i] = httptest.NewUnstartedServer(nil)
		ids[i] = fmt.Sprintf("n%d", i+1)
		peers[ids[i]] = srvs[i].Listener.Addr().String()
	}

	var gateway http.Handler
	for i, id := range ids {
		st, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		m, err := meta.Open([]string{meta.EndpointFromEnv()}, prefix)
		if err != nil {
			t.Fatalf("meta.Open (is etcd up? try `make etcd`): %v", err)
		}
		t.Cleanup(func() { m.Close() })

		c := cluster.New(id, peers[id], st, m, chunkSize)
		c.SetMembers(peers)
		srvs[i].Config.Handler = api.New(c, st)
		srvs[i].Start()
		t.Cleanup(srvs[i].Close)
		if i == 0 {
			gateway = s3.New(c, creds)
		}
	}

	// The S3 port is separate from the one peers use, as it is in the daemon.
	front := httptest.NewServer(gateway)
	t.Cleanup(front.Close)

	return awss3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, ""),
	}, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(front.URL)
		o.UsePathStyle = true // there is no DNS for buckets here
	}), front.URL
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

// The base claim: what the SDK puts is what the SDK gets, for objects on both
// sides of a chunk boundary and for the empty object nobody remembers to test.
func TestObjectsRoundTripThroughTheSDK(t *testing.T) {
	tests := []struct {
		name string
		key  string
		size int
	}{
		{name: "empty object", key: "empty", size: 0},
		{name: "one byte", key: "tiny", size: 1},
		{name: "smaller than a chunk", key: "small.bin", size: testChunkSize / 3},
		{name: "exactly one chunk", key: "one-chunk.bin", size: testChunkSize},
		{name: "several chunks and a remainder", key: "big.bin", size: 3*testChunkSize + 17},
		{name: "key with spaces and slashes", key: "some dir/two words.txt", size: 1024},
		{name: "key with a plus", key: "a+b.txt", size: 1024},
		{name: "key with unicode", key: "☕/café.txt", size: 1024},
	}
	client := newGateway(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := randBytes(tt.size)
			put, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
				Bucket: aws.String("bucket"),
				Key:    aws.String(tt.key),
				Body:   bytes.NewReader(data),
			})
			if err != nil {
				t.Fatalf("put: %v", err)
			}

			get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
				Bucket: aws.String("bucket"),
				Key:    aws.String(tt.key),
			})
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer get.Body.Close()
			got, err := io.ReadAll(get.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Errorf("read %d bytes, want the %d written", len(got), len(data))
			}
			// The ETag is how a client checks its upload arrived intact, so it
			// has to be the MD5 of the bytes and to survive the round trip.
			if aws.ToString(get.ETag) != aws.ToString(put.ETag) {
				t.Errorf("get says ETag %s, put said %s", aws.ToString(get.ETag), aws.ToString(put.ETag))
			}
			if want := fmt.Sprintf("%x", md5sum(data)); aws.ToString(put.ETag) != `"`+want+`"` {
				t.Errorf("ETag = %s, want the body's MD5 %q", aws.ToString(put.ETag), want)
			}
			if get.ContentLength == nil || *get.ContentLength != int64(len(data)) {
				t.Errorf("Content-Length = %v, want %d", get.ContentLength, len(data))
			}
		})
	}
}

// HEAD is what a client uses to decide how to download something, so it has to
// carry the same length, ETag and modification time a GET does — and no body,
// which is easy to get wrong when the error path shares code with the success one.
func TestHeadMatchesGetWithoutABody(t *testing.T) {
	client := newGateway(t)
	data := randBytes(2*testChunkSize + 5)
	put, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket:      aws.String("bucket"),
		Key:         aws.String("described.bin"),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/x-kavo-test"),
	})
	if err != nil {
		t.Fatal(err)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("described.bin"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(data)) {
		t.Errorf("Content-Length = %v, want %d", head.ContentLength, len(data))
	}
	if aws.ToString(head.ETag) != aws.ToString(put.ETag) {
		t.Errorf("ETag = %s, want %s", aws.ToString(head.ETag), aws.ToString(put.ETag))
	}
	if aws.ToString(head.ContentType) != "application/x-kavo-test" {
		t.Errorf("Content-Type = %q, want the one the client set", aws.ToString(head.ContentType))
	}
	if head.LastModified == nil || head.LastModified.IsZero() {
		t.Error("no Last-Modified; clients use it to decide whether to re-download")
	}
	if head.AcceptRanges == nil || *head.AcceptRanges != "bytes" {
		t.Errorf("Accept-Ranges = %v, want bytes, or clients will not use ranged GETs", head.AcceptRanges)
	}
}

// Ranged GETs are not optional: `aws s3 cp` downloads anything over 8 MB as
// parallel ranges, so an object store without them cannot serve a large object to
// the standard client at all.
func TestRangedReads(t *testing.T) {
	client := newGateway(t)
	data := randBytes(3 * testChunkSize)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("ranged.bin"),
		Body:   bytes.NewReader(data),
	}); err != nil {
		t.Fatal(err)
	}

	size := int64(len(data))
	tests := []struct {
		name       string
		header     string
		wantOffset int64
		wantLen    int64
	}{
		{name: "a whole chunk", header: fmt.Sprintf("bytes=0-%d", testChunkSize-1), wantOffset: 0, wantLen: testChunkSize},
		{name: "inside one chunk", header: "bytes=100-199", wantOffset: 100, wantLen: 100},
		{name: "across a chunk boundary", header: fmt.Sprintf("bytes=%d-%d", testChunkSize-10, testChunkSize+9), wantOffset: testChunkSize - 10, wantLen: 20},
		{name: "spanning three chunks", header: fmt.Sprintf("bytes=1-%d", 2*testChunkSize), wantOffset: 1, wantLen: 2 * testChunkSize},
		{name: "open ended, as the CLI sends", header: fmt.Sprintf("bytes=%d-", 2*testChunkSize), wantOffset: 2 * testChunkSize, wantLen: testChunkSize},
		{name: "the last bytes", header: "bytes=-500", wantOffset: size - 500, wantLen: 500},
		{name: "one byte", header: "bytes=7-7", wantOffset: 7, wantLen: 1},
		{name: "past the end is clipped", header: fmt.Sprintf("bytes=%d-%d", size-10, size+1000), wantOffset: size - 10, wantLen: 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
				Bucket: aws.String("bucket"),
				Key:    aws.String("ranged.bin"),
				Range:  aws.String(tt.header),
			})
			if err != nil {
				t.Fatalf("get %s: %v", tt.header, err)
			}
			defer get.Body.Close()
			got, err := io.ReadAll(get.Body)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			want := data[tt.wantOffset : tt.wantOffset+tt.wantLen]
			if !bytes.Equal(got, want) {
				t.Errorf("%s returned %d bytes, want %d starting at %d", tt.header, len(got), tt.wantLen, tt.wantOffset)
			}
			// The client trusts Content-Range to place the bytes in the file it
			// is assembling. Getting it wrong scrambles a parallel download.
			wantRange := fmt.Sprintf("bytes %d-%d/%d", tt.wantOffset, tt.wantOffset+tt.wantLen-1, size)
			if aws.ToString(get.ContentRange) != wantRange {
				t.Errorf("Content-Range = %q, want %q", aws.ToString(get.ContentRange), wantRange)
			}
		})
	}
}

// A range outside the object must be refused rather than silently clipped to
// nothing: a client that asked for bytes that do not exist has a wrong idea of the
// object's size, and an empty 200 would let it write a corrupt file.
func TestUnsatisfiableRangeIsRefused(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("short.bin"),
		Body:   bytes.NewReader(randBytes(100)),
	}); err != nil {
		t.Fatal(err)
	}
	for _, header := range []string{"bytes=100-200", "bytes=500-", "bytes=200-100"} {
		_, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("short.bin"),
			Range:  aws.String(header),
		})
		if status := httpStatus(err); status != http.StatusRequestedRangeNotSatisfiable {
			t.Errorf("%s = status %d (%v), want 416", header, status, err)
		}
	}
}

// A deleted object must be gone from the API, and deleting again must succeed:
// cleanup loops and `aws s3 rm --recursive` both delete keys they cannot be sure
// about, and a 404 from a delete turns that into an error to handle.
func TestDeleteRemovesTheObjectAndIsIdempotent(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("doomed.bin"),
		Body:   bytes.NewReader(randBytes(2 * testChunkSize)),
	}); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := client.DeleteObject(t.Context(), &awss3.DeleteObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("doomed.bin"),
		}); err != nil {
			t.Fatalf("delete %d: %v", i+1, err)
		}
	}

	_, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("doomed.bin"),
	})
	var missing *types.NoSuchKey
	if !errors.As(err, &missing) {
		t.Errorf("get after delete = %v, want NoSuchKey", err)
	}
}

// A server-side copy is what `aws s3 mv` and `aws s3 cp` between two keys do, and
// the SDK sends it as a PUT with a header naming the source. No byte of the object
// crosses the network in either direction.
func TestCopyingAnObjectServerSide(t *testing.T) {
	client := newGateway(t)
	body := randBytes(2 * testChunkSize)
	put, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"), Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Across buckets as well as keys, since a bucket here is a prefix and a copy
	// that only worked within one would be a surprise.
	copied, err := client.CopyObject(t.Context(), &awss3.CopyObjectInput{
		Bucket: aws.String("archive"), Key: aws.String("kept.bin"),
		CopySource: aws.String("bucket/source.bin"),
	})
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if copied.CopyObjectResult == nil || aws.ToString(copied.CopyObjectResult.ETag) != aws.ToString(put.ETag) {
		t.Errorf("the copy result carries etag %v, want the source's %v", copied.CopyObjectResult, aws.ToString(put.ETag))
	}

	out, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("archive"), Key: aws.String("kept.bin"),
	})
	if err != nil {
		t.Fatalf("get the copy: %v", err)
	}
	defer out.Body.Close()
	got, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("the copy is %d bytes of something else, want the %d written", len(got), len(body))
	}

	// And the source is still there, which is the difference between a copy and a
	// move: the client issues the delete itself, or not at all.
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"),
	}); err != nil {
		t.Errorf("the source is gone after being copied: %v", err)
	}
}

// What a copy has to refuse. A source that does not exist is NoSuchKey, because a
// client that mistyped a key must not be told the copy worked; a copy onto itself is
// InvalidRequest, since the only thing it could mean is a metadata rewrite and there
// is no metadata here to rewrite.
func TestCopiesThatMustBeRefused(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("real.bin"),
		Body: bytes.NewReader(randBytes(testChunkSize)),
	}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		source string
		bucket string
		key    string
		want   string
	}{
		{name: "source does not exist", source: "bucket/imaginary.bin",
			bucket: "bucket", key: "copy.bin", want: "NoSuchKey"},
		{name: "onto itself", source: "bucket/real.bin",
			bucket: "bucket", key: "real.bin", want: "InvalidRequest"},
		{name: "source names no key", source: "bucket",
			bucket: "bucket", key: "copy.bin", want: "InvalidArgument"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.CopyObject(t.Context(), &awss3.CopyObjectInput{
				Bucket: aws.String(tt.bucket), Key: aws.String(tt.key),
				CopySource: aws.String(tt.source),
			})
			var api smithy.APIError
			if !errors.As(err, &api) {
				t.Fatalf("copy = %v, want an S3 error code", err)
			}
			if api.ErrorCode() != tt.want {
				t.Errorf("copy = %s, want %s", api.ErrorCode(), tt.want)
			}
		})
	}
}

// A missing object must come back as NoSuchKey, not as a generic failure: the SDK
// turns that code into a typed error applications branch on, and anything else
// looks like an outage.
func TestMissingObjectIsNoSuchKey(t *testing.T) {
	client := newGateway(t)
	_, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("never/written"),
	})
	var missing *types.NoSuchKey
	if !errors.As(err, &missing) {
		t.Fatalf("get = %v, want the SDK to parse NoSuchKey out of it", err)
	}

	// HEAD carries the status and no body, and the SDK still has to see 404.
	_, err = client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("never/written"),
	})
	if status := httpStatus(err); status != http.StatusNotFound {
		t.Errorf("head = status %d (%v), want 404", status, err)
	}
}

// Buckets are prefixes, so a bucket exists as soon as it is named. Clients check
// before uploading, and "no such bucket" would stop them from writing anything.
func TestABucketExistsAsSoonAsItIsNamed(t *testing.T) {
	client := newGateway(t)
	if _, err := client.HeadBucket(t.Context(), &awss3.HeadBucketInput{Bucket: aws.String("never-created")}); err != nil {
		t.Fatalf("head bucket: %v", err)
	}
}

// Two buckets are two namespaces. Sharing one would let a key in one bucket
// overwrite a key in another, which is data loss dressed up as a naming quirk.
func TestBucketsDoNotShareKeys(t *testing.T) {
	client := newGateway(t)
	first, second := []byte("first bucket's bytes"), []byte("second bucket's bytes")
	for bucket, data := range map[string][]byte{"one": first, "two": second} {
		if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("same/key"),
			Body:   bytes.NewReader(data),
		}); err != nil {
			t.Fatal(err)
		}
	}
	for bucket, want := range map[string][]byte{"one": first, "two": second} {
		get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("same/key"),
		})
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(get.Body)
		get.Body.Close()
		if !bytes.Equal(got, want) {
			t.Errorf("bucket %s holds %q, want %q", bucket, got, want)
		}
	}
}

// An unsigned or wrongly signed request must be refused with the code that says
// which it was. A store that answers unsigned requests is a store anyone who can
// reach the port can empty.
func TestUnauthenticatedRequestsAreRefused(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("guarded"),
		Body:   bytes.NewReader([]byte("x")),
	}); err != nil {
		t.Fatal(err)
	}
	endpoint := *client.Options().BaseEndpoint

	tests := []struct {
		name string
		auth string
		want string
	}{
		{name: "no signature at all", auth: "", want: "AccessDenied"},
		{name: "not a signature", auth: "Bearer hunter2", want: "AuthorizationHeaderMalformed"},
		{
			name: "an unknown access key",
			auth: "AWS4-HMAC-SHA256 Credential=NOTAKEY/20260811/us-east-1/s3/aws4_request, " +
				"SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" + strings.Repeat("0", 64),
			want: "InvalidAccessKeyId",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, endpoint+"/bucket/guarded", nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
				req.Header.Set("x-amz-date", "20260811T120000Z")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode == http.StatusOK {
				t.Fatalf("an unsigned request was served: %q", body)
			}
			// The code is the part a client acts on, so it has to be the right
			// one rather than merely a failure.
			if !strings.Contains(string(body), "<Code>"+tt.want+"</Code>") {
				t.Errorf("status %d, body %s; want code %s", resp.StatusCode, body, tt.want)
			}
		})
	}
}

// A signed request still has to name when it was signed. S3 answers
// MissingSecurityHeader for that, not AccessDenied: the client held the key and
// the signature may even match, it just omitted a required header.
func TestMissingDateHeaderIsRefused(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("guarded"),
		Body:   bytes.NewReader([]byte("x")),
	}); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, *client.Options().BaseEndpoint+"/bucket/guarded", nil)
	if err != nil {
		t.Fatal(err)
	}
	signS3(t, req, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Del("X-Amz-Date")
	req.Header.Del("Date")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("a request without a date was served: %q", body)
	}
	if !strings.Contains(string(body), "<Code>MissingSecurityHeader</Code>") {
		t.Errorf("status %d, body %s; want MissingSecurityHeader", resp.StatusCode, body)
	}
}

// An overwrite must replace the object rather than blend with it: the manifest is
// the object, so the new one has to name only the new bytes.
func TestOverwriteReplacesTheObject(t *testing.T) {
	client := newGateway(t)
	long, short := randBytes(3*testChunkSize), randBytes(100)
	for _, data := range [][]byte{long, short} {
		if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("overwritten"),
			Body:   bytes.NewReader(data),
		}); err != nil {
			t.Fatal(err)
		}
	}
	get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("overwritten"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer get.Body.Close()
	got, _ := io.ReadAll(get.Body)
	if !bytes.Equal(got, short) {
		t.Errorf("read %d bytes, want the %d of the second write", len(got), len(short))
	}
}

// httpStatus digs the HTTP status out of an SDK error, which is the only way to
// check a status the SDK does not model as a typed error.
func httpStatus(err error) int {
	var resp interface{ HTTPStatusCode() int }
	var api smithy.APIError
	if errors.As(err, &resp) {
		return resp.HTTPStatusCode()
	}
	if errors.As(err, &api) {
		return -1
	}
	return 0
}

func md5sum(b []byte) []byte {
	h := md5.New()
	h.Write(b)
	return h.Sum(nil)
}

// Conditional reads are how a client avoids paying for bytes it already has, and
// `aws s3 sync` asks one of every file it considers. Answered from the committed
// manifest, so the cost is an etcd read rather than the object.
func TestConditionalReads(t *testing.T) {
	client := newGateway(t)
	data := randBytes(3 * testChunkSize)
	put, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("conditional.bin"),
		Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	tag := aws.ToString(put.ETag)

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("conditional.bin"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	stored := aws.ToTime(head.LastModified)

	tests := []struct {
		name  string
		in    awss3.GetObjectInput
		want  int
		bytes bool
	}{
		{name: "the tag the client holds is current", want: http.StatusNotModified,
			in: awss3.GetObjectInput{IfNoneMatch: aws.String(tag)}},
		{name: "the tag the client holds is stale", want: http.StatusOK, bytes: true,
			in: awss3.GetObjectInput{IfNoneMatch: aws.String(`"0000"`)}},
		{name: "the object is the one the client expects", want: http.StatusOK, bytes: true,
			in: awss3.GetObjectInput{IfMatch: aws.String(tag)}},
		{name: "the object is not the one the client expects", want: http.StatusPreconditionFailed,
			in: awss3.GetObjectInput{IfMatch: aws.String(`"0000"`)}},
		{name: "unchanged since the client last saw it", want: http.StatusNotModified,
			in: awss3.GetObjectInput{IfModifiedSince: aws.Time(stored)}},
		{name: "changed since the client last saw it", want: http.StatusOK, bytes: true,
			in: awss3.GetObjectInput{IfModifiedSince: aws.Time(stored.Add(-time.Hour))}},
		{name: "unmodified since a moment after it was stored", want: http.StatusOK, bytes: true,
			in: awss3.GetObjectInput{IfUnmodifiedSince: aws.Time(stored.Add(time.Hour))}},
		{name: "modified since the time the client demands", want: http.StatusPreconditionFailed,
			in: awss3.GetObjectInput{IfUnmodifiedSince: aws.Time(stored.Add(-time.Hour))}},
		// An entity tag is a better answer than a date, so it decides even when
		// the date alone would have said otherwise. Both directions, because
		// getting the precedence backwards passes one of them by luck.
		{name: "a current tag outranks a date that says changed", want: http.StatusNotModified,
			in: awss3.GetObjectInput{IfNoneMatch: aws.String(tag), IfModifiedSince: aws.Time(stored.Add(-time.Hour))}},
		{name: "a matching tag outranks a date that says modified", want: http.StatusOK, bytes: true,
			in: awss3.GetObjectInput{IfMatch: aws.String(tag), IfUnmodifiedSince: aws.Time(stored.Add(-time.Hour))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			in.Bucket, in.Key = aws.String("bucket"), aws.String("conditional.bin")
			out, err := client.GetObject(t.Context(), &in)
			if tt.want != http.StatusOK {
				if got := httpStatus(err); got != tt.want {
					t.Fatalf("get = %v (status %d), want status %d", err, got, tt.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer out.Body.Close()
			got, err := io.ReadAll(out.Body)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Errorf("read %d bytes, want the %d written", len(got), len(data))
			}
		})
	}
}

// A client that declares what it is sending is asking to be contradicted, and the
// only useful answer is a refusal: an object stored under a digest it does not have
// is corruption the client has been told is fine.
//
// The refusal has to be complete, which is the assertion below that matters. The
// digest covers the whole body, so nothing can be compared until the last byte has
// arrived and the chunks are already on disk — and if the manifest were committed
// anyway, the client would be left deleting an object it was told had failed.
func TestAWriteWhoseDigestDoesNotMatchIsRefusedEntirely(t *testing.T) {
	client := newGateway(t)
	data := randBytes(2 * testChunkSize)

	_, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("misdeclared.bin"),
		Body:       bytes.NewReader(data),
		ContentMD5: aws.String(base64.StdEncoding.EncodeToString(md5sum(randBytes(16)))),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Fatalf("put = %v, want BadDigest", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("misdeclared.bin"),
	}); httpStatus(err) != http.StatusNotFound {
		t.Errorf("the refused write left an object behind: %v", err)
	}

	// And a digest that is not a digest is a different mistake: one the client
	// should fix rather than retry.
	_, err = client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("malformed.bin"),
		Body:       bytes.NewReader(data),
		ContentMD5: aws.String("not base64 at all"),
	})
	if !errors.As(err, &api) || api.ErrorCode() != "InvalidDigest" {
		t.Fatalf("put with a malformed digest = %v, want InvalidDigest", err)
	}
}

// The other half of the same promise: a client that declares the right digest is
// not made to care that it did.
func TestAWriteWhoseDigestMatchesIsStored(t *testing.T) {
	client := newGateway(t)
	data := randBytes(2 * testChunkSize)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("declared.bin"),
		Body:       bytes.NewReader(data),
		ContentMD5: aws.String(base64.StdEncoding.EncodeToString(md5sum(data))),
	}); err != nil {
		t.Fatalf("put with a matching digest: %v", err)
	}
	out, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("declared.bin"),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer out.Body.Close()
	got, _ := io.ReadAll(out.Body)
	if !bytes.Equal(got, data) {
		t.Errorf("read %d bytes, want the %d written", len(got), len(data))
	}
}

// A copy takes its conditions on the source, not on the destination, which keeps
// them read-side: they are a question about the manifest being copied and need
// nothing from the commit. `aws s3 sync` between two buckets sends them.
//
// The answer differs from a read's in one way, and it is the whole reason this is
// tested separately: a copy has no "you already have it" outcome, so a condition
// that does not hold is 412 even where the same condition on a GET would have been
// answered 304.
func TestConditionalCopies(t *testing.T) {
	client := newGateway(t)
	data := randBytes(2 * testChunkSize)
	put, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"),
		Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	tag := aws.ToString(put.ETag)

	tests := []struct {
		name string
		in   awss3.CopyObjectInput
		want int
	}{
		{name: "the source is the one the client expects", want: http.StatusOK,
			in: awss3.CopyObjectInput{CopySourceIfMatch: aws.String(tag)}},
		{name: "the source is not the one the client expects", want: http.StatusPreconditionFailed,
			in: awss3.CopyObjectInput{CopySourceIfMatch: aws.String(`"0000"`)}},
		{name: "the client's copy of the source is stale", want: http.StatusOK,
			in: awss3.CopyObjectInput{CopySourceIfNoneMatch: aws.String(`"0000"`)}},
		{name: "the client already has this source", want: http.StatusPreconditionFailed,
			in: awss3.CopyObjectInput{CopySourceIfNoneMatch: aws.String(tag)}},
		{name: "the source is older than the client demands", want: http.StatusPreconditionFailed,
			in: awss3.CopyObjectInput{CopySourceIfModifiedSince: aws.Time(time.Now().Add(time.Hour))}},
		{name: "the source is older than the client requires", want: http.StatusOK,
			in: awss3.CopyObjectInput{CopySourceIfUnmodifiedSince: aws.Time(time.Now().Add(time.Hour))}},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			in.Bucket, in.Key = aws.String("bucket"), aws.String(fmt.Sprintf("copy%d.bin", i))
			in.CopySource = aws.String("bucket/source.bin")
			_, err := client.CopyObject(t.Context(), &in)
			if tt.want != http.StatusOK {
				if got := httpStatus(err); got != tt.want {
					t.Fatalf("copy = %v (status %d), want status %d", err, got, tt.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("copy: %v", err)
			}
			out, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
				Bucket: in.Bucket, Key: in.Key,
			})
			if err != nil {
				t.Fatalf("get the copy: %v", err)
			}
			defer out.Body.Close()
			got, _ := io.ReadAll(out.Body)
			if !bytes.Equal(got, data) {
				t.Errorf("the copy is %d bytes of something else, want the %d copied", len(got), len(data))
			}
		})
	}
}

// An empty Content-MD5 is not a missing one: the client said it was declaring a
// digest and then declared nothing. S3 calls that InvalidDigest, and treating it as
// "no digest was sent" would store an object under a promise nobody made.
func TestAnEmptyDeclaredDigestIsRefused(t *testing.T) {
	client := newGateway(t)
	_, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("empty-digest.bin"),
		Body:       bytes.NewReader(randBytes(64)),
		ContentMD5: aws.String(""),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "InvalidDigest" {
		t.Fatalf("put with an empty digest = %v, want InvalidDigest", err)
	}
}

// Metadata is stored and replayed rather than interpreted. The x-amz-meta-* are the
// client's own, and the five standard headers here describe the bytes rather than
// the exchange that delivered them — which is what makes them the object's to keep.
func TestMetadataIsStoredAndReplayed(t *testing.T) {
	client := newGateway(t)
	in := &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
		Body:               bytes.NewReader(randBytes(64)),
		Metadata:           map[string]string{"colour": "octarine", "unicode": "Hello Wörld"},
		CacheControl:       aws.String("max-age=31536000, immutable"),
		ContentDisposition: aws.String(`attachment; filename="report.pdf"`),
		ContentLanguage:    aws.String("en-GB"),
		ContentType:        aws.String("application/pdf"),
	}
	if _, err := client.PutObject(t.Context(), in); err != nil {
		t.Fatalf("put: %v", err)
	}

	// On the read and on the HEAD, because a client that asks only for the
	// headers is the one that cares most about them.
	out, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: in.Bucket, Key: in.Key,
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	out.Body.Close()
	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: in.Bucket, Key: in.Key,
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	for _, got := range []struct {
		what               string
		meta               map[string]string
		cache, disposition *string
		language, kind     *string
	}{
		{"get", out.Metadata, out.CacheControl, out.ContentDisposition, out.ContentLanguage, out.ContentType},
		{"head", head.Metadata, head.CacheControl, head.ContentDisposition, head.ContentLanguage, head.ContentType},
	} {
		if got.meta["colour"] != "octarine" || got.meta["unicode"] != "Hello Wörld" {
			t.Errorf("%s metadata = %v, want the two that were sent", got.what, got.meta)
		}
		if aws.ToString(got.cache) != aws.ToString(in.CacheControl) {
			t.Errorf("%s cache-control = %q, want %q", got.what, aws.ToString(got.cache), aws.ToString(in.CacheControl))
		}
		if aws.ToString(got.disposition) != aws.ToString(in.ContentDisposition) {
			t.Errorf("%s content-disposition = %q, want %q", got.what, aws.ToString(got.disposition), aws.ToString(in.ContentDisposition))
		}
		if aws.ToString(got.language) != "en-GB" || aws.ToString(got.kind) != "application/pdf" {
			t.Errorf("%s content-language/type = %q/%q, want en-GB/application/pdf",
				got.what, aws.ToString(got.language), aws.ToString(got.kind))
		}
	}
}

// aws-chunked describes the framing of the body that arrived, and kavo decoded it.
// Storing it would tell every later reader that the object's bytes are chunk-framed,
// which they are not — so it is dropped, and an encoding that was nothing else is
// not stored at all.
func TestAWSChunkedIsNotStoredAsTheObjectsEncoding(t *testing.T) {
	client := newGateway(t)
	tests := []struct{ sent, want string }{
		{"gzip", "gzip"},
		{"deflate, gzip", "deflate, gzip"},
		{"gzip, aws-chunked", "gzip"},
		{"aws-chunked, gzip", "gzip"},
		{"aws-chunked", ""},
		{"aws-chunked, aws-chunked", ""},
	}
	for i, tt := range tests {
		key := fmt.Sprintf("encoded%d.bin", i)
		_, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
			Bucket: aws.String("bucket"), Key: aws.String(key),
			Body:            bytes.NewReader(randBytes(32)),
			ContentEncoding: aws.String(tt.sent),
		})
		if err != nil {
			t.Fatalf("put with %q: %v", tt.sent, err)
		}
		head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
			Bucket: aws.String("bucket"), Key: aws.String(key),
		})
		if err != nil {
			t.Fatalf("head: %v", err)
		}
		if got := aws.ToString(head.ContentEncoding); got != tt.want {
			t.Errorf("stored %q as content-encoding %q, want %q", tt.sent, got, tt.want)
		}
	}
}

// A copy keeps the source's metadata unless the client says REPLACE, and REPLACE
// with no metadata headers means the copy has none — the only way a client can strip
// metadata from an object, since there is nothing else it could edit in place.
func TestACopyKeepsOrReplacesMetadata(t *testing.T) {
	client := newGateway(t)
	source := &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"),
		Body:        bytes.NewReader(randBytes(64)),
		Metadata:    map[string]string{"colour": "octarine"},
		ContentType: aws.String("application/pdf"),
	}
	if _, err := client.PutObject(t.Context(), source); err != nil {
		t.Fatalf("put: %v", err)
	}

	tests := []struct {
		name     string
		in       awss3.CopyObjectInput
		want     map[string]string
		wantKind string
	}{
		{name: "no directive keeps the source's", want: map[string]string{"colour": "octarine"},
			wantKind: "application/pdf"},
		{name: "COPY keeps the source's", want: map[string]string{"colour": "octarine"},
			wantKind: "application/pdf",
			in:       awss3.CopyObjectInput{MetadataDirective: "COPY"}},
		{name: "REPLACE takes the request's", want: map[string]string{"colour": "chartreuse"},
			wantKind: "text/plain",
			in: awss3.CopyObjectInput{MetadataDirective: "REPLACE",
				Metadata: map[string]string{"colour": "chartreuse"}, ContentType: aws.String("text/plain")}},
		{name: "REPLACE with nothing strips it", want: map[string]string{},
			in: awss3.CopyObjectInput{MetadataDirective: "REPLACE"}},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			in.Bucket, in.Key = aws.String("bucket"), aws.String(fmt.Sprintf("copy%d.bin", i))
			in.CopySource = aws.String("bucket/source.bin")
			if _, err := client.CopyObject(t.Context(), &in); err != nil {
				t.Fatalf("copy: %v", err)
			}
			head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
				Bucket: in.Bucket, Key: in.Key,
			})
			if err != nil {
				t.Fatalf("head the copy: %v", err)
			}
			if len(head.Metadata) != len(tt.want) {
				t.Fatalf("copy metadata = %v, want %v", head.Metadata, tt.want)
			}
			for k, v := range tt.want {
				if head.Metadata[k] != v {
					t.Errorf("copy metadata[%s] = %q, want %q", k, head.Metadata[k], v)
				}
			}
			if got := aws.ToString(head.ContentType); got != tt.wantKind {
				t.Errorf("copy content-type = %q, want %q", got, tt.wantKind)
			}
		})
	}
}

// A copy onto itself is a metadata rewrite or it is nothing, so it is allowed
// exactly when the request replaces the metadata.
func TestACopyOntoItselfNeedsREPLACE(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("self.bin"),
		Body:     bytes.NewReader(randBytes(64)),
		Metadata: map[string]string{"colour": "octarine"},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	_, err := client.CopyObject(t.Context(), &awss3.CopyObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("self.bin"),
		CopySource: aws.String("bucket/self.bin"),
	})
	if httpStatus(err) != http.StatusBadRequest {
		t.Errorf("copy onto itself without a directive = %v, want 400", err)
	}

	if _, err := client.CopyObject(t.Context(), &awss3.CopyObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("self.bin"),
		CopySource: aws.String("bucket/self.bin"), MetadataDirective: "REPLACE",
		Metadata: map[string]string{"colour": "chartreuse"},
	}); err != nil {
		t.Fatalf("copy onto itself with REPLACE: %v", err)
	}
	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("self.bin"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Metadata["colour"] != "chartreuse" {
		t.Errorf("metadata after rewriting it = %v, want chartreuse", head.Metadata)
	}
}

// Unbounded metadata is unbounded manifests, and a manifest is read by every
// request for the object and by every background pass over it.
func TestMetadataBeyondTheLimitIsRefused(t *testing.T) {
	client := newGateway(t)
	_, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("verbose.bin"),
		Body:     bytes.NewReader(randBytes(32)),
		Metadata: map[string]string{"essay": strings.Repeat("a", s3.MaxMeta+1)},
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "MetadataTooLarge" {
		t.Fatalf("put with %d bytes of metadata = %v, want MetadataTooLarge", s3.MaxMeta+1, err)
	}
}

// A multipart object takes its metadata from the call that began the upload: there
// is nowhere else for a client to put it, since the parts carry bytes and the
// completion carries only their etags.
func TestAMultipartUploadCarriesTheMetadataItBeganWith(t *testing.T) {
	client := newGateway(t)
	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
		Metadata:     map[string]string{"colour": "octarine"},
		ContentType:  aws.String("application/pdf"),
		CacheControl: aws.String("no-store"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	part, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
		UploadId: create.UploadId, PartNumber: aws.Int32(1),
		Body: bytes.NewReader(randBytes(testChunkSize)),
	})
	if err != nil {
		t.Fatalf("upload part: %v", err)
	}
	if _, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
		UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: part.ETag, PartNumber: aws.Int32(1)},
		}},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.Metadata["colour"] != "octarine" || aws.ToString(head.CacheControl) != "no-store" ||
		aws.ToString(head.ContentType) != "application/pdf" {
		t.Errorf("the assembled object carries %v, %q, %q; want octarine, no-store, application/pdf",
			head.Metadata, aws.ToString(head.CacheControl), aws.ToString(head.ContentType))
	}
}

// A request for encryption is refused, not ignored. Ignoring it meant a client that
// sent a customer key was told its object was stored — which it was, in plaintext,
// readable by anyone who asked without the key. The client cannot detect that, which
// is what makes silence the worst answer available.
func TestARequestForEncryptionIsRefused(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("plain.bin"),
		Body: bytes.NewReader(randBytes(64)),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	tests := []struct {
		name string
		in   awss3.PutObjectInput
	}{
		{name: "SSE-S3", in: awss3.PutObjectInput{ServerSideEncryption: types.ServerSideEncryptionAes256}},
		{name: "SSE-KMS", in: awss3.PutObjectInput{
			ServerSideEncryption: types.ServerSideEncryptionAwsKms,
			SSEKMSKeyId:          aws.String("kavo-key"),
		}},
		{name: "a customer key", in: awss3.PutObjectInput{
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			in.Bucket, in.Key = aws.String("bucket"), aws.String("encrypted.bin")
			in.Body = bytes.NewReader(randBytes(64))
			_, err := client.PutObject(t.Context(), &in)
			var api smithy.APIError
			if !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
				t.Fatalf("put asking for encryption = %v, want NotImplemented", err)
			}
		})
	}

	// And on the read, where ignoring the key means answering a request to decrypt
	// with plaintext the client never agreed to have stored.
	_, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("plain.bin"),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
		t.Fatalf("get with a customer key = %v, want NotImplemented", err)
	}
}

// S3 addresses an object's subresources as a query on the object's own path, so a
// server that ignores the query answers them with the object operation instead.
// That is how this store came to destroy objects: `put-object-tagging` reached the
// object PUT and replaced the object with the tagging XML, `put-object-acl`
// truncated it to nothing because its request carries no body, and
// `delete-object-tagging` deleted it — each answered 200, so a client tagging an
// object destroyed it and was told the tag was set.
//
// The assertion that matters is the last one in each case: not that the call is
// refused, but that the object is still there afterwards.
func TestAnObjectSubresourceCannotTouchTheObject(t *testing.T) {
	client := newGateway(t)
	data := randBytes(2 * testChunkSize)
	intact := func(t *testing.T, what string) {
		t.Helper()
		out, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
			Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
		})
		if err != nil {
			t.Fatalf("the object is gone after %s: %v", what, err)
		}
		defer out.Body.Close()
		got, _ := io.ReadAll(out.Body)
		if !bytes.Equal(got, data) {
			t.Fatalf("after %s the object is %d bytes of something else, want the %d written",
				what, len(got), len(data))
		}
	}

	tests := []struct {
		name string
		call func() error
	}{
		{"put-object-tagging", func() error {
			_, err := client.PutObjectTagging(t.Context(), &awss3.PutObjectTaggingInput{
				Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
				Tagging: &types.Tagging{TagSet: []types.Tag{
					{Key: aws.String("colour"), Value: aws.String("octarine")},
				}},
			})
			return err
		}},
		{"delete-object-tagging", func() error {
			_, err := client.DeleteObjectTagging(t.Context(), &awss3.DeleteObjectTaggingInput{
				Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
			})
			return err
		}},
		{"put-object-acl", func() error {
			_, err := client.PutObjectAcl(t.Context(), &awss3.PutObjectAclInput{
				Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
				ACL: types.ObjectCannedACLPrivate,
			})
			return err
		}},
		{"put-object-legal-hold", func() error {
			_, err := client.PutObjectLegalHold(t.Context(), &awss3.PutObjectLegalHoldInput{
				Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
				LegalHold: &types.ObjectLockLegalHold{Status: types.ObjectLockLegalHoldStatusOn},
			})
			return err
		}},
		{"put-object-retention", func() error {
			_, err := client.PutObjectRetention(t.Context(), &awss3.PutObjectRetentionInput{
				Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
				Retention: &types.ObjectLockRetention{
					Mode:            types.ObjectLockRetentionModeGovernance,
					RetainUntilDate: aws.Time(time.Now().Add(time.Hour)),
				},
			})
			return err
		}},
		{"get-object-attributes", func() error {
			_, err := client.GetObjectAttributes(t.Context(), &awss3.GetObjectAttributesInput{
				Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag},
			})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Rewritten for each, so that one call destroying the object cannot
			// be hidden by an earlier one having already replaced it.
			if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
				Bucket: aws.String("bucket"), Key: aws.String("described.bin"),
				Body: bytes.NewReader(data),
			}); err != nil {
				t.Fatalf("put: %v", err)
			}
			var api smithy.APIError
			if err := tt.call(); !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
				t.Errorf("%s = %v, want NotImplemented", tt.name, err)
			}
			intact(t, tt.name)
		})
	}
}

// A version id kavo never issued names something that does not exist. Answering it
// with the current object means a client asking to delete one old version deletes
// the live one instead — the same mistake as the subresources above, arriving
// through a query whose value is the part that matters. "null" is the exception,
// because that is the id ListObjectVersions reports for every object, and it is how
// a client empties a bucket.
func TestAVersionIdThatWasNeverIssuedIsRefused(t *testing.T) {
	client := newGateway(t)
	data := randBytes(64)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("versioned.bin"),
		Body: bytes.NewReader(data),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	_, err := client.DeleteObject(t.Context(), &awss3.DeleteObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("versioned.bin"),
		VersionId: aws.String("deadbeef"),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
		t.Fatalf("delete of an invented version = %v, want NotImplemented", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("versioned.bin"),
	}); err != nil {
		t.Fatalf("the object is gone after a delete of a version that never existed: %v", err)
	}
}

// Reading an object's tags is answered with none, and asking for tags to exist is
// refused. Both halves are needed for either to be honest: a store that dropped
// x-amz-tagging on a PUT and then reported the object had no tags would have told a
// client its tags were gone by way of two successes.
//
// The read exists because the aws CLI reads the source's tags before copying
// anything above 8 MB, so refusing it fails every large server-side copy on a call
// about a feature neither side wants.
func TestTagsAreReportedAsNoneAndRefusedWhenSet(t *testing.T) {
	client := newGateway(t)
	data := randBytes(128)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("tagged.bin"),
		Body: bytes.NewReader(data),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	out, err := client.GetObjectTagging(t.Context(), &awss3.GetObjectTaggingInput{
		Bucket: aws.String("bucket"), Key: aws.String("tagged.bin"),
	})
	if err != nil {
		t.Fatalf("get tags: %v", err)
	}
	if len(out.TagSet) != 0 {
		t.Errorf("an object with no tags reports %d of them", len(out.TagSet))
	}

	// Not an answer about objects that do not exist.
	_, err = client.GetObjectTagging(t.Context(), &awss3.GetObjectTaggingInput{
		Bucket: aws.String("bucket"), Key: aws.String("absent.bin"),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "NoSuchKey" {
		t.Errorf("tags of a missing object = %v, want NoSuchKey", err)
	}

	// And a request that asks for tags to exist is refused rather than dropped,
	// which is what makes the answer above true.
	_, err = client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("with-tags.bin"),
		Body: bytes.NewReader(data), Tagging: aws.String("colour=octarine"),
	})
	if !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
		t.Errorf("put with tags = %v, want NotImplemented", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("with-tags.bin"),
	}); err == nil {
		t.Error("the refused write stored the object anyway, tags silently dropped")
	}
	// A multipart upload asks for tags on the call that creates it.
	_, err = client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("with-tags.bin"),
		Tagging: aws.String("colour=octarine"),
	})
	if !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
		t.Errorf("create upload with tags = %v, want NotImplemented", err)
	}
}

func crc32cOf(data []byte) string {
	sum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], sum)
	return base64.StdEncoding.EncodeToString(b[:])
}

func crc32Of(data []byte) string {
	sum := crc32.ChecksumIEEE(data)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], sum)
	return base64.StdEncoding.EncodeToString(b[:])
}

func crc64nvmeOf(data []byte) string {
	sum := object.CRC64NVME(data)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], sum)
	return base64.StdEncoding.EncodeToString(b[:])
}

// A CRC32C named on a PUT is checked against the body, echoed on the response,
// omitted from a HEAD that did not ask, and returned when one did. A mismatch is
// BadDigest rather than a stored object whose checksum header the client will
// later read back as if it were true.
func TestCRC32COnAPutIsVerifiedAndReplayed(t *testing.T) {
	client := newGateway(t)
	data := []byte("hello checksum")
	sum := crc32cOf(data)

	put, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked.bin"),
		Body: bytes.NewReader(data), ChecksumCRC32C: aws.String(sum),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := aws.ToString(put.ChecksumCRC32C); got != sum {
		t.Errorf("PUT checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked.bin"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.ChecksumCRC32C != nil {
		t.Errorf("HEAD without checksum mode returned %q", aws.ToString(head.ChecksumCRC32C))
	}

	head, err = client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked.bin"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head with checksum mode: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC32C); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}
	if head.ChecksumType != types.ChecksumTypeFullObject {
		t.Errorf("HEAD checksum type = %q, want FULL_OBJECT", head.ChecksumType)
	}

	_, err = client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("wrong.bin"),
		Body: bytes.NewReader(data), ChecksumCRC32C: aws.String("AAAAAA=="),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched CRC32C = %v, want BadDigest", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("wrong.bin"),
	}); err == nil {
		t.Error("the mismatched write stored the object")
	}
}

func TestCRC32OnAPutIsVerifiedAndReplayed(t *testing.T) {
	client := newGateway(t)
	data := []byte("hello ieee checksum")
	sum := crc32Of(data)

	put, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked32.bin"),
		Body: bytes.NewReader(data), ChecksumCRC32: aws.String(sum),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := aws.ToString(put.ChecksumCRC32); got != sum {
		t.Errorf("PUT checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked32.bin"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.ChecksumCRC32 != nil {
		t.Errorf("HEAD without checksum mode returned %q", aws.ToString(head.ChecksumCRC32))
	}

	head, err = client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked32.bin"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head with checksum mode: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC32); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}

	_, err = client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("wrong32.bin"),
		Body: bytes.NewReader(data), ChecksumCRC32: aws.String("AAAAAA=="),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched CRC32 = %v, want BadDigest", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("wrong32.bin"),
	}); err == nil {
		t.Error("the mismatched write stored the object")
	}
}

func TestCRC64NVMEOnAPutIsVerifiedAndReplayed(t *testing.T) {
	client := newGateway(t)
	data := []byte("hello crc64nvme")
	sum := crc64nvmeOf(data)

	put, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked64.bin"),
		Body: bytes.NewReader(data), ChecksumCRC64NVME: aws.String(sum),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := aws.ToString(put.ChecksumCRC64NVME); got != sum {
		t.Errorf("PUT checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked64.bin"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if head.ChecksumCRC64NVME != nil {
		t.Errorf("HEAD without checksum mode returned %q", aws.ToString(head.ChecksumCRC64NVME))
	}

	head, err = client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("checked64.bin"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head with checksum mode: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC64NVME); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}

	_, err = client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("wrong64.bin"),
		Body: bytes.NewReader(data), ChecksumCRC64NVME: aws.String("AAAAAAAAAAA="),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched CRC64NVME = %v, want BadDigest", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("wrong64.bin"),
	}); err == nil {
		t.Error("the mismatched write stored the object")
	}
}

// S3 answers BadDigest for a checksum that is not a digest, even though a
// malformed Content-MD5 is InvalidDigest. The suite's CRC64NVME=bad is that case.
func TestAMalformedChecksumIsBadDigest(t *testing.T) {
	_, endpoint := newGatewayURL(t, 3, testChunkSize)
	body := []byte("hello")
	req, err := http.NewRequest(http.MethodPut, endpoint+"/bucket/bad-crc.bin", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amz-checksum-crc64nvme", "bad")
	req.Header.Set("x-amz-checksum-algorithm", "CRC64NVME")
	req.ContentLength = int64(len(body))
	signS3(t, req, "UNSIGNED-PAYLOAD")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(got, []byte("BadDigest")) {
		t.Errorf("malformed CRC64NVME = %d %s, want BadDigest", resp.StatusCode, got)
	}
}

// SHA-256, COMPOSITE, and a checksum on CopyObject are all requests to record a
// number this path would not look at. Refused rather than stored: the same rule
// as encryption and tagging. CRC32, CRC32C and CRC64NVME on a PUT or multipart
// upload are checked, so they are not among these.
func TestChecksumsThisServerDoesNotVerifyAreRefused(t *testing.T) {
	client := newGateway(t)
	data := randBytes(64)

	_, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("sha.bin"),
		Body: bytes.NewReader(data), ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256: aws.String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
		t.Errorf("SHA-256 put = %v, want NotImplemented", err)
	}

	_, err = client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("mpu.bin"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	if !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
		t.Errorf("multipart with SHA-256 = %v, want NotImplemented", err)
	}

	_, err = client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("composite.bin"),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
		ChecksumType:      types.ChecksumTypeComposite,
	})
	if !errors.As(err, &api) || api.ErrorCode() != "NotImplemented" {
		t.Errorf("COMPOSITE CRC32C = %v, want NotImplemented", err)
	}
}

// A CRC32C that arrives after the body, as aws-chunked trailers, is checked
// the same way as one named in a header: compared before commit, echoed, and a
// mismatch stores nothing. Built by hand rather than through the Go SDK: that
// client only frames a trailing checksum over TLS, and httptest is HTTP.
func TestTrailingCRC32COnAPutIsVerified(t *testing.T) {
	client, endpoint := newGatewayURL(t, 3, testChunkSize)
	data := []byte("hello trailing checksum")
	sum := crc32cOf(data)

	resp := putUnsignedTrailer(t, endpoint, "trailed.bin", data, sum, "x-amz-checksum-crc32c", "CRC32C")
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("X-Amz-Checksum-Crc32c"); got != sum {
		t.Errorf("PUT checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("trailed.bin"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC32C); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}

	resp = putUnsignedTrailer(t, endpoint, "wrong-trail.bin", data, "AAAAAA==", "x-amz-checksum-crc32c", "CRC32C")
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("BadDigest")) {
		t.Errorf("mismatched trailer = %d %s, want BadDigest", resp.StatusCode, body)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("wrong-trail.bin"),
	}); err == nil {
		t.Error("the mismatched write stored the object")
	}
}

func TestTrailingCRC32OnAPutIsVerified(t *testing.T) {
	client, endpoint := newGatewayURL(t, 3, testChunkSize)
	data := []byte("hello trailing ieee")
	sum := crc32Of(data)

	resp := putUnsignedTrailer(t, endpoint, "trailed32.bin", data, sum, "x-amz-checksum-crc32", "CRC32")
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("X-Amz-Checksum-Crc32"); got != sum {
		t.Errorf("PUT checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("trailed32.bin"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC32); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}

	resp = putUnsignedTrailer(t, endpoint, "wrong-trail32.bin", data, "AAAAAA==", "x-amz-checksum-crc32", "CRC32")
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("BadDigest")) {
		t.Errorf("mismatched trailer = %d %s, want BadDigest", resp.StatusCode, body)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("wrong-trail32.bin"),
	}); err == nil {
		t.Error("the mismatched write stored the object")
	}
}

func TestTrailingCRC64NVMEOnAPutIsVerified(t *testing.T) {
	client, endpoint := newGatewayURL(t, 3, testChunkSize)
	data := []byte("hello trailing crc64")
	sum := crc64nvmeOf(data)

	resp := putUnsignedTrailer(t, endpoint, "trailed64.bin", data, sum, "x-amz-checksum-crc64nvme", "CRC64NVME")
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("X-Amz-Checksum-Crc64nvme"); got != sum {
		t.Errorf("PUT checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("trailed64.bin"),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC64NVME); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}

	resp = putUnsignedTrailer(t, endpoint, "wrong-trail64.bin", data, "AAAAAAAAAAA=", "x-amz-checksum-crc64nvme", "CRC64NVME")
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("BadDigest")) {
		t.Errorf("mismatched trailer = %d %s, want BadDigest", resp.StatusCode, body)
	}
}

func putUnsignedTrailer(t *testing.T, endpoint, key string, data []byte, checksum, trailer, algo string) *http.Response {
	t.Helper()
	var framed bytes.Buffer
	if len(data) > 0 {
		fmt.Fprintf(&framed, "%x\r\n%s\r\n", len(data), data)
	}
	fmt.Fprintf(&framed, "0\r\n%s:%s\r\n\r\n", trailer, checksum)

	req, err := http.NewRequest(http.MethodPut, endpoint+"/bucket/"+key, bytes.NewReader(framed.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("x-amz-decoded-content-length", fmt.Sprint(len(data)))
	req.Header.Set("x-amz-trailer", trailer)
	req.Header.Set("x-amz-checksum-algorithm", algo)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.ContentLength = int64(framed.Len())
	signS3(t, req, "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func signS3(t *testing.T, r *http.Request, payloadHash string) {
	t.Helper()
	r.Header.Set("x-amz-content-sha256", payloadHash)
	c, err := credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, "").Retrieve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	if err := signer.SignHTTP(t.Context(), c, r, payloadHash, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatalf("sign: %v", err)
	}
}
