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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/0vertake/kavo/internal/api"
	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
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
	prefix := "/kavo-test/" + rand.Text()

	const n = 3
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

		c := cluster.New(id, peers[id], st, m, testChunkSize)
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
	})
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
