package s3_test

// Multipart upload, checked with the SDK's own multipart calls and with its
// high-level uploader — the thing `aws s3 cp` uses for anything large. The claims
// worth testing are the invariants, not the plumbing: the object does not exist
// until completion, it is exactly the parts concatenated, its ETag is the one a
// client recomputes, and a completion that names the wrong parts changes nothing.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// A multipart upload's parts arrive independently and become one object at one
// instant. The bytes must be the parts in order, and the ETag must be the one a
// client computes from the parts' ETags — that is the value `aws s3 cp` compares.
func TestMultipartUploadAssemblesTheObject(t *testing.T) {
	client := newGateway(t)
	const bucket, key = "bucket", "assembled.bin"

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String("application/x-kavo-multipart"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := create.UploadId

	// Deliberately uneven, and not a multiple of the chunk size: an object made of
	// parts that each end mid-chunk is where a naive concatenation shows up.
	sizes := []int{testChunkSize + 11, 3 * testChunkSize, 7}
	var want []byte
	var parts []types.CompletedPart
	var sums []byte
	for i, size := range sizes {
		data := randBytes(size)
		want = append(want, data...)
		up, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   id,
			PartNumber: aws.Int32(int32(i + 1)),
			Body:       bytes.NewReader(data),
		})
		if err != nil {
			t.Fatalf("upload part %d: %v", i+1, err)
		}
		sum := md5sum(data)
		if got, want := aws.ToString(up.ETag), fmt.Sprintf("%q", fmt.Sprintf("%x", sum)); got != want {
			t.Errorf("part %d ETag = %s, want the part's MD5 %s", i+1, got, want)
		}
		sums = append(sums, sum...)
		parts = append(parts, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(int32(i + 1))})
	}

	// Until the completion the object does not exist. A part visible as an object
	// is a partially written object, which invariant 2 forbids.
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}); err == nil {
		t.Fatal("the object exists before the upload was completed")
	}

	done, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	wantETag := fmt.Sprintf("%q", fmt.Sprintf("%x-%d", md5sum(sums), len(parts)))
	if got := aws.ToString(done.ETag); got != wantETag {
		t.Errorf("ETag = %s, want the MD5 of the part MD5s %s", got, wantETag)
	}

	get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer get.Body.Close()
	got, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("read %d bytes, want the %d uploaded, concatenated in part order", len(got), len(want))
	}
	if aws.ToString(get.ETag) != wantETag {
		t.Errorf("GET says ETag %s, completion said %s", aws.ToString(get.ETag), wantETag)
	}
	if aws.ToString(get.ContentType) != "application/x-kavo-multipart" {
		t.Errorf("Content-Type = %q, want the one set when the upload started",
			aws.ToString(get.ContentType))
	}
	// A completed object is an object like any other: ranges work, which is what
	// the CLI immediately uses to download it again.
	rng, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Range: aws.String("bytes=" + strconv.Itoa(testChunkSize) + "-" + strconv.Itoa(testChunkSize+99)),
	})
	if err != nil {
		t.Fatalf("ranged get: %v", err)
	}
	defer rng.Body.Close()
	window, err := io.ReadAll(rng.Body)
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if !bytes.Equal(window, want[testChunkSize:testChunkSize+100]) {
		t.Error("a range of the completed object is not the same window of the uploaded bytes")
	}
}

// GET ?partNumber is a ranged read of one completed part, not of the whole object.
// Answering it with the whole object is the same class of mistake as ignoring
// ?tagging: the client asked for a piece and was given something else.
func TestGetObjectByPartNumber(t *testing.T) {
	client := newGateway(t)
	const bucket, key, dest = "bucket", "parts.bin", "parts-copy.bin"

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Sparse numbers on purpose: partNumber names the number the client uploaded,
	// not the index in the assembled object. Completing 1, 3, 7 and then serving
	// "part 2" as the second body would be the wrong object.
	type uploaded struct {
		number int32
		body   []byte
	}
	parts := []uploaded{
		{1, randBytes(testChunkSize + 11)},
		{3, randBytes(7)},
		{7, randBytes(3 * testChunkSize)},
	}
	var completed []types.CompletedPart
	for _, p := range parts {
		up, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   create.UploadId,
			PartNumber: aws.Int32(p.number),
			Body:       bytes.NewReader(p.body),
		})
		if err != nil {
			t.Fatalf("upload part %d: %v", p.number, err)
		}
		completed = append(completed, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(p.number)})
	}
	if _, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	for _, p := range parts {
		get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(p.number),
		})
		if err != nil {
			t.Fatalf("get part %d: %v", p.number, err)
		}
		got, err := io.ReadAll(get.Body)
		get.Body.Close()
		if err != nil {
			t.Fatalf("read part %d: %v", p.number, err)
		}
		if !bytes.Equal(got, p.body) {
			t.Errorf("part %d is %d bytes, want the %d uploaded", p.number, len(got), len(p.body))
		}
		if get.PartsCount == nil || *get.PartsCount != int32(len(parts)) {
			t.Errorf("part %d PartsCount = %v, want %d", p.number, get.PartsCount, len(parts))
		}
		if aws.ToInt64(get.ContentLength) != int64(len(p.body)) {
			t.Errorf("part %d ContentLength = %v, want %d", p.number, get.ContentLength, len(p.body))
		}
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(3),
	})
	if err != nil {
		t.Fatalf("head part 3: %v", err)
	}
	if head.PartsCount == nil || *head.PartsCount != 3 {
		t.Errorf("HEAD PartsCount = %v, want 3", head.PartsCount)
	}
	if aws.ToInt64(head.ContentLength) != 7 {
		t.Errorf("HEAD ContentLength = %v, want the part's 7 bytes", head.ContentLength)
	}

	if _, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(2),
	}); errorCode(err) != "InvalidPartNumber" {
		t.Errorf("part 2 (never uploaded) = %v, want InvalidPartNumber", err)
	}
	if _, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(0),
	}); errorCode(err) != "InvalidArgument" {
		t.Errorf("part 0 = %v, want InvalidArgument", err)
	}

	// A range of a part is a range of that window, not of the object.
	window, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		PartNumber: aws.Int32(7), Range: aws.String("bytes=0-2"),
	})
	if err != nil {
		t.Fatalf("ranged part: %v", err)
	}
	got, err := io.ReadAll(window.Body)
	window.Body.Close()
	if err != nil {
		t.Fatalf("read ranged part: %v", err)
	}
	if !bytes.Equal(got, parts[2].body[:3]) {
		t.Error("a range of part 7 is not the start of that part")
	}

	if _, err := client.CopyObject(t.Context(), &awss3.CopyObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(dest),
		CopySource: aws.String(bucket + "/" + key),
	}); err != nil {
		t.Fatalf("copy: %v", err)
	}
	copied, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(dest), PartNumber: aws.Int32(3),
	})
	if err != nil {
		t.Fatalf("get copied part: %v", err)
	}
	got, err = io.ReadAll(copied.Body)
	copied.Body.Close()
	if err != nil {
		t.Fatalf("read copied part: %v", err)
	}
	if !bytes.Equal(got, parts[1].body) {
		t.Error("a copy did not keep the source's part boundaries")
	}
}

// A single PUT is not a multipart object: part 1 is the whole body and there is
// no PartsCount, because that header is how a client tells the two apart.
func TestPartNumberOnASinglePut(t *testing.T) {
	client := newGateway(t)
	const bucket, key = "bucket", "one.bin"
	data := randBytes(64)

	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	}); err != nil {
		t.Fatal(err)
	}

	get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(1),
	})
	if err != nil {
		t.Fatalf("part 1 of a PUT: %v", err)
	}
	got, err := io.ReadAll(get.Body)
	get.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Error("part 1 of a single PUT is not the object")
	}
	if get.PartsCount != nil {
		t.Errorf("PartsCount = %v, want absent on a non-multipart object", get.PartsCount)
	}

	if _, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), PartNumber: aws.Int32(2),
	}); errorCode(err) != "InvalidPartNumber" {
		t.Errorf("part 2 of a PUT = %v, want InvalidPartNumber", err)
	}
}

// Real clients send parts concurrently, so they arrive in no particular order and
// several are in flight at once. The object still has to be the parts in the order
// the completion names, which is the property a per-upload ordering bug hides
// behind when parts happen to arrive in sequence.
func TestPartsArrivingConcurrentlyAndOutOfOrder(t *testing.T) {
	client := newGateway(t)
	const bucket, key = "bucket", "concurrent.bin"

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 6
	bodies := make([][]byte, n)
	parts := make([]types.CompletedPart, n)
	var wg sync.WaitGroup
	for i := range n {
		bodies[i] = randBytes(testChunkSize + i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			up, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
				PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(bodies[i]),
			})
			if err != nil {
				t.Errorf("upload part %d: %v", i+1, err)
				return
			}
			parts[i] = types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(int32(i + 1))}
		}()
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	if _, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	data := bytes.Join(bodies, nil)
	get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer get.Body.Close()
	got, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("read %d bytes, want the %d uploaded", len(got), len(data))
	}
}

// An abort has to leave nothing behind: no object, and an upload id that no longer
// works. The chunks are dropped too, but the observable claim is that the upload
// is gone.
func TestAbortingAnUploadLeavesNothing(t *testing.T) {
	client := newGateway(t)
	const bucket, key = "bucket", "abandoned.bin"

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	data := randBytes(testChunkSize + 3)
	part, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.AbortMultipartUpload(t.Context(), &awss3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
	}); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}); err == nil {
		t.Error("the object exists after the upload was aborted")
	}

	// Completing an aborted upload must not resurrect it, or an abort would be
	// advisory rather than final.
	_, err = client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
		},
	})
	if code := errorCode(err); code != "NoSuchUpload" {
		t.Errorf("completing an aborted upload: error code %q, want NoSuchUpload", code)
	}
	// Aborting twice is NoSuchUpload, which reverses what this test used to
	// assert. It had aborting be idempotent on the grounds that a cleanup loop
	// aborts what it already aborted, but S3 answers 404 there and Ceph's suite
	// checks it (test_abort_multipart_upload_not_found): a client told it
	// successfully aborted an id that never existed cannot tell that from having
	// aborted the wrong one, and a cleanup loop that mistyped an id would report
	// success.
	if _, err := client.AbortMultipartUpload(t.Context(), &awss3.AbortMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
	}); errorCode(err) != "NoSuchUpload" {
		t.Errorf("aborting twice: %v, want NoSuchUpload", err)
	}
}

// A completion that does not describe what was uploaded must change nothing. The
// alternative is an object that is readable, checksum-valid, and not what anybody
// sent — the worst outcome available, since nothing downstream can detect it.
func TestABadCompletionCommitsNothing(t *testing.T) {
	client := newGateway(t)
	const bucket = "bucket"

	start := func(t *testing.T, key string) (*string, *string) {
		t.Helper()
		create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Fatal(err)
		}
		part, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(1), Body: bytes.NewReader(randBytes(1024)),
		})
		if err != nil {
			t.Fatal(err)
		}
		return create.UploadId, part.ETag
	}

	tests := []struct {
		name  string
		key   string
		parts func(etag *string) []types.CompletedPart
		code  string
	}{
		{
			name:  "no parts at all",
			key:   "empty-completion",
			parts: func(*string) []types.CompletedPart { return nil },
			code:  "InvalidPart",
		},
		{
			name: "a part that was never uploaded",
			key:  "missing-part",
			parts: func(etag *string) []types.CompletedPart {
				return []types.CompletedPart{
					{ETag: etag, PartNumber: aws.Int32(1)},
					{ETag: etag, PartNumber: aws.Int32(2)},
				}
			},
			code: "InvalidPart",
		},
		{
			name: "an etag that is not the part's",
			key:  "wrong-etag",
			parts: func(*string) []types.CompletedPart {
				return []types.CompletedPart{
					{ETag: aws.String(`"00000000000000000000000000000000"`), PartNumber: aws.Int32(1)},
				}
			},
			code: "InvalidPart",
		},
		{
			name: "the same part twice",
			key:  "repeated-part",
			parts: func(etag *string) []types.CompletedPart {
				return []types.CompletedPart{
					{ETag: etag, PartNumber: aws.Int32(1)},
					{ETag: etag, PartNumber: aws.Int32(1)},
				}
			},
			code: "InvalidPartOrder",
		},
		{
			// The client believes it knows what order its bytes go in. Sorting
			// this for it would accept a request whose meaning is a guess.
			name: "parts listed descending",
			key:  "descending-parts",
			parts: func(etag *string) []types.CompletedPart {
				return []types.CompletedPart{
					{ETag: etag, PartNumber: aws.Int32(2)},
					{ETag: etag, PartNumber: aws.Int32(1)},
				}
			},
			code: "InvalidPartOrder",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, etag := start(t, tt.key)
			_, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
				Bucket: aws.String(bucket), Key: aws.String(tt.key), UploadId: id,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: tt.parts(etag)},
			})
			if code := errorCode(err); code != tt.code {
				t.Errorf("error code %q, want %q (err: %v)", code, tt.code, err)
			}
			if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
				Bucket: aws.String(bucket), Key: aws.String(tt.key),
			}); err == nil {
				t.Error("the object exists after a completion that should have been refused")
			}
			// The upload survives a refused completion, so the client can fix its
			// request and try again. Aborting for it would lose the parts.
			if _, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
				Bucket: aws.String(bucket), Key: aws.String(tt.key), UploadId: id,
				PartNumber: aws.Int32(2), Body: bytes.NewReader(randBytes(16)),
			}); err != nil {
				t.Errorf("the upload did not survive a refused completion: %v", err)
			}
		})
	}
}

// An upload id nobody issued must be refused. Accepting one would let a client
// invent an id, upload parts under it, and complete an object — the signature is
// the only authority here, and it says nothing about which uploads exist.
func TestAnInventedUploadIDIsRefused(t *testing.T) {
	client := newGateway(t)
	_, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String("invented"),
		UploadId: aws.String("not-an-upload-id"), PartNumber: aws.Int32(1),
		Body: bytes.NewReader(randBytes(16)),
	})
	if code := errorCode(err); code != "NoSuchUpload" {
		t.Errorf("uploading a part to an invented id: code %q, want NoSuchUpload (err: %v)", code, err)
	}
}

// errorCode is the S3 error code an SDK call failed with, which is the part a
// client branches on.
func errorCode(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

// A part sent twice: the object gets the body that arrived last.
//
// Clients resend parts — a timeout, a retry, a flaky connection — and the AWS SDKs
// do it without telling the caller. If the first body won, or if both were somehow
// kept, an upload that merely retried would produce an object the client never sent
// and whose ETag it would still accept, since the ETag is computed from the parts
// the server chose. That is silent corruption arriving through the front door.
func TestAResentPartReplacesTheFirstOne(t *testing.T) {
	client := newGateway(t)
	const bucket, key = "bucket", "resent.bin"

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := create.UploadId

	// Two parts, and the first one sent twice with different bytes. Both are
	// larger than a chunk so that the second body has to displace the first
	// everywhere, not just in the manifest.
	first := bytes.Repeat([]byte("A"), 3*testChunkSize+11)
	replacement := bytes.Repeat([]byte("B"), 3*testChunkSize+11)
	second := bytes.Repeat([]byte("C"), testChunkSize/2)

	upload := func(number int32, body []byte) string {
		t.Helper()
		out, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
			PartNumber: aws.Int32(number), Body: bytes.NewReader(body),
		})
		if err != nil {
			t.Fatalf("upload part %d: %v", number, err)
		}
		return aws.ToString(out.ETag)
	}

	upload(1, first)
	etag1 := upload(1, replacement)
	etag2 := upload(2, second)

	if _, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: aws.String(etag1)},
			{PartNumber: aws.Int32(2), ETag: aws.String(etag2)},
		}},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer get.Body.Close()
	got, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatal(err)
	}

	want := append(append([]byte{}, replacement...), second...)
	if !bytes.Equal(got, want) {
		t.Errorf("the object is %d bytes and not the parts that arrived last (%d bytes)",
			len(got), len(want))
		if bytes.Contains(got, first[:16]) {
			t.Error("it still carries the body of the part that was replaced")
		}
	}
}

// upload creates an upload and sends one part, returning the id and the part's
// entry, since three tests below need exactly that much of a multipart upload.
func upload(t *testing.T, client *awss3.Client, key string) (*string, types.CompletedPart) {
	t.Helper()
	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	part, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String(key),
		UploadId: create.UploadId, PartNumber: aws.Int32(1),
		Body: bytes.NewReader(randBytes(testChunkSize + 3)),
	})
	if err != nil {
		t.Fatalf("upload part: %v", err)
	}
	return create.UploadId, types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(1)}
}

// A completion whose response never arrived is retried by every SDK. Answering the
// retry with NoSuchUpload tells the client its upload failed while the object is
// sitting there, so the id is remembered for as long as a retry could plausibly
// take (meta.CompletionMemory) and answers with what the upload produced.
func TestRecompletingAnUploadAnswersWithWhatItProduced(t *testing.T) {
	client := newGateway(t)
	id, part := upload(t, client, "retried.bin")
	in := &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("retried.bin"), UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{part}},
	}
	first, err := client.CompleteMultipartUpload(t.Context(), in)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	again, err := client.CompleteMultipartUpload(t.Context(), in)
	if err != nil {
		t.Fatalf("complete a second time: %v", err)
	}
	if aws.ToString(again.ETag) != aws.ToString(first.ETag) {
		t.Errorf("the retry answered %s, want the first answer %s",
			aws.ToString(again.ETag), aws.ToString(first.ETag))
	}

	// And the object is the one the first completion made, not a second object
	// assembled out of parts that no longer exist.
	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("retried.bin"),
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if aws.ToString(head.ETag) != aws.ToString(first.ETag) {
		t.Errorf("the object is %s, want %s", aws.ToString(head.ETag), aws.ToString(first.ETag))
	}
}

// Aborting an upload id that never existed used to succeed, which tells a client
// cleaning up that it cleaned up something it invented.
func TestAbortingAnUploadThatDoesNotExistSaysSo(t *testing.T) {
	client := newGateway(t)
	_, err := client.AbortMultipartUpload(t.Context(), &awss3.AbortMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("invented.bin"),
		UploadId: aws.String("NEVEREXISTEDATALL"),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "NoSuchUpload" {
		t.Fatalf("abort of an unknown id = %v, want NoSuchUpload", err)
	}
}

// A client resuming an upload asks which parts arrived. This used to be answered by
// the object read handler, so it said NoSuchKey — telling a client whose parts are
// all safely stored that there is nothing there.
func TestListingThePartsOfAnUpload(t *testing.T) {
	client := newGateway(t)
	id, part := upload(t, client, "resumed.bin")

	parts, err := client.ListParts(t.Context(), &awss3.ListPartsInput{
		Bucket: aws.String("bucket"), Key: aws.String("resumed.bin"), UploadId: id,
	})
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	if len(parts.Parts) != 1 {
		t.Fatalf("listed %d parts, want the 1 that was uploaded", len(parts.Parts))
	}
	if got := parts.Parts[0]; aws.ToInt32(got.PartNumber) != 1 ||
		aws.ToString(got.ETag) != aws.ToString(part.ETag) ||
		aws.ToInt64(got.Size) != int64(testChunkSize+3) {
		t.Errorf("listed part = %d/%s/%d, want 1/%s/%d", aws.ToInt32(got.PartNumber),
			aws.ToString(got.ETag), aws.ToInt64(got.Size), aws.ToString(part.ETag), testChunkSize+3)
	}

	_, err = client.ListParts(t.Context(), &awss3.ListPartsInput{
		Bucket: aws.String("bucket"), Key: aws.String("resumed.bin"),
		UploadId: aws.String("NEVEREXISTEDATALL"),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "NoSuchUpload" {
		t.Fatalf("listing the parts of an unknown upload = %v, want NoSuchUpload", err)
	}
}

// Listing uploads in flight is how a client finds what it abandoned. Answering it
// with an object listing, which is what a GET on the bucket used to do, parses as
// "nothing in flight" — worse than a refusal, because the client believes it.
func TestListingUploadsInFlight(t *testing.T) {
	client := newGateway(t)
	first, _ := upload(t, client, "flight/one.bin")
	second, _ := upload(t, client, "flight/two.bin")
	finished, part := upload(t, client, "flight/done.bin")
	if _, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("flight/done.bin"), UploadId: finished,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{part}},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	list, err := client.ListMultipartUploads(t.Context(), &awss3.ListMultipartUploadsInput{
		Bucket: aws.String("bucket"),
	})
	if err != nil {
		t.Fatalf("list uploads: %v", err)
	}
	got := make(map[string]string, len(list.Uploads))
	for _, u := range list.Uploads {
		got[aws.ToString(u.UploadId)] = aws.ToString(u.Key)
	}
	if len(got) != 2 || got[aws.ToString(first)] != "flight/one.bin" ||
		got[aws.ToString(second)] != "flight/two.bin" {
		t.Errorf("uploads in flight = %v, want the two that are", got)
	}
	if _, ok := got[aws.ToString(finished)]; ok {
		t.Error("a completed upload is still listed as in flight")
	}
}

// UploadPartCopy assembles a part out of another object's bytes, which is how every
// client copies an object too large for one call — above 8 MB the aws CLI does every
// copy this way. The header naming the source used to be ignored, so the request was
// read as a part with an empty body and answered 200 with an etag: a large
// server-side copy assembled an empty object and reported success.
//
// The assertion that matters is the last one: not that the calls are accepted, but
// that the completed object is byte-for-byte the range that was named.
func TestAPartCanBeCopiedFromAnotherObject(t *testing.T) {
	client := newGateway(t)
	// Larger than one chunk, so a copied range crosses a chunk boundary and the
	// part is re-chunked rather than handed the source's own references.
	source := randBytes(3*testChunkSize + 17)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"),
		Body: bytes.NewReader(source),
	}); err != nil {
		t.Fatalf("put source: %v", err)
	}

	tests := []struct {
		name   string
		ranges []string // one per part; "" means the whole object
		want   func() []byte
	}{
		{
			name:   "the whole object as one part",
			ranges: []string{""},
			want:   func() []byte { return source },
		},
		{
			name: "two halves reassembled",
			ranges: []string{
				fmt.Sprintf("bytes=0-%d", testChunkSize-1),
				fmt.Sprintf("bytes=%d-%d", testChunkSize, len(source)-1),
			},
			want: func() []byte { return source },
		},
		{
			name:   "one range twice, which is an object the source does not contain",
			ranges: []string{"bytes=10-99", "bytes=10-99"},
			want:   func() []byte { return append(append([]byte{}, source[10:100]...), source[10:100]...) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "assembled-" + strings.ReplaceAll(tt.name, " ", "-")
			create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
				Bucket: aws.String("bucket"), Key: aws.String(key),
			})
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			var done []types.CompletedPart
			for i, spec := range tt.ranges {
				in := &awss3.UploadPartCopyInput{
					Bucket: aws.String("bucket"), Key: aws.String(key),
					UploadId: create.UploadId, PartNumber: aws.Int32(int32(i + 1)),
					CopySource: aws.String("bucket/source.bin"),
				}
				if spec != "" {
					in.CopySourceRange = aws.String(spec)
				}
				out, err := client.UploadPartCopy(t.Context(), in)
				if err != nil {
					t.Fatalf("copy part %d: %v", i+1, err)
				}
				if out.CopyPartResult == nil || aws.ToString(out.CopyPartResult.ETag) == "" {
					t.Fatalf("copy part %d returned no etag, so the upload cannot be completed", i+1)
				}
				done = append(done, types.CompletedPart{
					PartNumber: aws.Int32(int32(i + 1)),
					ETag:       out.CopyPartResult.ETag,
				})
			}
			if _, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
				Bucket: aws.String("bucket"), Key: aws.String(key),
				UploadId:        create.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: done},
			}); err != nil {
				t.Fatalf("complete: %v", err)
			}

			out, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
				Bucket: aws.String("bucket"), Key: aws.String(key),
			})
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer out.Body.Close()
			got, err := io.ReadAll(out.Body)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if want := tt.want(); !bytes.Equal(got, want) {
				t.Fatalf("the copy is %d bytes, want %d, equal = %v",
					len(got), len(want), bytes.Equal(got, want))
			}
		})
	}
}

// A part may be copied and uploaded in the same upload, which is what a client does
// when it changes one region of a large object without re-sending the rest.
func TestAnUploadCanMixCopiedAndUploadedParts(t *testing.T) {
	client := newGateway(t)
	source := randBytes(2 * testChunkSize)
	replacement := randBytes(testChunkSize)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"),
		Body: bytes.NewReader(source),
	}); err != nil {
		t.Fatalf("put source: %v", err)
	}
	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("patched.bin"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	copied, err := client.UploadPartCopy(t.Context(), &awss3.UploadPartCopyInput{
		Bucket: aws.String("bucket"), Key: aws.String("patched.bin"),
		UploadId: create.UploadId, PartNumber: aws.Int32(1),
		CopySource:      aws.String("bucket/source.bin"),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", testChunkSize-1)),
	})
	if err != nil {
		t.Fatalf("copy part: %v", err)
	}
	sent, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
		Bucket: aws.String("bucket"), Key: aws.String("patched.bin"),
		UploadId: create.UploadId, PartNumber: aws.Int32(2),
		Body: bytes.NewReader(replacement),
	})
	if err != nil {
		t.Fatalf("upload part: %v", err)
	}
	if _, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("patched.bin"),
		UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{PartNumber: aws.Int32(1), ETag: copied.CopyPartResult.ETag},
			{PartNumber: aws.Int32(2), ETag: sent.ETag},
		}},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	out, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("patched.bin"),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer out.Body.Close()
	got, _ := io.ReadAll(out.Body)
	want := append(append([]byte{}, source[:testChunkSize]...), replacement...)
	if !bytes.Equal(got, want) {
		t.Fatalf("the patched object is %d bytes and not the halves it was assembled from", len(got))
	}
}

// What a copied part must never be is short. A read is satisfied by whatever exists,
// so a Range that runs past the end returns less; a copy names the bytes it intends
// to appear in the destination, and the client only ever sees the etag of what did
// get copied — so a silently shortened range assembles an object nobody described.
func TestACopiedPartIsRefusedRatherThanShortened(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"),
		Body: bytes.NewReader(randBytes(1024)),
	}); err != nil {
		t.Fatalf("put source: %v", err)
	}
	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	tests := []struct {
		name, spec, code string
	}{
		{"past the end", "bytes=512-4095", "InvalidRange"},
		{"entirely past the end", "bytes=2048-4095", "InvalidRange"},
		// Malformed rather than unsatisfiable, which S3 answers 400 where a range
		// that merely does not fit is 416. A client can act on the difference.
		{"open ended, whose meaning depends on a size the client did not check", "bytes=512-", "InvalidArgument"},
		{"backwards", "bytes=900-100", "InvalidArgument"},
		{"not bytes at all", "chunks=0-1", "InvalidArgument"},
		{"two ranges", "bytes=0-2,3-5", "InvalidArgument"},
		{"a source that does not exist", "", "NoSuchKey"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := &awss3.UploadPartCopyInput{
				Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
				UploadId: create.UploadId, PartNumber: aws.Int32(1),
				CopySource: aws.String("bucket/source.bin"),
			}
			if tt.spec != "" {
				in.CopySourceRange = aws.String(tt.spec)
			} else {
				in.CopySource = aws.String("bucket/absent.bin")
			}
			_, err := client.UploadPartCopy(t.Context(), in)
			var api smithy.APIError
			if !errors.As(err, &api) || api.ErrorCode() != tt.code {
				t.Fatalf("copy %q = %v, want %s", tt.spec, err, tt.code)
			}
		})
	}

	// And none of the refusals stored anything, so a completion cannot assemble an
	// object out of a copy that did not happen.
	parts, err := client.ListParts(t.Context(), &awss3.ListPartsInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
		UploadId: create.UploadId,
	})
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	if len(parts.Parts) != 0 {
		t.Errorf("the refused copies left %d parts behind, want none", len(parts.Parts))
	}
}

// The four x-amz-copy-source-if-* headers apply to a copied part as they do to a
// whole-object copy: a client that guards a 5 GB reassembly on the source's etag
// wants the guard checked on every part, since the source can change between them.
func TestACopiedPartHonoursAConditionOnItsSource(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"),
		Body: bytes.NewReader(randBytes(1024)),
	}); err != nil {
		t.Fatalf("put source: %v", err)
	}
	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = client.UploadPartCopy(t.Context(), &awss3.UploadPartCopyInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
		UploadId: create.UploadId, PartNumber: aws.Int32(1),
		CopySource:        aws.String("bucket/source.bin"),
		CopySourceIfMatch: aws.String(`"0000000000000000000000000000fail"`),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "PreconditionFailed" {
		t.Fatalf("copy with a failing condition = %v, want PreconditionFailed", err)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String("bucket"), Key: aws.String("source.bin"),
	})
	if err != nil {
		t.Fatalf("head source: %v", err)
	}
	if _, err := client.UploadPartCopy(t.Context(), &awss3.UploadPartCopyInput{
		Bucket: aws.String("bucket"), Key: aws.String("assembled.bin"),
		UploadId: create.UploadId, PartNumber: aws.Int32(1),
		CopySource:        aws.String("bucket/source.bin"),
		CopySourceIfMatch: head.ETag,
	}); err != nil {
		t.Fatalf("copy with the source's own etag: %v", err)
	}
}

// A CRC32C named on a multipart upload is checked on each part and combined at
// complete into the object's checksum, which a HEAD that asks for it returns. A
// mismatch stores neither the part nor the object.
func TestCRC32COnAMultipartUploadIsVerifiedAndReplayed(t *testing.T) {
	client := newGateway(t)
	const bucket, key = "bucket", "checked-mpu.bin"
	p1, p2 := []byte("hello "), []byte("checksum parts")
	want := append(append([]byte{}, p1...), p2...)
	sum := crc32cOf(want)

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
		ChecksumType:      types.ChecksumTypeFullObject,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if create.ChecksumAlgorithm != types.ChecksumAlgorithmCrc32c {
		t.Errorf("create algorithm = %q, want CRC32C", create.ChecksumAlgorithm)
	}
	id := create.UploadId

	_, err = client.UploadPart(t.Context(), &awss3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		PartNumber: aws.Int32(1), Body: bytes.NewReader(p1),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
		ChecksumCRC32C:    aws.String("AAAAAA=="),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched part CRC32C = %v, want BadDigest", err)
	}

	var parts []types.CompletedPart
	for i, data := range [][]byte{p1, p2} {
		up, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
			PartNumber:        aws.Int32(int32(i + 1)),
			Body:              bytes.NewReader(data),
			ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c,
			ChecksumCRC32C:    aws.String(crc32cOf(data)),
		})
		if err != nil {
			t.Fatalf("upload part %d: %v", i+1, err)
		}
		if got := aws.ToString(up.ChecksumCRC32C); got != crc32cOf(data) {
			t.Errorf("part %d checksum = %q, want %q", i+1, got, crc32cOf(data))
		}
		parts = append(parts, types.CompletedPart{
			ETag: up.ETag, PartNumber: aws.Int32(int32(i + 1)),
			ChecksumCRC32C: up.ChecksumCRC32C,
		})
	}

	_, err = client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		ChecksumCRC32C:  aws.String("AAAAAA=="),
		ChecksumType:    types.ChecksumTypeFullObject,
	})
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched complete CRC32C = %v, want BadDigest", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}); err == nil {
		t.Error("the mismatched completion stored the object")
	}

	done, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		ChecksumCRC32C:  aws.String(sum),
		ChecksumType:    types.ChecksumTypeFullObject,
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := aws.ToString(done.ChecksumCRC32C); got != sum {
		t.Errorf("complete checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC32C); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}
	if head.ChecksumType != types.ChecksumTypeFullObject {
		t.Errorf("HEAD checksum type = %q, want FULL_OBJECT", head.ChecksumType)
	}

	get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer get.Body.Close()
	got, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCRC32OnAMultipartUploadIsVerifiedAndReplayed(t *testing.T) {
	client := newGateway(t)
	const bucket, key = "bucket", "checked-mpu32.bin"
	p1, p2 := []byte("hello "), []byte("ieee parts")
	want := append(append([]byte{}, p1...), p2...)
	sum := crc32Of(want)

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
		ChecksumType:      types.ChecksumTypeFullObject,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if create.ChecksumAlgorithm != types.ChecksumAlgorithmCrc32 {
		t.Errorf("create algorithm = %q, want CRC32", create.ChecksumAlgorithm)
	}
	id := create.UploadId

	_, err = client.UploadPart(t.Context(), &awss3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		PartNumber: aws.Int32(1), Body: bytes.NewReader(p1),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
		ChecksumCRC32:     aws.String("AAAAAA=="),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched part CRC32 = %v, want BadDigest", err)
	}

	var parts []types.CompletedPart
	for i, data := range [][]byte{p1, p2} {
		up, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
			PartNumber:        aws.Int32(int32(i + 1)),
			Body:              bytes.NewReader(data),
			ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
			ChecksumCRC32:     aws.String(crc32Of(data)),
		})
		if err != nil {
			t.Fatalf("upload part %d: %v", i+1, err)
		}
		if got := aws.ToString(up.ChecksumCRC32); got != crc32Of(data) {
			t.Errorf("part %d checksum = %q, want %q", i+1, got, crc32Of(data))
		}
		parts = append(parts, types.CompletedPart{
			ETag: up.ETag, PartNumber: aws.Int32(int32(i + 1)),
			ChecksumCRC32: up.ChecksumCRC32,
		})
	}

	_, err = client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		ChecksumCRC32:   aws.String("AAAAAA=="),
		ChecksumType:    types.ChecksumTypeFullObject,
	})
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched complete CRC32 = %v, want BadDigest", err)
	}
	if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}); err == nil {
		t.Error("the mismatched completion stored the object")
	}

	done, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		ChecksumCRC32:   aws.String(sum),
		ChecksumType:    types.ChecksumTypeFullObject,
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := aws.ToString(done.ChecksumCRC32); got != sum {
		t.Errorf("complete checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC32); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}

	get, err := client.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer get.Body.Close()
	got, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestCRC64NVMEOnAMultipartUploadIsVerifiedAndReplayed(t *testing.T) {
	client := newGateway(t)
	const bucket, key = "bucket", "checked-mpu64.bin"
	p1, p2 := []byte("hello "), []byte("crc64 parts")
	want := append(append([]byte{}, p1...), p2...)
	sum := crc64nvmeOf(want)

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
		ChecksumType:      types.ChecksumTypeFullObject,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if create.ChecksumAlgorithm != types.ChecksumAlgorithmCrc64nvme {
		t.Errorf("create algorithm = %q, want CRC64NVME", create.ChecksumAlgorithm)
	}
	id := create.UploadId

	_, err = client.UploadPart(t.Context(), &awss3.UploadPartInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		PartNumber: aws.Int32(1), Body: bytes.NewReader(p1),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
		ChecksumCRC64NVME: aws.String("AAAAAAAAAAA="),
	})
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched part CRC64NVME = %v, want BadDigest", err)
	}

	var parts []types.CompletedPart
	for i, data := range [][]byte{p1, p2} {
		up, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
			PartNumber:        aws.Int32(int32(i + 1)),
			Body:              bytes.NewReader(data),
			ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
			ChecksumCRC64NVME: aws.String(crc64nvmeOf(data)),
		})
		if err != nil {
			t.Fatalf("upload part %d: %v", i+1, err)
		}
		if got := aws.ToString(up.ChecksumCRC64NVME); got != crc64nvmeOf(data) {
			t.Errorf("part %d checksum = %q, want %q", i+1, got, crc64nvmeOf(data))
		}
		parts = append(parts, types.CompletedPart{
			ETag:              up.ETag,
			PartNumber:        aws.Int32(int32(i + 1)),
			ChecksumCRC64NVME: up.ChecksumCRC64NVME,
		})
	}

	_, err = client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		MultipartUpload:   &types.CompletedMultipartUpload{Parts: parts},
		ChecksumCRC64NVME: aws.String("AAAAAAAAAAA="),
		ChecksumType:      types.ChecksumTypeFullObject,
	})
	if !errors.As(err, &api) || api.ErrorCode() != "BadDigest" {
		t.Errorf("mismatched complete CRC64NVME = %v, want BadDigest", err)
	}

	done, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		MultipartUpload:   &types.CompletedMultipartUpload{Parts: parts},
		ChecksumCRC64NVME: aws.String(sum),
		ChecksumType:      types.ChecksumTypeFullObject,
	})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if got := aws.ToString(done.ChecksumCRC64NVME); got != sum {
		t.Errorf("complete checksum = %q, want %q", got, sum)
	}

	head, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	if got := aws.ToString(head.ChecksumCRC64NVME); got != sum {
		t.Errorf("HEAD checksum = %q, want %q", got, sum)
	}
}
