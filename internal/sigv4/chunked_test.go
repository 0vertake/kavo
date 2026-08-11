package sigv4_test

// aws-chunked bodies, built with the AWS SDK's stream signer so the chained
// signatures are an independent implementation's idea of correct rather than
// this one's.

import (
	"bytes"
	"cmp"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/0vertake/kavo/internal/sigv4"
)

// chunkedBody encodes data as aws-chunked in chunks of at most size bytes,
// signing each one against the seed, and returns the encoded body.
func chunkedBody(t testing.TB, seed string, data []byte, size int, trailer string) []byte {
	t.Helper()
	provider := credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, "")
	c, err := provider.Retrieve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The SDK keeps signatures as raw bytes and hex-encodes them into the next
	// string to sign, so the seed goes in decoded.
	raw, err := hex.DecodeString(seed)
	if err != nil {
		t.Fatalf("seed signature %q: %v", seed, err)
	}
	signer := v4.NewStreamSigner(c, "s3", region, raw)

	var body bytes.Buffer
	write := func(chunk []byte) {
		sig, err := signer.GetSignature(t.Context(), nil, chunk, signedAt)
		if err != nil {
			t.Fatalf("sign chunk: %v", err)
		}
		fmt.Fprintf(&body, "%x;chunk-signature=%x\r\n", len(chunk), sig)
		body.Write(chunk)
		if len(chunk) > 0 {
			body.WriteString("\r\n")
		}
	}
	for len(data) > 0 {
		n := min(size, len(data))
		write(data[:n])
		data = data[n:]
	}
	// A zero-length chunk ends the body, and whatever follows it is trailers.
	write(nil)
	body.WriteString(cmp.Or(trailer, "\r\n"))
	return body.Bytes()
}

// streamingRequest builds a PUT whose body is aws-chunked, the way a client that
// cannot hash its payload in advance sends one.
func streamingRequest(t testing.TB, data []byte, chunkSize int, mode, trailer string) *http.Request {
	t.Helper()
	r := request(t, "PUT", "/bucket/streamed", nil)
	r.Header.Set("x-amz-decoded-content-length", fmt.Sprint(len(data)))
	r.ContentLength = 0 // filled in once the body is built, since it is signed

	// The seed signature is the request's own, so the body's chain is anchored to
	// the headers: chunks from another request cannot be spliced in.
	signWithAWS(t, r, nil, mode)
	seed := r.Header.Get("Authorization")
	seed = seed[strings.LastIndex(seed, "Signature=")+len("Signature="):]

	var body []byte
	if mode == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" {
		body = unsignedChunkedBody(data, chunkSize, trailer)
	} else {
		body = chunkedBody(t, seed, data, chunkSize, trailer)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return r
}

// unsignedChunkedBody is the same framing without the per-chunk signatures, which
// is what a client sending STREAMING-UNSIGNED-PAYLOAD-TRAILER produces.
func unsignedChunkedBody(data []byte, size int, trailer string) []byte {
	var body bytes.Buffer
	for len(data) > 0 {
		n := min(size, len(data))
		fmt.Fprintf(&body, "%x\r\n%s\r\n", n, data[:n])
		data = data[n:]
	}
	body.WriteString("0\r\n")
	body.WriteString(cmp.Or(trailer, "\r\n"))
	return body.Bytes()
}

// The claim: a chunked body reads back as the object, and its per-chunk
// signatures are checked on the way through.
func TestStreamingBodyReadsBackAsTheObject(t *testing.T) {
	data := []byte(strings.Repeat("kavo streams objects it cannot hold. ", 1000))
	tests := []struct {
		name      string
		mode      string
		chunkSize int
		data      []byte
		trailer   string
	}{
		{name: "one chunk", mode: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", chunkSize: len(data), data: data},
		{name: "many chunks", mode: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", chunkSize: 1024, data: data},
		{name: "chunk boundary at the exact end", mode: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", chunkSize: len(data) / 4, data: data[:(len(data)/4)*4]},
		{name: "empty object", mode: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", chunkSize: 1024, data: nil},
		{name: "with a checksum trailer", mode: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", chunkSize: 4096, data: data,
			trailer: "x-amz-checksum-crc32:AAAAAA==\r\n\r\n"},
		{name: "unsigned chunks with a trailer", mode: "STREAMING-UNSIGNED-PAYLOAD-TRAILER", chunkSize: 4096, data: data,
			trailer: "x-amz-checksum-crc32:AAAAAA==\r\n\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := streamingRequest(t, tt.data, tt.chunkSize, tt.mode, tt.trailer)
			if err := sigv4.Verify(r, creds, signedAt); err != nil {
				t.Fatalf("verify: %v", err)
			}
			// The handler is told the object's length, not the encoding's, or it
			// would store the framing as data.
			if r.ContentLength != int64(len(tt.data)) {
				t.Errorf("ContentLength = %d, want the decoded %d", r.ContentLength, len(tt.data))
			}
			got, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !bytes.Equal(got, tt.data) {
				t.Errorf("read %d bytes, want %d", len(got), len(tt.data))
			}
		})
	}
}

// Every way a chunked body can be wrong. All of them have to fail, because the
// body is the object: a chunked body accepted with a broken chain is an object
// stored from bytes nobody signed.
func TestStreamingRejections(t *testing.T) {
	data := []byte(strings.Repeat("payload ", 1000))
	const mode = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

	tests := []struct {
		name string
		// tamper rewrites the body as whole chunks, framing included, so that
		// each case breaks exactly the one thing it names.
		tamper func(testing.TB, [][]byte) []byte
		want   error
	}{
		{
			name: "a chunk's data was changed",
			tamper: func(t testing.TB, chunks [][]byte) []byte {
				chunks[1] = bytes.Replace(chunks[1], []byte("payload"), []byte("PAYLOAD"), 1)
				return bytes.Join(chunks, nil)
			},
			want: sigv4.ErrMismatch,
		},
		{
			name: "a chunk signature was changed",
			tamper: func(t testing.TB, chunks [][]byte) []byte {
				chunks[0] = []byte(flipSignature(string(chunks[0])))
				return bytes.Join(chunks, nil)
			},
			want: sigv4.ErrMismatch,
		},
		{
			name: "the body stops before the final chunk",
			// Every chunk present is valid and correctly chained: what is
			// missing is the statement that there are no more. Accepting this
			// stores a truncated object as a whole one.
			tamper: func(t testing.TB, chunks [][]byte) []byte {
				return bytes.Join(chunks[:len(chunks)-1], nil)
			},
			want: sigv4.ErrPayload,
		},
		{
			name: "the body is cut off in the middle of a chunk",
			tamper: func(t testing.TB, chunks [][]byte) []byte {
				body := bytes.Join(chunks[:len(chunks)-1], nil)
				return body[:len(body)-10]
			},
			want: sigv4.ErrPayload,
		},
		{
			name: "a chunk header is not a number",
			tamper: func(t testing.TB, chunks [][]byte) []byte {
				return append([]byte("zzzz;chunk-signature=00\r\n"), bytes.Join(chunks, nil)...)
			},
			want: sigv4.ErrMalformed,
		},
		{
			name: "a chunk carries no signature at all",
			tamper: func(t testing.TB, chunks [][]byte) []byte {
				return append([]byte("10\r\n0123456789abcdef\r\n"), bytes.Join(chunks, nil)...)
			},
			want: sigv4.ErrMalformed,
		},
		{
			name: "a framing line runs on forever",
			tamper: func(t testing.TB, chunks [][]byte) []byte {
				return append([]byte(strings.Repeat("f", 4096)), bytes.Join(chunks, nil)...)
			},
			want: sigv4.ErrMalformed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := streamingRequest(t, data, 1024, mode, "")
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatal(err)
			}
			r.Body = io.NopCloser(bytes.NewReader(tt.tamper(t, splitChunks(t, body))))

			// The headers are untouched, so authentication itself succeeds: what
			// is under test is that reading the body fails.
			if err := sigv4.Verify(r, creds, signedAt); err != nil {
				t.Fatalf("verify: %v", err)
			}
			if _, err := io.Copy(io.Discard, r.Body); !errors.Is(err, tt.want) {
				t.Fatalf("read = %v, want %v", err, tt.want)
			}
		})
	}
}

// A signed chunk chain must not be splittable: each signature covers the previous
// one, so a chunk lifted from anywhere else in the stream cannot verify where it
// was put. Without the chain, an attacker with one valid body could reorder or
// duplicate its chunks freely.
func TestChunksCannotBeReordered(t *testing.T) {
	data := []byte(strings.Repeat("A", 1024) + strings.Repeat("B", 1024) + strings.Repeat("C", 1024))
	r := streamingRequest(t, data, 1024, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}

	// Swap the second and third chunks, keeping every signature with its own data.
	chunks := splitChunks(t, body)
	if len(chunks) != 4 {
		t.Fatalf("built %d chunks, want 3 and a terminator", len(chunks))
	}
	chunks[1], chunks[2] = chunks[2], chunks[1]
	r.Body = io.NopCloser(bytes.NewReader(bytes.Join(chunks, nil)))

	if err := sigv4.Verify(r, creds, signedAt); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if _, err := io.Copy(io.Discard, r.Body); !errors.Is(err, sigv4.ErrMismatch) {
		t.Fatalf("read = %v, want %v", err, sigv4.ErrMismatch)
	}
}

// splitChunks cuts an encoded body into whole chunks, framing included.
func splitChunks(t testing.TB, body []byte) [][]byte {
	t.Helper()
	var chunks [][]byte
	for len(body) > 0 {
		head := bytes.Index(body, []byte("\r\n"))
		if head < 0 {
			t.Fatalf("no chunk header in %q", body[:min(64, len(body))])
		}
		var size int
		if _, err := fmt.Sscanf(string(body[:head]), "%x", &size); err != nil {
			t.Fatalf("chunk header %q: %v", body[:head], err)
		}
		end := head + 2 + size + 2
		if end > len(body) {
			end = len(body)
		}
		chunks = append(chunks, body[:end])
		body = body[end:]
	}
	return chunks
}

// Streaming exists so a client can send an object it has not hashed; the server
// must be able to receive one it cannot hold. Memory has to stay flat in the
// object's size, not in the chunk's.
func TestStreamingBodyIsVerifiedWithoutBeingHeld(t *testing.T) {
	// 64 MB in 64 KB chunks: a thousand links of chain, none of them retained.
	const size = 64 << 20
	data := bytes.Repeat([]byte("kavo"), size/4)
	r := streamingRequest(t, data, 64<<10, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", "")

	if err := sigv4.Verify(r, creds, signedAt); err != nil {
		t.Fatalf("verify: %v", err)
	}
	before := heapInUse()
	n, err := io.CopyBuffer(io.Discard, r.Body, make([]byte, 32<<10))
	if err != nil {
		t.Fatalf("read %d bytes then failed: %v", n, err)
	}
	if n != size {
		t.Fatalf("read %d bytes, want %d", n, size)
	}
	// Generous, because this is asserting the absence of a whole-object buffer,
	// not a memory budget: the encoded body itself is in memory in this test.
	if grew := heapInUse() - before; grew > 8<<20 {
		t.Errorf("reading %d bytes grew the heap by %d bytes", size, grew)
	}
}

// heapInUse is bytes of live heap, after a collection so that garbage from
// earlier reads is not counted as growth.
func heapInUse() int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapInuse)
}
