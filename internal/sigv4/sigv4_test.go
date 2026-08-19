package sigv4_test

// The oracle for these tests is AWS's own signer, used as a test-only
// dependency. Hand-written vectors would only prove that the implementation
// agrees with whatever this author believed the spec said; a second
// implementation disagreeing is the only thing that catches a misreading.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/0vertake/kavo/internal/sigv4"
)

var creds = sigv4.Credentials{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}

const region = "us-east-1"

var signedAt = time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)

// signWithAWS signs a request the way a client would, using the AWS SDK.
func signWithAWS(t testing.TB, r *http.Request, body []byte, payloadHash string) *http.Request {
	t.Helper()
	if payloadHash == "" {
		sum := sha256.Sum256(body)
		payloadHash = hex.EncodeToString(sum[:])
	}
	r.Header.Set("x-amz-content-sha256", payloadHash)
	if r.ContentLength == 0 && len(body) > 0 {
		r.ContentLength = int64(len(body))
	}

	provider := credentials.NewStaticCredentialsProvider(creds.AccessKey, creds.SecretKey, "")
	c, err := provider.Retrieve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// S3 signs the path exactly as it was sent, where every other AWS service
	// re-escapes it. Getting this wrong makes keys containing a space, a plus or
	// any non-ASCII character unreachable — and only those, which is why the
	// oracle has to be configured the way an S3 client configures it.
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	if err := signer.SignHTTP(t.Context(), c, r, payloadHash, "s3", region, signedAt); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return r
}

// request builds a server-side request: what the handler sees, with Host lifted
// out of the header map the way net/http does it.
func request(t testing.TB, method, target string, body []byte) *http.Request {
	t.Helper()
	r, err := http.NewRequest(method, "http://kavo.example.com"+target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Host = "kavo.example.com"
	return r
}

// The base claim: a request the AWS signer signed verifies, and the body reads
// back byte for byte. Each case is a part of the canonical request that is easy
// to get wrong and impossible to notice from the outside — every one of them
// fails closed, so a bug here looks like "every upload is rejected".
func TestRequestsSignedByTheAWSSignerVerify(t *testing.T) {
	body := []byte("the object's bytes")
	tests := []struct {
		name    string
		method  string
		target  string
		body    []byte
		headers map[string]string
	}{
		{name: "put an object", method: "PUT", target: "/bucket/key.txt", body: body},
		{name: "get an object", method: "GET", target: "/bucket/key.txt"},
		{name: "empty body", method: "PUT", target: "/bucket/empty", body: []byte{}},
		{name: "key with spaces", method: "PUT", target: "/bucket/two%20words.txt", body: body},
		{name: "key with a plus", method: "PUT", target: "/bucket/a%2Bb.txt", body: body},
		{name: "key with unicode", method: "PUT", target: "/bucket/%E2%98%95.txt", body: body},
		{name: "key that looks like a traversal", method: "PUT", target: "/bucket/a/../b.txt", body: body},
		{name: "query with a subresource", method: "POST", target: "/bucket/key.txt?uploads"},
		{name: "query needing sorted encoding", method: "GET", target: "/bucket?list-type=2&prefix=a%2Fb&max-keys=10"},
		{name: "query value with spaces", method: "GET", target: "/bucket?prefix=two%20words"},
		{name: "extra signed headers", method: "PUT", target: "/bucket/key.txt", body: body, headers: map[string]string{
			"Content-Type":            "text/plain",
			"x-amz-meta-source":       "test",
			"x-amz-storage-class":     "STANDARD",
			"x-amz-sdk-checksum-algo": "CRC32",
		}},
		{name: "header value with folded spaces", method: "PUT", target: "/bucket/key.txt", body: body, headers: map[string]string{
			"Content-Type": "text/plain;    charset=utf-8",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := request(t, tt.method, tt.target, tt.body)
			for k, v := range tt.headers {
				r.Header.Set(k, v)
			}
			signWithAWS(t, r, tt.body, "")

			if err := sigv4.Verify(r, creds, signedAt); err != nil {
				t.Fatalf("verify: %v", err)
			}
			got, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read verified body: %v", err)
			}
			if !bytes.Equal(got, tt.body) {
				t.Errorf("body = %q, want %q", got, tt.body)
			}
		})
	}
}

// Every way a request can fail authentication, and the error it must be
// distinguishable by: a client told "access denied" for a clock problem retries
// forever, and one told "bad signature" for an unknown key never learns.
func TestRejections(t *testing.T) {
	body := []byte("payload")
	tests := []struct {
		name string
		// tamper runs after the request is signed.
		tamper func(*http.Request)
		now    time.Time
		want   error
	}{
		{
			name:   "no authorization header",
			tamper: func(r *http.Request) { r.Header.Del("Authorization") },
			want:   sigv4.ErrMissingSignature,
		},
		{
			name:   "another scheme entirely",
			tamper: func(r *http.Request) { r.Header.Set("Authorization", "Basic dXNlcjpwYXNz") },
			want:   sigv4.ErrMalformed,
		},
		{
			name: "credential is not a scope",
			tamper: func(r *http.Request) {
				r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=key, SignedHeaders=host, Signature=aa")
			},
			want: sigv4.ErrMalformed,
		},
		{
			name: "host is not signed",
			tamper: func(r *http.Request) {
				r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "host;", "", 1))
			},
			want: sigv4.ErrMalformed,
		},
		{
			name: "unknown access key",
			tamper: func(r *http.Request) {
				r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"),
					creds.AccessKey, "AKIAINTRUDER00000000", 1))
			},
			want: sigv4.ErrUnknownKey,
		},
		{
			name:   "signature does not match",
			tamper: func(r *http.Request) { r.Header.Set("Authorization", flipSignature(r.Header.Get("Authorization"))) },
			want:   sigv4.ErrMismatch,
		},
		{
			name:   "a signed header was changed in flight",
			tamper: func(r *http.Request) { r.Header.Set("Content-Type", "application/json") },
			want:   sigv4.ErrMismatch,
		},
		{
			name:   "the path was changed in flight",
			tamper: func(r *http.Request) { r.URL.Path = "/bucket/other.txt" },
			want:   sigv4.ErrMismatch,
		},
		{
			name:   "the query was changed in flight",
			tamper: func(r *http.Request) { r.URL.RawQuery = "list-type=2&prefix=elsewhere" },
			want:   sigv4.ErrMismatch,
		},
		{
			name:   "the request was replayed at another endpoint",
			tamper: func(r *http.Request) { r.Host = "someone-else.example.com" },
			want:   sigv4.ErrMismatch,
		},
		{
			name:   "no date at all",
			tamper: func(r *http.Request) { r.Header.Del("X-Amz-Date") },
			want:   sigv4.ErrMissingDate,
		},
		{
			name: "x-amz-date is HTTP-date",
			tamper: func(r *http.Request) {
				r.Header.Set("X-Amz-Date", signedAt.UTC().Format(http.TimeFormat))
			},
			// Parsed, so this is a signature over a different header value rather
			// than a missing timestamp.
			want: sigv4.ErrMismatch,
		},
		{
			name: "x-amz-date is RFC822 with -0000",
			tamper: func(r *http.Request) {
				r.Header.Set("X-Amz-Date", "Tue, 11 Aug 2026 12:00:00 -0000")
			},
			want: sigv4.ErrMismatch,
		},
		{
			name:   "unparseable date",
			tamper: func(r *http.Request) { r.Header.Set("X-Amz-Date", "yesterday") },
			want:   sigv4.ErrMalformed,
		},
		{
			name: "signed an hour ago",
			now:  signedAt.Add(time.Hour),
			want: sigv4.ErrSkew,
		},
		{
			name: "signed an hour from now",
			now:  signedAt.Add(-time.Hour),
			want: sigv4.ErrSkew,
		},
		{
			name: "scope date is not the request's date",
			tamper: func(r *http.Request) {
				r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"),
					"/20260811/", "/20260810/", 1))
			},
			want: sigv4.ErrMalformed,
		},
		{
			name: "scope is for another service",
			tamper: func(r *http.Request) {
				r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"),
					"/s3/aws4_request", "/sqs/aws4_request", 1))
			},
			want: sigv4.ErrMalformed,
		},
		{
			name:   "payload hash is neither a hash nor a mode",
			tamper: func(r *http.Request) { r.Header.Set("x-amz-content-sha256", "MAYBE-SIGNED") },
			// Changing it invalidates the signature first, which is the stricter
			// of the two answers and the one a client can act on.
			want: sigv4.ErrMismatch,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := request(t, "PUT", "/bucket/key.txt?list-type=2&prefix=a", body)
			r.Header.Set("Content-Type", "text/plain")
			signWithAWS(t, r, body, "")
			if tt.tamper != nil {
				tt.tamper(r)
			}
			now := tt.now
			if now.IsZero() {
				now = signedAt
			}

			err := sigv4.Verify(r, creds, now)
			if !errors.Is(err, tt.want) {
				t.Fatalf("verify = %v, want %v", err, tt.want)
			}
		})
	}
}

// A signed payload is a promise about the body, so bytes that do not match it
// must not reach the caller as a complete read. The failure has to arrive in
// place of the final EOF: this is a stream, and by the time the last byte is
// hashed the earlier ones have already been handed on.
func TestASignedBodyThatChangesInFlightFailsAtTheEnd(t *testing.T) {
	body := []byte("the object the client signed")
	r := request(t, "PUT", "/bucket/key.txt", body)
	signWithAWS(t, r, body, "")

	// A bit flip, a proxy, or an attacker. The length is unchanged, because
	// content-length is signed and this is about the bytes rather than the
	// framing: the signature still verifies and only the body is wrong.
	tampered := bytes.Replace(body, []byte("client"), []byte("cli3nt"), 1)
	r.Body = io.NopCloser(bytes.NewReader(tampered))

	if err := sigv4.Verify(r, creds, signedAt); err != nil {
		t.Fatalf("verify: %v", err)
	}
	got, err := io.ReadAll(r.Body)
	if !errors.Is(err, sigv4.ErrPayload) {
		t.Fatalf("read = %v, want %v", err, sigv4.ErrPayload)
	}
	// Whatever it returned before failing is not a usable object, which is the
	// point of failing at the end rather than not at all.
	if bytes.Equal(got, body) {
		t.Error("the tampered body read back as the signed one")
	}
}

// UNSIGNED-PAYLOAD is a client saying it will not cover the body. The headers are
// still signed, so the request is authentic; the body simply is not checked, and
// pretending otherwise would reject every client that makes that choice.
func TestUnsignedPayloadIsAcceptedAndNotChecked(t *testing.T) {
	body := []byte("not covered by the signature")
	r := request(t, "PUT", "/bucket/key.txt", body)
	signWithAWS(t, r, body, "UNSIGNED-PAYLOAD")

	other := []byte("something else, same length!")
	r.Body = io.NopCloser(bytes.NewReader(other))

	if err := sigv4.Verify(r, creds, signedAt); err != nil {
		t.Fatalf("verify: %v", err)
	}
	got, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, other) {
		t.Errorf("body = %q, want %q", got, other)
	}
}

// Hashing the body must not mean holding it. The reader is handed a body far
// larger than any buffer it could reasonably allocate, and told to verify it.
func TestASignedBodyIsVerifiedWithoutBeingHeld(t *testing.T) {
	const size = 64 << 20
	sum := sha256.New()
	io.Copy(sum, io.LimitReader(zeros{}, size))
	hash := hex.EncodeToString(sum.Sum(nil))

	r := request(t, "PUT", "/bucket/big", nil)
	r.ContentLength = size
	signWithAWS(t, r, nil, hash)
	r.Body = io.NopCloser(io.LimitReader(zeros{}, size))

	if err := sigv4.Verify(r, creds, signedAt); err != nil {
		t.Fatalf("verify: %v", err)
	}
	n, err := io.Copy(io.Discard, r.Body)
	if err != nil {
		t.Fatalf("read %d bytes then failed: %v", n, err)
	}
	if n != size {
		t.Errorf("read %d bytes, want %d", n, size)
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// flipSignature changes one character of a Signature= or chunk-signature= value,
// keeping its shape so that what fails is the comparison and not the parsing.
func flipSignature(header string) string {
	i := strings.LastIndex(header, "ignature=") + len("ignature=")
	swap := map[byte]byte{'a': 'b'}
	c, ok := swap[header[i]]
	if !ok {
		c = 'a'
	}
	return fmt.Sprintf("%s%c%s", header[:i], c, header[i+1:])
}
