// Package sigv4 verifies AWS Signature Version 4 on incoming requests.
//
// Verification, not signing: kavo is the server. The signature is recomputed
// from the request as it arrived and compared with what the client sent, which
// means every byte the signature covers has to be reconstructed exactly —
// including the parts Go's http package moves out of the header map.
//
// The payload is verified as it streams. A body is never buffered to hash it,
// because an object may be larger than memory; instead the hash runs alongside
// the read and the mismatch is reported in place of the final EOF, so a caller
// that reads to the end cannot mistake unverified bytes for verified ones.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Errors are separate because each maps to a different S3 error code, and a
// client that gets "access denied" for a clock problem will keep retrying.
var (
	ErrMissingSignature = errors.New("sigv4: request is not signed")
	ErrMalformed        = errors.New("sigv4: authorization header is malformed")
	ErrUnknownKey       = errors.New("sigv4: unknown access key")
	ErrMismatch         = errors.New("sigv4: signature does not match")
	ErrSkew             = errors.New("sigv4: request time is too far from ours")
	ErrPayload          = errors.New("sigv4: body does not match the declared checksum")
)

// MaxSkew is how far a client's clock may be from this node's. AWS uses 15
// minutes; a request older than that cannot be replayed for longer than that.
const MaxSkew = 15 * time.Minute

const (
	algorithm    = "AWS4-HMAC-SHA256"
	unsigned     = "UNSIGNED-PAYLOAD"
	streaming    = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingTrl = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	unsignedTrl  = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	// emptyHash is sha256 of no bytes, which every chunk signature includes.
	emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// Credentials is the single key pair the cluster accepts. There is no user
// directory: IAM is an explicit anti-goal, so authentication is proof of holding
// the one secret and authorization does not exist.
type Credentials struct {
	AccessKey string
	SecretKey string
}

// Verify authenticates r against creds, using now as this node's clock.
//
// On success r.Body is replaced with a reader that yields the object's bytes and
// fails if they do not match what the client signed, and r.ContentLength is set
// to how many bytes that is — so a handler downstream reads a plain body and
// cannot tell which payload mode the client used.
func Verify(r *http.Request, creds Credentials, now time.Time) error {
	auth, err := parseAuthorization(r.Header.Get("Authorization"))
	if err != nil {
		return err
	}
	if auth.accessKey != creds.AccessKey {
		return fmt.Errorf("%w: %s", ErrUnknownKey, auth.accessKey)
	}

	signedAt, err := requestTime(r)
	if err != nil {
		return err
	}
	if skew := now.Sub(signedAt); skew > MaxSkew || skew < -MaxSkew {
		return fmt.Errorf("%w: signed at %s, now %s", ErrSkew,
			signedAt.Format(time.RFC3339), now.Format(time.RFC3339))
	}
	// The scope's date is part of the signing key, so a mismatch would surface as
	// a signature failure anyway — but saying which one it was is the difference
	// between a client fixing its clock and a client guessing.
	if want := signedAt.UTC().Format("20060102"); auth.date != want {
		return fmt.Errorf("%w: credential scope says %s, request is dated %s", ErrMalformed, auth.date, want)
	}
	if auth.service != "s3" {
		return fmt.Errorf("%w: credential scope is for service %q", ErrMalformed, auth.service)
	}

	payload := r.Header.Get("x-amz-content-sha256")
	canonical, err := canonicalRequest(r, auth.signedHeaders, payload)
	if err != nil {
		return err
	}
	key := signingKey(creds.SecretKey, auth.date, auth.region, auth.service)
	signature := sign(key, stringToSign(signedAt, auth.scope(), sha256hex([]byte(canonical))))
	if !hmac.Equal([]byte(signature), []byte(auth.signature)) {
		return fmt.Errorf("%w: computed %s over\n%s", ErrMismatch, signature, canonical)
	}

	body, size, err := verifiedBody(r, payload, signature, signedAt, auth.scope(), key)
	if err != nil {
		return err
	}
	r.Body = body
	r.ContentLength = size
	return nil
}

// verifiedBody wraps the request body so that reading it also checks it, and
// reports how many bytes of object it will yield.
func verifiedBody(r *http.Request, payload, seed string, signedAt time.Time, scope string, key []byte) (io.ReadCloser, int64, error) {
	switch payload {
	case streaming, streamingTrl, unsignedTrl:
		// The body is aws-chunked, so its length is the encoding's; the object's
		// own length is a separate header, and without it a handler would store
		// the framing as data.
		raw := r.Header.Get("x-amz-decoded-content-length")
		size, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: streaming payload without a usable x-amz-decoded-content-length (%q)", ErrMalformed, raw)
		}
		signer := &chunkSigner{key: key, date: signedAt, scope: scope, prev: seed}
		if payload == unsignedTrl {
			signer = nil
		}
		return newChunkedReader(r.Body, signer), size, nil

	case unsigned, "":
		// The client chose not to cover the body. The signature still covers the
		// headers, so this is a client trading integrity for not having to hash;
		// an empty header is the same choice made by omission.
		return r.Body, r.ContentLength, nil

	default:
		want, err := hex.DecodeString(payload)
		if err != nil || len(want) != sha256.Size {
			return nil, 0, fmt.Errorf("%w: x-amz-content-sha256 is neither a hash nor a known mode (%q)", ErrMalformed, payload)
		}
		return &hashChecker{r: r.Body, h: sha256.New(), want: want}, r.ContentLength, nil
	}
}

// hashChecker fails a read at the end of the body unless the bytes hash to what
// the client signed. Reported in place of the final EOF, on the same reasoning as
// the chunk store's checksum reader: a caller that reads to EOF must not be able
// to see unverified bytes as complete.
type hashChecker struct {
	r    io.ReadCloser
	h    hash.Hash
	want []byte
}

func (c *hashChecker) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.h.Write(p[:n])
	if errors.Is(err, io.EOF) && !hmac.Equal(c.h.Sum(nil), c.want) {
		return n, fmt.Errorf("%w: body hashes to %x, signature declares %x", ErrPayload, c.h.Sum(nil), c.want)
	}
	return n, err
}

func (c *hashChecker) Close() error { return c.r.Close() }

// authorization is the parsed Authorization header.
type authorization struct {
	accessKey     string
	date          string // yyyymmdd, the first element of the credential scope
	region        string
	service       string
	signedHeaders []string
	signature     string
}

func (a authorization) scope() string {
	return strings.Join([]string{a.date, a.region, a.service, "aws4_request"}, "/")
}

// parseAuthorization reads the header S3 clients send:
//
//	AWS4-HMAC-SHA256 Credential=AK/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=hex
func parseAuthorization(header string) (authorization, error) {
	rest, ok := strings.CutPrefix(header, algorithm+" ")
	if !ok {
		if header == "" {
			return authorization{}, ErrMissingSignature
		}
		return authorization{}, fmt.Errorf("%w: not %s", ErrMalformed, algorithm)
	}

	var a authorization
	var credential string
	for _, part := range strings.Split(rest, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return authorization{}, fmt.Errorf("%w: %q is not name=value", ErrMalformed, part)
		}
		switch name {
		case "Credential":
			credential = value
		case "SignedHeaders":
			a.signedHeaders = strings.Split(value, ";")
		case "Signature":
			a.signature = value
		}
	}

	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[4] != "aws4_request" {
		return authorization{}, fmt.Errorf("%w: credential %q is not key/date/region/service/aws4_request", ErrMalformed, credential)
	}
	a.accessKey, a.date, a.region, a.service = scope[0], scope[1], scope[2], scope[3]
	if a.signature == "" || len(a.signedHeaders) == 0 {
		return authorization{}, fmt.Errorf("%w: missing Signature or SignedHeaders", ErrMalformed)
	}
	// The host header is what ties a signature to this endpoint. Without it a
	// signature captured for another host would be replayable here.
	if !slices.Contains(a.signedHeaders, "host") {
		return authorization{}, fmt.Errorf("%w: host is not among the signed headers", ErrMalformed)
	}
	return a, nil
}

// requestTime is when the client says it signed, from the header AWS defines for
// it or, failing that, the ordinary Date header.
func requestTime(r *http.Request) (time.Time, error) {
	if raw := r.Header.Get("x-amz-date"); raw != "" {
		t, err := time.Parse("20060102T150405Z", raw)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: x-amz-date %q", ErrMalformed, raw)
		}
		return t, nil
	}
	if raw := r.Header.Get("Date"); raw != "" {
		t, err := http.ParseTime(raw)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: Date %q", ErrMalformed, raw)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("%w: neither x-amz-date nor Date", ErrMalformed)
}

// canonicalRequest rebuilds the string the client hashed. Every difference from
// what the client built is a rejected request, so the fiddly parts — which
// headers, in what order, how the path and query are encoded — are the whole job.
func canonicalRequest(r *http.Request, signedHeaders []string, payload string) (string, error) {
	headers := make([]string, 0, len(signedHeaders))
	for _, name := range signedHeaders {
		value, ok := headerValue(r, name)
		if !ok {
			return "", fmt.Errorf("%w: signed header %q is not in the request", ErrMalformed, name)
		}
		headers = append(headers, name+":"+value+"\n")
	}
	if payload == "" {
		payload = unsigned
	}
	return strings.Join([]string{
		r.Method,
		canonicalPath(r.URL),
		canonicalQuery(r.URL),
		strings.Join(headers, ""),
		strings.Join(signedHeaders, ";"),
		payload,
	}, "\n"), nil
}

// headerValue reads a signed header, including the two Go does not leave in the
// header map. Getting these from Header would silently sign an empty value and
// fail every request that signs them — which is most of them, since host is
// mandatory.
func headerValue(r *http.Request, name string) (string, bool) {
	switch name {
	case "host":
		return r.Host, r.Host != ""
	case "content-length":
		if r.ContentLength < 0 {
			return "", false
		}
		return strconv.FormatInt(r.ContentLength, 10), true
	}
	values, ok := r.Header[http.CanonicalHeaderKey(name)]
	if !ok {
		return "", false
	}
	// Sequential spaces in a value collapse, per the spec, because a proxy is
	// allowed to reformat whitespace and the signature must survive it.
	trimmed := make([]string, len(values))
	for i, v := range values {
		trimmed[i] = strings.Join(strings.Fields(v), " ")
	}
	return strings.Join(trimmed, ","), true
}

// canonicalPath is the request path as the client sent it. S3 signs the path
// without normalising or re-encoding it, unlike every other AWS service: a key
// may legitimately contain "." or ".." segments, and rewriting them would make
// such an object unreachable rather than merely oddly named.
func canonicalPath(u *url.URL) string {
	if p := u.EscapedPath(); p != "" {
		return p
	}
	return "/"
}

// canonicalQuery sorts the query and re-encodes it the way AWS specifies, which
// is not the way url.Values.Encode does: space is %20, not +, and every
// character outside the unreserved set is escaped.
func canonicalQuery(u *url.URL) string {
	values := u.Query()
	pairs := make([]string, 0, len(values))
	for name, vs := range values {
		slices.Sort(vs)
		for _, v := range vs {
			pairs = append(pairs, uriEncode(name)+"="+uriEncode(v))
		}
	}
	slices.Sort(pairs)
	return strings.Join(pairs, "&")
}

// uriEncode percent-encodes everything but the unreserved characters, as AWS
// defines it. url.QueryEscape is close but writes "+" for space and leaves "~"
// alone in some versions, and either difference breaks every signature.
func uriEncode(s string) string {
	const unreserved = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.~"
	var b strings.Builder
	for i := range len(s) {
		if c := s[i]; strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func stringToSign(t time.Time, scope, hashedRequest string) string {
	return strings.Join([]string{
		algorithm,
		t.UTC().Format("20060102T150405Z"),
		scope,
		hashedRequest,
	}, "\n")
}

// signingKey derives the key for one day, one region and one service, so a
// leaked signature is useless outside that scope.
func signingKey(secret, date, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	return hmacSHA256(k, "aws4_request")
}

func sign(key []byte, data string) string { return hex.EncodeToString(hmacSHA256(key, data)) }

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
