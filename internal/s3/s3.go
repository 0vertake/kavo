// Package s3 serves the S3 API subset kavo speaks: PUT, GET, HEAD and DELETE of
// objects, addressed path-style as /{bucket}/{key}.
//
// This is the only surface clients touch. The peer and debug endpoints live on a
// separate listener, because a client that can reach them can delete chunks.
//
// Buckets are key prefixes rather than a namespace: every object's key in etcd is
// "bucket/key", which is what makes a listing a prefix scan and keeps two buckets'
// objects from colliding. A bucket therefore exists exactly when an object names
// it, and the bucket operations in bucket.go answer for a record that is not there.
package s3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/sigv4"
)

type handler struct {
	cluster *cluster.Coordinator
	creds   sigv4.Credentials
}

// New returns the S3 gateway. Every request must be signed with creds: there is
// no anonymous access, because the alternative is an object store that anyone who
// can reach the port can empty.
func New(c *cluster.Coordinator, creds sigv4.Credentials) http.Handler {
	h := &handler{cluster: c, creds: creds}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /{bucket}/{key...}", h.authed(h.putObject))
	mux.HandleFunc("GET /{bucket}/{key...}", h.authed(h.getObject))
	mux.HandleFunc("HEAD /{bucket}/{key...}", h.authed(h.getObject))
	mux.HandleFunc("DELETE /{bucket}/{key...}", h.authed(h.deleteObject))
	mux.HandleFunc("POST /{bucket}/{key...}", h.authed(h.postObject))
	// A bucket is a prefix, so it exists as soon as it is named. Clients ask
	// before uploading, and answering "no such bucket" would be a lie that stops
	// them. "/{bucket}/" reaches the object routes with an empty key, which
	// getObject sends here.
	mux.HandleFunc("HEAD /{bucket}", h.authed(h.headBucket))
	mux.HandleFunc("GET /{bucket}", h.authed(h.listObjects))
	// Nothing to create or delete, but clients call these before their first
	// upload and after their last, and an unrouted PUT /{bucket} reaches the
	// object patterns as a redirect — an answer no client can make sense of.
	mux.HandleFunc("PUT /{bucket}", h.authed(h.createBucket))
	mux.HandleFunc("DELETE /{bucket}", h.authed(h.deleteBucket))
	mux.HandleFunc("POST /{bucket}", h.authed(h.postBucket))
	mux.HandleFunc("GET /{$}", h.authed(h.listBuckets))
	mux.HandleFunc("/", h.authed(h.unsupported))
	return mux
}

// authed verifies the signature before the request is allowed to touch anything.
func (h *handler) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := sigv4.Verify(r, h.creds, time.Now()); err != nil {
			fail(w, r, authError(err), err)
			return
		}
		next(w, r)
	}
}

// key is the etcd key for an object: the bucket and the key within it. Both parts
// come from the path, so this is also where a request that names neither is
// refused.
func objectKey(r *http.Request) (string, bool) {
	bucket, key := r.PathValue("bucket"), r.PathValue("key")
	if bucket == "" || key == "" || strings.Contains(bucket, "/") {
		return "", false
	}
	return bucket + "/" + key, true
}

func (h *handler) putObject(w http.ResponseWriter, r *http.Request) {
	key, ok := objectKey(r)
	if !ok {
		fail(w, r, errNotImplemented, nil)
		return
	}
	// A PUT carrying an upload id is a part, not the object. Checked before
	// anything else: treating it as an object would commit a manifest under the
	// object's key holding one part of it.
	if id := r.URL.Query().Get("uploadId"); id != "" {
		h.uploadPart(w, r, id)
		return
	}
	// A copy has no body, so it is answered before the length check below.
	if source := r.Header.Get("X-Amz-Copy-Source"); source != "" {
		h.copyObject(w, r, key, source)
		return
	}
	// Without a length there is no way to tell a complete upload from a
	// connection that died halfway, and S3 requires one.
	if r.ContentLength < 0 {
		fail(w, r, errMissingLength, nil)
		return
	}

	// Before the body is read, because a digest that cannot be a digest is worth
	// saying so about without first streaming a gigabyte to disk.
	digest, err := contentMD5(r.Header.Get("Content-MD5"))
	if err != nil {
		fail(w, r, errInvalidDigest, err)
		return
	}

	m, err := h.cluster.Put(r.Context(), key, r.Body, cluster.PutOptions{
		ContentType: r.Header.Get("Content-Type"),
		Size:        r.ContentLength,
		MD5:         digest,
	})
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	// Only now does the object exist: every chunk is fsynced on a quorum and the
	// manifest is committed. A client that sees this response can lose power in
	// the next instant and still read the object back.
	w.Header().Set("ETag", etag(m))
	w.WriteHeader(http.StatusOK)
}

// copyObject answers a PUT carrying X-Amz-Copy-Source: the destination becomes
// another name for the source's chunks. Clients reach for it constantly — `aws s3 mv`
// is a copy and a delete, and `aws s3 cp` between two keys never touches the network
// with the object itself.
func (h *handler) copyObject(w http.ResponseWriter, r *http.Request, key, source string) {
	from, ok := copySource(source)
	if !ok {
		fail(w, r, errInvalidCopySource, nil)
		return
	}
	// S3 refuses a copy onto itself, because the only thing it could mean is a
	// metadata rewrite, and metadata is not something this store keeps to rewrite.
	if from == key {
		fail(w, r, errCopyOntoItself, nil)
		return
	}

	m, err := h.cluster.Copy(r.Context(), from, key)
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	writeXML(w, r, copyResult{ETag: etag(m), LastModified: m.Modified.UTC().Format(time.RFC3339)})
}

// copySource parses the header's "/bucket/key" or "bucket/key", either of which a
// client may send, and either of which may be percent-encoded. A version id
// suffix is refused rather than ignored: nothing here is versioned, so honouring a
// request for one version by returning another would be a lie.
func copySource(source string) (string, bool) {
	if at := strings.IndexByte(source, '?'); at >= 0 {
		return "", false
	}
	decoded, err := url.PathUnescape(strings.TrimPrefix(source, "/"))
	if err != nil {
		return "", false
	}
	bucket, key, found := strings.Cut(decoded, "/")
	if !found || bucket == "" || key == "" {
		return "", false
	}
	return bucket + "/" + key, true
}

type copyResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	ETag         string
	LastModified string
}

func (h *handler) getObject(w http.ResponseWriter, r *http.Request) {
	key, ok := objectKey(r)
	if !ok {
		// A trailing slash names the bucket: HEAD is the existence check clients
		// make before uploading, GET is a listing.
		if r.Method == http.MethodHead {
			h.headBucket(w, r)
			return
		}
		h.listObjects(w, r)
		return
	}
	m, err := h.cluster.Resolve(r.Context(), key)
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}

	// Before the range, because a conditional request that fails is answered
	// whole: a client asking for bytes 0-99 of an object it already has wants to
	// hear that it already has it, not the bytes.
	if status, ok := precondition(r, m); !ok {
		if status == http.StatusNotModified {
			// No body, and the validators it was tested against, so the client
			// can go on using them. Not a fail(): 304 is a successful answer to
			// the question that was asked.
			validators(w.Header(), m)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		fail(w, r, errPreconditionFailed, nil)
		return
	}

	off, length, err := parseRange(r.Header.Get("Range"), m.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", m.Size))
		fail(w, r, errInvalidRange, err)
		return
	}

	header := w.Header()
	validators(header, m)
	header.Set("Accept-Ranges", "bytes")
	header.Set("Content-Length", strconv.FormatInt(length, 10))
	if m.ContentType != "" {
		header.Set("Content-Type", m.ContentType)
	}
	status := http.StatusOK
	if length != m.Size {
		header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, off+length-1, m.Size))
		status = http.StatusPartialContent
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}

	// The status and Content-Length are already sent, so the only way left to say
	// "do not trust these bytes" is to stop short of what was promised. The client
	// sees a truncated transfer, which is the point.
	if err := h.cluster.StreamRange(r.Context(), m, w, off, length); err != nil && r.Context().Err() == nil {
		log.Printf("s3: get %s failed after the response began: %v", key, err)
	}
}

func (h *handler) deleteObject(w http.ResponseWriter, r *http.Request) {
	key, ok := objectKey(r)
	if !ok {
		// A trailing slash names the bucket, same as it does for GET and HEAD.
		h.deleteBucket(w, r)
		return
	}
	if id := r.URL.Query().Get("uploadId"); id != "" {
		h.abortUpload(w, r, id)
		return
	}
	if err := h.cluster.Delete(r.Context(), key); err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	// S3 reports success whether or not the object was there, and clients rely on
	// it: a delete that 404s turns cleanup loops into error handling.
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) headBucket(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// unsupported answers everything outside the locked subset. Saying so plainly is
// better than a 404 that a client reads as "the object is missing".
func (h *handler) unsupported(w http.ResponseWriter, r *http.Request) {
	fail(w, r, errNotImplemented, nil)
}

// etag is the ETag header's value: quoted, as clients compare it verbatim
// including the quotes.
func etag(m object.Manifest) string { return `"` + m.ETag + `"` }

// validators writes the two headers a conditional request is answered against, so
// that a 304 carries the same pair a 200 would and the client's next request can
// use them.
func validators(header http.Header, m object.Manifest) {
	header.Set("ETag", etag(m))
	header.Set("Last-Modified", m.Modified.Format(http.TimeFormat))
}

// precondition evaluates the conditional headers on a read against the committed
// manifest, returning the status to answer with and whether the read may proceed.
//
// Answered from metadata alone, which is what makes it worth having: a client that
// already holds the object pays one etcd read instead of the object's bytes, and
// `aws s3 sync` asks this of every file it considers.
//
// The precedence is HTTP's, which S3 follows: an entity tag is a better answer than
// a date, so If-Match beats If-Unmodified-Since and If-None-Match beats
// If-Modified-Since. A failed If-Match is 412 because the client's assumption about
// what it was reading is wrong; a matched If-None-Match is 304 because the client's
// copy is current.
func precondition(r *http.Request, m object.Manifest) (int, bool) {
	tag := etag(m)
	if want := r.Header.Get("If-Match"); want != "" {
		if !matchesTag(want, tag) {
			return http.StatusPreconditionFailed, false
		}
	} else if since, ok := httpDate(r.Header.Get("If-Unmodified-Since")); ok && modified(m).After(since) {
		return http.StatusPreconditionFailed, false
	}

	if want := r.Header.Get("If-None-Match"); want != "" {
		if matchesTag(want, tag) {
			return http.StatusNotModified, false
		}
	} else if since, ok := httpDate(r.Header.Get("If-Modified-Since")); ok && !modified(m).After(since) {
		return http.StatusNotModified, false
	}
	return http.StatusOK, true
}

// matchesTag reports whether a list of entity tags contains one, with "*" meaning
// any. Weak tags are compared as strings: kavo only ever issues strong ones, so a
// W/-prefixed tag in a request simply matches nothing.
func matchesTag(header, tag string) bool {
	for candidate := range strings.SplitSeq(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == tag {
			return true
		}
	}
	return false
}

// modified is the object's Last-Modified as a client can see it. Truncated to the
// second, because that is the resolution the header has: comparing the stored
// nanoseconds against a date parsed from it would make an object appear modified
// after the very timestamp it reported.
func modified(m object.Manifest) time.Time { return m.Modified.Truncate(time.Second) }

// httpDate parses one of the date-based conditional headers. An unparseable date is
// ignored rather than rejected, which is what the HTTP spec requires.
func httpDate(header string) (time.Time, bool) {
	if header == "" {
		return time.Time{}, false
	}
	t, err := http.ParseTime(header)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// contentMD5 turns the Content-MD5 header into the hex digest the write path
// computes as it streams, so the comparison is against the object's own ETag.
//
// S3 defines the header as exactly a base64-encoded 128-bit digest, and anything
// else is InvalidDigest rather than a mismatch: the difference matters to a client,
// which should retry one and fix the other.
func contentMD5(header string) (string, error) {
	if header == "" {
		return "", nil
	}
	sum, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		return "", fmt.Errorf("s3: Content-MD5 %q is not base64: %w", header, err)
	}
	if len(sum) != md5.Size {
		return "", fmt.Errorf("s3: Content-MD5 %q decodes to %d bytes, want %d", header, len(sum), md5.Size)
	}
	return hex.EncodeToString(sum), nil
}

// parseRange reads a Range header and returns the window it asks for, defaulting
// to the whole object.
//
// Only a single range is honoured. A request for several is answered with the
// whole object, which the HTTP spec allows and which is what S3 does — the
// alternative is a multipart/byteranges response body no S3 client reads.
func parseRange(header string, size int64) (off, length int64, err error) {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if header == "" || !ok || strings.Contains(spec, ",") {
		return 0, size, nil
	}
	first, last, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, fmt.Errorf("range %q has no dash", header)
	}

	switch {
	case first == "": // bytes=-N, the last N bytes
		n, err := strconv.ParseInt(last, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, fmt.Errorf("range %q: suffix length", header)
		}
		return max(size-n, 0), min(n, size), nil

	case last == "": // bytes=N-, from N to the end
		off, err := strconv.ParseInt(first, 10, 64)
		if err != nil || off < 0 || off >= size {
			return 0, 0, fmt.Errorf("range %q: start past the end of %d bytes", header, size)
		}
		return off, size - off, nil

	default: // bytes=N-M inclusive
		off, err1 := strconv.ParseInt(first, 10, 64)
		end, err2 := strconv.ParseInt(last, 10, 64)
		if err1 != nil || err2 != nil || off < 0 || end < off || off >= size {
			return 0, 0, fmt.Errorf("range %q against %d bytes", header, size)
		}
		// A range may run past the end; it is satisfied by what exists.
		return off, min(end+1, size) - off, nil
	}
}
