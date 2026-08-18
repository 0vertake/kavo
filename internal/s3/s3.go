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
	"encoding/binary"
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
		// After the signature, so that an unsigned request is told about the
		// signature rather than about this.
		if encryptionRequested(r.Header) {
			fail(w, r, errEncryptionNotImplemented, nil)
			return
		}
		if taggingRequested(r.Header) {
			fail(w, r, errTaggingNotImplemented, nil)
			return
		}
		if checksumRefused(r) {
			fail(w, r, errChecksumNotImplemented, nil)
			return
		}
		if key := r.PathValue("key"); key != "" {
			for name := range r.URL.Query() {
				if !knownObjectQuery(r.Method, name, r.URL.Query().Get(name)) {
					fail(w, r, errNotImplemented, nil)
					return
				}
			}
		}
		next(w, r)
	}
}

// knownObjectQuery reports whether an object request may carry this query
// parameter. Everything else hanging off an object's path — ?tagging, ?acl,
// ?retention, ?legal-hold, ?attributes, ?versionId, the response-header overrides —
// names a subresource this server does not implement.
//
// It is an allowlist because the failure is asymmetric, and this was not a
// cosmetic bug. S3 addresses an object's subresources as a query on the object's
// own path, so a server that ignores the query answers them with the object
// operation instead: `put-object-tagging` reached the object PUT and replaced the
// object with the tagging XML, `put-object-acl` truncated it to nothing because its
// request has no body, and `delete-object-tagging` deleted it. All three answered
// 200. A client tagging an object destroyed it and was told the tag was set.
//
// The bucket path has had the same guard since the suite found the same shape of
// bug there (`bucketOnly`), where it only cost honesty. Here it cost the object.
func knownObjectQuery(method, name, value string) bool {
	switch name {
	case "x-id": // The SDKs' own operation label. It names no subresource.
		return true
	case "uploadId": // Every multipart call names one, on all four methods.
		return true
	case "partNumber":
		// On a PUT it numbers the part being uploaded. On a GET it asks for one
		// part of a completed object, which is not implemented — and answering
		// that with the whole object is the same class of mistake as the above.
		return method == http.MethodPut
	case "uploads":
		return method == http.MethodPost
	case "max-parts", "part-number-marker":
		return method == http.MethodGet
	case "tagging":
		// Reading an object's tags is answered — with none, which is true, since
		// nothing here stores any. Setting them is not: see taggingRequested.
		// The asymmetry is the point. A client asking what the tags are gets a
		// correct answer; a client asking for tags to exist is refused rather
		// than told they do.
		return method == http.MethodGet
	case "versionId":
		// Nothing here is versioned, and every object is reported by
		// ListObjectVersions as the single version "null", so a request naming
		// that version names the object — which is how a client empties a
		// bucket. Any other id names a version that never existed, and answering
		// it with the current object would return or delete something the client
		// did not ask for.
		return value == "null"
	}
	return false
}

// encryptionRequested reports whether a request asks for server-side encryption, in
// any of its forms: SSE-S3, SSE-KMS, a customer key, or a customer key for a copy's
// source.
//
// Refused rather than ignored, which is what this used to do. Encryption at rest is
// an anti-goal, and ignoring the headers meant a client that sent a customer key was
// answered 200 for an object stored in plaintext that anyone without the key could
// read — the client believing otherwise being the whole point of sending the key.
// Ceph's suite scored that arrangement as twenty passes, which is the clearest
// argument available that a pass count is not a measure of a store.
func encryptionRequested(header http.Header) bool {
	for name := range header {
		if strings.HasPrefix(name, "X-Amz-Server-Side-Encryption") ||
			strings.HasPrefix(name, "X-Amz-Copy-Source-Server-Side-Encryption") {
			return true
		}
	}
	return false
}

// taggingRequested reports whether a request asks for tags to be attached, which is
// x-amz-tagging on a PUT or a multipart creation.
//
// Refused rather than ignored, and refused for the same reason a read of tags is
// *answered*: a store that drops the header and then reports the object has no tags
// has told a client its tags are gone by way of two successful responses. Either
// both are honest or neither is. An empty value asks for no tags, which is what the
// object will have, so there is nothing there to drop.
func taggingRequested(header http.Header) bool {
	return header.Get("X-Amz-Tagging") != ""
}

// checksumRefused reports whether a request asks for a checksum this server would
// not actually verify. CRC32C on a whole-object PUT is the one that is checked:
// the write already hashes the body as it streams, because that is how the ETag
// is made. Everything else — a trailer whose value skipTrailers used to discard,
// SHA-256, CRC32, CRC64NVME, a checksum on a part or a copy — is refused rather
// than stored without being looked at.
func checksumRefused(r *http.Request) bool {
	if !writesObjectBytes(r) {
		return false
	}
	if r.Header.Get("X-Amz-Trailer") != "" {
		return true
	}
	algo, extras := checksumHeaders(r.Header)
	if extras {
		return true
	}
	if algo == "" {
		return false
	}
	if !strings.EqualFold(algo, "CRC32C") {
		return true
	}
	return r.Method != http.MethodPut ||
		r.URL.Query().Get("uploadId") != "" ||
		r.Header.Get("X-Amz-Copy-Source") != ""
}

// writesObjectBytes is a PUT of an object or part, a copy, a multipart create or
// a completion — the requests whose checksum header would be a claim about stored
// data. A DeleteObjects CRC32C is a checksum of the XML, which SigV4 already
// authenticates; treating it as an object checksum refused every SDK call that
// was not a PUT.
func writesObjectBytes(r *http.Request) bool {
	if r.Method == http.MethodPut && r.PathValue("key") != "" {
		return true
	}
	if r.Method == http.MethodPost && r.PathValue("key") != "" &&
		(r.URL.Query().Has("uploads") || r.URL.Query().Get("uploadId") != "") {
		return true
	}
	return false
}

func checksumHeaders(header http.Header) (algo string, extras bool) {
	if v := header.Get("X-Amz-Checksum-Type"); v != "" {
		return "", true
	}
	for _, name := range []string{
		"X-Amz-Checksum-Crc32", "X-Amz-Checksum-Sha1",
		"X-Amz-Checksum-Sha256", "X-Amz-Checksum-Crc64nvme",
	} {
		if header.Get(name) != "" {
			return "", true
		}
	}
	algo = header.Get("X-Amz-Checksum-Algorithm")
	if algo == "" {
		algo = header.Get("X-Amz-Sdk-Checksum-Algorithm")
	}
	if header.Get("X-Amz-Checksum-Crc32c") != "" {
		if algo == "" {
			algo = "CRC32C"
		} else if !strings.EqualFold(algo, "CRC32C") {
			return "", true
		}
	}
	return algo, false
}

func requestedCRC32C(header http.Header) bool {
	algo, extras := checksumHeaders(header)
	return !extras && strings.EqualFold(algo, "CRC32C")
}

func declaredCRC32C(header http.Header) (*uint32, error) {
	raw := header.Get("X-Amz-Checksum-Crc32c")
	if raw == "" {
		return nil, nil
	}
	sum, err := decodeCRC32C(raw)
	if err != nil {
		return nil, err
	}
	return &sum, nil
}

func encodeCRC32C(sum uint32) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], sum)
	return base64.StdEncoding.EncodeToString(b[:])
}

func decodeCRC32C(raw string) (uint32, error) {
	b, err := base64.StdEncoding.DecodeString(raw)
	if err != nil || len(b) != 4 {
		return 0, fmt.Errorf("s3: checksum CRC32C %q is not a base64-encoded 32-bit digest", raw)
	}
	return binary.BigEndian.Uint32(b), nil
}

func checksumModeEnabled(header http.Header) bool {
	return strings.EqualFold(header.Get("X-Amz-Checksum-Mode"), "ENABLED")
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
	digest, err := contentMD5(r.Header.Values("Content-MD5"))
	if err != nil {
		fail(w, r, errInvalidDigest, err)
		return
	}
	crc32c, err := declaredCRC32C(r.Header)
	if err != nil {
		fail(w, r, errInvalidDigest, err)
		return
	}

	stored, err := storedMeta(r.Header)
	if err != nil {
		fail(w, r, errMetadataTooLarge, err)
		return
	}

	m, err := h.cluster.Put(r.Context(), key, r.Body, cluster.PutOptions{
		ContentType: r.Header.Get("Content-Type"),
		Size:        r.ContentLength,
		MD5:         digest,
		CRC32C:      crc32c,
		Meta:        stored,
	})
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	// Only now does the object exist: every chunk is fsynced on a quorum and the
	// manifest is committed. A client that sees this response can lose power in
	// the next instant and still read the object back.
	w.Header().Set("ETag", etag(m))
	if m.CRC32C != nil && (crc32c != nil || requestedCRC32C(r.Header)) {
		w.Header().Set("X-Amz-Checksum-Crc32c", encodeCRC32C(*m.CRC32C))
	}
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
	directive := r.Header.Get("X-Amz-Metadata-Directive")
	replace := strings.EqualFold(directive, "REPLACE")
	if directive != "" && !replace && !strings.EqualFold(directive, "COPY") {
		fail(w, r, errInvalidDirective, nil)
		return
	}
	// A copy onto itself is only meaningful as a metadata rewrite, so S3 allows it
	// exactly when the request replaces the metadata. Without REPLACE it is a
	// request to overwrite an object with itself, which is either a mistake or a
	// client that meant to say REPLACE.
	if from == key && !replace {
		fail(w, r, errCopyOntoItself, nil)
		return
	}

	stored, err := storedMeta(r.Header)
	if err != nil {
		fail(w, r, errMetadataTooLarge, err)
		return
	}

	// Conditions on the source. Answered here rather than inside Copy because they
	// are a question about a manifest, and they cost an etcd read only when a client
	// asks one.
	if conditions := copyConditions(r.Header); len(conditions) > 0 {
		src, err := h.cluster.Resolve(r.Context(), from)
		if err != nil {
			fail(w, r, storeError(err), err)
			return
		}
		if _, ok := precondition(conditions, src); !ok {
			// A copy has no "you already have it" outcome, so a condition that
			// does not hold is 412 whichever kind it was — where the same
			// condition on a read would have been answered 304.
			fail(w, r, errPreconditionFailed, nil)
			return
		}
	}

	m, err := h.cluster.Copy(r.Context(), from, key, cluster.CopyOptions{
		Replace:     replace,
		ContentType: r.Header.Get("Content-Type"),
		Meta:        stored,
	})
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	writeXML(w, r, copyResult{ETag: etag(m), LastModified: m.Modified.UTC().Format(time.RFC3339)})
}

// taggingResult is the GetObjectTagging response: an object with no tags, which is
// every object here.
type taggingResult struct {
	XMLName xml.Name `xml:"Tagging"`
	XMLNS   string   `xml:"xmlns,attr"`
	TagSet  struct{} `xml:"TagSet"`
}

// objectTagging answers a read of an object's tags with none.
//
// The object is resolved first, so a read of a key that does not exist is NoSuchKey
// rather than an empty tag set — otherwise this would answer for objects that are
// not there, which is the mistake buckets-as-prefixes already makes and does not
// need company. It exists at all because the aws CLI reads the source's tags before
// copying anything larger than 8 MB, so refusing it makes every large server-side
// copy fail on a call about a feature neither side wants.
func (h *handler) objectTagging(w http.ResponseWriter, r *http.Request, key string) {
	if _, err := h.cluster.Resolve(r.Context(), key); err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	writeXML(w, r, taggingResult{XMLNS: s3XMLNS})
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
	if r.URL.Query().Has("tagging") {
		h.objectTagging(w, r, key)
		return
	}
	// A GET carrying an upload id asks what parts have arrived, not for the object:
	// the object does not exist yet, and answering NoSuchKey tells a client
	// resuming an upload that its work is gone.
	if id := r.URL.Query().Get("uploadId"); id != "" {
		h.listParts(w, r, key, id)
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
	if status, ok := precondition(r.Header, m); !ok {
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
	// Assigned rather than Set, because the map keys are already the form these
	// go out in and Set would canonicalise them — see storedMeta. Before the
	// headers below, so that a stored header can never displace one describing
	// this particular response: a Content-Length kept from an upload would make a
	// range read unreadable.
	for name, value := range m.Meta {
		header[name] = []string{value}
	}
	header.Set("Accept-Ranges", "bytes")
	header.Set("Content-Length", strconv.FormatInt(length, 10))
	if m.ContentType != "" {
		header.Set("Content-Type", m.ContentType)
	}
	if checksumModeEnabled(r.Header) && m.CRC32C != nil {
		header.Set("X-Amz-Checksum-Crc32c", encodeCRC32C(*m.CRC32C))
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
func precondition(header http.Header, m object.Manifest) (int, bool) {
	tag := etag(m)
	if want := header.Get("If-Match"); want != "" {
		if !matchesTag(want, tag) {
			return http.StatusPreconditionFailed, false
		}
	} else if since, ok := httpDate(header.Get("If-Unmodified-Since")); ok && modified(m).After(since) {
		return http.StatusPreconditionFailed, false
	}

	if want := header.Get("If-None-Match"); want != "" {
		if matchesTag(want, tag) {
			return http.StatusNotModified, false
		}
	} else if since, ok := httpDate(header.Get("If-Modified-Since")); ok && !modified(m).After(since) {
		return http.StatusNotModified, false
	}
	return http.StatusOK, true
}

// passthrough are the standard headers a write stores and a read replays. They
// describe the bytes rather than the transfer, which is what makes them the
// object's business and not this request's: Content-Length and Content-MD5 are
// about one HTTP exchange, and storing them would answer a later reader with facts
// about an exchange that is over.
var passthrough = []string{
	"Cache-Control",
	"Content-Disposition",
	"Content-Encoding",
	"Content-Language",
	"Expires",
}

// MaxMeta bounds what one object can carry, as S3 does. Unbounded metadata is
// unbounded manifests, and a manifest is read by every request for the object and
// by every background pass over it.
const MaxMeta = 2 << 10

// storedMeta collects what the object should carry: the client's x-amz-meta-*, and
// the passthrough headers above, in canonical HTTP form.
func storedMeta(header http.Header) (map[string]string, error) {
	var stored map[string]string
	size := 0
	keep := func(name, value string) {
		if stored == nil {
			stored = make(map[string]string)
		}
		stored[name] = value
		size += len(name) + len(value)
	}
	for name, values := range header {
		if strings.HasPrefix(name, "X-Amz-Meta-") && len(values) > 0 {
			// Lowercased, which is what S3 does and what clients compare
			// against: botocore hands the caller whatever case the response
			// used, so "Colour" and "colour" are different keys to the code on
			// the other end. Go canonicalises header names on the way in, so
			// without this every key comes back capitalised.
			keep(strings.ToLower(name), values[0])
		}
	}
	for _, name := range passthrough {
		if value := header.Get(name); value != "" {
			if name == "Content-Encoding" {
				// aws-chunked is the framing of the body that just arrived, and
				// kavo decoded it. Keeping it would tell a later reader the
				// stored bytes are chunk-framed, which they are not.
				if value = withoutAWSChunked(value); value == "" {
					continue
				}
			}
			keep(name, value)
		}
	}
	if size > MaxMeta {
		return nil, fmt.Errorf("%d bytes of metadata, at most %d", size, MaxMeta)
	}
	return stored, nil
}

func withoutAWSChunked(encoding string) string {
	kept := make([]string, 0, 2)
	for part := range strings.SplitSeq(encoding, ",") {
		if part = strings.TrimSpace(part); part != "" && !strings.EqualFold(part, "aws-chunked") {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, ", ")
}

// copyConditions translates the x-amz-copy-source-if-* headers into the plain
// conditional ones, so that a copy's conditions are evaluated by the rules above
// rather than by a second implementation of them that drifts from the first.
func copyConditions(header http.Header) http.Header {
	var conditions http.Header
	for _, name := range []string{"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		value := header.Get("X-Amz-Copy-Source-" + name)
		if value == "" {
			continue
		}
		if conditions == nil {
			conditions = http.Header{}
		}
		conditions.Set(name, value)
	}
	return conditions
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
func contentMD5(values []string) (string, error) {
	// Presence rather than emptiness, because the two mean different things: no
	// header declares nothing, and an empty header declares an empty digest, which
	// is malformed. It falls through to the length check below and is refused.
	if len(values) == 0 {
		return "", nil
	}
	header := values[0]
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
