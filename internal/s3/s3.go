// Package s3 serves the S3 API subset kavo speaks: PUT, GET, HEAD and DELETE of
// objects, addressed path-style as /{bucket}/{key}.
//
// This is the only surface clients touch. The peer and debug endpoints live on a
// separate listener, because a client that can reach them can delete chunks.
//
// Buckets are key prefixes, not a namespace: there is nothing to create and
// nothing to list. Every object's key in etcd is "bucket/key", which is what makes
// a listing a prefix scan and keeps two buckets' objects from colliding.
package s3

import (
	"fmt"
	"log"
	"net/http"
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
	// A bucket is a prefix, so it exists as soon as it is named. Clients ask
	// before uploading, and answering "no such bucket" would be a lie that stops
	// them. "/{bucket}/" reaches the object route with an empty key, which
	// getObject sends here.
	mux.HandleFunc("HEAD /{bucket}", h.authed(h.headBucket))
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
	// Without a length there is no way to tell a complete upload from a
	// connection that died halfway, and S3 requires one.
	if r.ContentLength < 0 {
		fail(w, r, errMissingLength, nil)
		return
	}

	m, err := h.cluster.Put(r.Context(), key, r.Body, cluster.PutOptions{
		ContentType: r.Header.Get("Content-Type"),
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

func (h *handler) getObject(w http.ResponseWriter, r *http.Request) {
	key, ok := objectKey(r)
	if !ok {
		// A trailing slash names the bucket. HEAD of one is the existence check
		// clients make before uploading; GET of one is a listing, which is the
		// next milestone.
		if r.Method == http.MethodHead {
			h.headBucket(w, r)
			return
		}
		fail(w, r, errNotImplemented, nil)
		return
	}
	m, err := h.cluster.Resolve(r.Context(), key)
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}

	off, length, err := parseRange(r.Header.Get("Range"), m.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", m.Size))
		fail(w, r, errInvalidRange, err)
		return
	}

	header := w.Header()
	header.Set("ETag", etag(m))
	header.Set("Last-Modified", m.Modified.Format(http.TimeFormat))
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
		fail(w, r, errNotImplemented, nil)
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
