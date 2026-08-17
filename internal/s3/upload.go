package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
)

// Multipart upload is how every S3 client sends a large object: the aws CLI
// switches to it above 8 MB, so without it a client cannot upload a big file at
// all.

type initiateResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		Number int    `xml:"PartNumber"`
		ETag   string `xml:"ETag"`
	} `xml:"Part"`
}

type completeResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	XMLNS    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// maxCompleteRequest bounds the completion body. 10,000 parts of roughly 60 bytes
// of XML each, with room to spare: enough for any real request, small enough that a
// bogus one cannot be used to exhaust memory.
const maxCompleteRequest = 4 << 20

// postObject handles the two POSTs S3 puts on an object: starting a multipart
// upload and completing one. Which it is depends on the query, since both are a
// POST to the same path.
func (h *handler) postObject(w http.ResponseWriter, r *http.Request) {
	key, ok := objectKey(r)
	if !ok {
		// A trailing slash names the bucket, as it does for GET and DELETE. Clients
		// really do send one: minio-go posts a bulk delete to "/{bucket}/".
		h.postBucket(w, r)
		return
	}
	q := r.URL.Query()
	switch {
	case q.Has("uploads"):
		h.createUpload(w, r, key)
	case q.Get("uploadId") != "":
		h.completeUpload(w, r, key, q.Get("uploadId"))
	default:
		// POST-as-PUT of a browser form upload, which is an anti-goal.
		fail(w, r, errNotImplemented, nil)
	}
}

func (h *handler) createUpload(w http.ResponseWriter, r *http.Request, key string) {
	stored, err := storedMeta(r.Header)
	if err != nil {
		fail(w, r, errMetadataTooLarge, err)
		return
	}
	// S3 takes a multipart object's metadata from this call and not from the
	// completion, so it is recorded now and carried by the upload.
	id, err := h.cluster.CreateUpload(r.Context(), key, r.Header.Get("Content-Type"), stored)
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	writeXML(w, r, initiateResult{
		XMLNS:    s3XMLNS,
		Bucket:   r.PathValue("bucket"),
		Key:      r.PathValue("key"),
		UploadID: id,
	})
}

// listPartsResult is the ListParts response. A client that lost track of what it
// has already sent asks this before resuming, so answering it with an object read —
// which is what a GET carrying an upload id used to do — told the client its object
// did not exist.
type listPartsResult struct {
	XMLName              xml.Name    `xml:"ListPartsResult"`
	XMLNS                string      `xml:"xmlns,attr"`
	Bucket               string      `xml:"Bucket"`
	Key                  string      `xml:"Key"`
	UploadID             string      `xml:"UploadId"`
	PartNumberMarker     int         `xml:"PartNumberMarker"`
	NextPartNumberMarker int         `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int         `xml:"MaxParts"`
	IsTruncated          bool        `xml:"IsTruncated"`
	Parts                []partEntry `xml:"Part"`
}

type partEntry struct {
	Number   int    `xml:"PartNumber"`
	Modified string `xml:"LastModified"`
	ETag     string `xml:"ETag"`
	Size     int64  `xml:"Size"`
}

type listUploadsResult struct {
	XMLName            xml.Name      `xml:"ListMultipartUploadsResult"`
	XMLNS              string        `xml:"xmlns,attr"`
	Bucket             string        `xml:"Bucket"`
	Prefix             string        `xml:"Prefix,omitempty"`
	KeyMarker          string        `xml:"KeyMarker"`
	UploadIDMarker     string        `xml:"UploadIdMarker"`
	NextKeyMarker      string        `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string        `xml:"NextUploadIdMarker,omitempty"`
	MaxUploads         int           `xml:"MaxUploads"`
	IsTruncated        bool          `xml:"IsTruncated"`
	Uploads            []uploadEntry `xml:"Upload"`
}

type uploadEntry struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

// maxPartsLimit and maxUploadsLimit are S3's own page caps. A client asking for
// more gets this many and pages.
const (
	maxPartsLimit   = 1000
	maxUploadsLimit = 1000
)

// listParts answers a GET that carries an upload id.
func (h *handler) listParts(w http.ResponseWriter, r *http.Request, key, id string) {
	q := r.URL.Query()
	max := boundedInt(q.Get("max-parts"), maxPartsLimit)
	marker := boundedInt(q.Get("part-number-marker"), 0)

	parts, err := h.cluster.Parts(r.Context(), id)
	if err != nil {
		fail(w, r, uploadError(err), err)
		return
	}
	result := listPartsResult{
		XMLNS:            s3XMLNS,
		Bucket:           r.PathValue("bucket"),
		Key:              key,
		UploadID:         id,
		PartNumberMarker: marker,
		MaxParts:         max,
	}
	for _, p := range parts {
		if p.Number <= marker {
			continue
		}
		if len(result.Parts) == max {
			result.IsTruncated = true
			result.NextPartNumberMarker = result.Parts[len(result.Parts)-1].Number
			break
		}
		result.Parts = append(result.Parts, partEntry{
			Number:   p.Number,
			Modified: p.Modified.UTC().Format(time.RFC3339),
			ETag:     `"` + p.ETag + `"`,
			Size:     p.Size,
		})
	}
	writeXML(w, r, result)
}

// listUploads answers a GET on the bucket that asks for uploads rather than
// objects. Without it that request was answered with an object listing, which a
// client parses as "no uploads in flight" — the kind of hollow answer that is worse
// than a refusal.
func (h *handler) listUploads(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	q := r.URL.Query()
	max := boundedInt(q.Get("max-uploads"), maxUploadsLimit)
	// The key marker is accepted and echoed but not used to resume: kavo pages
	// these in upload-id order, so the id marker is the one that continues a
	// listing. See meta.Store.Uploads.
	after := q.Get("upload-id-marker")

	// Buckets are prefixes, so an upload's key is "bucket/key" and the prefix a
	// client asked for goes after the bucket's.
	prefix := path.Join(bucket, q.Get("prefix"))
	if q.Get("prefix") == "" {
		prefix = bucket + "/"
	}
	uploads, more, err := h.cluster.Uploads(r.Context(), prefix, after, max)
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}

	result := listUploadsResult{
		XMLNS:          s3XMLNS,
		Bucket:         bucket,
		Prefix:         q.Get("prefix"),
		KeyMarker:      q.Get("key-marker"),
		UploadIDMarker: after,
		MaxUploads:     max,
		IsTruncated:    more,
	}
	for _, u := range uploads {
		result.Uploads = append(result.Uploads, uploadEntry{
			Key:       strings.TrimPrefix(u.Upload.Key, bucket+"/"),
			UploadID:  u.ID,
			Initiated: u.Upload.Created.UTC().Format(time.RFC3339),
		})
	}
	if more && len(result.Uploads) > 0 {
		last := result.Uploads[len(result.Uploads)-1]
		result.NextKeyMarker, result.NextUploadIDMarker = last.Key, last.UploadID
	}
	writeXML(w, r, result)
}

// boundedInt reads a page size or marker, falling back to fallback when it is
// missing or not a number, and never exceeding it.
func boundedInt(value string, fallback int) int {
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 || (fallback > 0 && n > fallback) {
		return fallback
	}
	return n
}

// uploadPart stores one part. It is a PUT to the object's path with a part number
// and an upload id, which is why the object PUT route has to check for them first.
func (h *handler) uploadPart(w http.ResponseWriter, r *http.Request, id string) {
	number, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil {
		fail(w, r, apiError{"InvalidArgument", http.StatusBadRequest,
			"partNumber must be an integer."}, err)
		return
	}
	// A part whose bytes come from another object is UploadPartCopy. The header is
	// the only thing that distinguishes the two requests, and reading past it once
	// meant a copy was stored as a part with an empty body.
	if source := r.Header.Get("X-Amz-Copy-Source"); source != "" {
		h.copyPart(w, r, id, number, source)
		return
	}
	if r.ContentLength < 0 {
		fail(w, r, errMissingLength, nil)
		return
	}

	etag, err := h.cluster.UploadPart(r.Context(), id, number, r.Body, r.ContentLength)
	if err != nil {
		fail(w, r, uploadError(err), err)
		return
	}
	// The client keeps this and sends it back on completion, which is how it finds
	// out that a part arrived intact.
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

// copyPartResult is the UploadPartCopy response. The etag arrives in the body
// rather than the header the plain part upload uses, and a client that sends the
// wrong one back cannot complete its upload — so an empty body here is a copy that
// can never be finished, which is what an ignored copy source produced.
type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	XMLNS        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

// copyPart stores a part copied from a range of another object.
func (h *handler) copyPart(w http.ResponseWriter, r *http.Request, id string, number int, source string) {
	from, ok := copySource(source)
	if !ok {
		fail(w, r, errInvalidCopySource, nil)
		return
	}
	// Resolved here rather than inside the coordinator because the range is
	// declared against the source's size, so the size has to be known before the
	// request can be judged valid at all.
	src, err := h.cluster.Resolve(r.Context(), from)
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	if conditions := copyConditions(r.Header); len(conditions) > 0 {
		if _, ok := precondition(conditions, src); !ok {
			fail(w, r, errPreconditionFailed, nil)
			return
		}
	}
	off, length, err := parseCopyRange(r.Header.Get("X-Amz-Copy-Source-Range"), src.Size)
	if errors.Is(err, errRangeSyntax) {
		fail(w, r, errMalformedRange, err)
		return
	} else if err != nil {
		fail(w, r, errInvalidRange, err)
		return
	}

	etag, err := h.cluster.CopyPart(r.Context(), id, number, src, off, length)
	if err != nil {
		fail(w, r, uploadError(err), err)
		return
	}
	writeXML(w, r, copyPartResult{
		XMLNS:        s3XMLNS,
		ETag:         `"` + etag + `"`,
		LastModified: time.Now().UTC().Format(time.RFC3339),
	})
}

// parseCopyRange reads x-amz-copy-source-range, which names the bytes of the source
// that are to become this part, and defaults to all of them.
//
// Stricter than the Range header on a read, deliberately. A read asks for whatever
// is there and is satisfied by less, so parseRange clamps a window that runs past
// the end. A copy names the bytes it intends to appear in the destination, and
// copying fewer than were asked for without saying so assembles an object the client
// did not describe — and the client cannot tell, since it only ever sees the etag of
// whatever did get copied. Both ends are required for the same reason: `bytes=8-` on
// a source that grew means something different from what the client meant.
func parseCopyRange(header string, size int64) (off, length int64, err error) {
	if header == "" {
		return 0, size, nil
	}
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok {
		return 0, 0, fmt.Errorf("%w: %q does not begin with bytes=", errRangeSyntax, header)
	}
	first, last, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, fmt.Errorf("%w: %q has no dash", errRangeSyntax, header)
	}
	start, err1 := strconv.ParseInt(first, 10, 64)
	end, err2 := strconv.ParseInt(last, 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end < start {
		// Both ends must be numbers and in order. This covers the open-ended
		// forms a read allows, and "bytes=0-2,3-5", whose second range lands in
		// last and fails to parse.
		return 0, 0, fmt.Errorf("%w: %q is not a byte range", errRangeSyntax, header)
	}
	if end >= size {
		return 0, 0, fmt.Errorf("copy source range %q against %d bytes", header, size)
	}
	return start, end - start + 1, nil
}

// errRangeSyntax separates a copy range this server cannot read from one it read and
// cannot satisfy. S3 answers the first 400 and the second 416, and the distinction is
// worth keeping: one says the request is malformed, the other says something true
// about the object, and a client retrying after growing the source only wants to
// retry the second.
var errRangeSyntax = errors.New("s3: copy source range is malformed")

func (h *handler) completeUpload(w http.ResponseWriter, r *http.Request, key, id string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxCompleteRequest))
	if err != nil {
		fail(w, r, errMalformedXML, err)
		return
	}
	var req completeRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		fail(w, r, errMalformedXML, err)
		return
	}

	parts := make([]cluster.CompletedPart, len(req.Parts))
	for i, p := range req.Parts {
		parts[i] = cluster.CompletedPart{Number: p.Number, ETag: p.ETag}
	}

	// A completion can take a while — it reads every part's manifest — and S3
	// clients tolerate that. What they do not tolerate is a 200 with an error in
	// the body, which S3 itself does and which is a well-known trap; kavo answers
	// with a status that means what it says.
	m, err := h.cluster.CompleteUpload(r.Context(), id, parts)
	if err != nil {
		fail(w, r, uploadError(err), err)
		return
	}
	writeXML(w, r, completeResult{
		XMLNS:    s3XMLNS,
		Location: "/" + key,
		Bucket:   r.PathValue("bucket"),
		Key:      r.PathValue("key"),
		ETag:     etag(m),
	})
}

func (h *handler) abortUpload(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.cluster.AbortUpload(r.Context(), id); err != nil {
		fail(w, r, uploadError(err), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// uploadError maps the ways a multipart request can be wrong. The codes matter:
// NoSuchUpload tells a client its upload is gone and retrying the part is
// pointless, where InvalidPart says the part it named is the problem.
func uploadError(err error) apiError {
	switch {
	case errors.Is(err, meta.ErrNotFound):
		return apiError{"NoSuchUpload", http.StatusNotFound,
			"The upload id does not exist. It may have been completed or aborted."}
	case errors.Is(err, cluster.ErrNoSuchPart):
		return apiError{"InvalidPart", http.StatusBadRequest,
			"A part named by the completion does not exist."}
	case errors.Is(err, cluster.ErrPartOrder):
		return apiError{"InvalidPartOrder", http.StatusBadRequest,
			"Parts must be listed in ascending part-number order, each at most once."}
	case errors.Is(err, cluster.ErrPartMismatch):
		return apiError{"InvalidPart", http.StatusBadRequest,
			"A part's etag does not match the part this server stored."}
	default:
		return storeError(err)
	}
}

var errMalformedXML = apiError{"MalformedXML", http.StatusBadRequest,
	"The request body is not XML this server can read."}

const s3XMLNS = "http://s3.amazonaws.com/doc/2006-03-01/"
