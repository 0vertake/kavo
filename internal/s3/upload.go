package s3

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"

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
		fail(w, r, errNotImplemented, nil)
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
	id, err := h.cluster.CreateUpload(r.Context(), key, r.Header.Get("Content-Type"))
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

// uploadPart stores one part. It is a PUT to the object's path with a part number
// and an upload id, which is why the object PUT route has to check for them first.
func (h *handler) uploadPart(w http.ResponseWriter, r *http.Request, id string) {
	number, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil {
		fail(w, r, apiError{"InvalidArgument", http.StatusBadRequest,
			"partNumber must be an integer."}, err)
		return
	}
	if r.ContentLength < 0 {
		fail(w, r, errMissingLength, nil)
		return
	}

	etag, err := h.cluster.UploadPart(r.Context(), id, number, r.Body)
	if err != nil {
		fail(w, r, uploadError(err), err)
		return
	}
	// The client keeps this and sends it back on completion, which is how it finds
	// out that a part arrived intact.
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

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
