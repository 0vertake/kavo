package s3

import (
	"encoding/xml"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/sigv4"
	"github.com/0vertake/kavo/internal/store"
)

// errorResponse is the body S3 clients parse on failure. The code is the part
// they act on: an SDK retries SlowDown, refuses to retry SignatureDoesNotMatch,
// and reports NoSuchKey to the caller as a missing object rather than an outage.
type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

// apiError is an S3 error code with the status that goes with it.
type apiError struct {
	code    string
	status  int
	message string
}

func (e apiError) Error() string { return e.code + ": " + e.message }

var (
	errNoSuchKey = apiError{"NoSuchKey", http.StatusNotFound,
		"The specified key does not exist."}
	errInvalidRange = apiError{"InvalidRange", http.StatusRequestedRangeNotSatisfiable,
		"The requested range is not satisfiable."}
	errMissingLength = apiError{"MissingContentLength", http.StatusLengthRequired,
		"An object write must declare its length."}
	errSlowDown = apiError{"SlowDown", http.StatusServiceUnavailable,
		"Not enough nodes accepted the write to acknowledge it. Retry."}
	errInternal = apiError{"InternalError", http.StatusInternalServerError,
		"The request could not be completed."}
	errAccessDenied = apiError{"AccessDenied", http.StatusForbidden,
		"Access denied."}
	errInvalidKey = apiError{"InvalidAccessKeyId", http.StatusForbidden,
		"The access key id does not exist in our records."}
	errBadSignature = apiError{"SignatureDoesNotMatch", http.StatusForbidden,
		"The request signature does not match the signature this server computed."}
	errSkew = apiError{"RequestTimeTooSkewed", http.StatusForbidden,
		"The difference between the request time and the server's time is too large."}
	errMalformedAuth = apiError{"AuthorizationHeaderMalformed", http.StatusBadRequest,
		"The authorization header is malformed."}
	errBadDigest = apiError{"XAmzContentSHA256Mismatch", http.StatusBadRequest,
		"The body does not match the checksum the request declared."}
	errNotImplemented = apiError{"NotImplemented", http.StatusNotImplemented,
		"This server does not implement that operation."}
	errBucketNotEmpty = apiError{"BucketNotEmpty", http.StatusConflict,
		"The bucket still holds objects."}
	errInvalidCopySource = apiError{"InvalidArgument", http.StatusBadRequest,
		"The copy source must name a bucket and a key, and must not name a version."}
	errCopyOntoItself = apiError{"InvalidRequest", http.StatusBadRequest,
		"A copy onto the object itself would change nothing: there is no metadata here to rewrite."}
)

// authError maps a verification failure to the code that tells the client what to
// do about it. Collapsing them all into AccessDenied is how a client ends up
// retrying a clock problem forever.
func authError(err error) apiError {
	switch {
	case errors.Is(err, sigv4.ErrMissingSignature):
		return errAccessDenied
	case errors.Is(err, sigv4.ErrUnknownKey):
		return errInvalidKey
	case errors.Is(err, sigv4.ErrMismatch):
		return errBadSignature
	case errors.Is(err, sigv4.ErrSkew):
		return errSkew
	case errors.Is(err, sigv4.ErrPayload):
		return errBadDigest
	default:
		return errMalformedAuth
	}
}

// storeError maps a failure from the data path. Which of these a client sees
// matters: SlowDown says the cluster is short of nodes and the write may succeed
// later, where InternalError says nothing useful.
func storeError(err error) apiError {
	switch {
	case errors.Is(err, meta.ErrNotFound):
		return errNoSuchKey
	case errors.Is(err, cluster.ErrQuorum):
		return errSlowDown
	case errors.Is(err, sigv4.ErrPayload), errors.Is(err, sigv4.ErrMismatch):
		// The body failed its signature midway through being stored. The object
		// was never committed, so nothing partial is readable.
		return errBadDigest
	case errors.Is(err, store.ErrChecksumMismatch):
		return apiError{"BadDigest", http.StatusBadRequest,
			"The bytes received did not match their checksum."}
	default:
		return errInternal
	}
}

// writeXML sends a marshalled S3 response document.
func writeXML(w http.ResponseWriter, r *http.Request, doc any) {
	body, err := xml.Marshal(doc)
	if err != nil {
		fail(w, r, errInternal, err)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)+len(xml.Header)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	w.Write(body)
}

// fail writes an S3 error document. Nothing may have been written to w yet: a
// response whose status is already sent cannot be turned into an error, which is
// why every handler resolves what it needs before writing bytes.
func fail(w http.ResponseWriter, r *http.Request, e apiError, cause error) {
	if cause != nil && e.status >= 500 {
		log.Printf("s3: %s %s: %v", r.Method, r.URL.Path, cause)
	}
	body, err := xml.Marshal(errorResponse{Code: e.code, Message: e.message, Resource: r.URL.Path})
	if err != nil {
		http.Error(w, e.code, e.status)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)+len(xml.Header)))
	w.WriteHeader(e.status)
	// A HEAD carries the status and no body, which is how a client distinguishes
	// a missing object from a missing bucket without reading one.
	if r.Method != http.MethodHead {
		w.Write([]byte(xml.Header))
		w.Write(body)
	}
}
