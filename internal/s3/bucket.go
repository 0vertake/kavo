package s3

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/0vertake/kavo/internal/cluster"
)

// Buckets are prefixes, so this file is thinner than the API it answers: a bucket
// exists exactly when an object names it. There is no bucket record to create, and
// so no bucket record to be inconsistent with the objects in it.
//
// The three operations exist anyway because clients call them unprompted. An SDK
// creates the bucket before its first upload, `aws s3 ls` with no argument lists
// buckets, and a test suite tears down what it made. Answering those with a
// redirect or a NotImplemented makes the store unusable by a client kavo did not
// choose, which is the only kind of compatibility worth claiming.

// listAllBucketsResult is the ListBuckets response.
type listAllBucketsResult struct {
	XMLName xml.Name      `xml:"ListAllMyBucketsResult"`
	XMLNS   string        `xml:"xmlns,attr"`
	Owner   bucketOwner   `xml:"Owner"`
	Buckets bucketWrapper `xml:"Buckets"`
}

type bucketWrapper struct {
	Bucket []bucketEntry `xml:"Bucket"`
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type bucketOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// listBuckets answers ListBuckets by asking which first path components exist,
// which is a root listing grouped on "/" — the same prefix scan that answers
// ListObjectsV2, one level up.
func (h *handler) listBuckets(w http.ResponseWriter, r *http.Request) {
	result := listAllBucketsResult{
		XMLNS: s3XMLNS,
		// One key pair, so one owner. Clients display this and some refuse a
		// response without it.
		Owner: bucketOwner{ID: h.creds.AccessKey, DisplayName: h.creds.AccessKey},
	}

	var from string
	for {
		page, err := h.cluster.List(r.Context(), cluster.ListRequest{
			Delimiter: "/",
			From:      from,
			Limit:     maxKeysLimit,
		})
		if err != nil {
			fail(w, r, storeError(err), err)
			return
		}
		for _, p := range page.Prefixes {
			result.Buckets.Bucket = append(result.Buckets.Bucket, bucketEntry{
				Name: strings.TrimSuffix(p, "/"),
				// Nothing was created, so there is no creation time to report.
				// The epoch is the honest placeholder: a client that sorts on
				// this gets a stable order rather than one that changes per call.
				CreationDate: time.Unix(0, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
			})
		}
		if page.Next == "" {
			writeXML(w, r, result)
			return
		}
		from = page.Next
	}
}

// bucketOnly names the bucket a request addresses, and refuses one that addresses
// something hanging off it instead.
//
// Every bucket subresource is a query on the same path — ?versioning, ?acl,
// ?tagging, ?policy, ?lifecycle, ?cors, ?object-lock — and none of them exist here.
// Without this check they would all reach the handler that creates or deletes a
// bucket and be answered with a success, which is worse than refusing them: a
// client told that versioning is now enabled will go on overwriting objects
// believing the old ones are still there.
func bucketOnly(r *http.Request) (string, bool) {
	bucket := r.PathValue("bucket")
	if bucket == "" || strings.Contains(bucket, "/") {
		return "", false
	}
	for name := range r.URL.Query() {
		// x-id is the SDKs' own operation label and names no subresource.
		if name != "x-id" {
			return "", false
		}
	}
	return bucket, true
}

// locationResult is the GetBucketLocation response, whose whole body is one value.
type locationResult struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	XMLNS   string   `xml:"xmlns,attr"`
	Region  string   `xml:",chardata"`
}

// bucketLocation answers GetBucketLocation with the empty constraint, which is
// what S3 returns for us-east-1 and what a client reads as "no special region".
//
// There are no regions here, so there is nothing to look up. It is answered
// because clients ask before doing anything else — minio-go calls it while merely
// checking whether a bucket exists, so a store that refuses it cannot be reached by
// mc or warp at all.
func (h *handler) bucketLocation(w http.ResponseWriter, r *http.Request) {
	writeXML(w, r, locationResult{XMLNS: s3XMLNS})
}

// createBucket answers CreateBucket, which has nothing to do: naming a bucket is
// what brings it into existence. Succeeding is not a lie — after this call the
// client can write to the bucket, which is all CreateBucket promises. It is also
// idempotent for the same reason, where S3's own is not.
func (h *handler) createBucket(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketOnly(r)
	if !ok {
		fail(w, r, errNotImplemented, nil)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

// deleteRequest is the body of a bulk delete.
type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	// Quiet asks for only the failures back, which is what a client emptying a
	// bucket wants: it does not need a thousand keys read back to it.
	Quiet   bool `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name       `xml:"DeleteResult"`
	XMLNS   string         `xml:"xmlns,attr"`
	Deleted []deletedEntry `xml:"Deleted"`
	Errors  []deleteError  `xml:"Error"`
}

type deletedEntry struct {
	Key string `xml:"Key"`
}

type deleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// maxDeleteKeys is the most keys one bulk delete may name, which is S3's own limit
// and what every client batches against.
const maxDeleteKeys = 1000

// maxDeleteRequest bounds the body: a thousand keys of the maximum 1,024 bytes,
// with room for the XML around them.
const maxDeleteRequest = 2 << 20

// postBucket handles the POSTs addressed to a bucket rather than an object. Only
// one exists in the subset, and an unrouted POST here would otherwise be answered
// with a redirect that resends the body to the object handler.
func (h *handler) postBucket(w http.ResponseWriter, r *http.Request) {
	if !r.URL.Query().Has("delete") {
		fail(w, r, errNotImplemented, nil)
		return
	}
	h.deleteObjects(w, r)
}

// deleteObjects answers DeleteObjects, the bulk delete an SDK uses to empty a
// bucket. Deleting a thousand objects one request at a time is a thousand round
// trips, which is why clients reach for this and why its absence shows up as a
// bucket a client cannot clean up.
//
// This one really does answer 200 with the failures in the body, unlike the
// multipart completion next door: a partial result is the operation's defined
// outcome rather than an error being smuggled past a status code, and clients read
// the per-key list to decide what to retry.
func (h *handler) deleteObjects(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	body, err := io.ReadAll(io.LimitReader(r.Body, maxDeleteRequest))
	if err != nil {
		fail(w, r, errMalformedXML, err)
		return
	}
	var req deleteRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		fail(w, r, errMalformedXML, err)
		return
	}
	if len(req.Objects) > maxDeleteKeys {
		fail(w, r, apiError{"MalformedXML", http.StatusBadRequest,
			"A bulk delete may name at most 1000 keys."}, nil)
		return
	}

	result := deleteResult{XMLNS: s3XMLNS}
	for _, o := range req.Objects {
		if o.Key == "" {
			result.Errors = append(result.Errors, deleteError{
				Code: "InvalidArgument", Message: "A delete entry must name a key."})
			continue
		}
		// A version id, if the client sent one, names the only version there is.
		if err := h.cluster.Delete(r.Context(), bucket+"/"+o.Key); err != nil {
			e := storeError(err)
			result.Errors = append(result.Errors, deleteError{
				Key: o.Key, Code: e.code, Message: e.message})
			continue
		}
		if !req.Quiet {
			result.Deleted = append(result.Deleted, deletedEntry{Key: o.Key})
		}
	}
	writeXML(w, r, result)
}

// deleteBucket answers DeleteBucket. There is no record to remove, so the only
// thing this can get wrong is the answer: S3 refuses to delete a bucket that still
// holds objects, and a client that deletes a bucket believing it emptied it would
// otherwise be told the objects are gone while every one of them is still readable.
func (h *handler) deleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketOnly(r)
	if !ok {
		fail(w, r, errNotImplemented, nil)
		return
	}
	page, err := h.cluster.List(r.Context(), cluster.ListRequest{
		Prefix: bucket + "/",
		Limit:  1,
	})
	if err != nil {
		fail(w, r, storeError(err), err)
		return
	}
	if len(page.Objects) > 0 {
		fail(w, r, errBucketNotEmpty, nil)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
