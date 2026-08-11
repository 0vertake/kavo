package s3

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
)

// maxKeysLimit is the most keys one page may hold. S3's own cap, and clients are
// written against it: a client that asks for more gets this many and pages.
const maxKeysLimit = 1000

// listResult is the ListObjectsV2 response. The field order is S3's, since some
// clients parse positionally rather than by name.
type listResult struct {
	XMLName               xml.Name     `xml:"ListBucketResult"`
	XMLNS                 string       `xml:"xmlns,attr"`
	Name                  string       `xml:"Name"`
	Prefix                string       `xml:"Prefix"`
	Delimiter             string       `xml:"Delimiter,omitempty"`
	MaxKeys               int          `xml:"MaxKeys"`
	KeyCount              int          `xml:"KeyCount"`
	IsTruncated           bool         `xml:"IsTruncated"`
	ContinuationToken     string       `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string       `xml:"NextContinuationToken,omitempty"`
	StartAfter            string       `xml:"StartAfter,omitempty"`
	EncodingType          string       `xml:"EncodingType,omitempty"`
	Contents              []listEntry  `xml:"Contents"`
	CommonPrefixes        []listPrefix `xml:"CommonPrefixes"`
}

type listEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	// Owner appears only when the client asked for it with fetch-owner, which is
	// what S3 does — and a client that asked and got nothing back reads it as an
	// object with no owner.
	Owner *bucketOwner `xml:"Owner,omitempty"`
}

type listPrefix struct {
	Prefix string `xml:"Prefix"`
}

// listObjects answers ListObjectsV2.
//
// Only v2: every current client uses it, and v1 differs in how it carries the
// resume point. A v1 request is refused rather than answered with the wrong shape.
func (h *handler) listObjects(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	q := r.URL.Query()
	// Two other GETs live on the bucket path and carry no list-type, so they are
	// checked before the listing refuses the request for not having one.
	if q.Has("versions") {
		h.listVersions(w, r)
		return
	}
	if q.Has("location") {
		h.bucketLocation(w, r)
		return
	}
	if q.Get("list-type") != "2" {
		fail(w, r, errNotImplemented, nil)
		return
	}

	maxKeys, ok := maxKeysOf(w, r, q)
	if !ok {
		return
	}

	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	// The resume point is carried as an opaque token so that a client cannot be
	// tempted to construct one, and because it is a position rather than a key: a
	// page that ended inside a grouped prefix resumes past the whole group.
	from, err := decodeToken(q.Get("continuation-token"))
	if err != nil {
		fail(w, r, apiError{"InvalidArgument", http.StatusBadRequest,
			"The continuation token is not one this server issued."}, err)
		return
	}
	// start-after names a key to begin beyond, which is a different thing.
	if from == "" && q.Get("start-after") != "" {
		from = meta.After(q.Get("start-after"))
	}

	// Keys are stored with the bucket in front, so a listing is a prefix scan and
	// the bucket boundary is the prefix's first component.
	page := cluster.ListPage{}
	if maxKeys > 0 {
		page, err = h.cluster.List(r.Context(), cluster.ListRequest{
			Prefix:    bucket + "/" + prefix,
			Delimiter: delimiter,
			From:      scoped(bucket, from),
			Limit:     maxKeys,
		})
		if err != nil {
			fail(w, r, storeError(err), err)
			return
		}
	}

	// encoding-type=url is not decoration: without it a key containing a character
	// XML cannot carry, or one the client will unescape anyway, comes back as a
	// different key than was stored.
	encode := func(s string) string { return s }
	if q.Get("encoding-type") == "url" {
		encode = escapeKey
	}

	result := listResult{
		XMLNS:             s3XMLNS,
		Name:              bucket,
		Prefix:            encode(prefix),
		Delimiter:         encode(delimiter),
		MaxKeys:           maxKeys,
		IsTruncated:       page.Next != "",
		ContinuationToken: q.Get("continuation-token"),
		StartAfter:        encode(q.Get("start-after")),
		EncodingType:      q.Get("encoding-type"),
	}
	if page.Next != "" {
		result.NextContinuationToken = encodeToken(unscoped(bucket, page.Next))
	}
	// One key pair, so every object has the same owner. Built once rather than per
	// entry, since every entry would point at the same thing.
	var owner *bucketOwner
	if q.Get("fetch-owner") == "true" {
		owner = &bucketOwner{ID: h.creds.AccessKey, DisplayName: h.creds.AccessKey}
	}
	for _, o := range page.Objects {
		result.Contents = append(result.Contents, listEntry{
			Key:          encode(unscoped(bucket, o.Key)),
			LastModified: o.Modified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"` + o.ETag + `"`,
			Size:         o.Size,
			// One class, because tiering is an anti-goal. Clients still expect
			// the field, and an absent one has been known to crash them.
			StorageClass: "STANDARD",
			Owner:        owner,
		})
	}
	for _, p := range page.Prefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, listPrefix{Prefix: encode(unscoped(bucket, p))})
	}
	result.KeyCount = len(result.Contents) + len(result.CommonPrefixes)

	writeXML(w, r, result)
}

// maxKeysOf reads the max-keys parameter, capped at S3's own limit, and answers
// the request itself if it is not a number.
func maxKeysOf(w http.ResponseWriter, r *http.Request, q url.Values) (int, bool) {
	raw := q.Get("max-keys")
	if raw == "" {
		return maxKeysLimit, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		fail(w, r, apiError{"InvalidArgument", http.StatusBadRequest,
			"max-keys must be a non-negative integer."}, nil)
		return 0, false
	}
	return min(n, maxKeysLimit), true
}

// listVersionsResult is the ListObjectVersions response.
type listVersionsResult struct {
	XMLName             xml.Name       `xml:"ListVersionsResult"`
	XMLNS               string         `xml:"xmlns,attr"`
	Name                string         `xml:"Name"`
	Prefix              string         `xml:"Prefix"`
	KeyMarker           string         `xml:"KeyMarker"`
	VersionIDMarker     string         `xml:"VersionIdMarker"`
	NextKeyMarker       string         `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string         `xml:"NextVersionIdMarker,omitempty"`
	Delimiter           string         `xml:"Delimiter,omitempty"`
	MaxKeys             int            `xml:"MaxKeys"`
	IsTruncated         bool           `xml:"IsTruncated"`
	EncodingType        string         `xml:"EncodingType,omitempty"`
	Versions            []versionEntry `xml:"Version"`
	CommonPrefixes      []listPrefix   `xml:"CommonPrefixes"`
}

type versionEntry struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// listVersions answers ListObjectVersions, which is not versioning.
//
// Nothing here keeps old copies of an object: a write replaces the manifest under
// its key. What this reports is the answer S3 defines for a bucket that has never
// had versioning enabled — every current object once, with the version id "null"
// and IsLatest true. The operation is worth answering because clients do not ask
// permission before calling it: it is how the AWS SDKs and the Ceph suite empty a
// bucket, and refusing it makes a store they cannot clean up after.
func (h *handler) listVersions(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	q := r.URL.Query()

	maxKeys, ok := maxKeysOf(w, r, q)
	if !ok {
		return
	}
	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	// This listing resumes from the last key reported rather than from an opaque
	// token. There is one version per key, so the version id marker names nothing
	// this store can be positioned by and is echoed rather than used.
	var from string
	if marker := q.Get("key-marker"); marker != "" {
		// The marker is whatever this server sent last, so if that response was
		// encoded then so is this, and the raw key is what positions the scan.
		if q.Get("encoding-type") == "url" {
			if decoded, err := url.PathUnescape(marker); err == nil {
				marker = decoded
			}
		}
		from = meta.After(marker)
		// A marker ending in the delimiter names a group that has already been
		// reported, and resuming just after it would land inside that group and
		// report it again, with the same marker, forever.
		//
		// Except for one key: the object whose key is the listing's prefix. A key
		// is grouped only when the delimiter appears after the prefix, so that one
		// is reported as an object even though it ends in the delimiter — and
		// stepping past its "group" would skip every key underneath it.
		if delimiter != "" && marker != prefix && strings.HasSuffix(marker, delimiter) {
			from = meta.PastPrefix(marker)
		}
	}

	page := cluster.ListPage{}
	var err error
	if maxKeys > 0 {
		page, err = h.cluster.List(r.Context(), cluster.ListRequest{
			Prefix:    bucket + "/" + prefix,
			Delimiter: delimiter,
			From:      scoped(bucket, from),
			Limit:     maxKeys,
		})
		if err != nil {
			fail(w, r, storeError(err), err)
			return
		}
	}

	encode := func(s string) string { return s }
	if q.Get("encoding-type") == "url" {
		encode = escapeKey
	}

	result := listVersionsResult{
		XMLNS:           s3XMLNS,
		Name:            bucket,
		Prefix:          encode(prefix),
		Delimiter:       encode(delimiter),
		KeyMarker:       encode(q.Get("key-marker")),
		VersionIDMarker: q.Get("version-id-marker"),
		MaxKeys:         maxKeys,
		IsTruncated:     page.Next != "",
		EncodingType:    q.Get("encoding-type"),
	}
	for _, o := range page.Objects {
		result.Versions = append(result.Versions, versionEntry{
			Key: encode(unscoped(bucket, o.Key)),
			// Not an invented id: "null" is what S3 reports for an object written
			// to a bucket that was never versioned, and clients pass it back as
			// the version to delete.
			VersionID:    "null",
			IsLatest:     true,
			LastModified: o.Modified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"` + o.ETag + `"`,
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, p := range page.Prefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, listPrefix{Prefix: encode(unscoped(bucket, p))})
	}
	// A truncated listing has to carry both markers, and not because this store
	// needs them back: clients feed each response's markers straight into the next
	// request, so one that is missing arrives as a null and the paging loop breaks
	// before it has read the second page.
	if page.Next != "" {
		// From the raw keys, not the encoded ones: escaping maps every reserved
		// byte to "%", which sorts below the characters it leaves alone, so
		// comparing encoded strings can pick the earlier of the two and hand the
		// client a frontier behind what it has already seen.
		result.NextKeyMarker = encode(lastReported(bucket, page))
		// "null" is the version id of every object here, so it is also the last one
		// reported. S3 sends the same thing for a bucket that was never versioned.
		result.NextVersionIDMarker = "null"
	}

	writeXML(w, r, result)
}

// lastReported is the key or group a version listing ended on, which is where the
// next page resumes from. Keys and groups are reported in two separate lists, so
// the last thing the client saw is whichever of the two sorts later.
func lastReported(bucket string, page cluster.ListPage) string {
	var last string
	if n := len(page.Objects); n > 0 {
		last = unscoped(bucket, page.Objects[n-1].Key)
	}
	if n := len(page.Prefixes); n > 0 {
		if p := unscoped(bucket, page.Prefixes[n-1]); p > last {
			last = p
		}
	}
	return last
}

// escapeKey percent-encodes everything a client could read two ways.
//
// Not url.QueryEscape and not url.PathEscape: the first turns a space into "+",
// which a client unescaping paths hands back as a literal plus, and the second
// leaves "+" alone, which a client unescaping queries hands back as a space. Either
// mistake renames the key. Encoding down to the unreserved set leaves nothing whose
// meaning depends on which of the two the client chose.
//
// "/" is left alone on top of that set, as S3 leaves it: it means the same thing to
// either unescaper, so encoding it buys no safety, and a client that compares the
// delimiter it sent against the one echoed back is looking for "/" rather than
// "%2F".
func escapeKey(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~/"
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

// scoped puts the bucket back in front of a key, and unscoped takes it off. Only
// the storage layer sees the scoped form.
func scoped(bucket, key string) string {
	if key == "" {
		return ""
	}
	return bucket + "/" + key
}

func unscoped(bucket, key string) string { return strings.TrimPrefix(key, bucket+"/") }

// encodeToken hides a resume key behind base64, because it travels in a query
// string and because it is this server's business, not the client's.
func encodeToken(key string) string {
	return base64.StdEncoding.EncodeToString([]byte(key))
}

func decodeToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}
	key, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	return string(key), nil
}
