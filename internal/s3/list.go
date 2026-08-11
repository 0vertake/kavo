package s3

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
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
	if q.Get("list-type") != "2" {
		fail(w, r, errNotImplemented, nil)
		return
	}

	maxKeys := maxKeysLimit
	if raw := q.Get("max-keys"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			fail(w, r, apiError{"InvalidArgument", http.StatusBadRequest,
				"max-keys must be a non-negative integer."}, nil)
			return
		}
		maxKeys = min(n, maxKeysLimit)
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
	for _, o := range page.Objects {
		result.Contents = append(result.Contents, listEntry{
			Key:          encode(unscoped(bucket, o.Key)),
			LastModified: o.Modified.UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         `"` + o.ETag + `"`,
			Size:         o.Size,
			// One class, because tiering is an anti-goal. Clients still expect
			// the field, and an absent one has been known to crash them.
			StorageClass: "STANDARD",
		})
	}
	for _, p := range page.Prefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, listPrefix{Prefix: encode(unscoped(bucket, p))})
	}
	result.KeyCount = len(result.Contents) + len(result.CommonPrefixes)

	writeXML(w, r, result)
}

// escapeKey percent-encodes everything a client could read two ways.
//
// Not url.QueryEscape and not url.PathEscape: the first turns a space into "+",
// which a client unescaping paths hands back as a literal plus, and the second
// leaves "+" alone, which a client unescaping queries hands back as a space. Either
// mistake renames the key. Encoding down to the unreserved set leaves nothing whose
// meaning depends on which of the two the client chose.
func escapeKey(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"
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
