package meta

// In-flight multipart uploads.
//
// They live under their own prefix, not among the objects: a part is not an object
// and must never appear in a listing or resolve as one. An upload becomes an object
// at exactly one moment — the commit of a manifest under the object's key — which
// is the same commit point every other write goes through.

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/0vertake/kavo/internal/object"
)

// Upload is what an in-flight multipart upload remembers between requests: which
// object it will become, and what the client said about it when it started.
type Upload struct {
	Key         string
	ContentType string
	Created     time.Time
	// Meta is what the client attached when it began the upload. S3 takes the
	// object's metadata from the creation call rather than from the parts or the
	// completion, so it has to survive here for as long as the upload does.
	Meta map[string]string `json:",omitempty"`
}

// CreateUpload records a new multipart upload.
func (s *Store) CreateUpload(ctx context.Context, id string, u Upload) error {
	data, err := json.Marshal(u)
	if err != nil {
		return fmt.Errorf("meta: marshal upload %s: %w", id, err)
	}
	if _, err := s.client.Put(ctx, s.uploadKey(id), string(data)); err != nil {
		return fmt.Errorf("meta: create upload %s: %w", id, err)
	}
	return nil
}

// Upload returns a recorded upload, or ErrNotFound. An upload id a client made up,
// or one that has already been completed or aborted, is indistinguishable — and S3
// treats them the same way, since both mean "there is no such upload".
func (s *Store) Upload(ctx context.Context, id string) (Upload, error) {
	resp, err := s.client.Get(ctx, s.uploadKey(id))
	if err != nil {
		return Upload{}, fmt.Errorf("meta: read upload %s: %w", id, err)
	}
	if len(resp.Kvs) == 0 {
		return Upload{}, fmt.Errorf("%w: upload %s", ErrNotFound, id)
	}
	var u Upload
	if err := json.Unmarshal(resp.Kvs[0].Value, &u); err != nil {
		return Upload{}, fmt.Errorf("meta: corrupt upload %s: %w", id, err)
	}
	return u, nil
}

// CommitPart records a part's manifest. The part's chunks are already durable by
// the time this is called, so a part behaves like a small object that only the
// upload can see.
func (s *Store) CommitPart(ctx context.Context, id string, number int, m object.Manifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("meta: marshal part %d of upload %s: %w", number, id, err)
	}
	if _, err := s.client.Put(ctx, s.partKey(id, number), string(data)); err != nil {
		return fmt.Errorf("meta: commit part %d of upload %s: %w", number, id, err)
	}
	return nil
}

// Parts returns every part recorded for an upload, keyed by part number.
//
// All of them at once, deliberately: 10,000 parts is S3's limit, so the worst case
// is bounded and small, and completing an upload has to see every part anyway.
func (s *Store) Parts(ctx context.Context, id string) (map[int]object.Manifest, error) {
	resp, err := s.client.Get(ctx, s.uploadPrefix(id)+"parts/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("meta: read parts of upload %s: %w", id, err)
	}

	parts := make(map[int]object.Manifest, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		number, err := strconv.Atoi(path.Base(string(kv.Key)))
		if err != nil {
			return nil, fmt.Errorf("meta: part key %q of upload %s: %w", kv.Key, id, err)
		}
		var m object.Manifest
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			return nil, fmt.Errorf("meta: corrupt part %d of upload %s: %w", number, id, err)
		}
		parts[number] = m
	}
	return parts, nil
}

// PartRef is one part manifest and the etcd key it lives under, so that a scan of
// every upload's parts can resume past it.
type PartRef struct {
	Key      string
	Manifest object.Manifest
}

// ScanParts returns up to limit part manifests belonging to any in-flight upload,
// in key order, starting after from. An upload's own record is returned too, with a
// zero manifest, so that a caller paging by key does not have to know which of an
// upload's keys are parts.
//
// Garbage collection is what needs this. A part's chunks are durable long before
// the upload they belong to becomes an object — for as long as the client takes,
// which S3 allows to be days — so a sweep that only consulted object manifests
// would find those chunks referenced by nothing and delete the upload out from
// under a client that is still uploading to it.
func (s *Store) ScanParts(ctx context.Context, from string, limit int64) ([]PartRef, error) {
	start := path.Join(s.prefix, "uploads") + "/"
	if from != "" {
		start = After(from)
	}
	end := clientv3.GetPrefixRangeEnd(path.Join(s.prefix, "uploads") + "/")
	if start >= end {
		return nil, nil
	}
	resp, err := s.client.Get(ctx, start,
		clientv3.WithRange(end),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit),
	)
	if err != nil {
		return nil, fmt.Errorf("meta: scan upload parts from %q: %w", from, err)
	}

	parts := make([]PartRef, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		// Every upload has one record that is not a part, and it does not
		// decode as a manifest.
		if !strings.Contains(key, "/parts/") {
			parts = append(parts, PartRef{Key: key})
			continue
		}
		var m object.Manifest
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			return nil, fmt.Errorf("meta: corrupt part %s: %w", key, err)
		}
		parts = append(parts, PartRef{Key: key, Manifest: m})
	}
	return parts, nil
}

// UploadRef is an in-flight upload and the id it is listed under.
type UploadRef struct {
	ID     string
	Upload Upload
}

// scanBudget bounds how many etcd keys one listing may read. An upload's parts
// share its prefix, so the keys in this range are uploads *and* their parts, and a
// store with a few thousand abandoned uploads should not turn one listing into an
// unbounded read. A listing that runs out of budget says so and hands back a marker,
// which is what pagination is for.
const scanBudget = 1024

// Uploads returns in-flight uploads whose object key begins with prefix, in id
// order, starting after the id after, and no more than limit of them. The second
// result reports whether the scan stopped early, meaning there may be more.
//
// Id order rather than key order, which is what S3 documents: ordering by key would
// mean reading every in-flight upload in the store into memory to sort them, and
// nothing here reads an unbounded set to answer one request. The markers work, so a
// client that pages sees every upload exactly once.
func (s *Store) Uploads(ctx context.Context, prefix, after string, limit int) ([]UploadRef, bool, error) {
	start := path.Join(s.prefix, "uploads") + "/"
	if after != "" {
		start = After(s.uploadPrefix(after))
	}
	end := clientv3.GetPrefixRangeEnd(path.Join(s.prefix, "uploads") + "/")
	if start >= end {
		return nil, false, nil
	}
	resp, err := s.client.Get(ctx, start,
		clientv3.WithRange(end),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(scanBudget),
	)
	if err != nil {
		return nil, false, fmt.Errorf("meta: list uploads after %q: %w", after, err)
	}

	uploads := make([]UploadRef, 0, limit)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasSuffix(key, "/upload") {
			continue // A part of some upload, not the record of one.
		}
		var u Upload
		if err := json.Unmarshal(kv.Value, &u); err != nil {
			return nil, false, fmt.Errorf("meta: corrupt upload %s: %w", key, err)
		}
		if !strings.HasPrefix(u.Key, prefix) {
			continue
		}
		if len(uploads) == limit {
			return uploads, true, nil
		}
		id := path.Base(path.Dir(key))
		uploads = append(uploads, UploadRef{ID: id, Upload: u})
	}
	// More is also true when the budget ran out mid-range: the range may hold
	// nothing else, but saying "no more" and being wrong loses an upload.
	return uploads, resp.More || int64(len(resp.Kvs)) == scanBudget, nil
}

// Completion is what an upload turned into, remembered after the upload itself is
// gone so that a client repeating a completion it never saw the answer to gets the
// answer rather than NoSuchUpload.
type Completion struct {
	Key   string
	ETag  string
	When  time.Time
	Parts int
}

// CompletionMemory is how long a finished upload id is remembered. Long enough for
// a client to retry a request whose response was lost — which is the only thing that
// asks — and short enough that the records cannot accumulate. It is an etcd lease
// rather than a background pass, so the record goes even if every node is down.
const CompletionMemory = time.Hour

// FinishUpload records what an upload became. It is called after the object's
// manifest is committed: this record is a convenience for retries, and an upload
// that failed to leave one is still an object.
func (s *Store) FinishUpload(ctx context.Context, id string, c Completion) error {
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("meta: marshal completion %s: %w", id, err)
	}
	lease, err := s.client.Grant(ctx, int64(CompletionMemory.Seconds()))
	if err != nil {
		return fmt.Errorf("meta: lease for completion %s: %w", id, err)
	}
	if _, err := s.client.Put(ctx, s.completionKey(id), string(data), clientv3.WithLease(lease.ID)); err != nil {
		return fmt.Errorf("meta: record completion %s: %w", id, err)
	}
	return nil
}

// Completion returns what an upload became, or ErrNotFound if this id was never
// completed or was completed longer ago than CompletionMemory.
func (s *Store) Completion(ctx context.Context, id string) (Completion, error) {
	resp, err := s.client.Get(ctx, s.completionKey(id))
	if err != nil {
		return Completion{}, fmt.Errorf("meta: read completion %s: %w", id, err)
	}
	if len(resp.Kvs) == 0 {
		return Completion{}, fmt.Errorf("%w: completion %s", ErrNotFound, id)
	}
	var c Completion
	if err := json.Unmarshal(resp.Kvs[0].Value, &c); err != nil {
		return Completion{}, fmt.Errorf("meta: corrupt completion %s: %w", id, err)
	}
	return c, nil
}

// completionKey lives outside the uploads range, so that a finished upload is not
// something a scan of in-flight uploads has to know to skip.
func (s *Store) completionKey(id string) string {
	return path.Join(s.prefix, "completed", id)
}

// DeleteUpload forgets an upload and its parts. It says nothing about the parts'
// chunks: a completed upload's chunks belong to the object's manifest now, and an
// aborted one's are reclaimed by the caller before this is called.
func (s *Store) DeleteUpload(ctx context.Context, id string) error {
	if _, err := s.client.Delete(ctx, s.uploadPrefix(id), clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("meta: delete upload %s: %w", id, err)
	}
	return nil
}

// uploadPrefix covers an upload's record and all its parts, so that forgetting an
// upload is one ranged delete.
func (s *Store) uploadPrefix(id string) string { return path.Join(s.prefix, "uploads", id) + "/" }

func (s *Store) uploadKey(id string) string { return s.uploadPrefix(id) + "upload" }

// partKey sorts parts by number, which is the order they are concatenated in.
func (s *Store) partKey(id string, number int) string {
	return fmt.Sprintf("%sparts/%05d", s.uploadPrefix(id), number)
}
