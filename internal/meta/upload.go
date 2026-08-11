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
