// Package meta stores object manifests in etcd.
//
// This is kavo's commit point. Chunks on disk are just bytes nobody references;
// an object exists exactly when its manifest is in etcd, and readers resolve
// objects only through committed manifests. That is what makes a torn object
// structurally impossible rather than merely unlikely.
//
// etcd holds metadata only — never object data.
package meta

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/0vertake/kavo/internal/object"
)

// ErrNotFound reports that no manifest is committed for an object key.
var ErrNotFound = errors.New("meta: object not found")

// DefaultEndpoint is the etcd address the dev Compose stack exposes.
const DefaultEndpoint = "127.0.0.1:2379"

// EndpointFromEnv resolves the etcd address from KAVO_ETCD, falling back to the
// dev default. Containers get their endpoint from the environment; a developer
// gets Compose without configuring anything.
func EndpointFromEnv() string {
	return cmp.Or(os.Getenv("KAVO_ETCD"), DefaultEndpoint)
}

// Store is the manifest store for one cluster. Several clusters can share an
// etcd by using different prefixes.
type Store struct {
	client *clientv3.Client
	prefix string
	// objects is the key prefix every manifest lives under, kept rather than
	// rebuilt: it is joined onto every read and written off every listed key, so
	// composing it per key made a page of a thousand allocate two thousand copies
	// of the same constant.
	objects string
}

// Open connects to etcd. It does not block on reachability: etcd may be
// starting, and a request will report the problem with context anyway.
func Open(endpoints []string, prefix string) (*Store, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("meta: connect to etcd %v: %w", endpoints, err)
	}
	return &Store{
		client:  client,
		prefix:  prefix,
		objects: path.Join(prefix, "objects") + "/",
	}, nil
}

func (s *Store) Close() error { return s.client.Close() }

// Commit makes the object readable. Returning nil is the moment a write may be
// acknowledged to the client, and never before: every chunk the manifest
// references must already be durable on disk.
//
// A single etcd Put is enough. It is atomic and serialized, so a concurrent
// overwrite of the same key yields one manifest or the other and never a mix.
//
// It stayed a plain Put once garbage collection existed. Reclaiming the chunks an
// overwrite superseded looked like it needed the revision this commit replaced, and
// it does if the reclaiming is done from a record of what each write superseded.
// Collection is mark-and-sweep instead, which needs nothing from here — and which
// is what allows two keys to name the same chunks.
func (s *Store) Commit(ctx context.Context, key string, m object.Manifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("meta: marshal manifest for %s: %w", key, err)
	}
	if _, err := s.client.Put(ctx, s.key(key), string(data)); err != nil {
		return fmt.Errorf("meta: commit manifest for %s: %w", key, err)
	}
	return nil
}

// Get resolves an object key to its committed manifest, or ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) (object.Manifest, error) {
	resp, err := s.client.Get(ctx, s.key(key))
	if err != nil {
		return object.Manifest{}, fmt.Errorf("meta: read manifest for %s: %w", key, err)
	}
	if len(resp.Kvs) == 0 {
		return object.Manifest{}, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	var m object.Manifest
	if err := json.Unmarshal(resp.Kvs[0].Value, &m); err != nil {
		return object.Manifest{}, fmt.Errorf("meta: corrupt manifest for %s: %w", key, err)
	}
	return m, nil
}

// Delete removes an object's manifest, which is the moment the object stops
// existing: readers resolve objects only through committed manifests, so its
// chunks become unreachable garbage the instant this returns.
//
// Deleting a key that is not there is not an error. S3 promises an idempotent
// delete, and a caller that has to distinguish "gone" from "was never here" can
// read first.
func (s *Store) Delete(ctx context.Context, key string) error {
	if _, err := s.client.Delete(ctx, s.key(key)); err != nil {
		return fmt.Errorf("meta: delete manifest for %s: %w", key, err)
	}
	return nil
}

// ErrChanged reports that a manifest was modified since it was read, so a
// conditional commit did nothing.
var ErrChanged = errors.New("meta: manifest changed since it was read")

// Object is a manifest together with the key it is committed under and the
// revision it was read at.
type Object struct {
	Key      string
	Manifest object.Manifest
	// Revision is what makes a read-modify-write safe. Rebalancing rewrites a
	// manifest it read earlier, and a client may have overwritten the object in
	// between: without the revision, moving the old object's chunks would
	// clobber the new object's manifest and lose an acknowledged write.
	Revision int64
}

// CommitIfUnchanged replaces a manifest only if it is still at revision, and
// reports ErrChanged if it is not.
//
// This is the compare-and-swap the plain Commit path does not need: a client
// PUT is the newest truth by definition and may overwrite freely, but a
// background pass acting on what it read minutes ago may not.
func (s *Store) CommitIfUnchanged(ctx context.Context, key string, m object.Manifest, revision int64) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("meta: marshal manifest for %s: %w", key, err)
	}
	k := s.key(key)
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(k), "=", revision)).
		Then(clientv3.OpPut(k, string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("meta: commit manifest for %s at revision %d: %w", key, revision, err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("%w: %s", ErrChanged, key)
	}
	return nil
}

// After is the smallest key that sorts strictly after key, for a caller that has
// finished with one key and wants the scan to resume past it.
func After(key string) string { return key + "\x00" }

// PastPrefix is the smallest key that sorts after everything beginning with
// prefix, which is how a listing steps over a whole group of keys without reading
// them. It is empty for the empty prefix, since nothing sorts past everything.
func PastPrefix(prefix string) string {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] < 0xff {
			return prefix[:i] + string(prefix[i]+1)
		}
	}
	return ""
}

// ScanObjects returns up to limit objects whose keys start with prefix, starting
// at from and in key order. Empty strings mean "no prefix" and "from the
// beginning"; use After to resume past a key already handled.
//
// Repair walks every object this way rather than holding them all in memory, and
// a listing pages through one bucket's worth: the scan is what finds missing
// copies, and a cluster's manifest list outgrows a single response long before its
// data outgrows its disks.
func (s *Store) ScanObjects(ctx context.Context, prefix, from string, limit int64) ([]Object, error) {
	resp, err := s.scan(ctx, prefix, from, limit)
	if err != nil || resp == nil {
		return nil, err
	}

	objects := make([]Object, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), s.objects)
		var m object.Manifest
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			return nil, fmt.Errorf("meta: corrupt manifest for %s: %w", key, err)
		}
		objects = append(objects, Object{Key: key, Manifest: m, Revision: kv.ModRevision})
	}
	return objects, nil
}

// Entry is an object as a listing sees it: the key and the three fields S3
// reports for it. No chunks — that is the whole point of the type.
type Entry struct {
	Key      string
	Size     int64
	ETag     string
	Modified time.Time
}

// ScanEntries is ScanObjects for a listing: the same range, decoding only what a
// listing reports.
//
// Worth its own method because the chunk list is most of a manifest and none of
// what a listing wants. A page of a thousand 1 GB objects carries 32,000 chunk
// references, and decoding them to report three scalars per key cost 6x the whole
// listing: 55 ms a page against 9 ms, and 12 MB of garbage per request.
func (s *Store) ScanEntries(ctx context.Context, prefix, from string, limit int64) ([]Entry, error) {
	resp, err := s.scan(ctx, prefix, from, limit)
	if err != nil || resp == nil {
		return nil, err
	}

	entries := make([]Entry, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), s.objects)
		// The fields not named here are skipped by the decoder rather than built.
		var m struct {
			Size     int64
			ETag     string
			Modified time.Time
		}
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			return nil, fmt.Errorf("meta: corrupt manifest for %s: %w", key, err)
		}
		entries = append(entries, Entry{Key: key, Size: m.Size, ETag: m.ETag, Modified: m.Modified})
	}
	return entries, nil
}

// scan is the range both scans read, and returns nil when the range is empty.
func (s *Store) scan(ctx context.Context, prefix, from string, limit int64) (*clientv3.GetResponse, error) {
	start := s.key(prefix)
	if from > prefix {
		start = s.key(from)
	}
	end := clientv3.GetPrefixRangeEnd(s.key(prefix))
	if start >= end {
		return nil, nil // the scan starts past everything the prefix covers
	}
	resp, err := s.client.Get(ctx, start,
		clientv3.WithRange(end),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit),
	)
	if err != nil {
		return nil, fmt.Errorf("meta: scan objects with prefix %q from %q: %w", prefix, from, err)
	}
	return resp, nil
}

// SaveCursor records how far a node got in a background walk over the cluster's
// objects, so that a restart resumes instead of starting over. A pass that begins
// again from the first object every time a process restarts never reaches the
// last one.
func (s *Store) SaveCursor(ctx context.Context, task, node, key string) error {
	if _, err := s.client.Put(ctx, s.cursorKey(task, node), key); err != nil {
		return fmt.Errorf("meta: save %s cursor for %s: %w", task, node, err)
	}
	return nil
}

// Cursor returns where a node's walk should resume, or "" to start from the
// beginning.
func (s *Store) Cursor(ctx context.Context, task, node string) (string, error) {
	resp, err := s.client.Get(ctx, s.cursorKey(task, node))
	if err != nil {
		return "", fmt.Errorf("meta: read %s cursor for %s: %w", task, node, err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// key namespaces an object key. Object keys may contain anything, including
// slashes, which is fine: etcd keys are opaque byte strings, so the only thing
// that matters is that the mapping is unambiguous.
func (s *Store) key(objectKey string) string { return s.objects + objectKey }

func (s *Store) cursorKey(task, node string) string {
	return path.Join(s.prefix, "cursors", task, node)
}
