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
	return &Store{client: client, prefix: prefix}, nil
}

func (s *Store) Close() error { return s.client.Close() }

// Commit makes the object readable. Returning nil is the moment a write may be
// acknowledged to the client, and never before: every chunk the manifest
// references must already be durable on disk.
//
// A single etcd Put is enough. It is atomic and serialized, so a concurrent
// overwrite of the same key yields one manifest or the other and never a mix.
//
// ponytail: no compare-and-swap yet. It is needed to reclaim the chunks of a
// manifest this one replaces, since that requires knowing which revision was
// superseded; add it with garbage collection.
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

// Object is a manifest together with the key it is committed under.
type Object struct {
	Key      string
	Manifest object.Manifest
}

// ScanObjects returns up to limit objects whose keys sort after the given key, in
// key order. Passing "" starts at the beginning.
//
// Repair walks every object this way rather than holding them all in memory: the
// scan is what finds missing copies, and a cluster's manifest list outgrows a
// single response long before its data outgrows its disks.
func (s *Store) ScanObjects(ctx context.Context, after string, limit int64) ([]Object, error) {
	// A key sorts after `after` if it is greater; \x00 is the smallest possible
	// successor, so this skips exactly the key already seen.
	from := s.key(after) + "\x00"
	resp, err := s.client.Get(ctx, from,
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(s.objectPrefix())),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(limit),
	)
	if err != nil {
		return nil, fmt.Errorf("meta: scan objects after %q: %w", after, err)
	}

	objects := make([]Object, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), s.objectPrefix())
		var m object.Manifest
		if err := json.Unmarshal(kv.Value, &m); err != nil {
			return nil, fmt.Errorf("meta: corrupt manifest for %s: %w", key, err)
		}
		objects = append(objects, Object{Key: key, Manifest: m})
	}
	return objects, nil
}

// SaveRepairCursor records how far a node's repair pass got, so that a restart
// resumes instead of starting over. A heal that begins again from the first
// object every time a process restarts never reaches the last one.
func (s *Store) SaveRepairCursor(ctx context.Context, node, key string) error {
	if _, err := s.client.Put(ctx, s.cursorKey(node), key); err != nil {
		return fmt.Errorf("meta: save repair cursor for %s: %w", node, err)
	}
	return nil
}

// RepairCursor returns where this node's repair pass should resume, or "" to
// start from the beginning.
func (s *Store) RepairCursor(ctx context.Context, node string) (string, error) {
	resp, err := s.client.Get(ctx, s.cursorKey(node))
	if err != nil {
		return "", fmt.Errorf("meta: read repair cursor for %s: %w", node, err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// key namespaces an object key. Object keys may contain anything, including
// slashes, which is fine: etcd keys are opaque byte strings, so the only thing
// that matters is that the mapping is unambiguous.
func (s *Store) key(objectKey string) string { return s.objectPrefix() + objectKey }

func (s *Store) objectPrefix() string { return path.Join(s.prefix, "objects") + "/" }

func (s *Store) cursorKey(node string) string {
	return path.Join(s.prefix, "repair", node, "cursor")
}
