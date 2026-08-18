package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/0vertake/kavo/internal/object"
)

// ErrNotWriting says a write's record was gone by the time it tried to commit, so
// the chunks it stored may already have been collected. The write failed; nothing
// was acknowledged, which is the only outcome that keeps invariant 1.
var ErrNotWriting = fmt.Errorf("meta: the record of this write is gone")

// A write's chunks are durable before the manifest naming them is committed, and for
// that whole window nothing in etcd says they exist. Collection used to defend them
// by age alone, which is a guess about how long a write takes: S3 allows a single PUT
// of 5 GB, and a slow link turns that into hours. So a write that can store more than
// one chunk says so here first, and the sweep reads it.
//
// The record is one key naming the write, because every chunk of a write shares an id
// prefix — one key protects a five-gigabyte upload as cheaply as a two-chunk one. Its
// value is the node coordinating the write, which is how a record outlives its writer
// by no more than a membership lease: the sweep drops records belonging to nodes that
// are no longer members, and CommitWhileWriting refuses to commit a write whose
// record has been dropped. A writer that loses etcd for longer than the lease
// therefore fails its write rather than acknowledging one whose early chunks are gone.

// MarkWriting records that node has begun a write whose chunk ids all start with id.
func (s *Store) MarkWriting(ctx context.Context, id, node string) error {
	if id == "" || strings.Contains(id, "/") {
		return fmt.Errorf("meta: write id %q must be non-empty and contain no slashes", id)
	}
	if _, err := s.client.Put(ctx, s.writingKey(id), node); err != nil {
		return fmt.Errorf("meta: record write %s: %w", id, err)
	}
	return nil
}

// DoneWriting removes the record, which is the last step of a write rather than the
// commit: until it returns, the sweep still treats the write's chunks as live. Doing
// it in the other order would leave a window where the chunks are protected by
// neither the record nor the manifest.
func (s *Store) DoneWriting(ctx context.Context, id string) error {
	if _, err := s.client.Delete(ctx, s.writingKey(id)); err != nil {
		return fmt.Errorf("meta: clear the record of write %s: %w", id, err)
	}
	return nil
}

// Writing returns the writes in flight as write id -> the node coordinating it.
func (s *Store) Writing(ctx context.Context) (map[string]string, error) {
	resp, err := s.client.Get(ctx, s.writingPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("meta: list writes in flight: %w", err)
	}
	writes := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		writes[path.Base(string(kv.Key))] = string(kv.Value)
	}
	return writes, nil
}

// CommitWhileWriting commits a manifest only if the write's record is still there,
// and returns ErrNotWriting if it is not.
//
// This is the commit point for a write that announced itself, and the condition is
// what makes announcing worth anything. A record is removed by the sweep when the
// node holding it stops being a member — a crash, or etcd unreachable for longer
// than the lease — and the chunks it was protecting become collectable at that
// moment. If such a writer could still commit, it would acknowledge an object whose
// first chunks are being deleted. It cannot: the commit and the condition are one
// etcd transaction, so either the record was live and the manifest is committed, or
// the write fails and the client retries.
func (s *Store) CommitWhileWriting(ctx context.Context, key string, m object.Manifest, id string) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("meta: marshal manifest for %s: %w", key, err)
	}
	if err := s.whileWriting(ctx, id, clientv3.OpPut(s.key(key), string(data))); err != nil {
		return fmt.Errorf("meta: commit manifest for %s: %w", key, err)
	}
	return nil
}

// CommitPartWhileWriting is CommitPart under the same condition, because a part
// upload is a write like any other: its chunks are unreferenced until the part's
// manifest lands, and a 5 GB part takes as long to arrive as a 5 GB object.
func (s *Store) CommitPartWhileWriting(ctx context.Context, id string, number int, m object.Manifest, writeID string) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("meta: marshal part %d of upload %s: %w", number, id, err)
	}
	resp, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.CreateRevision(s.writingKey(writeID)), ">", 0),
			clientv3.Compare(clientv3.CreateRevision(s.uploadKey(id)), ">", 0),
		).
		Then(clientv3.OpPut(s.partKey(id, number), string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("meta: commit part %d of upload %s: %w", number, id, err)
	}
	if resp.Succeeded {
		return nil
	}
	if _, err := s.Upload(ctx, id); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", ErrNotWriting, writeID)
}

func (s *Store) whileWriting(ctx context.Context, id string, op clientv3.Op) error {
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(s.writingKey(id)), ">", 0)).
		Then(op).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("%w: %s", ErrNotWriting, id)
	}
	return nil
}

func (s *Store) writingPrefix() string       { return path.Join(s.prefix, "writing") + "/" }
func (s *Store) writingKey(id string) string { return s.writingPrefix() + id }
