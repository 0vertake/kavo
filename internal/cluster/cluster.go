// Package cluster coordinates a request across nodes. Any node can coordinate
// any request: it maps the object key to its partition's owners, replicates each
// chunk to them, and commits the manifest only once enough copies are durable.
package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/peer"
	"github.com/0vertake/kavo/internal/ring"
	"github.com/0vertake/kavo/internal/store"
)

const (
	// Replicas (N) is how many nodes a chunk is placed on.
	Replicas = 3
	// WriteQuorum (W) is how many of those must have fsynced the chunk before
	// the write may be acknowledged. W < N is what lets a write survive a node
	// being down; repair restores the rest.
	WriteQuorum = 2
)

// ErrQuorum reports that a chunk did not reach enough nodes, so the write was
// refused. No manifest is committed, so nothing partial becomes readable.
var ErrQuorum = errors.New("cluster: write quorum not met")

// Coordinator handles client requests on behalf of the whole cluster.
type Coordinator struct {
	self      string
	peers     map[string]string // node id -> host:port, including self
	ring      *ring.Ring
	store     *store.Store
	meta      *meta.Store
	chunkSize int64
}

// New builds a coordinator. peers must list every node in the cluster including
// self, so that every node derives the same ring and therefore the same
// placement; disagreeing rings would scatter an object's chunks.
func New(self string, peers map[string]string, s *store.Store, m *meta.Store, chunkSize int64) (*Coordinator, error) {
	if _, ok := peers[self]; !ok {
		return nil, fmt.Errorf("cluster: own id %q missing from the peer list %v", self, peers)
	}
	return &Coordinator{
		self:      self,
		peers:     peers,
		ring:      ring.New(slices.Sorted(maps.Keys(peers)), ring.DefaultVNodes),
		store:     s,
		meta:      m,
		chunkSize: chunkSize,
	}, nil
}

// Put streams an object into the cluster and returns its committed manifest.
//
// Returning nil is the acknowledgement point. By then every chunk is fsynced on
// at least W owners and the manifest is in etcd; before then the object does not
// exist, however many chunks are already on disk.
func (c *Coordinator) Put(ctx context.Context, key string, body io.Reader) (object.Manifest, error) {
	owners := c.ring.Owners(ring.PartitionFor(key), Replicas)
	if len(owners) == 0 {
		return object.Manifest{}, fmt.Errorf("cluster: no nodes available to place %q", key)
	}

	m, err := object.Write(body, c.chunkSize, func(ref object.ChunkRef, data []byte) error {
		return c.replicate(ctx, ref, data, owners)
	})
	if err != nil {
		return object.Manifest{}, err
	}
	m.Nodes = owners

	if err := c.meta.Commit(ctx, key, m); err != nil {
		return object.Manifest{}, err
	}
	return m, nil
}

// Resolve returns the committed manifest for an object key, or meta.ErrNotFound.
// Callers that must know the object's size before streaming it resolve first.
func (c *Coordinator) Resolve(ctx context.Context, key string) (object.Manifest, error) {
	return c.meta.Get(ctx, key)
}

// Stream writes a resolved object to w, reading each chunk from whichever owner
// still has it.
func (c *Coordinator) Stream(ctx context.Context, m object.Manifest, w io.Writer) error {
	return object.Read(m, w, func(ref object.ChunkRef) (io.ReadCloser, error) {
		return c.fetch(ctx, ref, m.Nodes)
	})
}

// replicate writes one chunk to every owner and insists on W successes.
//
// It waits for all N rather than returning at W: the extra copy is the whole
// point of N > W, and a push abandoned when the request ends would leave the
// object under-replicated until repair noticed.
func (c *Coordinator) replicate(ctx context.Context, ref object.ChunkRef, data []byte, owners []string) error {
	errs := make([]error, len(owners))
	var wg sync.WaitGroup
	for i, owner := range owners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.storeChunk(ctx, owner, ref, data)
		}()
	}
	wg.Wait()

	acks := 0
	for _, err := range errs {
		if err == nil {
			acks++
		}
	}
	// A cluster smaller than W cannot promise W copies, so the requirement is
	// every owner there is. Never acknowledge fewer copies than are available.
	if want := min(WriteQuorum, len(owners)); acks < want {
		return fmt.Errorf("%w: chunk %s reached %d of %d owners (needed %d): %w",
			ErrQuorum, ref.ID, acks, len(owners), want, errors.Join(errs...))
	}
	return nil
}

func (c *Coordinator) storeChunk(ctx context.Context, owner string, ref object.ChunkRef, data []byte) error {
	if owner == c.self {
		return c.store.WriteChunkVerified(ref.ID, bytes.NewReader(data), ref.CRC, ref.Size)
	}
	return peer.PushChunk(ctx, c.peers[owner], ref.ID, ref.CRC, ref.Size, bytes.NewReader(data))
}

// fetch returns a reader for one chunk, trying owners until one answers. A chunk
// that only reached W of N owners is genuinely absent from the others, so a miss
// is expected rather than a fault.
func (c *Coordinator) fetch(ctx context.Context, ref object.ChunkRef, nodes []string) (io.ReadCloser, error) {
	var errs []error
	for _, node := range c.localFirst(nodes) {
		var rc io.ReadCloser
		var err error
		if node == c.self {
			rc, err = c.store.ReadChunk(ref.ID, ref.CRC)
		} else {
			rc, err = peer.FetchChunk(ctx, c.peers[node], ref.ID, ref.CRC)
		}
		if err == nil {
			return rc, nil
		}
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("cluster: no owner of chunk %s could serve it: %w", ref.ID, errors.Join(errs...))
}

// localFirst prefers this node's own copy, which costs no network round trip.
func (c *Coordinator) localFirst(nodes []string) []string {
	i := slices.Index(nodes, c.self)
	if i <= 0 {
		return nodes
	}
	ordered := slices.Clone(nodes)
	ordered[0], ordered[i] = ordered[i], ordered[0]
	return ordered
}
