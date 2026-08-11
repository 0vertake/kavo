// Package cluster coordinates a request across nodes. Any node can coordinate
// any request: it maps the object key to its partition's owners, replicates each
// chunk to them, and commits the manifest only once enough copies are durable.
package cluster

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0vertake/kavo/internal/ec"
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

// membership is an immutable snapshot of who is live and where they are, with the
// ring that follows from it. Replacing it wholesale means a request always sees
// one coherent view of the cluster rather than a map changing underneath it.
type membership struct {
	peers map[string]string // node id -> host:port, including self
	ring  *ring.Ring
}

// Coordinator handles client requests on behalf of the whole cluster.
type Coordinator struct {
	self      string
	addr      string
	live      atomic.Pointer[membership]
	store     *store.Store
	meta      *meta.Store
	chunkSize int64
	// scheme is how new objects are written: the zero Scheme replicates, and
	// anything else erasure-codes. Only writes consult it — a reader follows the
	// manifest, so changing the mode never strands data written under the old one.
	scheme ec.Scheme
}

// New builds a coordinator that initially knows only about itself. Callers feed
// it the cluster's membership as etcd reports it.
func New(self, addr string, s *store.Store, m *meta.Store, chunkSize int64) *Coordinator {
	c := &Coordinator{self: self, addr: addr, store: s, meta: m, chunkSize: chunkSize}
	c.SetMembers(nil)
	return c
}

// EncodeWith switches new writes to an erasure code. The zero Scheme means
// replication, which is the default.
//
// Call it before serving requests. It is not synchronised, because the redundancy
// mode is configuration and configuration does not change under load — unlike
// membership, which does and is.
func (c *Coordinator) EncodeWith(s ec.Scheme) error {
	if s != (ec.Scheme{}) && !s.Valid() {
		return fmt.Errorf("cluster: %s is not a usable erasure code", s)
	}
	c.scheme = s
	return nil
}

// width is how many nodes an object's chunks are spread over: N copies on N
// nodes, or one shard each on as many nodes as the code has shards.
func (c *Coordinator) width() int {
	if c.scheme == (ec.Scheme{}) {
		return Replicas
	}
	return c.scheme.Shards()
}

// SetMembers replaces this node's view of the cluster. Self is always included:
// a node is alive by definition, so it belongs in its own ring even if its
// registration in etcd has not landed or has briefly lapsed.
func (c *Coordinator) SetMembers(peers map[string]string) {
	next := maps.Clone(peers)
	if next == nil {
		next = make(map[string]string, 1)
	}
	next[c.self] = c.addr
	c.live.Store(&membership{
		peers: next,
		ring:  ring.New(slices.Sorted(maps.Keys(next)), ring.DefaultVNodes),
	})
}

// Members reports this node's view of the cluster, which is what makes failure
// detection observable from outside.
func (c *Coordinator) Members() map[string]string {
	return maps.Clone(c.live.Load().peers)
}

// PutOptions carries what an object remembers beyond its bytes. Everything in it
// comes from the client and is handed back unchanged.
type PutOptions struct {
	ContentType string
	// ETag overrides the MD5 this write would compute. Only a multipart
	// completion sets it: that object's ETag is derived from its parts' and there
	// are no bytes here to hash.
	ETag string
}

// Put streams an object into the cluster and returns its committed manifest.
//
// Returning nil is the acknowledgement point. By then every chunk is fsynced on
// at least W owners and the manifest is in etcd; before then the object does not
// exist, however many chunks are already on disk.
func (c *Coordinator) Put(ctx context.Context, key string, body io.Reader, opts PutOptions) (object.Manifest, error) {
	// One snapshot for the whole object: membership may change mid-upload, and
	// spreading an object's chunks over two different rings would leave later
	// chunks on nodes the manifest does not name.
	live := c.live.Load()
	width := c.width()
	owners := live.ring.Owners(ring.PartitionFor(key), width)
	if len(owners) == 0 {
		return object.Manifest{}, fmt.Errorf("cluster: no nodes available to place %q", key)
	}
	// A shard is identified by position, so a cluster smaller than the code has
	// nowhere to put the rest. Replication can narrow to the nodes that exist;
	// erasure coding cannot, and writing a chunk it can never rebuild would be
	// worse than refusing it.
	if c.scheme != (ec.Scheme{}) && len(owners) < width {
		return object.Manifest{}, fmt.Errorf("cluster: %s needs %d nodes, cluster has %d",
			c.scheme, width, len(owners))
	}

	// The ETag is the object's MD5 because that is what S3 clients check an
	// upload against, and hashing here is the only place the whole object passes
	// through in order. Alongside the write, not after it: reading the object
	// back to hash it would double the traffic of every PUT.
	sum := md5.New()
	m, err := object.Write(io.TeeReader(body, sum), c.chunkSize, func(ref *object.ChunkRef, data []byte) error {
		if c.scheme != (ec.Scheme{}) {
			return c.encode(ctx, ref, data, owners, live)
		}
		return c.replicate(ctx, *ref, data, owners, live)
	})
	if err != nil {
		return object.Manifest{}, err
	}
	m.Nodes = owners
	m.Coding = c.scheme
	m.ETag = cmp.Or(opts.ETag, hex.EncodeToString(sum.Sum(nil)))
	m.ContentType = opts.ContentType
	// Truncated to the millisecond S3 reports, so that what a client is told and
	// what a later listing says are the same instant.
	m.Modified = time.Now().UTC().Truncate(time.Millisecond)

	if err := c.meta.Commit(ctx, key, m); err != nil {
		return object.Manifest{}, err
	}
	return m, nil
}

// Delete removes an object. Its manifest goes first, which is the instant the
// object stops existing, and only then are its chunks reclaimed: dropping chunks
// a live manifest still names would be a read served from nothing.
//
// Deleting what is not there succeeds, because S3 promises an idempotent delete.
func (c *Coordinator) Delete(ctx context.Context, key string) error {
	m, err := c.meta.Get(ctx, key)
	if errors.Is(err, meta.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := c.meta.Delete(ctx, key); err != nil {
		return err
	}

	// Failures here are logged rather than returned: the object is already gone
	// as far as any reader is concerned, so a copy that could not be deleted is
	// wasted disk and not a correctness problem.
	live := c.live.Load()
	for _, ref := range m.Chunks {
		for i, node := range m.Nodes {
			id := ref.ID
			if m.Coding != (ec.Scheme{}) {
				id = ref.ShardID(i)
			}
			if err := c.drop(ctx, node, id, live); err != nil {
				log.Printf("delete %s: %v", key, err)
			}
		}
	}
	return nil
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
		return c.chunk(ctx, m, ref)
	})
}

// StreamRange writes length bytes of a resolved object to w, starting at off.
func (c *Coordinator) StreamRange(ctx context.Context, m object.Manifest, w io.Writer, off, length int64) error {
	return object.ReadRange(m, w, off, length, func(ref object.ChunkRef) (io.ReadCloser, error) {
		return c.chunk(ctx, m, ref)
	})
}

// chunk returns a reader for one chunk of an object, whichever way it is stored.
func (c *Coordinator) chunk(ctx context.Context, m object.Manifest, ref object.ChunkRef) (io.ReadCloser, error) {
	if m.Coding == (ec.Scheme{}) {
		return c.fetch(ctx, ref, m.Nodes)
	}
	// An erasure-coded chunk has to be whole before any of it is valid, so it is
	// decoded into memory and then streamed. One chunk at a time either way: the
	// object's size still does not decide the footprint.
	data, err := c.decode(ctx, ref, m.Nodes, m.Coding)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// replicate writes one chunk to every owner and insists on W successes.
//
// It waits for all N rather than returning at W: the extra copy is the whole
// point of N > W, and a push abandoned when the request ends would leave the
// object under-replicated until repair noticed.
func (c *Coordinator) replicate(ctx context.Context, ref object.ChunkRef, data []byte, owners []string, live *membership) error {
	errs := make([]error, len(owners))
	var wg sync.WaitGroup
	for i, owner := range owners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.storeChunk(ctx, owner, ref, data, live)
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

func (c *Coordinator) storeChunk(ctx context.Context, owner string, ref object.ChunkRef, data []byte, live *membership) error {
	if owner == c.self {
		return c.store.WriteChunkVerified(ref.ID, bytes.NewReader(data), ref.CRC, ref.Size)
	}
	return peer.PushChunk(ctx, live.peers[owner], ref.ID, ref.CRC, ref.Size, bytes.NewReader(data))
}

// fetch returns a reader for one chunk, trying owners until one answers. A chunk
// that only reached W of N owners is genuinely absent from the others, so a miss
// is expected rather than a fault.
func (c *Coordinator) fetch(ctx context.Context, ref object.ChunkRef, nodes []string) (io.ReadCloser, error) {
	live := c.live.Load()
	var errs []error
	for _, node := range c.localFirst(nodes) {
		var rc io.ReadCloser
		var err error
		switch addr := live.peers[node]; {
		case node == c.self:
			rc, err = c.store.ReadChunk(ref.ID, ref.CRC)
		case addr == "":
			// The manifest names a node that has since left the cluster, so
			// there is no address left to try. Its copy may well still exist.
			err = fmt.Errorf("cluster: node %s holding chunk %s is not a member", node, ref.ID)
		default:
			rc, err = peer.FetchChunk(ctx, addr, ref.ID, ref.CRC)
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
