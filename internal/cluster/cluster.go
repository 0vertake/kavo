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
	"hash/crc32"
	"io"
	"log"
	"maps"
	"slices"
	"strings"
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
	// DefaultWriteQuorum (W) is how many of those must have fsynced the chunk
	// before the write may be acknowledged. W < N is what lets a write survive a
	// node being down; repair restores the rest.
	//
	// A default rather than a constant because W is the durability an operator is
	// asking for, and one node running kavo can only be asking for one copy. What
	// it is not is a function of how many nodes happen to be reachable — see the
	// placement check in write.
	DefaultWriteQuorum = 2
)

// ErrQuorum reports that a chunk did not reach enough nodes, so the write was
// refused. No manifest is committed, so nothing partial becomes readable.
var ErrQuorum = errors.New("cluster: write quorum not met")

// ErrBadDigest says the body did not hash to the digest the client declared for
// it, so the write was refused. Nothing was committed.
var ErrBadDigest = errors.New("cluster: the body does not match the digest declared for it")

// membership is an immutable snapshot of who is live and where they are, with the
// ring that follows from it. Replacing it wholesale means a request always sees
// one coherent view of the cluster rather than a map changing underneath it.
type membership struct {
	peers map[string]string // node id -> host:port, including self
	// known is every address this node has ever seen, including nodes that have
	// since left. Reads fall back to it: a lease that lapsed under load does not
	// move a chunk off the disk it is on, and an object that cannot be read is
	// lost as far as the client is concerned. Placement never consults it, because
	// acknowledging a write to a node the cluster has given up on would promise
	// durability nobody is maintaining.
	known map[string]string
	ring  *ring.Ring
}

// readAddr is where to try reaching a node for a read: its current address, or the
// last one it was known at. Reading from a node that has left is safe because
// chunks are immutable and checksum-verified — a stale address either answers with
// the bytes the manifest names or fails.
//
// Which divides every peer call in this package: anything that wants *bytes* asks
// here, and anything that hands bytes over or counts on them being somewhere uses
// peers, since giving a copy to a node that has left, or calling one there a copy,
// is the whole thing rebalancing exists to correct. The scrubber sat on the wrong
// side of that line and left rot unrepaired whenever the good copies were on nodes
// the cluster had dropped.
func (m *membership) readAddr(node string) string {
	return cmp.Or(m.peers[node], m.known[node])
}

// Coordinator handles client requests on behalf of the whole cluster.
type Coordinator struct {
	self      string
	quorum    int
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
	c := &Coordinator{self: self, addr: addr, store: s, meta: m, chunkSize: chunkSize,
		quorum: DefaultWriteQuorum}
	c.SetMembers(nil)
	return c
}

// AcknowledgeAt sets how many distinct nodes must have fsynced a chunk before the
// write it belongs to may be acknowledged.
//
// Call it before serving requests, like EncodeWith and for the same reason. One is
// the only honest setting for a single-node store, and it is honest about what it
// gives up: nothing survives that node's disk.
func (c *Coordinator) AcknowledgeAt(w int) error {
	if w < 1 || w > Replicas {
		return fmt.Errorf("cluster: write quorum %d is outside 1..%d", w, Replicas)
	}
	c.quorum = w
	return nil
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

// redundancy is how many nodes an object's chunks belong on.
//
// Read from the object rather than from this node's configuration, because an
// object carries the code it was written with and a cluster can hold both kinds at
// once: asking c.width() would target three nodes for a nine-shard object the
// moment the coordinator was started in replication mode.
//
// It is not len(m.Nodes) either, which is the mistake this replaced. That reads the
// object's redundancy off its current placement, so an object written while fewer
// nodes were visible — acknowledged at W, correctly — asks the ring for as many
// owners as it already has and stays a copy short of its configuration forever,
// with nothing to notice it, since every other check asks only whether the copies a
// manifest names are present.
func redundancy(m object.Manifest) int {
	if m.Coding.Valid() {
		return m.Coding.Shards()
	}
	return Replicas
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
	// Addresses accumulate rather than being replaced, so that a node dropping out
	// of the membership does not take the way to reach its copies with it. The
	// snapshot stays immutable, so readers need no lock.
	known := maps.Clone(next)
	if was := c.live.Load(); was != nil {
		for id, addr := range was.known {
			if _, still := known[id]; !still {
				known[id] = addr
			}
		}
	}
	c.live.Store(&membership{
		peers: next,
		known: known,
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
	// Size is the body's length as the client declared it, and is passed on as a
	// hint for sizing the write buffer. Zero means "not declared", and nothing
	// here is trusted for anything else.
	Size int64
	// Meta is stored with the object and returned on every read. It is not
	// interpreted, so it is also not validated beyond its size: see MaxMeta.
	Meta map[string]string
	// MD5 is the hex digest the client declared for the body, or "" if it did
	// not. A write whose bytes hash to something else is refused rather than
	// committed: the client is saying it knows what it sent, and a store that
	// accepted the other thing would have made a liar of one of them silently.
	MD5 string
	// CRC32C is the Castagnoli checksum the client declared for the body, or nil
	// if it did not. Compared after the write the same way as MD5, and for the
	// same reason.
	CRC32C *uint32
	// TrailingCRC32C is consulted after the body is consumed, when the checksum
	// arrived in a trailer rather than a header. The comparison is the same;
	// only the moment it can run differs.
	TrailingCRC32C func() (*uint32, error)
	// CRC32 is the IEEE checksum the client declared, or nil if it did not.
	CRC32 *uint32
	// TrailingCRC32 is the trailer form, same split as CRC32C.
	TrailingCRC32 func() (*uint32, error)
	// CRC64NVME is the CRC-64/NVME the client declared, or nil if it did not.
	CRC64NVME *uint64
	// TrailingCRC64NVME is the trailer form, same split as CRC32C.
	TrailingCRC64NVME func() (*uint64, error)
}

// Put streams an object into the cluster and returns its committed manifest.
//
// Returning nil is the acknowledgement point. By then every chunk is fsynced on
// at least W owners and the manifest is in etcd; before then the object does not
// exist, however many chunks are already on disk.
func (c *Coordinator) Put(ctx context.Context, key string, body io.Reader, opts PutOptions) (object.Manifest, error) {
	m, writing, err := c.write(ctx, key, body, opts.Size)
	if err != nil {
		return object.Manifest{}, err
	}
	// Before the commit, so a mismatch leaves an object that never existed rather
	// than one the client has to delete. The chunks are already on disk — the
	// digest is of the whole body, so there was nothing to compare until the last
	// byte arrived — and they are unreferenced, which makes them collection's.
	//
	// Against the computed MD5 rather than the ETag below, since a caller may
	// override that.
	if opts.MD5 != "" && !strings.EqualFold(opts.MD5, m.ETag) {
		c.stopWriting(ctx, writing)
		return object.Manifest{}, fmt.Errorf("%w: declared %s, received %s", ErrBadDigest, opts.MD5, m.ETag)
	}
	if err := checkCRC32C(m, opts); err != nil {
		c.stopWriting(ctx, writing)
		return object.Manifest{}, err
	}
	if err := checkCRC32(m, opts); err != nil {
		c.stopWriting(ctx, writing)
		return object.Manifest{}, err
	}
	if err := checkCRC64NVME(m, opts); err != nil {
		c.stopWriting(ctx, writing)
		return object.Manifest{}, err
	}

	m.ETag = cmp.Or(opts.ETag, m.ETag)
	m.ContentType = opts.ContentType
	m.Meta = opts.Meta

	if writing == "" {
		err = c.meta.Commit(ctx, key, m)
	} else {
		err = c.meta.CommitWhileWriting(ctx, key, m, writing)
	}
	// After the commit, always: until the record is gone the sweep still treats
	// this write's chunks as live, and a chunk protected by neither the record nor
	// a manifest — even for an instant — is a chunk a sweep may take.
	c.stopWriting(ctx, writing)
	if err != nil {
		return object.Manifest{}, err
	}
	return m, nil
}

// write stores a stream's chunks on the owners of key and returns the manifest
// naming them. Nothing is committed: the chunks are on disk and unreferenced until
// a caller commits a manifest that names them, which is what makes a torn object
// impossible rather than unlikely.
//
// It also returns the id of the record it took out for itself, if it took one: a
// write that can store more than one chunk says so in etcd before it stores any,
// because until the commit nothing else says its chunks are wanted. The caller
// commits under that record and clears it afterwards — see Put.
//
// Multipart upload is why this is separate from Put. A part's chunks are placed by
// the final object's key so that completing the upload is a single manifest commit
// — the parts are already where the object's manifest will say they are.
func (c *Coordinator) write(ctx context.Context, key string, body io.Reader, expect int64) (object.Manifest, string, error) {
	// One snapshot for the whole object: membership may change mid-upload, and
	// spreading an object's chunks over two different rings would leave later
	// chunks on nodes the manifest does not name.
	live := c.live.Load()
	width := c.width()
	owners := live.ring.Owners(ring.PartitionFor(key), width)
	if len(owners) == 0 {
		return object.Manifest{}, "", fmt.Errorf("cluster: no nodes available to place %q", key)
	}
	// A shard is identified by position, so a cluster smaller than the code has
	// nowhere to put the rest: writing a chunk it can never rebuild would be worse
	// than refusing it.
	if c.scheme != (ec.Scheme{}) && len(owners) < width {
		return object.Manifest{}, "", fmt.Errorf("cluster: %s needs %d nodes, cluster has %d",
			c.scheme, width, len(owners))
	}
	// Replication does not narrow either, and that is the whole of invariant 1. A
	// node whose peers have all lapsed still has a ring — itself — because a node is
	// alive by definition and must not be written out of its own cluster. Placing on
	// it alone acknowledges an object that one disk can take with it, which is what
	// happened: with three of four nodes frozen, the survivor accepted writes with a
	// single copy, and the wipe that followed destroyed them
	// (TestAWriteThatCannotReachWNodesIsRefused, and the chaos seed in its comment).
	// Refused before a byte is stored rather than after, since the answer cannot
	// change once the placement is this narrow.
	if c.scheme == (ec.Scheme{}) && len(owners) < c.quorum {
		return object.Manifest{}, "", fmt.Errorf(
			"%w: placing %q needs %d nodes, this node can see %d",
			ErrQuorum, key, c.quorum, len(owners))
	}

	// The ETag is the object's MD5 because that is what S3 clients check an
	// upload against, and hashing here is the only place the whole object passes
	// through in order. Alongside the write, not after it: reading the object
	// back to hash it would double the traffic of every PUT.
	//
	// Concurrently with storing each chunk rather than while reading it. MD5 has
	// no hardware acceleration on arm64 and runs at ~550 MB/s, which made it 117
	// ms of a 268 ms 64 MB PUT — nearly half the write, spent in front of the
	// network instead of beside it. Storing a chunk waits on disks and peers, so
	// the two overlap almost perfectly: 64 MB went from 268 ms to 154 ms, against
	// 151 ms for not hashing at all.
	//
	// Both only read the chunk, which is what makes this safe, and chunks are
	// hashed in the order they are cut, which is what keeps the sum the object's.
	sum := md5.New()
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	ieee := crc32.NewIEEE()
	crc64 := object.NewCRC64NVME()
	var writing string
	m, err := object.Write(body, c.chunkSize, expect, func(ref *object.ChunkRef, data []byte) error {
		// A write whose chunks will outlive the sweep's patience says so before it
		// stores any of them. Only a write that can have a second chunk needs to:
		// a chunk short of the chunk size is the last one, so its manifest is
		// committed a moment after it is stored, and the window this closes does
		// not exist. That is also why the ordinary small object pays nothing for
		// this — it is one short chunk.
		if writing == "" && int64(len(data)) == c.chunkSize {
			writing = object.WriteID(ref.ID)
			if err := c.meta.MarkWriting(ctx, writing, c.self); err != nil {
				return err
			}
		}

		hashed := make(chan struct{})
		go func() {
			defer close(hashed)
			sum.Write(data)
			crc.Write(data)
			ieee.Write(data)
			crc64.Write(data)
		}()
		// Before returning on any path: the next chunk overwrites this buffer.
		defer func() { <-hashed }()

		if c.scheme != (ec.Scheme{}) {
			// Capacity clamped so that splitting the chunk into shards cannot
			// pad into the buffer's spare room, which the hash is reading. A
			// partial last chunk costs one small copy for that.
			return c.encode(ctx, ref, data[:len(data):len(data)], owners, live)
		}
		return c.replicate(ctx, *ref, data, owners, live)
	})
	if err != nil {
		// The record outlives the failure by a sweep at most: nothing will commit
		// under it now, and leaving it would protect the chunks this write is
		// abandoning. Clearing it is what makes them collectable.
		c.stopWriting(ctx, writing)
		return object.Manifest{}, "", err
	}
	m.Nodes = owners
	m.Coding = c.scheme
	m.ETag = hex.EncodeToString(sum.Sum(nil))
	crc32c := crc.Sum32()
	m.CRC32C = &crc32c
	sum32 := ieee.Sum32()
	m.CRC32 = &sum32
	nvme := crc64.Sum64()
	m.CRC64NVME = &nvme
	// Truncated to the second, because that is all the Last-Modified header can
	// carry. A listing reports the same field to the millisecond, so keeping any
	// finer resolution would have a HEAD and a listing disagree about when the same
	// object was written.
	m.Modified = time.Now().UTC().Truncate(time.Second)
	return m, writing, nil
}

// checkCRC32C compares the checksum the client declared against the one the write
// computed. Trailing values are read here because they only exist after the body
// has been consumed. A mismatch is the caller's to refuse the commit.
func checkCRC32C(m object.Manifest, opts PutOptions) error {
	declared := opts.CRC32C
	if opts.TrailingCRC32C != nil {
		v, err := opts.TrailingCRC32C()
		if err != nil {
			return err
		}
		if declared == nil {
			declared = v
		} else if v != nil && *v != *declared {
			return fmt.Errorf("%w: header CRC32C %08x, trailer %08x", ErrBadDigest, *declared, *v)
		}
	}
	if declared != nil && m.CRC32C != nil && *declared != *m.CRC32C {
		return fmt.Errorf("%w: declared CRC32C %08x, received %08x", ErrBadDigest, *declared, *m.CRC32C)
	}
	return nil
}

func checkCRC32(m object.Manifest, opts PutOptions) error {
	declared := opts.CRC32
	if opts.TrailingCRC32 != nil {
		v, err := opts.TrailingCRC32()
		if err != nil {
			return err
		}
		if declared == nil {
			declared = v
		} else if v != nil && *v != *declared {
			return fmt.Errorf("%w: header CRC32 %08x, trailer %08x", ErrBadDigest, *declared, *v)
		}
	}
	if declared != nil && m.CRC32 != nil && *declared != *m.CRC32 {
		return fmt.Errorf("%w: declared CRC32 %08x, received %08x", ErrBadDigest, *declared, *m.CRC32)
	}
	return nil
}

func checkCRC64NVME(m object.Manifest, opts PutOptions) error {
	declared := opts.CRC64NVME
	if opts.TrailingCRC64NVME != nil {
		v, err := opts.TrailingCRC64NVME()
		if err != nil {
			return err
		}
		if declared == nil {
			declared = v
		} else if v != nil && *v != *declared {
			return fmt.Errorf("%w: header CRC64NVME %016x, trailer %016x", ErrBadDigest, *declared, *v)
		}
	}
	if declared != nil && m.CRC64NVME != nil && *declared != *m.CRC64NVME {
		return fmt.Errorf("%w: declared CRC64NVME %016x, received %016x", ErrBadDigest, *declared, *m.CRC64NVME)
	}
	return nil
}

// stopWriting clears a write's record. A failure to clear it is logged and no more:
// the object is already committed or already abandoned, so the client's answer does
// not depend on this, and what is left behind is one small key that keeps some
// chunks alive until the node stops being a member.
func (c *Coordinator) stopWriting(ctx context.Context, writing string) {
	if writing == "" {
		return
	}
	// Not the request's context: it may already be cancelled, and that is exactly
	// when leaving the record behind costs the most.
	if err := c.meta.DoneWriting(context.WithoutCancel(ctx), writing); err != nil {
		log.Printf("collect: write %s finished but its record remains: %v", writing, err)
	}
}

// Delete removes an object, which is to say it removes the manifest: that is the
// instant the object stops existing, since a reader can only reach chunks through
// one.
//
// It does not touch the chunks, and no longer may. Copying an object server-side
// copies its manifest, so the chunks under one key can be the chunks under another,
// and dropping them here on the strength of this manifest alone would take the other
// object's data with it. Collection reclaims them, having first read every manifest
// in the cluster and found no name for them. The cost of that is the only cost:
// deleted space comes back within a collection cycle rather than at once.
//
// Deleting what is not there succeeds, because S3 promises an idempotent delete, and
// an etcd delete of a key that does not exist is already that.
func (c *Coordinator) Delete(ctx context.Context, key string) error {
	return c.meta.Delete(ctx, key)
}

// Copy makes to another name for the object at from, without moving a byte of it:
// the source's manifest is committed under the destination's key, so both keys name
// the same chunks. A server-side copy of a terabyte is one etcd write.
//
// Which makes a chunk something more than one object's property, and that is the
// whole cost of this operation. Nothing may delete a chunk on the strength of a
// single manifest any more — see the collection pass, which is the only thing that
// deletes one, and which reads every manifest before it does.
//
// The copy keeps the source's placement rather than its own key's. Rebalancing will
// notice, since the ring puts the destination key's partition somewhere else, and
// move it in the background; until it does, the copy is readable exactly where the
// source is. What it must not do is drop the source's copies once it has moved the
// destination's — the reason the move no longer drops anything at all.
func (c *Coordinator) Copy(ctx context.Context, from, to string, opts CopyOptions) (object.Manifest, error) {
	m, err := c.meta.Get(ctx, from)
	if err != nil {
		return object.Manifest{}, err
	}
	m.Modified = time.Now().UTC().Truncate(time.Second)
	if opts.Replace {
		// Not merged with the source's: REPLACE with no metadata headers means
		// the copy has none, which is how a client strips metadata from an
		// object it cannot otherwise edit.
		m.ContentType, m.Meta = opts.ContentType, opts.Meta
	}
	if err := c.meta.Commit(ctx, to, m); err != nil {
		return object.Manifest{}, err
	}
	return m, nil
}

// CopyOptions carries the destination's metadata when the client asked to replace
// it rather than keep the source's, which is x-amz-metadata-directive: REPLACE.
// Replace is explicit because "replace with nothing" and "keep the source's" are
// different requests that both arrive with an empty Meta.
type CopyOptions struct {
	Replace     bool
	ContentType string
	Meta        map[string]string
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
	// W copies on distinct nodes or none at all. This used to require only every
	// owner there was, which reads as graceful degradation and behaves as data loss:
	// see the placement check in write, which now refuses the narrow ring this was
	// forgiving.
	if acks < c.quorum {
		return fmt.Errorf("%w: chunk %s reached %d of %d owners (needed %d): %w",
			ErrQuorum, ref.ID, acks, len(owners), c.quorum, errors.Join(errs...))
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
		switch addr := live.readAddr(node); {
		case node == c.self:
			rc, err = c.store.ReadChunk(ref.ID, ref.CRC)
		case addr == "":
			// A node this cluster has never seen. There is nowhere to try, and
			// guessing is not a thing a read can do.
			err = fmt.Errorf("cluster: node %s holding chunk %s has never been a member", node, ref.ID)
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
