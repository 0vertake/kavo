package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"sync"

	"github.com/0vertake/kavo/internal/ec"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/peer"
	"github.com/0vertake/kavo/internal/store"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// shardSize is how big each shard of a chunk is: the chunk split Data ways and
// rounded up, which is what the padding is for.
func shardSize(chunk int64, scheme ec.Scheme) int64 {
	return (chunk + int64(scheme.Data) - 1) / int64(scheme.Data)
}

// encode splits a chunk into shards, places each on its own node, and records a
// checksum per shard in the ref.
//
// Shard i goes to owners[i] and nowhere else. Unlike a replica, a shard is not
// interchangeable: the decoder identifies a shard by position, so where it lives
// is part of the object's structure rather than a placement preference.
func (c *Coordinator) encode(ctx context.Context, ref *object.ChunkRef, data []byte, owners []string, live *membership) error {
	scheme := c.scheme
	if len(owners) != scheme.Shards() {
		return fmt.Errorf("cluster: %s needs %d owners for chunk %s, ring gave %d",
			scheme, scheme.Shards(), ref.ID, len(owners))
	}

	shards, err := scheme.Encode(data)
	if err != nil {
		return err
	}
	ref.Shards = make([]uint32, len(shards))
	for i, s := range shards {
		ref.Shards[i] = crc32.Checksum(s, castagnoli)
	}

	errs := make([]error, len(shards))
	var wg sync.WaitGroup
	for i := range shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.storeShard(ctx, owners[i], *ref, i, shards[i], live)
		}()
	}
	wg.Wait()

	acks := 0
	for _, err := range errs {
		if err == nil {
			acks++
		}
	}
	// Acknowledging at exactly Data shards would mean a single later loss makes
	// the chunk unreadable — an acknowledged write lost to one failure. One
	// spare, matching how W=2 of N=3 leaves a spare copy; repair rebuilds the
	// rest.
	if want := min(scheme.Data+1, scheme.Shards()); acks < want {
		return fmt.Errorf("%w: chunk %s placed %d of %d shards (needed %d): %w",
			ErrQuorum, ref.ID, acks, scheme.Shards(), want, errors.Join(errs...))
	}
	return nil
}

func (c *Coordinator) storeShard(ctx context.Context, owner string, ref object.ChunkRef, i int, shard []byte, live *membership) error {
	id, crc, size := ref.ShardID(i), ref.Shards[i], int64(len(shard))
	if owner == c.self {
		return c.store.WriteChunkVerified(id, bytes.NewReader(shard), crc, size)
	}
	return peer.PushChunk(ctx, live.peers[owner], id, crc, size, bytes.NewReader(shard))
}

// decode rebuilds an erasure-coded chunk from whichever shards can be reached.
//
// The data shards are tried first and in parallel, because a chunk whose data
// shards are all present needs no arithmetic to put back together. Parity is
// fetched only to replace what is missing: reading all Data+Parity shards every
// time would move 1.5x the object's bytes to serve it.
func (c *Coordinator) decode(ctx context.Context, ref object.ChunkRef, nodes []string, scheme ec.Scheme) ([]byte, error) {
	if len(nodes) != scheme.Shards() || len(ref.Shards) != scheme.Shards() {
		return nil, fmt.Errorf("cluster: chunk %s has %d shard checksums on %d nodes, want %d of each",
			ref.ID, len(ref.Shards), len(nodes), scheme.Shards())
	}

	shards := make([][]byte, scheme.Shards())
	errs := c.gather(ctx, ref, nodes, shards, scheme, 0, scheme.Data)
	if slices.ContainsFunc(shards, func(s []byte) bool { return s == nil }) {
		errs = append(errs, c.gather(ctx, ref, nodes, shards, scheme, scheme.Data, scheme.Shards())...)
	}

	chunk, err := scheme.Reconstruct(shards, ref.Size)
	if err != nil {
		return nil, fmt.Errorf("cluster: chunk %s: %w", ref.ID, errors.Join(append(errs, err)...))
	}
	// The shards verified individually, but a decode combines them: checking the
	// chunk again is what catches a shard that was stored in the wrong position.
	if got := crc32.Checksum(chunk, castagnoli); got != ref.CRC {
		return nil, fmt.Errorf("cluster: chunk %s reconstructed to %08x, manifest says %08x",
			ref.ID, got, ref.CRC)
	}
	return chunk, nil
}

// gather fetches shards [from, to) in parallel into their own slots. A shard that
// cannot be fetched leaves its slot nil, which is exactly what Reconstruct wants.
func (c *Coordinator) gather(ctx context.Context, ref object.ChunkRef, nodes []string, shards [][]byte, scheme ec.Scheme, from, to int) []error {
	size := shardSize(ref.Size, scheme)
	errs := make([]error, to-from)
	var wg sync.WaitGroup
	for i := from; i < to; i++ {
		if shards[i] != nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			shard, err := c.fetchShard(ctx, ref, i, nodes[i], size)
			if err != nil {
				errs[i-from] = err
				return
			}
			shards[i] = shard
		}()
	}
	wg.Wait()
	return slices.DeleteFunc(errs, func(err error) bool { return err == nil })
}

func (c *Coordinator) fetchShard(ctx context.Context, ref object.ChunkRef, i int, node string, size int64) ([]byte, error) {
	id, crc := ref.ShardID(i), ref.Shards[i]

	var rc io.ReadCloser
	var err error
	switch addr := c.live.Load().readAddr(node); {
	case node == c.self:
		rc, err = c.store.ReadChunk(id, crc)
	case addr == "":
		err = fmt.Errorf("cluster: node %s holding shard %s has never been a member", node, id)
	default:
		rc, err = peer.FetchChunk(ctx, addr, id, crc)
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	// Sized from the manifest rather than grown, because a decode holds every
	// shard of a chunk at once: letting each one double its way to the right
	// length allocates the chunk over again in garbage.
	shard := make([]byte, size)
	if _, err := io.ReadFull(rc, shard); err != nil {
		return nil, fmt.Errorf("cluster: read shard %s from %s: %w", id, node, err)
	}
	// One read past the end is what verifies it: the reader reports a checksum
	// mismatch in place of the final EOF, so stopping at the last byte would
	// accept a rotted shard as a complete one.
	var end [1]byte
	if n, err := rc.Read(end[:]); n != 0 || !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("cluster: shard %s from %s is not the %d bytes the manifest declares: %w", id, node, size, err)
	}
	return shard, nil
}

// holdsShard reports whether a node still has its shard of a chunk.
func (c *Coordinator) holdsShard(ctx context.Context, node string, ref object.ChunkRef, i int, live *membership) (bool, error) {
	if node == c.self {
		return c.store.HasChunk(ref.ShardID(i))
	}
	return peer.HasChunk(ctx, live.peers[node], ref.ShardID(i))
}

// restoreShards rebuilds the named shards of a chunk from the nodes that have it
// and places each on the node that should. Repair and scrubbing pass the same
// nodes for both, since they put a shard back where it belongs; a rebalance
// reads from the old owners and writes to the new ones.
//
// Reconstructing gives back the missing shards along with the chunk, so one
// decode restores however many shards are gone — which is the whole difference
// from replication, where a missing copy is fetched rather than recomputed.
func (c *Coordinator) restoreShards(ctx context.Context, ref object.ChunkRef, from, to []string, missing []int, scheme ec.Scheme, live *membership, pace *pacer) error {
	// Fetching every shard, including the ones being rebuilt: a shard that is
	// gone leaves its slot nil, and a shard that has rotted fails its checksum
	// and leaves it nil too, so neither can be mistaken for a source.
	shards := make([][]byte, scheme.Shards())
	c.gather(ctx, ref, from, shards, scheme, 0, scheme.Shards())
	if _, err := scheme.Reconstruct(shards, ref.Size); err != nil {
		return fmt.Errorf("cluster: rebuild shards of chunk %s: %w", ref.ID, err)
	}

	var errs []error
	for _, i := range missing {
		if err := c.storeShard(ctx, to[i], ref, i, shards[i], live); err != nil {
			errs = append(errs, fmt.Errorf("cluster: place rebuilt shard %s on %s: %w", ref.ShardID(i), to[i], err))
			continue
		}
		pace.wait(ctx, int64(len(shards[i])))
	}
	return errors.Join(errs...)
}

// verifyLocalShard re-reads one shard this node holds and checks it against the
// manifest, reporting whether it is present and whether it is still good.
func (c *Coordinator) verifyLocalShard(ref object.ChunkRef, i int) (held bool, size int64, err error) {
	id := ref.ShardID(i)
	held, err = c.store.HasChunk(id)
	if err != nil || !held {
		return held, 0, err
	}
	rc, err := c.store.ReadChunk(id, ref.Shards[i])
	if err != nil {
		return true, 0, err
	}
	defer rc.Close()
	size, err = io.Copy(io.Discard, rc)
	if errors.Is(err, store.ErrChecksumMismatch) {
		return true, size, err
	}
	if err != nil {
		return true, size, fmt.Errorf("cluster: read local shard %s: %w", id, err)
	}
	return true, size, nil
}
