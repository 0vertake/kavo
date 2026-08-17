package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/0vertake/kavo/internal/ec"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/peer"
	"github.com/0vertake/kavo/internal/ring"
)

// DefaultRebalanceInterval is how long a node waits before checking placement
// again. Slower than repair on purpose: repair restores redundancy that is
// missing, while rebalancing corrects redundancy that is merely in the wrong
// place, and moving data is the more expensive of the two.
const DefaultRebalanceInterval = 5 * time.Minute

// RebalanceStats reports what one pass did.
type RebalanceStats struct {
	Objects   int   // manifests this node was responsible for
	Misplaced int   // objects whose owners no longer match the ring
	Moved     int   // objects re-placed and re-committed
	Copies    int   // chunk copies written to a new owner
	Bytes     int64 // how much data that moved
	Raced     int   // objects a client overwrote mid-move, left for the next pass
	Failed    int   // objects that could not be moved, left where they were
}

// RebalanceLoop corrects placement until ctx is done, pausing between passes.
func (c *Coordinator) RebalanceLoop(ctx context.Context, rate int64, interval time.Duration) {
	for {
		start := time.Now()
		st, err := c.Rebalance(ctx, rate)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			log.Printf("rebalance: pass failed after %v: %v", time.Since(start), err)
		case st.Moved > 0 || st.Raced > 0 || st.Failed > 0:
			log.Printf("rebalance: moved %d of %d misplaced objects in %v (%d copies, %d bytes, %d raced, %d failed)",
				st.Moved, st.Misplaced, time.Since(start), st.Copies, st.Bytes, st.Raced, st.Failed)
		}
		if !sleep(ctx, interval) {
			return
		}
	}
}

// Rebalance makes one pass over the objects this node is responsible for and
// moves any whose owners no longer match the ring, at no more than rate bytes
// per second. A rate of zero is unlimited.
//
// This is what closes the gap repair deliberately leaves. Repair restores the
// copies a manifest promises and refuses to put them anywhere else, so a node
// that leaves for good takes its copy's *place* with it: the object stays at N-1
// copies forever, and no amount of healing fixes it. Rebalancing is the part
// that changes where the copies belong.
func (c *Coordinator) Rebalance(ctx context.Context, rate int64) (RebalanceStats, error) {
	var st RebalanceStats
	pace := &pacer{rate: rate, since: time.Now()}

	err := c.walk(ctx, "rebalance", func(o meta.Object) error {
		// Counted rather than returned: one unreachable node must not stop the
		// pass, and a move that failed is not data loss — the object is still
		// readable from the nodes its manifest names. The next pass retries.
		if err := c.rebalanceObject(ctx, o, &st, pace); err != nil {
			st.Failed++
			return err
		}
		return nil
	})
	return st, err
}

// rebalanceObject moves one object onto its current owners, in an order that
// keeps it readable throughout.
func (c *Coordinator) rebalanceObject(ctx context.Context, o meta.Object, st *RebalanceStats, pace *pacer) error {
	live := c.live.Load()
	// One node moves each partition — the same first-owner rule repair uses, for
	// the same reason: without it every node would move every object and several
	// would race to copy the same chunk to the same place.
	if first := live.ring.Owners(ring.PartitionFor(o.Key), 1); len(first) == 0 || first[0] != c.self {
		return nil
	}
	st.Objects++

	// The object's own redundancy, so this pass widens as well as moves: a write
	// acknowledged while fewer nodes were visible names fewer nodes, and only this
	// pass can add the owner it is missing — repair will not put a copy anywhere the
	// manifest does not already name.
	want := live.ring.Owners(ring.PartitionFor(o.Key), redundancy(o.Manifest))
	if len(want) < len(o.Manifest.Nodes) {
		// The cluster is smaller than the object's redundancy. Moving it now
		// would commit a manifest promising fewer copies than the object was
		// acknowledged with; waiting costs nothing, since the copies that exist
		// are still where the manifest says.
		return nil
	}
	if slices.Equal(want, o.Manifest.Nodes) {
		return nil
	}
	st.Misplaced++

	// Copy, then commit. Every reader in between resolves the old manifest and
	// finds the old copies exactly where it expects them: at no point is the object
	// readable only from nodes no manifest names.
	moved, bytes, err := c.copyToNewOwners(ctx, o, want, live, pace)
	st.Copies += moved
	st.Bytes += bytes
	if err != nil {
		return err
	}

	next := o.Manifest
	next.Nodes = want
	if err := c.meta.CommitIfUnchanged(ctx, o.Key, next, o.Revision); err != nil {
		if errors.Is(err, meta.ErrChanged) {
			// A client overwrote the object while it was being moved. The new
			// manifest is the newer truth and was written to the current owners
			// anyway; the copies just made are garbage the next pass collects.
			st.Raced++
			return nil
		}
		return err
	}
	st.Moved++

	// There is no third step. The copies this move superseded are left where they
	// are for collection to reclaim, because "no manifest names them" is a question
	// about every manifest in the cluster and this pass has read one. Since a copied
	// object shares its source's chunks, the answer here would sometimes be wrong,
	// and wrong in the direction of deleting an object nobody touched.
	return nil
}

// copyToNewOwners writes every chunk to the owners that should have it and do not.
func (c *Coordinator) copyToNewOwners(ctx context.Context, o meta.Object, want []string, live *membership, pace *pacer) (copies int, moved int64, err error) {
	if o.Manifest.Coding != (ec.Scheme{}) {
		return c.moveShards(ctx, o, want, live, pace)
	}

	var errs []error
	for _, ref := range o.Manifest.Chunks {
		for _, node := range want {
			// Any owner may hold any copy, so a node that already has one needs
			// nothing sent to it.
			if slices.Contains(o.Manifest.Nodes, node) {
				continue
			}
			if err := c.copyChunk(ctx, o.Manifest, ref, node, live); err != nil {
				errs = append(errs, err)
				continue
			}
			copies++
			moved += ref.Size
			pace.wait(ctx, ref.Size)
		}
	}
	// A partial move must not be committed: the manifest would name a node that
	// does not have the data.
	return copies, moved, errors.Join(errs...)
}

func (c *Coordinator) copyChunk(ctx context.Context, m object.Manifest, ref object.ChunkRef, node string, live *membership) error {
	src, err := c.fetch(ctx, ref, m.Nodes)
	if err != nil {
		return fmt.Errorf("cluster: no source for chunk %s to move: %w", ref.ID, err)
	}
	defer src.Close()

	// Streamed from the source's body into the destination's request, so moving a
	// chunk costs no more memory than serving one.
	if node == c.self {
		err = c.store.WriteChunkVerified(ref.ID, src, ref.CRC, ref.Size)
	} else {
		err = peer.PushChunk(ctx, live.peers[node], ref.ID, ref.CRC, ref.Size, src)
	}
	if err != nil {
		return fmt.Errorf("cluster: move chunk %s to %s: %w", ref.ID, node, err)
	}
	return nil
}

// moveShards re-places the shards of a coded object, a chunk at a time.
//
// Per chunk rather than per shard, because there is no copy of a shard to fetch:
// moving one means decoding its chunk, and one decode produces every shard that
// has to move. Per shard would decode the chunk again for each position.
func (c *Coordinator) moveShards(ctx context.Context, o meta.Object, want []string, live *membership, pace *pacer) (copies int, moved int64, err error) {
	scheme := o.Manifest.Coding

	// Shard i belongs to owner i and nowhere else, so what moves is decided per
	// position rather than per node: the same node in a different position holds
	// the wrong equation's shard. Every chunk of the object moves the same
	// positions, since placement is the object's, not the chunk's.
	var missing []int
	for i, node := range want {
		if i < len(o.Manifest.Nodes) && o.Manifest.Nodes[i] == node {
			continue
		}
		missing = append(missing, i)
	}
	if len(missing) == 0 {
		return 0, 0, nil
	}

	var errs []error
	for _, ref := range o.Manifest.Chunks {
		if err := c.restoreShards(ctx, ref, o.Manifest.Nodes, want, missing, scheme, live, pace); err != nil {
			errs = append(errs, err)
			continue
		}
		copies += len(missing)
		moved += int64(len(missing)) * shardSize(ref.Size, scheme)
	}
	return copies, moved, errors.Join(errs...)
}
