package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/peer"
	"github.com/0vertake/kavo/internal/ring"
)

const (
	// DefaultRepairRate is how fast a node restores missing copies, in bytes per
	// second. Repair competes with clients for the same disks and the same
	// network, so it is deliberately slower than the cluster can go: an
	// unthrottled heal turns one dead node into a cluster-wide latency spike.
	DefaultRepairRate = 32 << 20

	// DefaultRepairInterval is how long a node waits before scanning again.
	DefaultRepairInterval = 30 * time.Second

	// scanPage is how many manifests are read from etcd at a time. Repair must
	// not depend on holding every manifest in memory.
	scanPage = 256
)

// ErrUnrepairable reports that a pass found chunk copies missing everywhere. This
// is data loss, and it must not be reported as a clean pass — it is precisely
// what the durability invariants exist to catch.
var ErrUnrepairable = errors.New("repair: copies could not be rebuilt")

// Stats reports what one repair pass did.
type Stats struct {
	Objects       int   // manifests this node was responsible for
	CopiesChecked int   // chunk copies surveyed, across all live owners
	Restored      int   // copies that were missing and have been rebuilt
	BytesRestored int64 // how much data that moved
	Unrepairable  int   // copies missing with no source left to rebuild from
}

// RepairLoop restores missing copies until ctx is done, pausing between passes.
//
// Running forever is the point: under-replication is not an event to react to but
// a state to keep converging out of. A write acknowledged at W=2 of N=3 leaves a
// copy that never existed, and a disk that comes back empty leaves copies that
// did. Neither announces itself.
func (c *Coordinator) RepairLoop(ctx context.Context, rate int64, interval time.Duration) {
	for {
		start := time.Now()
		st, err := c.Repair(ctx, rate)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("repair: pass failed after %v: %v", time.Since(start), err)
		} else if st.Restored > 0 || st.Unrepairable > 0 {
			log.Printf("repair: restored %d copies (%d bytes) in %v, %d unrepairable, %d objects checked",
				st.Restored, st.BytesRestored, time.Since(start), st.Unrepairable, st.Objects)
		}
		if !sleep(ctx, interval) {
			return
		}
	}
}

// Repair makes one pass over the objects this node is responsible for and
// restores every copy their manifests promise, at no more than rate bytes per
// second. A rate of zero is unlimited.
//
// The pass resumes from where the last one stopped, so restarting a node does not
// restart the heal. It is idempotent: a pass over a healthy cluster moves nothing.
func (c *Coordinator) Repair(ctx context.Context, rate int64) (Stats, error) {
	var st Stats
	pace := &pacer{rate: rate, since: time.Now()}

	cursor, err := c.meta.RepairCursor(ctx, c.self)
	if err != nil {
		return st, err
	}
	for {
		objects, err := c.meta.ScanObjects(ctx, cursor, scanPage)
		if err != nil {
			return st, err
		}
		if len(objects) == 0 {
			// The end of the cluster's objects: the next pass starts over.
			if err := c.meta.SaveRepairCursor(ctx, c.self, ""); err != nil {
				return st, err
			}
			break
		}

		for _, o := range objects {
			if err := c.repairObject(ctx, o, &st, pace); err != nil {
				if ctx.Err() != nil {
					return st, ctx.Err()
				}
				// One object nobody can rebuild must not stop the pass; the
				// rest of the cluster still needs repairing.
				log.Printf("repair: %s: %v", o.Key, err)
			}
			cursor = o.Key
		}
		if err := c.meta.SaveRepairCursor(ctx, c.self, cursor); err != nil {
			return st, err
		}
	}

	// The pass finishes either way — the rest of the cluster still needs
	// repairing — but a pass that found data with no copies left has not
	// succeeded, whatever else it fixed.
	if st.Unrepairable > 0 {
		return st, fmt.Errorf("%w: %d copies with no source left to rebuild from", ErrUnrepairable, st.Unrepairable)
	}
	return st, nil
}

// repairObject restores any copy of o's chunks that a live owner should have and
// does not.
func (c *Coordinator) repairObject(ctx context.Context, o meta.Object, st *Stats, pace *pacer) error {
	// The partition's first owner repairs it. Without a rule like this every node
	// would survey every object — N times the work — and several would race to
	// push the same chunk to the same node.
	live := c.live.Load()
	owners := live.ring.Owners(ring.PartitionFor(o.Key), Replicas)
	if len(owners) == 0 || owners[0] != c.self {
		return nil
	}
	st.Objects++

	var errs []error
	for _, ref := range o.Manifest.Chunks {
		for _, node := range o.Manifest.Nodes {
			// A node the manifest names but the cluster has lost cannot be
			// given anything. Putting the copy somewhere else instead would mean
			// rewriting the manifest, which is rebalancing, not repair.
			if _, member := live.peers[node]; !member {
				continue
			}
			st.CopiesChecked++

			held, err := c.holds(ctx, node, ref, live)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if held {
				continue
			}
			if err := c.restore(ctx, node, ref, o.Manifest.Nodes, live, pace); err != nil {
				st.Unrepairable++
				errs = append(errs, err)
				continue
			}
			st.Restored++
			st.BytesRestored += ref.Size
		}
	}
	return errors.Join(errs...)
}

// holds reports whether a node still has its copy of a chunk.
func (c *Coordinator) holds(ctx context.Context, node string, ref object.ChunkRef, live *membership) (bool, error) {
	if node == c.self {
		return c.store.HasChunk(ref.ID)
	}
	return peer.HasChunk(ctx, live.peers[node], ref.ID)
}

// restore rebuilds one missing copy by streaming it from a node that still has
// one. The copy is verified against the manifest's checksum at both ends, so
// repair cannot spread a corrupt chunk: the receiver refuses to commit it.
func (c *Coordinator) restore(ctx context.Context, node string, ref object.ChunkRef, nodes []string, live *membership, pace *pacer) error {
	src, err := c.fetch(ctx, ref, nodes)
	if err != nil {
		return fmt.Errorf("repair: no source for chunk %s to rebuild on %s: %w", ref.ID, node, err)
	}
	defer src.Close()

	// Streamed straight from the source's body into the destination's request, so
	// rebuilding a chunk costs no more memory than serving one.
	if node == c.self {
		err = c.store.WriteChunkVerified(ref.ID, src, ref.CRC, ref.Size)
	} else {
		err = peer.PushChunk(ctx, live.peers[node], ref.ID, ref.CRC, ref.Size, src)
	}
	if err != nil {
		return fmt.Errorf("repair: rebuild chunk %s on %s: %w", ref.ID, node, err)
	}
	pace.wait(ctx, ref.Size)
	return nil
}

// pacer holds repair traffic to a byte rate by making the bytes it has sent so
// far take at least as long as that rate allows.
type pacer struct {
	rate  int64 // bytes per second; zero means unlimited
	since time.Time
	sent  int64
}

func (p *pacer) wait(ctx context.Context, n int64) {
	if p.rate <= 0 {
		return
	}
	p.sent += n
	owed := time.Duration(float64(p.sent) / float64(p.rate) * float64(time.Second))
	if delay := owed - time.Since(p.since); delay > 0 {
		sleep(ctx, delay)
	}
}

// sleep waits for d, reporting false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
