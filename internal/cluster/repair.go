package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/0vertake/kavo/internal/ec"
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

	err := c.walk(ctx, "repair", func(o meta.Object) error {
		return c.repairObject(ctx, o, &st, pace)
	})
	if err != nil {
		return st, err
	}
	// The pass finishes either way — the rest of the cluster still needs
	// repairing — but a pass that found data with no copies left has not
	// succeeded, whatever else it fixed.
	if st.Unrepairable > 0 {
		return st, fmt.Errorf("%w: %d copies with no source left to rebuild from", ErrUnrepairable, st.Unrepairable)
	}
	return st, nil
}

// walk visits every object in the cluster in key order, resuming where this
// node's last walk of the same task stopped and starting over once it reaches the
// end. Objects are read a page at a time: a background pass must not depend on
// holding every manifest in memory.
//
// A failure on one object is logged and the walk continues. The alternative is a
// single unrebuildable object stopping every other object from being repaired.
func (c *Coordinator) walk(ctx context.Context, task string, visit func(meta.Object) error) error {
	cursor, err := c.meta.Cursor(ctx, task, c.self)
	if err != nil {
		return err
	}
	for {
		// The cursor names the last object handled, so the walk resumes past it.
		from := ""
		if cursor != "" {
			from = meta.After(cursor)
		}
		objects, err := c.meta.ScanObjects(ctx, "", from, scanPage)
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			return c.meta.SaveCursor(ctx, task, c.self, "")
		}
		for _, o := range objects {
			if err := visit(o); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("%s: %s: %v", task, o.Key, err)
			}
			cursor = o.Key
		}
		if err := c.meta.SaveCursor(ctx, task, c.self, cursor); err != nil {
			return err
		}
	}
}

// repairObject restores any copy of o's chunks that a live owner should have and
// does not.
func (c *Coordinator) repairObject(ctx context.Context, o meta.Object, st *Stats, pace *pacer) error {
	// The partition's first owner repairs it. Without a rule like this every node
	// would survey every object — N times the work — and several would race to
	// push the same chunk to the same node.
	live := c.live.Load()
	// One owner is all this asks for: the ring walks the same nodes in the same
	// order whatever the width, so the first of three is the first of nine.
	owners := live.ring.Owners(ring.PartitionFor(o.Key), 1)
	if len(owners) == 0 || owners[0] != c.self {
		return nil
	}
	st.Objects++

	if o.Manifest.Coding != (ec.Scheme{}) {
		return c.repairShards(ctx, o, st, pace, live)
	}

	// Survey all chunk IDs this object needs from each node in one call rather
	// than one HEAD per chunk. For a multi-chunk object the batched path sends
	// one request per live node instead of nodes×chunks requests.
	ids := make([]string, len(o.Manifest.Chunks))
	for i, ref := range o.Manifest.Chunks {
		ids[i] = ref.ID
	}
	held, err := c.surveyNodes(ctx, o.Manifest.Nodes, ids, live)
	if err != nil {
		return err
	}

	var errs []error
	for _, ref := range o.Manifest.Chunks {
		for _, node := range o.Manifest.Nodes {
			if _, member := live.peers[node]; !member {
				continue
			}
			st.CopiesChecked++
			if held[node][ref.ID] {
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

// surveyNodes asks each live node in nodes which of ids it holds, returning a
// map of node → set of held IDs. A survey error on one node is non-fatal: the
// repair loop treats unknown presence as missing and attempts a restore.
func (c *Coordinator) surveyNodes(ctx context.Context, nodes, ids []string, live *membership) (map[string]map[string]bool, error) {
	result := make(map[string]map[string]bool, len(nodes))
	for _, node := range nodes {
		if _, member := live.peers[node]; !member {
			continue
		}
		if node == c.self {
			have, err := c.store.HasChunks(ids)
			if err != nil {
				return nil, fmt.Errorf("repair: survey self: %w", err)
			}
			m := make(map[string]bool, len(have))
			for _, id := range have {
				m[id] = true
			}
			result[node] = m
		} else {
			have, err := peer.HasChunks(ctx, live.peers[node], ids)
			if err != nil {
				// Treat a survey error as nothing held: repair will
				// attempt to restore and fail fast if the node is gone.
				log.Printf("repair: survey %s: %v", node, err)
				result[node] = map[string]bool{}
			} else {
				result[node] = have
			}
		}
	}
	return result, nil
}

// repairShards restores the shards of an erasure-coded object that their nodes no
// longer have.
//
// The survey is per chunk rather than per shard, because rebuilding is per chunk:
// one decode produces every missing shard of that chunk, so finding two gone
// costs the same as finding one.
func (c *Coordinator) repairShards(ctx context.Context, o meta.Object, st *Stats, pace *pacer, live *membership) error {
	nodes := o.Manifest.Nodes

	// Collect all shard IDs each node is responsible for and ask in one call.
	// node index i holds ref.ShardID(i) for each chunk.
	nodeShards := make([][]string, len(nodes))
	for _, ref := range o.Manifest.Chunks {
		for i := range nodes {
			nodeShards[i] = append(nodeShards[i], ref.ShardID(i))
		}
	}
	held := make([]map[string]bool, len(nodes))
	for i, node := range nodes {
		if _, member := live.peers[node]; !member {
			held[i] = map[string]bool{}
			continue
		}
		if node == c.self {
			have, err := c.store.HasChunks(nodeShards[i])
			if err != nil {
				held[i] = map[string]bool{}
				log.Printf("repair: survey self shards: %v", err)
				continue
			}
			m := make(map[string]bool, len(have))
			for _, id := range have {
				m[id] = true
			}
			held[i] = m
		} else {
			m, err := peer.HasChunks(ctx, live.peers[node], nodeShards[i])
			if err != nil {
				log.Printf("repair: survey %s shards: %v", node, err)
				held[i] = map[string]bool{}
			} else {
				held[i] = m
			}
		}
	}

	var errs []error
	for _, ref := range o.Manifest.Chunks {
		var missing []int
		for i, node := range nodes {
			// A shard's node is fixed by its position, so a node the cluster has
			// lost cannot be substituted: putting shard 4 somewhere else would
			// mean rewriting the manifest, which is rebalancing, not repair.
			if _, member := live.peers[node]; !member {
				continue
			}
			st.CopiesChecked++

			if !held[i][ref.ShardID(i)] {
				missing = append(missing, i)
			}
		}
		if len(missing) == 0 {
			continue
		}
		if err := c.restoreShards(ctx, ref, nodes, nodes, missing, o.Manifest.Coding, live, pace); err != nil {
			st.Unrepairable += len(missing)
			errs = append(errs, err)
			continue
		}
		st.Restored += len(missing)
		st.BytesRestored += int64(len(missing)) * shardSize(ref.Size, o.Manifest.Coding)
	}
	return errors.Join(errs...)
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
