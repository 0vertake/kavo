package cluster

// Reclaiming the chunks no manifest references any more.
//
// An overwrite supersedes the chunks of the manifest it replaces and nothing
// reclaims them. A write that fails after storing chunks leaves chunks no manifest
// was ever committed for. A rebalance leaves a copy where it moved data from if the
// delete that should follow the move does not happen. And deletes and aborted
// uploads drop their own chunks, but only as a best effort that is logged rather
// than returned — because by then the object is already gone as far as any reader
// is concerned, so a copy that could not be deleted is wasted disk rather than a
// correctness problem. Every one of those is unreachable, since readers resolve
// objects only through committed manifests. It is storage that is paid for and not
// counted, and without something like this it only ever grows.
//
// The pass is mark-and-sweep rather than a record of what each write superseded.
// That costs a scan where a record would cost a lookup, and buys two things. It
// reclaims garbage nobody wrote a record for, which is every category above except
// the first. And a chunk is live if any manifest names it, so two objects may name
// the same chunk — which is what lets an object be copied without copying its
// data, and what a per-write record could not express without reference counts in
// the commit path.

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/0vertake/kavo/internal/ec"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/ring"
	"github.com/0vertake/kavo/internal/store"
)

const (
	// DefaultCollectInterval is how long a node waits between collection passes.
	//
	// A pass sweeps one of 32 slices of the id space, so a full cycle is 32 intervals
	// and that is most of the answer to the question an operator actually asks — how
	// long after a delete does the disk shrink? A minute here makes it half an hour,
	// plus the grace period below. The price of a pass is a scan of every manifest, so
	// raising this trades space back for etcd reads.
	//
	// It is not longer because this pass is the only thing that deletes a chunk. A
	// copied object shares its source's chunks, so no single manifest can be trusted
	// to speak for one, which leaves every delete, abort and rebalance waiting here.
	DefaultCollectInterval = time.Minute

	// DefaultCollectGrace is how long an unreferenced chunk is left alone before
	// it is treated as garbage.
	//
	// It used to be the whole defence of a write in flight, and it was an hour
	// because a chunk is durable before the manifest naming it is committed and
	// nobody knows how slow a client is. That was a guess about the outside world,
	// and the wrong kind: S3 allows a single PUT of 5 GB, so a link slow enough
	// beat it, and the write was acknowledged with its first chunks already
	// collected.
	//
	// A write that can have more than one chunk now records itself before it stores
	// any of them, and the sweep reads those records, so this defends only the gap
	// between the last chunk of a *single*-chunk write and its commit — the tail of
	// one request. A minute is enormous next to that, and it keeps garbage no more
	// than a minute past the cycle that would have taken it.
	//
	// Zero is legal and means "trust the records completely", which is what the
	// tests that sweep inside a write are asserting.
	DefaultCollectGrace = time.Minute

	// collectTask names this pass's cursor, which records the slice of the id
	// space to sweep next.
	collectTask = "collect"

	// idAlphabet is what chunk ids are drawn from, and so what the slices of the
	// id space are: crypto/rand.Text's base32 alphabet.
	idAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

	// CollectSlices is how many passes sweep the whole id space, and so how many
	// intervals a full cycle takes: at the default interval, garbage is reclaimed
	// within about five hours of being made.
	CollectSlices = len(idAlphabet)
)

// CollectStats reports what one collection pass did.
type CollectStats struct {
	Slice          string // the slice of the id space this pass swept
	Referenced     int    // local chunks a manifest still names on this node
	Examined       int    // local chunks in the slice, referenced or not
	Collected      int    // chunks no manifest named, and old enough to prove it
	BytesCollected int64
	Young          int // unreferenced, but inside the grace period and so left alone
	Writing        int // unreferenced because the write that made them is still running
}

// CollectLoop reclaims unreferenced chunks until ctx is done, pausing between
// passes.
func (c *Coordinator) CollectLoop(ctx context.Context, grace, interval time.Duration) {
	for {
		start := time.Now()
		if n, err := c.ExpireUploads(ctx); ctx.Err() != nil {
			return
		} else if err != nil {
			log.Printf("collect: expiring abandoned uploads: %v", err)
		} else if n > 0 {
			log.Printf("collect: expired %d abandoned uploads", n)
		}
		st, err := c.Collect(ctx, grace)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("collect: pass over slice %s failed after %v: %v", st.Slice, time.Since(start), err)
		} else if st.Collected > 0 {
			log.Printf("collect: reclaimed %d chunks (%d bytes) in slice %s in %v, %d referenced, %d too young",
				st.Collected, st.BytesCollected, st.Slice, time.Since(start), st.Referenced, st.Young)
		}
		if !sleep(ctx, interval) {
			return
		}
	}
}

// Collect sweeps one slice of the chunk id space: it reads every manifest in the
// cluster to learn which of this node's chunks are still referenced, then deletes
// the local chunks in that slice which none of them names and which are older than
// grace. Successive passes move through the id space, so a full cycle takes as
// many passes as there are slices.
//
// It deletes nothing at all unless it read every manifest successfully. A partial
// view of the manifests is indistinguishable from a cluster where those objects do
// not exist, and acting on it would delete data that is referenced.
func (c *Coordinator) Collect(ctx context.Context, grace time.Duration) (CollectStats, error) {
	slice, err := c.nextSlice(ctx)
	if err != nil {
		return CollectStats{}, err
	}
	st := CollectStats{Slice: string(slice)}

	start := time.Now()
	referenced, writing, err := c.referenced(ctx, slice)
	if err != nil {
		return st, err
	}

	// Measured from when the manifests were read, not from now, and the difference
	// is a correctness one. An object written during the scan under a key the scan
	// had already passed is missed by it, so nothing written after the scan began
	// may be deleted on the strength of it, however long the scan took.
	//
	// The grace period covers the other window: a chunk is durable before the
	// manifest naming it is committed, so a write that began before the scan and
	// committed during it is missed too, and only its age says it is not garbage.
	cutoff := start.Add(-grace)
	var failed int
	var first error
	err = c.store.ScanChunks(st.Slice, func(ci store.ChunkInfo) error {
		st.Examined++
		if _, live := referenced[ci.ID]; live {
			st.Referenced++
			return nil
		}
		// A write in flight names its chunks by prefix rather than one at a time,
		// because it does not know yet how many there will be.
		if inFlight(writing, ci.ID) {
			st.Writing++
			return nil
		}
		if ci.Modified.After(cutoff) {
			st.Young++
			return nil
		}
		if err := c.store.RemoveChunk(ci.ID); err != nil {
			// A chunk that will not go away is garbage that stays garbage, and the
			// next pass tries again. It is not a reason to leave the rest, and one
			// example with a count says as much as a thousand joined errors.
			failed++
			if first == nil {
				first = err
			}
			return nil
		}
		st.Collected++
		st.BytesCollected += ci.Size
		return nil
	})
	if err != nil {
		return st, err
	}
	if failed > 0 {
		return st, fmt.Errorf("collect: %d unreferenced chunks could not be removed, for example: %w", failed, first)
	}
	return st, nil
}

// referenced is the set of chunk ids that some manifest says this node should be
// holding, restricted to one slice of the id space.
//
// Restricted, because the alternative is a set of every chunk id on the node: at a
// million chunks that is tens of megabytes of live map, and a background pass that
// grows with the disk is a background pass that eventually cannot run. Each pass
// pays one read of the manifests to sweep a thirty-second of the disk.
//
// Not c.walk, which every other pass uses: that one resumes from a cursor and stops
// at the end of the keyspace, which is right for work that can be done in pieces.
// This cannot. A sweep acts on the absence of a reference, so it needs the whole
// keyspace in one pass or it is reading a partial answer.
//
// ponytail: a full cycle therefore reads every manifest thirty-two times. Fine while
// the metadata fits comfortably in etcd, which is a documented ceiling of its own;
// if manifest reads ever dominate, the fix is an index from partition to keys so a
// pass reads only the manifests that could name this node.
func (c *Coordinator) referenced(ctx context.Context, slice byte) (map[string]struct{}, []string, error) {
	referenced := make(map[string]struct{})

	// Writes in flight before both scans below, and for the same kind of reason. A
	// write's record is taken out before its first chunk is stored and removed after
	// its manifest is committed, so reading it first means the handover cannot fall
	// between the two: a write whose record was already gone had committed its
	// manifest before this read, and the scans below come after it.
	//
	// Reading it at all is what keeps a slow upload's early chunks. Until it commits
	// nothing names them, and by then they are older than any grace period worth
	// having — a 5 GB PUT on a bad link takes hours.
	writing, err := c.writesInFlight(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Parts before objects, which is the ordering that makes completing a
	// multipart upload safe to race with. A part's chunks can be hours old by the
	// time the upload completes, so they are not protected by their age: they are
	// protected by being referenced, first by the part and then by the object.
	// Reading the parts first means the handover cannot fall between the two — a
	// completion that happens after the parts were read was seen as parts, and one
	// that happened before them committed its object manifest before the object
	// scan began, so the object scan sees it.
	from := ""
	for {
		parts, err := c.meta.ScanParts(ctx, from, scanPage)
		if err != nil {
			return nil, nil, err
		}
		if len(parts) == 0 {
			break
		}
		for _, p := range parts {
			// No key, so no ring clause: a part is never moved by a rebalance, so
			// the only thing that can make its chunks live is the part itself.
			c.reference(referenced, "", p.Manifest, slice)
			from = p.Key
		}
	}

	from = ""
	for {
		objects, err := c.meta.ScanObjects(ctx, "", from, scanPage)
		if err != nil {
			return nil, nil, err
		}
		if len(objects) == 0 {
			break
		}
		for _, o := range objects {
			c.reference(referenced, o.Key, o.Manifest, slice)
			from = meta.After(o.Key)
		}
	}
	return referenced, writing, nil
}

// writesInFlight is the ids of the writes that are still running, and it drops the
// records of the ones that are not.
//
// A record names the node coordinating the write, and a node that is no longer a
// member is not coordinating anything: it crashed, or it cannot reach etcd, and
// either way it cannot commit. So its record protects chunks that nothing will ever
// name, and no other pass would remove it. Dropping it here is safe because the
// commit is conditional on it — a writer that comes back to find its record gone
// fails its write instead of acknowledging an object whose first chunks were
// collected on the strength of this decision.
func (c *Coordinator) writesInFlight(ctx context.Context) ([]string, error) {
	writes, err := c.meta.Writing(ctx)
	if err != nil {
		return nil, err
	}
	// An empty view is not the news that everyone died, it is a node that has not
	// read the membership yet — a restart, one pass ahead of its first watch. Acting
	// on it would refuse every write in the cluster, so it protects them instead.
	// The same rule as a partial manifest scan: an answer that might be nothing at
	// all deletes nothing at all.
	members := c.live.Load().peers
	unknown := len(members) == 0

	ids := make([]string, 0, len(writes))
	for id, node := range writes {
		if _, live := members[node]; live || unknown {
			ids = append(ids, id)
			continue
		}
		if err := c.meta.DoneWriting(ctx, id); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// inFlight is whether a chunk belongs to one of the writes still running. A list
// rather than a set: there are as many entries as there are large uploads happening
// at this instant, which is a handful, and a prefix cannot be looked up in a map.
func inFlight(writing []string, id string) bool {
	for _, w := range writing {
		if strings.HasPrefix(id, w) {
			return true
		}
	}
	return false
}

// reference adds the chunks m says this node holds, of those in the slice.
//
// What the manifest says, not what exists: a copy on a node the manifest does not
// name is unreachable, because a reader tries the nodes the manifest names and
// stops. That is the same rule rebalancing already relies on when it deletes the
// copy it moved away from, and it is what lets this pass reclaim the copies a
// rebalance failed to.
//
// And what the ring says, for the one case where the manifest is behind the truth
// rather than ahead of it. A move copies every chunk to the new owners before
// committing the manifest that names them — it has to, because until that commit a
// reader must still find the object where the old manifest says it is — and the
// copying is rate-limited. So for the length of a move, which is the object's size
// over the repair rate, the destination holds chunks no manifest mentions. Without
// this clause a sweep deletes them as fast as the move makes them, and then the
// commit promises them and the old copies are dropped on the strength of that
// promise. Watching it happen, repair restored what the sweep took and nothing was
// lost, but only because some copy of each chunk outlived the window: two passes
// racing is not a durability argument.
//
// At the object's own redundancy rather than the width of the placement in front of
// us, because a move that *widens* a narrow placement is the case where the two
// differ, and its destination is exactly the owner the manifest does not name yet.
//
// This spares nothing the pass was written for. A copy a move left behind sits on a
// node that lost the partition, so it is named by neither the manifest nor the ring.
func (c *Coordinator) reference(referenced map[string]struct{}, key string, m object.Manifest, slice byte) {
	mine := positionsOf(m.Nodes, c.self)
	if key != "" {
		want := c.live.Load().ring.Owners(ring.PartitionFor(key), redundancy(m))
		mine = append(mine, positionsOf(want, c.self)...)
	}
	if len(mine) == 0 {
		return
	}

	for _, ref := range m.Chunks {
		// A shard id begins with its chunk's id, so one slice covers a chunk and
		// all of its shards.
		if ref.ID == "" || ref.ID[0] != slice {
			continue
		}
		if m.Coding == (ec.Scheme{}) {
			referenced[ref.ID] = struct{}{}
			continue
		}
		// Erasure-coded: position is identity. This node holds the shard at its
		// own index, and a move to it would deliver the shard at the index it has
		// in the placement being moved to.
		for _, i := range mine {
			referenced[ref.ShardID(i)] = struct{}{}
		}
	}
}

// positionsOf is where node appears in a placement. A list, because a manifest's
// placement and the ring's may put the same node in different positions, and for a
// coded object the position is which shard it holds.
func positionsOf(nodes []string, node string) []int {
	var at []int
	for i, id := range nodes {
		if id == node {
			at = append(at, i)
		}
	}
	return at
}

// nextSlice returns the slice of the id space this pass should sweep, and records
// the following one before the pass runs rather than after it.
//
// Before, so that a slice which fails every time — a directory that cannot be read,
// a manifest that cannot be decoded — costs one pass rather than blocking the other
// thirty-one behind it forever. The cost is that a crash mid-pass leaves that
// slice's garbage until the cycle comes round again, and garbage is exactly the
// thing that can wait.
func (c *Coordinator) nextSlice(ctx context.Context) (byte, error) {
	at, err := c.meta.Cursor(ctx, collectTask, c.self)
	if err != nil {
		return 0, err
	}
	i := 0
	if len(at) == 1 {
		if found := strings.IndexByte(idAlphabet, at[0]); found >= 0 {
			i = found
		}
	}
	next := idAlphabet[(i+1)%len(idAlphabet)]
	if err := c.meta.SaveCursor(ctx, collectTask, c.self, string(next)); err != nil {
		return 0, err
	}
	return idAlphabet[i], nil
}
