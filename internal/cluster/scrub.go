package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/0vertake/kavo/internal/ec"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/peer"
	"github.com/0vertake/kavo/internal/store"
)

// DefaultScrubInterval is how long a node waits between scrub passes. Long,
// because a pass reads every byte this node stores: rot is rare and disks are
// large, so scrubbing is a slow background sweep rather than a reaction.
const DefaultScrubInterval = 24 * time.Hour

// ScrubStats reports what one scrub pass did.
type ScrubStats struct {
	CopiesRead  int   // local copies read and checksummed
	BytesRead   int64 // how much was read to do it
	Rotted      int   // copies whose bytes no longer match the manifest
	Rebuilt     int   // rotted copies replaced from a good peer
	Unrecovered int   // rotted copies with no good copy left anywhere
}

// ErrRotUnrecovered reports that a scrub found corruption it could not replace,
// which means the last good copy of that data is gone.
var ErrRotUnrecovered = errors.New("scrub: rot could not be repaired")

// ScrubLoop verifies this node's own chunks against their manifests until ctx is
// done, pausing between passes.
//
// Repair asks whether a copy exists; this asks whether it is still the data it is
// supposed to be. A disk that quietly rewrites a sector answers every survey
// correctly and only fails at the moment a client reads it, which is far too late
// for that to be the first time anyone looked.
func (c *Coordinator) ScrubLoop(ctx context.Context, rate int64, interval time.Duration) {
	for {
		start := time.Now()
		st, err := c.Scrub(ctx, rate)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			log.Printf("scrub: pass failed after %v: %v", time.Since(start), err)
		case st.Rotted > 0:
			log.Printf("scrub: found %d rotted copies in %v, rebuilt %d, %d unrecovered",
				st.Rotted, time.Since(start), st.Rebuilt, st.Unrecovered)
		default:
			log.Printf("scrub: verified %d copies (%d bytes) in %v, no rot",
				st.CopiesRead, st.BytesRead, time.Since(start))
		}
		if !sleep(ctx, interval) {
			return
		}
	}
}

// Scrub makes one pass over the objects stored on this node, re-reads its own copy
// of every chunk, and replaces any whose bytes no longer match the manifest's
// checksum. Reads are paced to rate bytes per second; zero is unlimited.
//
// Every node scrubs its own disk, because it is the only node that can read it.
// That is a different rule from repair, where one node surveys a partition on
// everyone's behalf.
func (c *Coordinator) Scrub(ctx context.Context, rate int64) (ScrubStats, error) {
	var st ScrubStats
	pace := &pacer{rate: rate, since: time.Now()}

	err := c.walk(ctx, "scrub", func(o meta.Object) error {
		return c.scrubObject(ctx, o, &st, pace)
	})
	if err != nil {
		return st, err
	}
	if st.Unrecovered > 0 {
		return st, fmt.Errorf("%w: %d copies rotted with no good copy left", ErrRotUnrecovered, st.Unrecovered)
	}
	return st, nil
}

func (c *Coordinator) scrubObject(ctx context.Context, o meta.Object, st *ScrubStats, pace *pacer) error {
	live := c.live.Load()
	if o.Manifest.Coding != (ec.Scheme{}) {
		return c.scrubShards(ctx, o, st, pace, live)
	}

	var errs []error
	for _, ref := range o.Manifest.Chunks {
		held, err := c.store.HasChunk(ref.ID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// A chunk this node does not have is repair's problem, not the
		// scrubber's: there is nothing here to check.
		if !held {
			continue
		}

		st.CopiesRead++
		st.BytesRead += ref.Size
		pace.wait(ctx, ref.Size)

		if err := c.verifyLocal(ref); err == nil {
			continue
		} else if !errors.Is(err, store.ErrChecksumMismatch) {
			errs = append(errs, err)
			continue
		}
		st.Rotted++

		// Rot is only recoverable from another node: this node's copy is the
		// thing that is wrong. WriteChunkVerified stages and renames, so the bad
		// copy is replaced atomically and never half-overwritten.
		if err := c.rebuildFromPeers(ctx, ref, o.Manifest.Nodes, live); err != nil {
			st.Unrecovered++
			errs = append(errs, fmt.Errorf("scrub: chunk %s rotted on %s: %w", ref.ID, c.self, err))
			continue
		}
		st.Rebuilt++
		log.Printf("scrub: replaced rotted chunk %s from a peer", ref.ID)
	}
	return errors.Join(errs...)
}

// scrubShards checks the erasure-coded shards this node holds and replaces any
// that have rotted.
//
// A rotted shard is rebuilt from the other shards rather than copied from a peer:
// there is no other copy of shard 4 anywhere, only the arithmetic that produces
// it. Which means rot in a shard costs a full decode to fix, where rot in a
// replica costs a fetch — the read-side price of storing 1.5x instead of 3x.
func (c *Coordinator) scrubShards(ctx context.Context, o meta.Object, st *ScrubStats, pace *pacer, live *membership) error {
	var errs []error
	for _, ref := range o.Manifest.Chunks {
		var rotted []int
		for i, node := range o.Manifest.Nodes {
			if node != c.self {
				continue
			}
			held, size, err := c.verifyLocalShard(ref, i)
			if !held {
				if err != nil {
					errs = append(errs, err)
				}
				continue
			}
			st.CopiesRead++
			st.BytesRead += size
			pace.wait(ctx, size)

			if errors.Is(err, store.ErrChecksumMismatch) {
				st.Rotted++
				rotted = append(rotted, i)
				continue
			}
			if err != nil {
				errs = append(errs, err)
			}
		}
		if len(rotted) == 0 {
			continue
		}
		if err := c.restoreShards(ctx, ref, o.Manifest.Nodes, o.Manifest.Nodes, rotted, o.Manifest.Coding, live, pace); err != nil {
			st.Unrecovered += len(rotted)
			errs = append(errs, fmt.Errorf("scrub: shards of chunk %s rotted on %s: %w", ref.ID, c.self, err))
			continue
		}
		st.Rebuilt += len(rotted)
		log.Printf("scrub: rebuilt %d rotted shards of chunk %s", len(rotted), ref.ID)
	}
	return errors.Join(errs...)
}

// verifyLocal reads this node's copy of a chunk to the end, which is what makes
// the checksum mean anything: the store reports a mismatch instead of a final EOF.
func (c *Coordinator) verifyLocal(ref object.ChunkRef) error {
	rc, err := c.store.ReadChunk(ref.ID, ref.CRC)
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(io.Discard, rc)
	return err
}

// rebuildFromPeers replaces this node's copy with one from a node whose copy still
// verifies. This node is deliberately not asked, since its copy is the bad one.
func (c *Coordinator) rebuildFromPeers(ctx context.Context, ref object.ChunkRef, nodes []string, live *membership) error {
	var errs []error
	for _, node := range nodes {
		// Whatever address the node was last known at, which is what the read path
		// uses and for the same reason: a chunk is immutable and verified against
		// the manifest's checksum at both ends, so a node that has left either
		// hands over the bytes the manifest names or fails to answer.
		//
		// Asking only current members made rot unrecoverable in the case that needs
		// recovery most — the good copies on nodes whose leases have lapsed, the bad
		// one here. Reads went on working through those same addresses, so nothing
		// reported it, and the rot waited for the good copies to disappear too.
		addr := live.readAddr(node)
		if node == c.self || addr == "" {
			continue
		}
		src, err := peer.FetchChunk(ctx, addr, ref.ID, ref.CRC)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		err = c.store.WriteChunkVerified(ref.ID, src, ref.CRC, ref.Size)
		src.Close()
		if err == nil {
			return nil
		}
		errs = append(errs, err)
	}
	if len(errs) == 0 {
		// Nobody was even asked: this node is the only one the manifest names that
		// the cluster has ever heard of. Worth saying plainly, since "no peer could
		// supply a good copy" reads as though peers were tried and failed.
		return fmt.Errorf("no other node has ever held chunk %s", ref.ID)
	}
	return fmt.Errorf("no peer could supply a good copy: %w", errors.Join(errs...))
}
