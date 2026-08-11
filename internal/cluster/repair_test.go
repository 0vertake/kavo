package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/cluster"
)

func mustRepair(t *testing.T, n *node, rate int64) cluster.Stats {
	t.Helper()
	st, err := n.c.Repair(context.Background(), rate)
	if err != nil {
		t.Fatalf("repair via %s: %v", n.id, err)
	}
	return st
}

// Invariant 4: after healing, redundancy is back to the configured level. A
// replaced disk is the plain case — the node is healthy and answers, it just has
// nothing.
//
// Each owner is tried in turn because the repairing node restoring its own copy
// and restoring someone else's are different paths.
func TestRepairRestoresCopiesLostToADisk(t *testing.T) {
	const key = "repaired/object"
	for victim := range cluster.Replicas {
		t.Run(fmt.Sprint("owner", victim), func(t *testing.T) {
			tc := newCluster(t, 5)
			owners, outsider := tc.owners(t, key)
			data := randBytes(3 * testChunkSize)
			m := mustPut(t, outsider, key, data)

			owners[victim].loseChunks(t, m.Chunks)

			st := mustRepair(t, owners[0], 0)
			if st.Restored != len(m.Chunks) {
				t.Errorf("restored %d copies, want %d", st.Restored, len(m.Chunks))
			}
			if st.BytesRestored != m.Size {
				t.Errorf("restored %d bytes, want %d", st.BytesRestored, m.Size)
			}
			for _, ref := range m.Chunks {
				if !owners[victim].has(ref) {
					t.Errorf("chunk %s is still missing from %s", ref.ID, owners[victim].id)
				}
			}
			if got := mustGet(t, outsider, key); !bytes.Equal(got, data) {
				t.Error("object read after repair differs from what was written")
			}
		})
	}
}

// The other source of under-replication, and the one nothing announces: a write
// acknowledged at W=2 of N=3 leaves a copy that never existed at all.
func TestRepairCompletesADegradedWrite(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "degraded/then/repaired"
	owners, outsider := tc.owners(t, key)
	missing := owners[2]

	missing.kill()
	data := randBytes(2 * testChunkSize)
	m := mustPut(t, outsider, key, data)
	for _, ref := range m.Chunks {
		if missing.has(ref) {
			t.Fatalf("chunk %s reached the node that was down", ref.ID)
		}
	}

	missing.revive(t)
	st := mustRepair(t, owners[0], 0)

	if st.Restored != len(m.Chunks) {
		t.Errorf("restored %d copies, want %d", st.Restored, len(m.Chunks))
	}
	for _, ref := range m.Chunks {
		if !missing.has(ref) {
			t.Errorf("chunk %s never reached %s", ref.ID, missing.id)
		}
	}
	if got := mustGet(t, outsider, key); !bytes.Equal(got, data) {
		t.Error("object read after repair differs from what was written")
	}
}

// Repair runs forever, so a pass over a healthy cluster has to be free of side
// effects. One that re-pushed every chunk would spend the cluster's bandwidth
// proving nothing was wrong.
func TestRepairMovesNothingWhenNothingIsMissing(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "healthy/object"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))

	for pass := range 2 {
		st := mustRepair(t, owners[0], 0)
		if st.Restored != 0 || st.Unrepairable != 0 {
			t.Errorf("pass %d restored %d and failed %d, want a no-op", pass, st.Restored, st.Unrepairable)
		}
		if want := len(m.Chunks) * cluster.Replicas; st.CopiesChecked != want {
			t.Errorf("pass %d checked %d copies, want %d", pass, st.CopiesChecked, want)
		}
	}
}

// One node per partition repairs it. Without that rule every node would survey
// every object — N times the work — and several would race to push the same chunk
// to the same place.
func TestOnlyTheFirstOwnerRepairsAPartition(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "repaired/once"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))
	owners[1].loseChunks(t, m.Chunks)

	for _, n := range append([]*node{outsider}, owners[1:]...) {
		st := mustRepair(t, n, 0)
		if st.Objects != 0 || st.Restored != 0 {
			t.Errorf("%s repaired %d objects and %d copies, want none: it does not own the partition first",
				n.id, st.Objects, st.Restored)
		}
	}
	if st := mustRepair(t, owners[0], 0); st.Restored != len(m.Chunks) {
		t.Errorf("the first owner restored %d copies, want %d", st.Restored, len(m.Chunks))
	}
}

// Resumability is what makes a long heal finish: a pass that started over every
// time a process restarted would never reach the last object.
func TestRepairResumesFromWhereItStopped(t *testing.T) {
	tc := newCluster(t, 5)
	ctx := context.Background()

	// Two keys either side of the cursor that the same node is responsible for,
	// so one pass can be shown to cover one and not the other.
	early, late := "a/early", ""
	earlyOwners, outsider := tc.owners(t, early)
	repairer := earlyOwners[0]
	var lateOwners []*node
	for i := range 200 {
		candidate := fmt.Sprintf("z/late-%d", i)
		if owners, _ := tc.owners(t, candidate); owners[0].id == repairer.id {
			late, lateOwners = candidate, owners
			break
		}
	}
	if late == "" {
		t.Fatalf("no key after the cursor is repaired by %s", repairer.id)
	}

	earlyManifest := mustPut(t, outsider, early, randBytes(testChunkSize))
	lateManifest := mustPut(t, outsider, late, randBytes(testChunkSize))
	earlyOwners[1].loseChunks(t, earlyManifest.Chunks)
	lateOwners[1].loseChunks(t, lateManifest.Chunks)

	// Resuming past the early object is exactly what a restart mid-pass looks like.
	if err := repairer.m.SaveRepairCursor(ctx, repairer.id, "b"); err != nil {
		t.Fatalf("save cursor: %v", err)
	}
	if st := mustRepair(t, repairer, 0); st.Objects != 1 {
		t.Fatalf("resumed pass covered %d objects, want only the one after the cursor", st.Objects)
	}
	for _, ref := range earlyManifest.Chunks {
		if earlyOwners[1].has(ref) {
			t.Errorf("chunk %s before the cursor was repaired, want it left for the next pass", ref.ID)
		}
	}
	for _, ref := range lateManifest.Chunks {
		if !lateOwners[1].has(ref) {
			t.Errorf("chunk %s after the cursor was not repaired", ref.ID)
		}
	}

	// Reaching the end resets the cursor, so the next pass sees everything again.
	if cursor, err := repairer.m.RepairCursor(ctx, repairer.id); err != nil || cursor != "" {
		t.Fatalf("cursor after a completed pass = (%q, %v), want empty", cursor, err)
	}
	if st := mustRepair(t, repairer, 0); st.Restored != len(earlyManifest.Chunks) {
		t.Errorf("second pass restored %d copies, want %d", st.Restored, len(earlyManifest.Chunks))
	}
}

// Repair competes with clients for the same disks and the same network, so the
// rate limit is the difference between healing and a cluster-wide latency spike.
func TestRepairPacesItselfToTheGivenRate(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "paced/object"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(8*testChunkSize))
	owners[1].loseChunks(t, m.Chunks)

	// Slow enough that the pacing dominates, short enough to keep the test quick.
	rate := m.Size // one second's worth of repair
	start := time.Now()
	st := mustRepair(t, owners[0], rate)
	elapsed := time.Since(start)

	if st.BytesRestored != m.Size {
		t.Fatalf("restored %d bytes, want %d", st.BytesRestored, m.Size)
	}
	want := time.Duration(float64(m.Size)/float64(rate)*float64(time.Second)) * 8 / 10
	if elapsed < want {
		t.Errorf("restored %d bytes at %d B/s in %v, too fast to have been paced (want >= %v)",
			st.BytesRestored, rate, elapsed, want)
	}
	t.Logf("restored %d bytes at %d B/s in %v", st.BytesRestored, rate, elapsed)
}

// A copy on a node the cluster has lost cannot be restored to it. Putting it
// somewhere else instead would mean rewriting the manifest, which is rebalancing.
// What matters is that repair says so rather than reporting success.
func TestRepairSkipsOwnersThatLeftTheCluster(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "partly/homeless"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))

	// The repairing node stops counting one owner as a member.
	remaining := map[string]string{}
	for id, n := range tc.nodes {
		if id != owners[2].id {
			remaining[id] = n.addr
		}
	}
	owners[0].c.SetMembers(remaining)

	st := mustRepair(t, owners[0], 0)
	if want := len(m.Chunks) * (cluster.Replicas - 1); st.CopiesChecked != want {
		t.Errorf("checked %d copies, want %d: the departed owner has no address to probe", st.CopiesChecked, want)
	}
	if st.Restored != 0 || st.Unrepairable != 0 {
		t.Errorf("restored %d and failed %d, want neither", st.Restored, st.Unrepairable)
	}
}

// Losing every copy is data loss, and repair has to say so. Reporting a clean
// pass would hide exactly the thing the invariants exist to catch.
func TestRepairReportsCopiesItCannotRebuild(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "lost/object"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))
	for _, o := range owners {
		o.loseChunks(t, m.Chunks)
	}

	st, err := owners[0].c.Repair(context.Background(), 0)
	if !errors.Is(err, cluster.ErrUnrepairable) {
		t.Fatalf("repair error = %v, want cluster.ErrUnrepairable", err)
	}
	if want := len(m.Chunks) * cluster.Replicas; st.Unrepairable != want {
		t.Errorf("reported %d unrepairable copies, want %d", st.Unrepairable, want)
	}
	if st.Restored != 0 {
		t.Errorf("restored %d copies out of nothing", st.Restored)
	}
}
