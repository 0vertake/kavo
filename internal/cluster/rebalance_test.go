package cluster_test

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/ring"
)

func mustRebalance(t testing.TB, n *node, rate int64) cluster.RebalanceStats {
	t.Helper()
	st, err := n.c.Rebalance(context.Background(), rate)
	if err != nil {
		t.Fatalf("rebalance on %s: %v", n.id, err)
	}
	return st
}

// tellEveryone replaces every node's view of the cluster, which is what etcd's
// membership watch does in a running cluster.
func (tc *testCluster) tellEveryone(peers map[string]string) {
	for _, n := range tc.nodes {
		n.c.SetMembers(peers)
	}
}

// without is the membership minus one node: what every survivor sees once a
// node's lease expires.
func (tc *testCluster) without(gone ...string) map[string]string {
	peers := map[string]string{}
	for id, n := range tc.nodes {
		if !slices.Contains(gone, id) {
			peers[id] = n.addr
		}
	}
	return peers
}

// The hole repair leaves on purpose. A node that leaves for good takes its copy's
// place with it: repair restores the copies a manifest promises and will not put
// them anywhere else, so the object sits at N-1 copies forever. Rebalancing is
// what moves the place, and this is the test that says redundancy comes back.
func TestRedundancyReturnsAfterANodeLeavesForGood(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "left/for/good"
	data := randBytes(2 * testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)

	// The departing node is an owner but not the one doing the work.
	gone := m.Nodes[len(m.Nodes)-1]
	tc.nodes[gone].srv.Close()
	tc.tellEveryone(tc.without(gone))

	repairer := tc.nodes[m.Nodes[0]]
	if st := mustRepair(t, repairer, 0); st.Restored != 0 {
		t.Fatalf("repair restored %d copies for a node that is gone; it is supposed to leave that to rebalancing", st.Restored)
	}

	st := mustRebalance(t, repairer, 0)
	if st.Moved != 1 || st.Copies != len(m.Chunks) {
		t.Fatalf("rebalance moved %d objects and %d copies, want 1 and %d", st.Moved, st.Copies, len(m.Chunks))
	}

	// Back to N copies, on N live nodes, and every one of them can serve it.
	after, err := repairer.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Nodes) != cluster.Replicas {
		t.Fatalf("object now names %d nodes, want %d", len(after.Nodes), cluster.Replicas)
	}
	if slices.Contains(after.Nodes, gone) {
		t.Fatalf("object still names the departed node %s", gone)
	}
	for _, id := range after.Nodes {
		if held := tc.nodes[id].holds(t, after.Chunks[0].ID); len(held) != 1 {
			t.Errorf("new owner %s does not hold the object's chunks", id)
		}
		if got := mustGet(t, tc.nodes[id], key); !bytes.Equal(got, data) {
			t.Errorf("%s cannot serve the moved object", id)
		}
	}
}

// A joining node changes who owns what, and the data has to follow — otherwise a
// new node is a node that stores nothing until every old object is deleted.
func TestDataFollowsOwnershipWhenANodeJoins(t *testing.T) {
	tc := newCluster(t, 5)
	// Five nodes exist, but everyone is told about four, so writing and then
	// revealing the fifth is a join rather than a fresh cluster.
	key := keyMovedBy(t, tc, "n5", cluster.Replicas)
	tc.tellEveryone(tc.without("n5"))

	data := randBytes(2 * testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)
	if slices.Contains(m.Nodes, "n5") {
		t.Fatal("object placed on a node that was not a member yet")
	}

	tc.tellEveryone(tc.without())
	owner := tc.nodes[m.Nodes[0]]
	want := ownersOf(t, tc, key, len(m.Nodes))

	if st := mustRebalance(t, owner, 0); st.Moved != 1 {
		t.Fatalf("rebalance moved %d objects, want 1", st.Moved)
	}
	after, err := owner.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after.Nodes, want) {
		t.Fatalf("object now names %v, want the ring's owners %v", after.Nodes, want)
	}
	if got := mustGet(t, tc.nodes[want[len(want)-1]], key); !bytes.Equal(got, data) {
		t.Error("the new owner cannot serve the object it now owns")
	}
}

// Moving an object must not cost it its old copies before the new ones are
// committed, and must not leave them for good afterwards: a rebalance that only
// ever copied would double the cluster's disk use every time a node joins.
//
// The move itself no longer deletes anything — a copied object shares its source's
// chunks, so no pass may delete one on the strength of the single manifest in front
// of it. Collection is what takes the superseded copies, so the collection is part
// of what this test asserts rather than a detail behind it.
func TestMovedObjectLeavesNothingBehindOnceCollected(t *testing.T) {
	tc := newCluster(t, 5)
	// A join rather than a departure, because a departure leaves the stale copy
	// on a node that is gone and cannot be told to drop it. A join is what makes
	// a *live* node stop owning data it still holds, which is the case where
	// failing to clean up doubles the cluster's disk use.
	key := keyMovedBy(t, tc, "n5", cluster.Replicas)
	tc.tellEveryone(tc.without("n5"))
	m := mustPut(t, tc.nodes["n1"], key, randBytes(testChunkSize))

	tc.tellEveryone(tc.without())
	mustRebalance(t, tc.nodes[m.Nodes[0]], 0)
	collectEverywhere(t, tc, 0)

	after, err := tc.nodes[m.Nodes[0]].c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	demoted := 0
	for id, n := range tc.nodes {
		held := len(n.holds(t, m.Chunks[0].ID)) == 1
		switch {
		case slices.Contains(after.Nodes, id):
			if !held {
				t.Errorf("%s owns the object but does not hold its chunk", id)
			}
		case slices.Contains(m.Nodes, id):
			demoted++
			if held {
				t.Errorf("%s no longer owns the object but still holds its chunk", id)
			}
		case held:
			t.Errorf("%s holds a chunk of an object it has never owned", id)
		}
	}
	if demoted == 0 {
		t.Fatal("no live node lost ownership, so this test proves nothing")
	}
}

// A move that cannot finish must change nothing at all. Committing a manifest
// naming a node that does not have the data would be a promise the cluster
// cannot keep, and dropping the old copies first would delete the only ones a
// live manifest points at.
func TestAFailedMoveChangesNothing(t *testing.T) {
	tc := newCluster(t, 5)
	key := keyMovedBy(t, tc, "n5", cluster.Replicas)
	tc.tellEveryone(tc.without("n5"))
	m := mustPut(t, tc.nodes["n1"], key, randBytes(testChunkSize))

	// n5 joins, so it should receive a copy — but it is unreachable, so the move
	// cannot complete.
	tc.tellEveryone(tc.without())
	tc.nodes["n5"].srv.Close()

	owner := tc.nodes[m.Nodes[0]]
	// The pass reports the failure and carries on — one unreachable node must not
	// stop it — but it must not have touched this object.
	st := mustRebalance(t, owner, 0)
	if st.Failed != 1 || st.Moved != 0 {
		t.Fatalf("pass reported %d failed and %d moved, want 1 and 0", st.Failed, st.Moved)
	}

	after, err := owner.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after.Nodes, m.Nodes) {
		t.Fatalf("a failed move rewrote the manifest to %v, want it left at %v", after.Nodes, m.Nodes)
	}
	for _, id := range m.Nodes {
		if held := tc.nodes[id].holds(t, m.Chunks[0].ID); len(held) != 1 {
			t.Errorf("original owner %s lost its copy to a move that failed", id)
		}
	}
}

// A client overwriting an object mid-move must win: the manifest it wrote is the
// newer truth, and a rebalance that committed on top of it would resurrect the
// old placement and lose an acknowledged write. The pass must also have deleted
// nothing by then — copy, commit, delete, in that order, or a refused commit
// takes real copies with it.
func TestAClientOverwriteBeatsARebalance(t *testing.T) {
	tc := newCluster(t, 5)
	key := keyMovedBy(t, tc, "n5", cluster.Replicas)
	tc.tellEveryone(tc.without("n5"))
	data := randBytes(testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)

	// n5 joins, so the object is misplaced and a pass has a move to make.
	tc.tellEveryone(tc.without())
	want := ownersOf(t, tc, key, len(m.Nodes))
	owner := tc.nodes[want[0]]

	// What the pass read at the start of its walk.
	stale := scanFor(t, owner, key)

	// The client overwrites the object before the pass gets to its commit. Same
	// bytes, so the chunk ids are unchanged and the copies the pass is about to
	// orphan are the live manifest's copies too: deleting them early would be
	// data loss rather than collecting garbage.
	if _, err := owner.c.Put(context.Background(), key, bytes.NewReader(data), cluster.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	st, err := owner.c.RebalanceOne(context.Background(), stale)
	if err != nil {
		t.Fatalf("a raced pass failed instead of leaving the object alone: %v", err)
	}
	if st.Misplaced != 1 {
		t.Fatalf("pass found %d misplaced objects, want 1; the rest of this test would prove nothing", st.Misplaced)
	}
	if st.Moved != 0 || st.Raced != 1 {
		t.Errorf("raced pass moved %d and raced %d, want 0 and 1", st.Moved, st.Raced)
	}
	// The client's object is intact, manifest and chunks both.
	after, err := owner.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after.Nodes, want) {
		t.Fatalf("live manifest names %v, want the client's owners %v", after.Nodes, want)
	}
	for _, id := range after.Nodes {
		if held := tc.nodes[id].holds(t, after.Chunks[0].ID); len(held) != 1 {
			t.Errorf("%s lost a chunk the live manifest names", id)
		}
	}
	if got := mustGet(t, owner, key); !bytes.Equal(got, data) {
		t.Error("the client's object did not survive the raced pass")
	}
}

// scanFor reads an object the way a rebalance pass does: through a scan, so it
// carries the revision it was read at.
func scanFor(t testing.TB, n *node, key string) meta.Object {
	t.Helper()
	objs, err := n.m.ScanObjects(context.Background(), "", "", 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range objs {
		if o.Key == key {
			return o
		}
	}
	t.Fatalf("object %s not found in a scan", key)
	return meta.Object{}
}

// A pass over a correctly placed cluster must move nothing. Rebalancing that is
// not idempotent is rebalancing that never stops moving data.
func TestRebalanceMovesNothingWhenPlacementIsCorrect(t *testing.T) {
	tc := newCluster(t, 5)
	for i := range 4 {
		mustPut(t, tc.nodes["n1"], keyFor(i), randBytes(testChunkSize))
	}
	var total cluster.RebalanceStats
	for _, n := range tc.nodes {
		st := mustRebalance(t, n, 0)
		total.Objects += st.Objects
		total.Misplaced += st.Misplaced
		total.Copies += st.Copies
	}
	if total.Objects != 4 {
		t.Errorf("nodes between them checked %d objects, want each of the 4 checked once", total.Objects)
	}
	if total.Misplaced != 0 || total.Copies != 0 {
		t.Errorf("a healthy cluster moved data: %+v", total)
	}
}

// Erasure-coded objects rebalance too, and their shards have to land in the right
// positions: shard i on the i'th owner, or the chunk decodes to garbage.
func TestCodedShardsMoveToTheRightPositions(t *testing.T) {
	tc := newECCluster(t, 7, testChunkSize)
	key := keyMovedBy(t, tc, "n7", testScheme.Shards())
	tc.tellEveryone(tc.without("n7"))

	data := randBytes(2 * testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)
	if len(m.Nodes) != testScheme.Shards() {
		t.Fatalf("coded object spread over %d nodes, want %d", len(m.Nodes), testScheme.Shards())
	}

	tc.tellEveryone(tc.without())
	owner := tc.nodes[m.Nodes[0]]
	want := ownersOf(t, tc, key, testScheme.Shards())

	if st := mustRebalance(t, owner, 0); st.Moved != 1 {
		t.Fatalf("rebalance moved %d coded objects, want 1", st.Moved)
	}
	after, err := owner.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(after.Nodes, want) {
		t.Fatalf("coded object now names %v, want %v", after.Nodes, want)
	}
	for i, id := range after.Nodes {
		if held := tc.nodes[id].holds(t, after.Chunks[0].ShardID(i)); len(held) != 1 {
			t.Errorf("%s does not hold shard %d, which it now owns", id, i)
		}
	}
	// Decodable is the real assertion: shards in the wrong positions still
	// exist, they just reconstruct to nonsense.
	if got := mustGet(t, tc.nodes[want[len(want)-1]], key); !bytes.Equal(got, data) {
		t.Error("the moved coded object does not read back")
	}
}

func keyFor(i int) string { return fmt.Sprintf("rebalance/object%d", i) }

// keyMovedBy returns a key whose owners differ between a cluster with the named
// node and one without it. Most keys are unaffected by a membership change —
// that is the point of consistent hashing — so a test that picked a key at random
// would usually be testing nothing at all.
func keyMovedBy(t testing.TB, tc *testCluster, node string, width int) string {
	t.Helper()
	full := slices.Sorted(maps.Keys(tc.nodes))
	partial := slices.DeleteFunc(slices.Clone(full), func(id string) bool { return id == node })
	for i := range 200 {
		key := keyFor(i)
		if !slices.Equal(ringOwners(partial, key, width), ringOwners(full, key, width)) {
			return key
		}
	}
	t.Fatalf("no key in 200 changed owners when %s joined a %d-wide placement", node, width)
	return ""
}

// ringOwners is what the ring says for a key given exactly this membership, which
// is how a test reasons about a cluster the nodes have not been told about yet.
func ringOwners(members []string, key string, width int) []string {
	return ring.New(members, ring.DefaultVNodes).Owners(ring.PartitionFor(key), width)
}

// ownersOf is the ring's answer for a key at a given width, as node ids.
func ownersOf(t testing.TB, tc *testCluster, key string, width int) []string {
	t.Helper()
	owners, _ := tc.ownersN(t, key, width)
	ids := make([]string, len(owners))
	for i, n := range owners {
		ids[i] = n.id
	}
	return ids
}
