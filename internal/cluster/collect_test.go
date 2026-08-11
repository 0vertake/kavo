package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/object"
)

// collectEverywhere sweeps the whole id space on every node, which is what a full
// cycle of the background loop eventually does.
func collectEverywhere(t testing.TB, tc *testCluster, grace time.Duration) cluster.CollectStats {
	t.Helper()
	var total cluster.CollectStats
	for _, id := range slices.Sorted(maps.Keys(tc.nodes)) {
		n := tc.nodes[id]
		for range cluster.CollectSlices {
			st, err := n.c.Collect(context.Background(), grace)
			if err != nil {
				t.Fatalf("collect on %s: %v", n.id, err)
			}
			total.Examined += st.Examined
			total.Referenced += st.Referenced
			total.Collected += st.Collected
			total.BytesCollected += st.BytesCollected
			total.Young += st.Young
		}
	}
	return total
}

// chunksOn lists every chunk file on a node's disk, shards included.
func chunksOn(t testing.TB, n *node) []string {
	t.Helper()
	root := filepath.Join(n.root, "chunks")
	dirs, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("list chunks on %s: %v", n.id, err)
	}
	var ids []string
	for _, d := range dirs {
		files, err := os.ReadDir(filepath.Join(root, d.Name()))
		if err != nil {
			t.Fatalf("list chunks on %s: %v", n.id, err)
		}
		for _, f := range files {
			ids = append(ids, f.Name())
		}
	}
	slices.Sort(ids)
	return ids
}

// held reports which nodes still have a copy of any of these chunks.
func held(t testing.TB, tc *testCluster, refs []object.ChunkRef) []string {
	t.Helper()
	var where []string
	for _, id := range slices.Sorted(maps.Keys(tc.nodes)) {
		on := chunksOn(t, tc.nodes[id])
		for _, ref := range refs {
			if slices.Contains(on, ref.ID) {
				where = append(where, id+"/"+ref.ID)
			}
		}
	}
	return where
}

// The point of the whole pass: an overwrite supersedes the chunks it replaces, and
// nothing else was ever going to delete them. The object has to still read
// afterwards, because a pass that reclaims the wrong version has lost a write.
func TestOverwrittenChunksAreReclaimed(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "notes/draft.txt"
	_, outsider := tc.owners(t, key)

	first := randBytes(3 * testChunkSize)
	old := mustPut(t, outsider, key, first)
	second := randBytes(2 * testChunkSize)
	current := mustPut(t, outsider, key, second)

	if where := held(t, tc, old.Chunks); len(where) == 0 {
		t.Fatal("the overwritten version's chunks were already gone before collection")
	}

	st := collectEverywhere(t, tc, 0)
	if st.Collected != len(old.Chunks)*cluster.Replicas {
		t.Errorf("collected %d chunks, want the %d copies of the %d superseded chunks",
			st.Collected, len(old.Chunks)*cluster.Replicas, len(old.Chunks))
	}
	if where := held(t, tc, old.Chunks); len(where) != 0 {
		t.Errorf("the overwritten version's chunks survived on %v", where)
	}
	if where := held(t, tc, current.Chunks); len(where) != len(current.Chunks)*cluster.Replicas {
		t.Errorf("the current version holds %d copies, want %d", len(where), len(current.Chunks)*cluster.Replicas)
	}
	if got := mustGet(t, outsider, key); !bytes.Equal(got, second) {
		t.Error("the object no longer reads back as what was written last")
	}
}

// A delete drops its own chunks, but only as a best effort: the object is gone the
// moment its manifest is, so a copy that could not be deleted is logged and left.
// Removing the manifest alone is the state that leaves — a node that was down when
// the drop was attempted, or a process that died between the two.
func TestChunksAnInterruptedDeleteLeftBehindAreReclaimed(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "gone.bin"
	_, outsider := tc.owners(t, key)

	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))
	if err := outsider.m.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete the manifest of %s: %v", key, err)
	}
	if where := held(t, tc, m.Chunks); len(where) != len(m.Chunks)*cluster.Replicas {
		t.Fatalf("%d copies on disk before collecting, want %d", len(where), len(m.Chunks)*cluster.Replicas)
	}

	collectEverywhere(t, tc, 0)
	if where := held(t, tc, m.Chunks); len(where) != 0 {
		t.Errorf("chunks of an object with no manifest survived on %v", where)
	}
}

// The grace period is what stands between this pass and a write that is in flight:
// a chunk is durable before the manifest naming it is committed, so for that window
// perfectly good data is referenced by nothing at all.
func TestAnUnreferencedChunkInsideTheGracePeriodIsKept(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "recent.bin"
	_, outsider := tc.owners(t, key)

	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))
	if err := outsider.m.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete the manifest of %s: %v", key, err)
	}

	st := collectEverywhere(t, tc, time.Hour)
	if st.Collected != 0 {
		t.Errorf("collected %d chunks written seconds ago", st.Collected)
	}
	if st.Young != len(m.Chunks)*cluster.Replicas {
		t.Errorf("%d chunks were spared for being young, want %d", st.Young, len(m.Chunks)*cluster.Replicas)
	}
	if where := held(t, tc, m.Chunks); len(where) != len(m.Chunks)*cluster.Replicas {
		t.Errorf("only %d copies survived the grace period, want %d", len(where), len(m.Chunks)*cluster.Replicas)
	}
}

// A multipart upload's parts are durable for as long as the client takes, which S3
// allows to be days, so their age cannot be what protects them. Being referenced by
// the part is.
func TestChunksOfAnUploadInProgressAreKept(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "big/object.bin"
	_, outsider := tc.owners(t, key)

	id, err := outsider.c.CreateUpload(context.Background(), key, "application/octet-stream")
	if err != nil {
		t.Fatalf("create upload: %v", err)
	}
	part := randBytes(2 * testChunkSize)
	if _, err := outsider.c.UploadPart(context.Background(), id, 1, bytes.NewReader(part), int64(len(part))); err != nil {
		t.Fatalf("upload part: %v", err)
	}

	parts, err := outsider.m.Parts(context.Background(), id)
	if err != nil {
		t.Fatalf("read parts: %v", err)
	}
	uploaded := parts[1]
	if len(uploaded.Chunks) == 0 {
		t.Fatal("the part recorded no chunks")
	}

	// No grace at all: the only thing that can save these chunks is the part
	// manifest that names them.
	if st := collectEverywhere(t, tc, 0); st.Collected != 0 {
		t.Errorf("collected %d chunks belonging to an upload in progress", st.Collected)
	}
	if where := held(t, tc, uploaded.Chunks); len(where) != len(uploaded.Chunks)*cluster.Replicas {
		t.Errorf("an in-progress part holds %d copies, want %d", len(where), len(uploaded.Chunks)*cluster.Replicas)
	}

	// And once the upload becomes an object, the same chunks are still referenced —
	// by the object now, not the part.
	if _, err := outsider.c.CompleteUpload(context.Background(), id, []cluster.CompletedPart{{Number: 1, ETag: ""}}); err != nil {
		t.Fatalf("complete upload: %v", err)
	}
	if st := collectEverywhere(t, tc, 0); st.Collected != 0 {
		t.Errorf("collected %d chunks of a completed object", st.Collected)
	}
	if got := mustGet(t, outsider, key); !bytes.Equal(got, part) {
		t.Error("the completed object does not read back as the part that was uploaded")
	}
}

// The one thing this pass must never do. A node that cannot read the manifests
// cannot tell an object it failed to read from an object that does not exist, and
// the difference between those two is an acknowledged write.
func TestAPassThatCannotReadTheMetadataDeletesNothing(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "unreadable.bin"
	_, outsider := tc.owners(t, key)

	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))
	owner := tc.nodes[m.Nodes[0]]
	before := chunksOn(t, owner)

	// etcd goes away from under this node, which is the fault a sweep has to
	// survive by doing nothing at all.
	if err := owner.m.Close(); err != nil {
		t.Fatalf("close meta store: %v", err)
	}
	if _, err := owner.c.Collect(context.Background(), 0); err == nil {
		t.Fatal("a pass with no metadata to read reported success")
	}
	if after := chunksOn(t, owner); !slices.Equal(after, before) {
		t.Errorf("a pass with no metadata to read deleted chunks: %d on disk, was %d", len(after), len(before))
	}
}

// What makes copying an object possible without copying its data: a chunk is live
// if any manifest names it, so two keys may name the same chunks and deleting one
// of them must not touch the other's data.
func TestAChunkTwoObjectsNameSurvivesOneOfThemGoing(t *testing.T) {
	tc := newCluster(t, 5)
	const original, copied = "original.bin", "copy.bin"
	_, outsider := tc.owners(t, original)

	data := randBytes(2 * testChunkSize)
	m := mustPut(t, outsider, original, data)

	// Committing the same manifest under a second key is what a server-side copy
	// does: new name, same chunks, no data moved.
	if err := outsider.m.Commit(context.Background(), copied, m); err != nil {
		t.Fatalf("commit the copy: %v", err)
	}
	// The manifest only. Delete drops the chunks of the manifest it removes
	// without asking whether anything else names them, which is safe exactly as
	// long as nothing does — so sharing chunks between keys means that eager drop
	// has to go and this sweep has to be what reclaims them.
	if err := outsider.m.Delete(context.Background(), original); err != nil {
		t.Fatalf("delete the manifest of %s: %v", original, err)
	}

	if st := collectEverywhere(t, tc, 0); st.Collected != 0 {
		t.Errorf("collected %d chunks the surviving copy still names", st.Collected)
	}
	if got := mustGet(t, outsider, copied); !bytes.Equal(got, data) {
		t.Error("the copy does not read back after the original was deleted and collected")
	}
}

// Erasure-coded objects store a shard per node rather than a copy, and a shard's id
// is derived from its chunk's. Reclaiming them has to follow the same rule.
func TestCodedShardsAreReclaimedWhenTheObjectGoes(t *testing.T) {
	tc := newECCluster(t, 7, testChunkSize)
	const key = "coded/object.bin"
	_, outsider := tc.ownersN(t, key, testScheme.Shards())

	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))
	if err := outsider.m.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete the manifest of %s: %v", key, err)
	}

	shards := 0
	for i, id := range m.Nodes {
		for _, ref := range m.Chunks {
			if slices.Contains(chunksOn(t, tc.nodes[id]), ref.ShardID(i)) {
				shards++
			}
		}
	}
	if shards != len(m.Chunks)*testScheme.Shards() {
		t.Fatalf("found %d shards on disk before collecting, want %d", shards, len(m.Chunks)*testScheme.Shards())
	}

	if st := collectEverywhere(t, tc, 0); st.Collected != shards {
		t.Errorf("collected %d shards, want %d", st.Collected, shards)
	}
	for i, id := range m.Nodes {
		on := chunksOn(t, tc.nodes[id])
		for _, ref := range m.Chunks {
			if slices.Contains(on, ref.ShardID(i)) {
				t.Errorf("shard %s survived on %s", ref.ShardID(i), id)
			}
		}
	}
}

// The same in the direction that costs data rather than disk: a coded object that
// still exists has to survive a sweep. A shard is named by neither its chunk's id
// nor its own position alone, and a pass that looked for the wrong one would find
// every shard in the cluster unreferenced.
func TestCollectingLeavesACodedObjectAlone(t *testing.T) {
	tc := newECCluster(t, 7, testChunkSize)
	const key = "coded/kept.bin"
	_, outsider := tc.ownersN(t, key, testScheme.Shards())

	data := randBytes(2 * testChunkSize)
	m := mustPut(t, outsider, key, data)
	want := len(m.Chunks) * testScheme.Shards()

	st := collectEverywhere(t, tc, 0)
	if st.Collected != 0 {
		t.Errorf("collected %d shards of an object that still exists", st.Collected)
	}
	if st.Referenced != want {
		t.Errorf("%d shards were found referenced, want %d", st.Referenced, want)
	}
	if got := mustGet(t, outsider, key); !bytes.Equal(got, data) {
		t.Error("the coded object does not read back after a collection pass")
	}
}

// A pass over a cluster with nothing to reclaim has to be a pass that deletes
// nothing, or every object in the store is one bug away from being collected.
func TestCollectingACleanClusterReclaimsNothing(t *testing.T) {
	tc := newCluster(t, 5)
	var refs []object.ChunkRef
	for i := range 8 {
		refs = append(refs, mustPut(t, tc.nodes["n1"], keyFor(i), randBytes(2*testChunkSize)).Chunks...)
	}

	st := collectEverywhere(t, tc, 0)
	if st.Collected != 0 {
		t.Errorf("collected %d chunks from a cluster with no garbage in it", st.Collected)
	}
	if st.Referenced != len(refs)*cluster.Replicas {
		t.Errorf("%d copies were found referenced, want %d", st.Referenced, len(refs)*cluster.Replicas)
	}
	if where := held(t, tc, refs); len(where) != len(refs)*cluster.Replicas {
		t.Errorf("%d copies on disk after collecting, want %d", len(where), len(refs)*cluster.Replicas)
	}
	if st.Examined != st.Referenced {
		t.Errorf("examined %d chunks but only %d were referenced, so something unreferenced is on disk", st.Examined, st.Referenced)
	}
}

// One pass sweeps one slice of the id space and leaves the rest untouched, which is
// what keeps the live set a fraction of the disk rather than all of it.
func TestOnePassSweepsOneSliceOfTheIDSpace(t *testing.T) {
	tc := newCluster(t, 5)
	for i := range 8 {
		mustPut(t, tc.nodes["n1"], keyFor(i), randBytes(2*testChunkSize))
	}

	n := tc.nodes["n1"]
	on := chunksOn(t, n)
	for range cluster.CollectSlices {
		st, err := n.c.Collect(context.Background(), 0)
		if err != nil {
			t.Fatalf("collect: %v", err)
		}
		want := 0
		for _, id := range on {
			if strings.HasPrefix(id, st.Slice) {
				want++
			}
		}
		if st.Examined != want {
			t.Errorf("slice %s examined %d chunks, want the %d whose ids begin with it", st.Slice, st.Examined, want)
		}
	}
}

// A copy on a node that neither the manifest nor the ring names is unreachable,
// because a reader tries the nodes the manifest names and stops. That is the residue
// a rebalance leaves when it moves data and the delete afterwards does not happen —
// and it is the case this pass was written for, so it has to be built the way it
// really arises: the ring hands the partition elsewhere.
func TestACopyNeitherTheManifestNorTheRingNamesIsReclaimed(t *testing.T) {
	tc := newCluster(t, 5)
	// Five nodes exist and everyone is told about four, so revealing the fifth is
	// a join, and this key is one the join reassigns.
	key := keyMovedBy(t, tc, "n5", cluster.Replicas)
	tc.tellEveryone(tc.without("n5"))

	data := randBytes(2 * testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)
	tc.tellEveryone(tc.without())

	// The move has happened: the manifest names the ring's owners and the copies
	// are there. What is left over is the copy on the node that stopped owning it,
	// which is what a move whose delete did not happen leaves behind.
	moved := m
	moved.Nodes = ownersOf(t, tc, key, len(m.Nodes))
	stale := ""
	for _, id := range m.Nodes {
		if !slices.Contains(moved.Nodes, id) {
			stale = id
		}
	}
	if stale == "" {
		t.Fatalf("the join left placement %v unchanged, so there is no residue to collect", m.Nodes)
	}
	for _, id := range moved.Nodes {
		placeChunks(t, tc.nodes[id], m, data)
	}
	if err := tc.nodes["n1"].m.Commit(context.Background(), key, moved); err != nil {
		t.Fatalf("commit the moved manifest: %v", err)
	}

	collectEverywhere(t, tc, 0)
	for _, ref := range m.Chunks {
		if slices.Contains(chunksOn(t, tc.nodes[stale]), ref.ID) {
			t.Errorf("chunk %s survived on %s, which neither the manifest nor the ring names", ref.ID, stale)
		}
	}
	if got := mustGet(t, tc.nodes["n1"], key); !bytes.Equal(got, data) {
		t.Error("the object stopped reading once its residue was reclaimed")
	}
}

// The other side of that rule, and the seam between two passes that are each
// correct alone. A move copies every chunk to the new owners before committing the
// manifest that names them, and the copying is rate-limited, so for the length of a
// move the destination holds chunks no manifest mentions. A sweep that went by the
// manifest alone would delete them as fast as the move made them.
func TestAChunkAMoveIsAboutToDeliverIsKept(t *testing.T) {
	tc := newCluster(t, 5)
	key := keyMovedBy(t, tc, "n5", cluster.Replicas)
	tc.tellEveryone(tc.without("n5"))

	data := randBytes(2 * testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)
	tc.tellEveryone(tc.without())

	// Mid-move: the destination has been given a chunk, and the manifest still
	// names the old owners because until it is committed a reader has to find the
	// object where it was.
	destination := ""
	for _, id := range ownersOf(t, tc, key, len(m.Nodes)) {
		if !slices.Contains(m.Nodes, id) {
			destination = id
		}
	}
	if destination == "" {
		t.Fatalf("the join left placement %v unchanged, so no move is in flight", m.Nodes)
	}
	placeChunks(t, tc.nodes[destination], m, data)

	// No grace at all, so age cannot be what saves them.
	if st := collectEverywhere(t, tc, 0); st.Collected != 0 {
		t.Errorf("collected %d chunks a move is in the middle of delivering", st.Collected)
	}
	for _, ref := range m.Chunks {
		if !slices.Contains(chunksOn(t, tc.nodes[destination]), ref.ID) {
			t.Errorf("chunk %s was taken from %s, which the ring makes an owner", ref.ID, destination)
		}
	}
}

// placeChunks writes an object's chunks onto a node's disk the way a move does, so
// that a test can stand a cluster in the middle of one.
func placeChunks(t testing.TB, n *node, m object.Manifest, data []byte) {
	t.Helper()
	var at int64
	for _, ref := range m.Chunks {
		if err := n.s.WriteChunkVerified(ref.ID, bytes.NewReader(data[at:at+ref.Size]), ref.CRC, ref.Size); err != nil {
			t.Fatalf("place chunk %s on %s: %v", ref.ID, n.id, err)
		}
		at += ref.Size
	}
}
