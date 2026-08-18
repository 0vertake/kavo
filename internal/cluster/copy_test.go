package cluster_test

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/object"
)

// A server-side copy moves no data: both keys name the same chunks, so a copy of a
// terabyte costs one etcd write.
func TestACopySharesTheSourcesChunks(t *testing.T) {
	tc := newCluster(t, 5)
	const from, to = "original.bin", "duplicate.bin"
	data := randBytes(2 * testChunkSize)
	m := mustPut(t, tc.nodes["n1"], from, data)

	copied, err := tc.nodes["n1"].c.Copy(context.Background(), from, to, cluster.CopyOptions{})
	if err != nil {
		t.Fatalf("copy: %v", err)
	}
	if !slices.Equal(chunkIDs(copied), chunkIDs(m)) {
		t.Errorf("the copy names chunks %v, want the source's %v", chunkIDs(copied), chunkIDs(m))
	}
	if got := mustGet(t, tc.nodes["n2"], to); !bytes.Equal(got, data) {
		t.Error("the copy does not read back as the source's bytes")
	}
	if copied.ETag != m.ETag {
		t.Errorf("the copy's etag is %q, want the source's %q", copied.ETag, m.ETag)
	}
}

// Deleting one of two names for the same chunks must not take the chunks. The
// delete path does not drop them at all — collection does, once it has read every
// manifest and found the other name.
func TestDeletingTheSourceLeavesTheCopyReadable(t *testing.T) {
	tc := newCluster(t, 5)
	const from, to = "original.bin", "duplicate.bin"
	data := randBytes(2 * testChunkSize)
	mustPut(t, tc.nodes["n1"], from, data)
	if _, err := tc.nodes["n1"].c.Copy(context.Background(), from, to, cluster.CopyOptions{}); err != nil {
		t.Fatalf("copy: %v", err)
	}

	if err := tc.nodes["n1"].c.Delete(context.Background(), from); err != nil {
		t.Fatalf("delete the source: %v", err)
	}
	collectEverywhere(t, tc, 0)

	if got := mustGet(t, tc.nodes["n1"], to); !bytes.Equal(got, data) {
		t.Error("the copy stopped reading once the source was deleted")
	}
}

// The interesting one, because it is not the copy that is at risk.
//
// A copy keeps the source's placement, and its own key belongs to a different
// partition, so rebalancing sees it as misplaced and moves it. A move that dropped
// the copies it moved away from — as one did, for as long as no two manifests could
// name the same chunk — would be dropping the chunks the *source* is still pointing
// at, and the object nobody touched would be the one that broke.
func TestMovingACopyLeavesTheSourceReadable(t *testing.T) {
	// Seven nodes, because the destination's owners have to be able to avoid all
	// three of the source's for the source to be left with nothing.
	tc := newCluster(t, 7)
	data := randBytes(2 * testChunkSize)

	from := "original.bin"
	m := mustPut(t, tc.nodes["n1"], from, data)
	to := keyOwnedElsewhere(t, tc, m.Nodes)
	if _, err := tc.nodes["n1"].c.Copy(context.Background(), from, to, cluster.CopyOptions{}); err != nil {
		t.Fatalf("copy: %v", err)
	}

	// Every node rebalances, since the mover is whichever node the ring makes the
	// destination's first owner.
	for _, n := range tc.nodes {
		mustRebalance(t, n, 0)
	}

	if got := mustGet(t, tc.nodes["n1"], from); !bytes.Equal(got, data) {
		t.Error("the source stopped reading after its copy was moved away")
	}
	if got := mustGet(t, tc.nodes["n1"], to); !bytes.Equal(got, data) {
		t.Error("the copy stopped reading after it was moved")
	}
}

// keyOwnedElsewhere finds a key whose partition the ring hands to nodes that hold
// none of the source's copies, which is what makes a copy of it misplaced from the
// moment it exists.
func keyOwnedElsewhere(t testing.TB, tc *testCluster, source []string) string {
	t.Helper()
	for i := range 400 {
		key := keyFor(i)
		owners := ownersOf(t, tc, key, len(source))
		if len(owners) != len(source) {
			continue
		}
		shared := false
		for _, id := range owners {
			shared = shared || slices.Contains(source, id)
		}
		if !shared {
			return key
		}
	}
	t.Fatalf("no key in 400 is owned entirely away from %v", source)
	return ""
}

func chunkIDs(m object.Manifest) []string {
	ids := make([]string, len(m.Chunks))
	for i, ref := range m.Chunks {
		ids[i] = ref.ID
	}
	return ids
}

// A copied part is read from the source and written through the ordinary write path,
// so the question a fault asks is what happens when the source stops being readable
// halfway. The answer has to be that the part does not exist: the client's only
// evidence a copy worked is the etag it gets back, and an etag for the first half of
// a range is an etag the client will happily complete an upload with — assembling an
// object that is not the copy it asked for and never learning otherwise.
//
// Made deterministic by taking the source's second chunk away from every node, which
// is a source that reads for exactly one chunk and then cannot.
func TestACopiedPartIsAllOrNothingWhenItsSourceStopsReading(t *testing.T) {
	tc := newCluster(t, 5)
	const source = "copy/source"
	owners, outsider := tc.owners(t, source)
	data := randBytes(3 * testChunkSize)
	m := mustPut(t, outsider, source, data)
	if len(m.Chunks) < 3 {
		t.Fatalf("the source has %d chunks, want at least 3 for a mid-stream failure", len(m.Chunks))
	}

	id, err := outsider.c.CreateUpload(context.Background(), "copy/assembled", "", nil)
	if err != nil {
		t.Fatalf("create upload: %v", err)
	}

	// Every copy of the middle chunk goes, so the read cannot be served from
	// anywhere. The first chunk stays, so the copy gets underway before it fails.
	for _, n := range append([]*node{outsider}, owners...) {
		if n.has(m.Chunks[1]) {
			n.loseChunks(t, m.Chunks[1:2])
		}
	}

	if _, err := outsider.c.CopyPart(context.Background(), id, 1, m, 0, m.Size, cluster.PutOptions{}); err == nil {
		t.Fatal("copying a part from a source that stops reading succeeded, so a client holds an etag for half a range")
	}

	// And nothing was recorded under that part number, so a completion cannot
	// assemble an object out of the half that was readable.
	parts, err := outsider.c.Parts(context.Background(), id)
	if err != nil {
		t.Fatalf("parts: %v", err)
	}
	if len(parts) != 0 {
		t.Errorf("the failed copy left part %d behind (%d bytes)", parts[0].Number, parts[0].Size)
	}
}
