package cluster_test

// Multipart upload from the coordinator's side. The S3 tests cover what a client
// can ask for; this covers the one thing a client cannot arrange — the ring moving
// between two parts of the same upload.

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"testing"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/ring"
)

// A manifest names one set of nodes for every chunk it holds. If membership moves
// between two parts they are placed differently, and no single manifest can
// describe both — so the completion has to be refused. Committing the first part's
// placement over chunks that are somewhere else would produce an object that
// resolves to nodes which never held it: a GET that fails on data that is present,
// or worse, repair "restoring" copies of chunks it cannot find.
func TestACompletionIsRefusedWhenPlacementMovedMidUpload(t *testing.T) {
	tc := newCluster(t, 5)
	driver := tc.nodes["n1"]
	ctx := context.Background()

	// A key whose owners differ between the two memberships. Most keys are
	// unaffected by one node leaving — that is the point of consistent hashing —
	// so the test has to pick one that is.
	full := tc.without()
	fewer := tc.without("n5")
	key := movedKey(t, full, fewer)

	id, err := driver.c.CreateUpload(ctx, key, "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := driver.c.UploadPart(ctx, id, 1, bytes.NewReader(randBytes(testChunkSize)))
	if err != nil {
		t.Fatal(err)
	}

	tc.tellEveryone(fewer)

	second, err := driver.c.UploadPart(ctx, id, 2, bytes.NewReader(randBytes(1024)))
	if err != nil {
		t.Fatal(err)
	}

	_, err = driver.c.CompleteUpload(ctx, id, []cluster.CompletedPart{
		{Number: 1, ETag: first}, {Number: 2, ETag: second},
	})
	if err == nil {
		t.Fatal("completed an upload whose parts are on different nodes than the manifest would name")
	}
	if _, err := driver.c.Resolve(ctx, key); !errors.Is(err, meta.ErrNotFound) {
		t.Errorf("the object exists after a refused completion: %v", err)
	}
}

// An aborted upload's chunks are reclaimed. They are unreferenced the moment the
// upload is forgotten, so leaving them would mean an interrupted 5 GB upload costs
// 15 GB of disk until a human notices.
func TestAbortReclaimsThePartsChunks(t *testing.T) {
	tc := newCluster(t, 3)
	driver := tc.nodes["n1"]
	ctx := context.Background()
	const key = "aborted/object"

	id, err := driver.c.CreateUpload(ctx, key, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.c.UploadPart(ctx, id, 1, bytes.NewReader(randBytes(2*testChunkSize))); err != nil {
		t.Fatal(err)
	}

	parts, err := driver.m.Parts(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	m := parts[1]
	if len(m.Chunks) < 2 {
		t.Fatalf("the part has %d chunks; the test needs more than one", len(m.Chunks))
	}
	// Present first, or "gone afterwards" would pass on a part that was never
	// written.
	for _, node := range m.Nodes {
		for _, ref := range m.Chunks {
			if !tc.nodes[node].has(ref) {
				t.Fatalf("%s does not hold chunk %s before the abort", node, ref.ID)
			}
		}
	}

	if err := driver.c.AbortUpload(ctx, id); err != nil {
		t.Fatalf("abort: %v", err)
	}
	for _, node := range m.Nodes {
		for _, ref := range m.Chunks {
			if tc.nodes[node].has(ref) {
				t.Errorf("%s still holds chunk %s after the upload was aborted", node, ref.ID)
			}
		}
	}
}

// movedKey finds a key the two memberships place differently.
func movedKey(t testing.TB, before, after map[string]string) string {
	t.Helper()
	const width = 3
	ringBefore := ring.New(slices.Sorted(maps.Keys(before)), ring.DefaultVNodes)
	ringAfter := ring.New(slices.Sorted(maps.Keys(after)), ring.DefaultVNodes)
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"} {
		p := ring.PartitionFor(key)
		if !slices.Equal(ringBefore.Owners(p, width), ringAfter.Owners(p, width)) {
			return key
		}
	}
	t.Fatal("no key of the ones tried is placed differently by the two memberships")
	return ""
}
