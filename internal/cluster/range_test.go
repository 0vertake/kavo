package cluster_test

// Ranged reads and deletes. Both are S3 features, but both make claims about
// integrity and reclamation that only a fault can prove, so they are tested here
// where a chunk on disk can be corrupted or counted.

import (
	"bytes"
	"context"
	"testing"
)

// The claim: a range returns exactly the bytes asked for, wherever the window
// falls relative to the chunk boundaries it has to cross.
func TestARangeReturnsExactlyItsWindow(t *testing.T) {
	tc := newCluster(t, 4)
	key := "ranged/object"
	data := randBytes(3 * testChunkSize)
	_, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, data)

	tests := []struct {
		name   string
		off    int64
		length int64
	}{
		{name: "the whole object", off: 0, length: int64(len(data))},
		{name: "the first byte", off: 0, length: 1},
		{name: "the last byte", off: int64(len(data)) - 1, length: 1},
		{name: "inside one chunk", off: 10, length: 100},
		{name: "up to a chunk boundary", off: 0, length: testChunkSize},
		{name: "from a chunk boundary", off: testChunkSize, length: testChunkSize},
		{name: "across a boundary", off: testChunkSize - 5, length: 10},
		{name: "across two boundaries", off: 1, length: 2 * testChunkSize},
		{name: "nothing at all", off: 42, length: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got bytes.Buffer
			if err := outsider.c.StreamRange(context.Background(), m, &got, tt.off, tt.length); err != nil {
				t.Fatalf("range %d+%d: %v", tt.off, tt.length, err)
			}
			if want := data[tt.off : tt.off+tt.length]; !bytes.Equal(got.Bytes(), want) {
				t.Errorf("range %d+%d returned %d bytes, want %d", tt.off, tt.length, got.Len(), len(want))
			}
		})
	}
}

// Invariant 3 does not have a range exemption: a chunk's checksum covers the
// whole chunk, so serving a slice of a rotted chunk has to fail even when the rot
// is outside the window. Reading only the requested bytes would return them
// happily, which is silent corruption of the part of the object the client asked
// for — it has no way to know the chunk it came from is broken.
func TestARangedReadStillVerifiesTheWholeChunk(t *testing.T) {
	tc := newCluster(t, 4)
	key := "ranged/rotted"
	data := randBytes(2 * testChunkSize)
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, data)

	// Rot the middle of the first chunk on every copy, so no failover hides it.
	for _, o := range owners {
		o.rot(t, m.Chunks[0])
	}

	// A window at the very start of the chunk, ending long before the rot.
	var got bytes.Buffer
	err := outsider.c.StreamRange(context.Background(), m, &got, 0, 16)
	if err == nil {
		t.Fatalf("a range from a rotted chunk was served as %d clean bytes", got.Len())
	}
}

// A delete has to reclaim the chunks, not just the manifest. Leaving them is a
// store that never gets smaller: every overwrite and every delete would add to a
// pile nothing ever reads.
//
// The delete does not take them itself, and must not: a copied object shares its
// source's chunks, so this manifest's word alone is not enough to delete anything.
// Collection reads every manifest and then takes them, which is why the pass is here
// in the middle of what looks like a test about deleting.
func TestDeleteReclaimsTheChunks(t *testing.T) {
	tc := newCluster(t, 4)
	key := "doomed/object"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))

	for _, o := range owners {
		for _, ref := range m.Chunks {
			if !o.has(ref) {
				t.Fatalf("%s does not hold chunk %s before the delete; the rest of this test would prove nothing", o.id, ref.ID)
			}
		}
	}

	if err := outsider.c.Delete(context.Background(), key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	collectEverywhere(t, tc, 0)
	for _, o := range owners {
		for _, ref := range m.Chunks {
			if o.has(ref) {
				t.Errorf("%s still holds chunk %s of a deleted object", o.id, ref.ID)
			}
		}
	}
	// And the object itself is gone, which is the manifest being gone.
	if _, err := outsider.c.Resolve(context.Background(), key); err == nil {
		t.Error("a deleted object still resolves")
	}
}

// A delete must not take an unrelated object's chunks with it, which is the
// failure mode of reclaiming by anything other than the manifest's own ids.
func TestDeleteLeavesOtherObjectsAlone(t *testing.T) {
	tc := newCluster(t, 4)
	_, outsider := tc.owners(t, "doomed")
	mustPut(t, outsider, "doomed", randBytes(testChunkSize))
	keeper := randBytes(testChunkSize)
	mustPut(t, outsider, "keeper", keeper)

	if err := outsider.c.Delete(context.Background(), "doomed"); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, outsider, "keeper"); !bytes.Equal(got, keeper) {
		t.Error("deleting one object damaged another")
	}
}
