package cluster_test

// Reading an object whose owners are no longer in the membership.
//
// This is invariant 1's other half. "No acknowledged write is lost" is not only
// about bytes surviving on disk: an object nothing can read is lost as far as the
// client is concerned, and a lease that lapsed for a second is not a reason to
// stop serving data that is sitting right there.

import (
	"bytes"
	"context"
	"testing"
)

// A node whose lease lapses drops out of the membership without losing anything:
// same process, same disk, still answering. Reads have to keep working, because
// under load — which is exactly when leases lapse — every owner of some object is
// eventually in that state at the same moment.
//
// Placement is a different matter and deliberately unchanged: a write still goes
// only to live nodes, since acknowledging a write to a node the cluster has given
// up on would promise durability nobody is maintaining.
func TestReadsSurviveOwnersLeavingTheMembership(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "readable/through/a/blip"
	data := randBytes(3 * testChunkSize)

	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, data)

	// Every owner's lease lapses. Nothing has moved: the chunks are on the same
	// disks and the processes are still serving.
	gone := make([]string, len(owners))
	for i, o := range owners {
		gone[i] = o.id
	}
	tc.tellEveryone(tc.without(gone...))

	var got bytes.Buffer
	if err := outsider.c.Stream(context.Background(), m, &got); err != nil {
		t.Fatalf("reading an object whose owners left the membership: %v", err)
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Errorf("read %d bytes, want %d", got.Len(), len(data))
	}

	// And through an owner, which is the path that reads its own disk first.
	got.Reset()
	if err := owners[0].c.Stream(context.Background(), m, &got); err != nil {
		t.Fatalf("an owner could not read its own object: %v", err)
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Errorf("owner read %d bytes, want %d", got.Len(), len(data))
	}
}

// A node that never was a member is not somewhere to guess at. The fallback is
// last known addresses, not a scan of the network: an unknown node has no address
// to try, and a read that names one must fail rather than hang.
func TestAnUnknownOwnerIsNotGuessedAt(t *testing.T) {
	tc := newCluster(t, 4)
	const key = "named/a/stranger"
	_, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(testChunkSize))

	// A manifest naming nodes this cluster has never heard of, which is what a
	// corrupt manifest or a misconfigured cluster looks like.
	m.Nodes = []string{"nowhere-1", "nowhere-2", "nowhere-3"}
	if err := outsider.c.Stream(context.Background(), m, &bytes.Buffer{}); err == nil {
		t.Error("a read from nodes that were never members reported success")
	}
}
