package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/0vertake/kavo/internal/object"
)

// The record is what a slow write's chunks are protected by, so committing has to
// depend on it still being there. A writer that lost etcd for longer than its
// membership lease comes back to find the record dropped and its early chunks
// collectable: the one thing it must not then do is tell the client the object is
// stored.
func TestCommittingAfterTheRecordIsGoneIsRefused(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	m := object.Manifest{Size: 1, Nodes: []string{"n1"}}

	if err := s.MarkWriting(ctx, "WRITE1", "n1"); err != nil {
		t.Fatal(err)
	}
	if err := s.CommitWhileWriting(ctx, "b/slow.bin", m, "WRITE1"); err != nil {
		t.Fatalf("commit while the record was live: %v", err)
	}

	// As the sweep would, having decided the node coordinating it was gone.
	if err := s.DoneWriting(ctx, "WRITE1"); err != nil {
		t.Fatal(err)
	}
	err := s.CommitWhileWriting(ctx, "b/slow2.bin", m, "WRITE1")
	if !errors.Is(err, ErrNotWriting) {
		t.Errorf("commit after the record was dropped: %v, want ErrNotWriting", err)
	}
	if _, err := s.Get(ctx, "b/slow2.bin"); !errors.Is(err, ErrNotFound) {
		t.Errorf("the refused commit left a manifest behind: %v", err)
	}
}

func TestWritingListsWhatIsInFlight(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for _, w := range []struct{ id, node string }{{"WRITE1", "n1"}, {"WRITE2", "n2"}} {
		if err := s.MarkWriting(ctx, w.id, w.node); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DoneWriting(ctx, "WRITE1"); err != nil {
		t.Fatal(err)
	}

	writes, err := s.Writing(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 || writes["WRITE2"] != "n2" {
		t.Errorf("writes in flight: %v, want only WRITE2 on n2", writes)
	}
}
