package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/object"
)

// rot flips a bit in this node's copy on disk, without touching its length. This
// is what a bad sector or a lying drive looks like: the file is there, it is the
// right size, and one byte is wrong.
func (n *node) rot(t testing.TB, ref object.ChunkRef) {
	t.Helper()
	path := filepath.Join(n.root, "chunks", ref.ID[:2], ref.ID)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read chunk %s on %s: %v", ref.ID, n.id, err)
	}
	data[len(data)/2] ^= 0x01
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("rot chunk %s on %s: %v", ref.ID, n.id, err)
	}
}

func (n *node) chunkBytes(t testing.TB, ref object.ChunkRef) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(n.root, "chunks", ref.ID[:2], ref.ID))
	if err != nil {
		t.Fatalf("read chunk %s on %s: %v", ref.ID, n.id, err)
	}
	return data
}

func mustScrub(t testing.TB, n *node, rate int64) cluster.ScrubStats {
	t.Helper()
	st, err := n.c.Scrub(context.Background(), rate)
	if err != nil {
		t.Fatalf("scrub via %s: %v", n.id, err)
	}
	return st
}

// The point of scrubbing: rot is found by looking, not by waiting for a client to
// trip over it, and the bad copy is replaced from a peer.
func TestScrubReplacesRottedCopies(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "rotted/object"
	owners, outsider := tc.owners(t, key)
	data := randBytes(3 * testChunkSize)
	m := mustPut(t, outsider, key, data)

	victim, rotted := owners[1], m.Chunks[1]
	want := victim.chunkBytes(t, rotted)
	victim.rot(t, rotted)
	if bytes.Equal(victim.chunkBytes(t, rotted), want) {
		t.Fatal("rot did not change the chunk on disk")
	}

	st := mustScrub(t, victim, 0)
	if st.Rotted != 1 || st.Rebuilt != 1 {
		t.Fatalf("scrub found %d rotted and rebuilt %d, want 1 and 1", st.Rotted, st.Rebuilt)
	}
	if got := victim.chunkBytes(t, rotted); !bytes.Equal(got, want) {
		t.Error("the rebuilt chunk on disk is not the original data")
	}
	if got := mustGet(t, victim, key); !bytes.Equal(got, data) {
		t.Error("object read from the scrubbed node differs from what was written")
	}
}

// A node scrubs its own disk, since it is the only node that can read it. A
// scrub run anywhere else must not claim to have checked these bytes.
func TestScrubOnlyChecksTheNodesOwnCopies(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "someone/elses/rot"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(2*testChunkSize))
	owners[1].rot(t, m.Chunks[0])

	// The outsider holds none of these chunks, so it has nothing to verify.
	if st := mustScrub(t, outsider, 0); st.CopiesRead != 0 || st.Rotted != 0 {
		t.Errorf("a node holding no copies read %d and found %d rotted, want none",
			st.CopiesRead, st.Rotted)
	}
	// The owner that is not rotted verifies its own copies and finds them good.
	if st := mustScrub(t, owners[0], 0); st.CopiesRead != len(m.Chunks) || st.Rotted != 0 {
		t.Errorf("a healthy owner read %d copies and found %d rotted, want %d and 0",
			st.CopiesRead, st.Rotted, len(m.Chunks))
	}
	// And the rot is still there, because only its own node can find it.
	if st := mustScrub(t, owners[1], 0); st.Rotted != 1 {
		t.Errorf("the rotted node found %d rotted copies, want 1", st.Rotted)
	}
}

// Scrubbing runs forever over every byte a node stores, so a pass over healthy
// data must read and report, and change nothing.
func TestScrubLeavesHealthyDataAlone(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "healthy/bytes"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(3*testChunkSize))

	before := map[string][]byte{}
	for _, ref := range m.Chunks {
		before[ref.ID] = owners[0].chunkBytes(t, ref)
	}

	st := mustScrub(t, owners[0], 0)
	if st.Rotted != 0 || st.Rebuilt != 0 {
		t.Errorf("found %d rotted and rebuilt %d on healthy data", st.Rotted, st.Rebuilt)
	}
	if st.CopiesRead != len(m.Chunks) || st.BytesRead != m.Size {
		t.Errorf("read %d copies and %d bytes, want %d and %d",
			st.CopiesRead, st.BytesRead, len(m.Chunks), m.Size)
	}
	for _, ref := range m.Chunks {
		if !bytes.Equal(owners[0].chunkBytes(t, ref), before[ref.ID]) {
			t.Errorf("chunk %s changed during a scrub of healthy data", ref.ID)
		}
	}
}

// Rot in every copy is unrecoverable, and that has to be said out loud: it is the
// one outcome the durability invariants exist to make visible.
func TestScrubReportsRotItCannotRepair(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "rotted/everywhere"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(testChunkSize))
	for _, o := range owners {
		o.rot(t, m.Chunks[0])
	}

	st, err := owners[0].c.Scrub(context.Background(), 0)
	if !errors.Is(err, cluster.ErrRotUnrecovered) {
		t.Fatalf("scrub error = %v, want cluster.ErrRotUnrecovered", err)
	}
	if st.Rotted != 1 || st.Unrecovered != 1 || st.Rebuilt != 0 {
		t.Errorf("scrub reported rotted=%d unrecovered=%d rebuilt=%d, want 1, 1, 0",
			st.Rotted, st.Unrecovered, st.Rebuilt)
	}

	// A reader must still be refused rather than served the corrupt bytes.
	var got bytes.Buffer
	manifest, err := outsider.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := outsider.c.Stream(context.Background(), manifest, &got); err == nil {
		t.Fatal("read succeeded with every copy rotted, want an error")
	}
}

// Scrubbing reads every byte a node holds, so it competes with clients for the
// same disk. The rate limit is what keeps a sweep from becoming an outage.
func TestScrubPacesItselfToTheGivenRate(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "paced/scrub"
	owners, outsider := tc.owners(t, key)
	m := mustPut(t, outsider, key, randBytes(8*testChunkSize))

	rate := m.Size // one second's worth of scrubbing
	start := time.Now()
	st := mustScrub(t, owners[0], rate)
	elapsed := time.Since(start)

	if st.BytesRead != m.Size {
		t.Fatalf("read %d bytes, want %d", st.BytesRead, m.Size)
	}
	want := time.Duration(float64(m.Size)/float64(rate)*float64(time.Second)) * 8 / 10
	if elapsed < want {
		t.Errorf("read %d bytes at %d B/s in %v, too fast to have been paced (want >= %v)",
			st.BytesRead, rate, elapsed, want)
	}
	t.Logf("verified %d bytes at %d B/s in %v", st.BytesRead, rate, elapsed)
}
