package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/ec"
)

// 4+2 on six nodes, rather than the 6+3 default, because a shard is pinned to a
// node by its position: a k+m code needs k+m nodes and the test cluster has six.
var testScheme = ec.Scheme{Data: 4, Parity: 2}

// newECCluster is the same cluster with new writes erasure-coded.
func newECCluster(t testing.TB, nodes int, chunkSize int64) *testCluster {
	t.Helper()
	tc := newClusterChunked(t, nodes, chunkSize)
	for _, n := range tc.nodes {
		if err := n.c.EncodeWith(testScheme); err != nil {
			t.Fatalf("EncodeWith(%s): %v", testScheme, err)
		}
	}
	return tc
}

// holds reports which of these ids this node actually has on disk, which is how a
// test asks what is stored rather than what the manifest claims.
func (n *node) holds(t testing.TB, ids ...string) []string {
	t.Helper()
	var held []string
	for _, id := range ids {
		ok, err := n.s.HasChunk(id)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			held = append(held, id)
		}
	}
	return held
}

// The placement claim for coded data: every chunk becomes Data+Parity shards, one
// per owner, and shard i is on owner i. Position is not a preference here — the
// decoder identifies a shard by where it is in the list.
func TestEachShardLandsOnItsOwnOwner(t *testing.T) {
	tc := newECCluster(t, 6, testChunkSize)
	const key = "coded/object"
	m := mustPut(t, tc.nodes["n1"], key, randBytes(3*testChunkSize))

	if m.Coding != testScheme {
		t.Fatalf("manifest coding = %v, want %s", m.Coding, testScheme)
	}
	if len(m.Nodes) != testScheme.Shards() {
		t.Fatalf("object placed on %d nodes, want %d", len(m.Nodes), testScheme.Shards())
	}
	for _, ref := range m.Chunks {
		if len(ref.Shards) != testScheme.Shards() {
			t.Fatalf("chunk %s has %d shard checksums, want %d", ref.ID, len(ref.Shards), testScheme.Shards())
		}
		for i, id := range m.Nodes {
			owner := tc.nodes[id]
			if got := owner.holds(t, ref.ShardID(i)); len(got) != 1 {
				t.Errorf("%s does not hold shard %d of chunk %s", id, i, ref.ID)
			}
			// And holds no other shard of it: a node with two shards of one
			// chunk is a node whose loss costs two.
			for j := range ref.Shards {
				if j == i {
					continue
				}
				if got := owner.holds(t, ref.ShardID(j)); len(got) != 0 {
					t.Errorf("%s holds shard %d as well as %d of chunk %s", id, j, i, ref.ID)
				}
			}
		}
	}
}

// The point of the whole mode: the object reads back with any Parity nodes gone,
// and the data is recomputed rather than found.
func TestObjectSurvivesEveryCombinationOfParityLosses(t *testing.T) {
	tc := newECCluster(t, 6, testChunkSize)
	const key = "coded/survives"
	data := randBytes(2*testChunkSize + 101)
	m := mustPut(t, tc.nodes["n1"], key, data)

	reader := tc.nodes[m.Nodes[0]]
	for _, lost := range parityCombinations(testScheme) {
		var restore []func()
		for _, i := range lost {
			for _, ref := range m.Chunks {
				restore = append(restore, tc.nodes[m.Nodes[i]].hide(t, ref.ShardID(i)))
			}
		}
		got := mustGet(t, reader, key)
		if !bytes.Equal(got, data) {
			t.Fatalf("with shards %v gone, read %d bytes, want %d", lost, len(got), len(data))
		}
		for _, undo := range restore {
			undo()
		}
	}
}

// hide makes one chunk or shard unreadable and returns the undo, so a test can
// walk every combination of losses without restarting anything. A missing file is
// what a peer that has lost a shard looks like to a reader.
func (n *node) hide(t testing.TB, id string) func() {
	t.Helper()
	path := filepath.Join(n.root, "chunks", id[:2], id)
	aside := path + ".hidden"
	if err := os.Rename(path, aside); err != nil {
		t.Fatalf("hide %s on %s: %v", id, n.id, err)
	}
	return func() {
		if err := os.Rename(aside, path); err != nil {
			t.Fatalf("restore %s on %s: %v", id, n.id, err)
		}
	}
}

// One loss past the parity count is data loss, and a read must fail rather than
// return a chunk of zeros where the missing shards were.
func TestReadFailsRatherThanInventDataPastTheParityCount(t *testing.T) {
	tc := newECCluster(t, 6, testChunkSize)
	const key = "coded/toomanygone"
	data := randBytes(testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)

	for i := range testScheme.Parity + 1 {
		tc.nodes[m.Nodes[i]].srv.Close()
	}
	reader := tc.nodes[m.Nodes[len(m.Nodes)-1]]
	var got bytes.Buffer
	err := reader.c.Stream(context.Background(), m, &got)
	if err == nil {
		t.Fatal("read succeeded with more shards gone than the code tolerates")
	}
	if got.Len() == len(data) && bytes.Equal(got.Bytes(), data) {
		t.Fatal("read returned the whole object it should not have been able to rebuild")
	}
}

// Shards verify one at a time, but a decode combines them, and Reed–Solomon will
// happily solve equations it was given in the wrong order. The assembled chunk is
// checked against the manifest for that reason: a manifest that disagrees with
// the data it names must produce an error, never bytes.
func TestReconstructedChunkIsCheckedAgainstTheManifest(t *testing.T) {
	tc := newECCluster(t, 6, testChunkSize)
	const key = "coded/liar"
	data := randBytes(testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)

	// Every shard still matches its own checksum; only the chunk's does not.
	m.Chunks[0].CRC ^= 0xFFFFFFFF
	reader := tc.nodes[m.Nodes[0]]
	if err := reader.m.Commit(context.Background(), key, m); err != nil {
		t.Fatal(err)
	}

	stale, err := reader.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := reader.c.Stream(context.Background(), stale, &got); err == nil {
		t.Fatal("a chunk that does not match its manifest was served as a successful read")
	}
}

// A write that cannot place Data+1 shards must place nothing readable: the object
// does not exist rather than existing in a state one loss from unreadable.
func TestWriteIsRefusedWhenTooFewShardsCanBePlaced(t *testing.T) {
	tc := newECCluster(t, 6, testChunkSize)
	const key = "coded/refused"

	// Down to Data shards' worth of nodes: enough to reconstruct today, not
	// enough to survive anything happening tomorrow.
	owners, _ := tc.ownersN(t, key, testScheme.Shards())
	for _, n := range owners[:testScheme.Parity] {
		n.srv.Close()
	}
	if _, err := owners[len(owners)-1].c.Put(context.Background(), key, bytes.NewReader(randBytes(1024)), cluster.PutOptions{}); !errors.Is(err, cluster.ErrQuorum) {
		t.Fatalf("Put with %d owners down = %v, want ErrQuorum", testScheme.Parity, err)
	}
	if _, err := owners[len(owners)-1].c.Resolve(context.Background(), key); err == nil {
		t.Fatal("a refused write committed a manifest anyway")
	}
}

// A cluster smaller than the code cannot place shard k+1 anywhere, and must say
// so instead of writing an object it could never rebuild.
func TestCodeWiderThanTheClusterIsRefused(t *testing.T) {
	tc := newClusterChunked(t, 3, testChunkSize)
	for _, n := range tc.nodes {
		if err := n.c.EncodeWith(ec.Scheme{Data: 4, Parity: 2}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tc.nodes["n1"].c.Put(context.Background(), "coded/toowide", bytes.NewReader(randBytes(1024)), cluster.PutOptions{}); err == nil {
		t.Fatal("a 4+2 write succeeded on a three-node cluster")
	}
}

// Repair for coded data has to rebuild, not copy: there is no other copy of
// shard 3 anywhere, only the arithmetic that produces it.
func TestRepairRebuildsLostShards(t *testing.T) {
	tc := newECCluster(t, 6, testChunkSize)
	key, repairer := codedKeyOwnedBy(t, tc)
	data := randBytes(2 * testChunkSize)
	m := mustPut(t, repairer, key, data)

	// Two nodes lose everything, which is exactly the code's tolerance.
	victims := []*node{tc.nodes[m.Nodes[1]], tc.nodes[m.Nodes[2]]}
	for _, v := range victims {
		v.loseEverything(t)
	}

	st := mustRepair(t, repairer, 0)
	if want := len(m.Chunks) * len(victims); st.Restored != want {
		t.Fatalf("repair restored %d shards, want %d", st.Restored, want)
	}
	for i, v := range victims {
		for _, ref := range m.Chunks {
			id := ref.ShardID(slices.Index(m.Nodes, v.id))
			if got := v.holds(t, id); len(got) != 1 {
				t.Errorf("shard %d of chunk %s was not rebuilt on %s", i+1, ref.ID, v.id)
			}
		}
	}

	// Rebuilt correctly, not merely present: the object reads back byte-identical
	// with the surviving nodes' shards no longer sufficient on their own.
	if got := mustGet(t, repairer, key); !bytes.Equal(got, data) {
		t.Fatalf("after repair, read %d bytes, want %d", len(got), len(data))
	}
}

// Rot in a shard cannot be replaced from a peer — no peer has that shard — so the
// scrubber has to recompute it from the others.
func TestScrubRebuildsRottedShards(t *testing.T) {
	tc := newECCluster(t, 6, testChunkSize)
	const key = "coded/rot"
	data := randBytes(testChunkSize)
	m := mustPut(t, tc.nodes["n1"], key, data)

	victim := tc.nodes[m.Nodes[2]]
	ref := m.Chunks[0]
	id := ref.ShardID(2)
	path := filepath.Join(victim.root, "chunks", id[:2], id)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rotted := bytes.Clone(good)
	rotted[len(rotted)/2] ^= 0xFF
	if err := os.WriteFile(path, rotted, 0o644); err != nil {
		t.Fatal(err)
	}

	st, err := victim.c.Scrub(context.Background(), 0)
	if err != nil {
		t.Fatalf("Scrub: %v", err)
	}
	if st.Rotted != 1 || st.Rebuilt != 1 {
		t.Fatalf("scrub found %d rotted and rebuilt %d, want 1 and 1", st.Rotted, st.Rebuilt)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, good) {
		t.Error("the rebuilt shard does not match the original bytes")
	}
	if got := mustGet(t, victim, key); !bytes.Equal(got, data) {
		t.Error("object does not read back after its rotted shard was rebuilt")
	}
}

// Replicated and coded objects have to coexist, because the mode is a property of
// each object and clusters change their default.
func TestBothModesReadBackFromTheSameCluster(t *testing.T) {
	tc := newClusterChunked(t, 6, testChunkSize)
	replicated := randBytes(2 * testChunkSize)
	mustPut(t, tc.nodes["n1"], "mixed/replicated", replicated)

	for _, n := range tc.nodes {
		if err := n.c.EncodeWith(testScheme); err != nil {
			t.Fatal(err)
		}
	}
	coded := randBytes(2 * testChunkSize)
	mustPut(t, tc.nodes["n1"], "mixed/coded", coded)

	for _, n := range tc.nodes {
		if got := mustGet(t, n, "mixed/replicated"); !bytes.Equal(got, replicated) {
			t.Errorf("%s read the replicated object wrong after switching modes", n.id)
		}
		if got := mustGet(t, n, "mixed/coded"); !bytes.Equal(got, coded) {
			t.Errorf("%s read the coded object wrong", n.id)
		}
	}
}

// Coding costs 1.5x the bytes for the same tolerance as three copies at 3x, which
// is the entire reason the mode exists. Asserted on what is on disk.
func TestCodedObjectStoresLessThanReplication(t *testing.T) {
	size := int64(4 * testChunkSize)

	replicated := newClusterChunked(t, 6, testChunkSize)
	mustPut(t, replicated.nodes["n1"], "cost/replicated", randBytes(int(size)))

	coded := newECCluster(t, 6, testChunkSize)
	mustPut(t, coded.nodes["n1"], "cost/coded", randBytes(int(size)))

	rep, cod := replicated.bytesOnDisk(t), coded.bytesOnDisk(t)
	wantRep := float64(size) * cluster.Replicas
	wantCod := float64(size) * float64(testScheme.Shards()) / float64(testScheme.Data)
	if !within(float64(rep), wantRep, 0.05) {
		t.Errorf("replication stored %d bytes for %d of object, want ~%.0f", rep, size, wantRep)
	}
	if !within(float64(cod), wantCod, 0.05) {
		t.Errorf("%s stored %d bytes for %d of object, want ~%.0f", testScheme, cod, size, wantCod)
	}
	t.Logf("%d bytes of object: replication stored %d (%.2fx), %s stored %d (%.2fx)",
		size, rep, float64(rep)/float64(size), testScheme, cod, float64(cod)/float64(size))
}

func within(got, want, tolerance float64) bool {
	return got >= want*(1-tolerance) && got <= want*(1+tolerance)
}

// bytesOnDisk totals what the whole cluster is storing, which is the only honest
// way to compare the cost of two redundancy schemes.
func (tc *testCluster) bytesOnDisk(t testing.TB) int64 {
	t.Helper()
	var total int64
	for _, n := range tc.nodes {
		err := filepath.WalkDir(filepath.Join(n.root, "chunks"), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	return total
}

// codedKeyOwnedBy finds a key whose partition this cluster's repairing node owns
// first, since only the first owner repairs.
func codedKeyOwnedBy(t testing.TB, tc *testCluster) (string, *node) {
	t.Helper()
	key := "coded/heal"
	owners, _ := tc.ownersN(t, key, testScheme.Shards())
	if len(owners) != testScheme.Shards() {
		t.Fatalf("key %q has %d owners, want %d", key, len(owners), testScheme.Shards())
	}
	return key, owners[0]
}

// parityCombinations lists every way to lose exactly Parity nodes.
func parityCombinations(s ec.Scheme) [][]int {
	var out [][]int
	var walk func(start int, pick []int)
	walk = func(start int, pick []int) {
		if len(pick) == s.Parity {
			out = append(out, append([]int(nil), pick...))
			return
		}
		for i := start; i < s.Shards(); i++ {
			walk(i+1, append(pick, i))
		}
	}
	walk(0, nil)
	return out
}
