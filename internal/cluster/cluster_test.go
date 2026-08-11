package cluster_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/0vertake/kavo/internal/api"
	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/ring"
	"github.com/0vertake/kavo/internal/store"
)

// Small enough to make a handful of chunks per object, large enough that a chunk
// boundary is not the only thing under test.
const testChunkSize = 1024

type node struct {
	id      string
	root    string
	addr    string
	handler http.Handler
	srv     *httptest.Server
	revived *http.Server
	m       *meta.Store
	s       *store.Store
	c       *cluster.Coordinator
}

// loseEverything empties this node's disk, which is what a replaced drive looks
// like: the node is healthy and answering, it simply has nothing.
func (n *node) loseEverything(t testing.TB) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(n.root, "chunks")); err != nil {
		t.Fatalf("empty %s: %v", n.id, err)
	}
}

// kill takes the node off the network. Its chunks stay on disk, so this is a
// crashed or partitioned node rather than a lost disk.
func (n *node) kill() {
	if n.revived != nil {
		n.revived.Close()
		n.revived = nil
		return
	}
	n.srv.Close()
}

// revive brings a killed node back on the same address, which is what a restarted
// process is: same id, same disk, same place in the ring.
func (n *node) revive(t testing.TB) {
	t.Helper()
	l, err := net.Listen("tcp", n.addr)
	if err != nil {
		t.Fatalf("relisten on %s: %v", n.addr, err)
	}
	srv := &http.Server{Handler: n.handler}
	n.revived = srv
	go srv.Serve(l)
	t.Cleanup(func() { srv.Close() })
}

// has reports whether this node's disk holds the chunk.
func (n *node) has(ref object.ChunkRef) bool {
	_, err := os.Stat(filepath.Join(n.root, "chunks", ref.ID[:2], ref.ID))
	return err == nil
}

// loseChunks deletes this node's copies, which is what a replaced disk looks
// like: the node is healthy and answers, it simply has nothing.
func (n *node) loseChunks(t testing.TB, refs []object.ChunkRef) {
	t.Helper()
	for _, ref := range refs {
		if err := os.Remove(filepath.Join(n.root, "chunks", ref.ID[:2], ref.ID)); err != nil {
			t.Fatalf("remove chunk %s from %s: %v", ref.ID, n.id, err)
		}
	}
}

type testCluster struct {
	nodes map[string]*node
}

func newCluster(t testing.TB, n int) *testCluster {
	t.Helper()
	return newClusterChunked(t, n, testChunkSize)
}

// newClusterChunked starts n real nodes over real HTTP sharing one etcd prefix.
// Nothing here is faked: the point of these tests is what happens between
// processes.
func newClusterChunked(t testing.TB, n int, chunkSize int64) *testCluster {
	t.Helper()
	prefix := "/kavo-test/" + rand.Text()

	// Listeners come up first so that every node can be told every address, and
	// so all nodes derive the same ring.
	srvs := make([]*httptest.Server, n)
	ids := make([]string, n)
	peers := make(map[string]string, n)
	for i := range srvs {
		srvs[i] = httptest.NewUnstartedServer(nil)
		ids[i] = fmt.Sprintf("n%d", i+1)
		peers[ids[i]] = srvs[i].Listener.Addr().String()
	}

	tc := &testCluster{nodes: make(map[string]*node, n)}
	for i, id := range ids {
		root := t.TempDir()
		s, err := store.Open(root)
		if err != nil {
			t.Fatalf("store.Open: %v", err)
		}
		m, err := meta.Open([]string{meta.EndpointFromEnv()}, prefix)
		if err != nil {
			t.Fatalf("meta.Open (is etcd up? try `make etcd`): %v", err)
		}
		t.Cleanup(func() { m.Close() })

		c := cluster.New(id, peers[id], s, m, chunkSize)
		c.SetMembers(peers)
		handler := api.New(c, s)
		srvs[i].Config.Handler = handler
		srvs[i].Start()
		t.Cleanup(srvs[i].Close)

		tc.nodes[id] = &node{
			id:      id,
			root:    root,
			addr:    peers[id],
			handler: handler,
			srv:     srvs[i],
			m:       m,
			s:       s,
			c:       c,
		}
	}
	return tc
}

// owners returns the nodes that should hold key, and one node that should not.
// Reads and writes are driven from the outsider where possible, since any node
// coordinates any request and going through a non-owner exercises the network.
func (tc *testCluster) owners(t testing.TB, key string) (owners []*node, outsider *node) {
	t.Helper()
	return tc.ownersN(t, key, cluster.Replicas)
}

// ownersN is owners for a placement that is not three wide, which erasure coding
// needs: a k+m code spreads a chunk over k+m nodes.
func (tc *testCluster) ownersN(t testing.TB, key string, width int) (owners []*node, outsider *node) {
	t.Helper()
	ids := slices.Sorted(maps.Keys(tc.nodes))
	want := ring.New(ids, ring.DefaultVNodes).Owners(ring.PartitionFor(key), width)
	for _, id := range want {
		owners = append(owners, tc.nodes[id])
	}
	for _, id := range ids {
		if !slices.Contains(want, id) {
			return owners, tc.nodes[id]
		}
	}
	return owners, nil
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
}

func mustPut(t testing.TB, n *node, key string, data []byte) object.Manifest {
	t.Helper()
	m, err := n.c.Put(context.Background(), key, bytes.NewReader(data), cluster.PutOptions{})
	if err != nil {
		t.Fatalf("put %s via %s: %v", key, n.id, err)
	}
	return m
}

func mustGet(t testing.TB, n *node, key string) []byte {
	t.Helper()
	m, err := n.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatalf("resolve %s via %s: %v", key, n.id, err)
	}
	var got bytes.Buffer
	if err := n.c.Stream(context.Background(), m, &got); err != nil {
		t.Fatalf("stream %s via %s: %v", key, n.id, err)
	}
	return got.Bytes()
}

// The placement claim: a chunk goes to the N owners of its key's partition, and
// nowhere else. Extra copies would be wasted disk; missing ones would make the
// quorum a lie.
func TestChunksLandOnEveryOwnerAndNowhereElse(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "photos/cat.jpg"
	owners, outsider := tc.owners(t, key)

	m := mustPut(t, outsider, key, randBytes(3*testChunkSize+11))
	if len(m.Chunks) != 4 {
		t.Fatalf("got %d chunks, want 4", len(m.Chunks))
	}
	if got := slices.Sorted(slices.Values(m.Nodes)); !slices.Equal(got, sortedIDs(owners)) {
		t.Fatalf("manifest nodes = %v, want the ring's owners %v", got, sortedIDs(owners))
	}

	for _, ref := range m.Chunks {
		for _, o := range owners {
			if !o.has(ref) {
				t.Errorf("owner %s is missing chunk %s", o.id, ref.ID)
			}
		}
		if outsider.has(ref) {
			t.Errorf("non-owner %s holds chunk %s", outsider.id, ref.ID)
		}
	}
}

// Any node can serve any object: that is what makes the gateway symmetric.
func TestAnyNodeCoordinatesAnyObject(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "shared/object"
	data := randBytes(2*testChunkSize + 5)

	owners, outsider := tc.owners(t, key)
	mustPut(t, owners[0], key, data)

	for _, n := range append(slices.Clone(owners), outsider) {
		if got := mustGet(t, n, key); !bytes.Equal(got, data) {
			t.Errorf("node %s served %d bytes, want the original %d", n.id, len(got), len(data))
		}
	}
}

// W < N exists so that a write survives a node being down. It must be
// acknowledged with W copies and no more, since the third owner never got it.
func TestWriteSurvivesOneOwnerDown(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "written/while/degraded"
	owners, outsider := tc.owners(t, key)
	down := owners[2]
	down.kill()

	data := randBytes(2 * testChunkSize)
	m := mustPut(t, outsider, key, data)

	for _, ref := range m.Chunks {
		live := 0
		for _, o := range owners[:2] {
			if o.has(ref) {
				live++
			}
		}
		if live != cluster.WriteQuorum {
			t.Errorf("chunk %s is on %d live owners, want %d", ref.ID, live, cluster.WriteQuorum)
		}
		if down.has(ref) {
			t.Errorf("chunk %s reached the node that was down", ref.ID)
		}
	}
	if got := mustGet(t, outsider, key); !bytes.Equal(got, data) {
		t.Error("object read back after a degraded write differs from what was written")
	}
}

// Invariant 1 in its negative form: a write that cannot be made durable is
// refused, and refusing it must leave nothing behind that a reader could find.
func TestWriteBelowQuorumIsRefusedAndCommitsNothing(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "never/committed"
	owners, outsider := tc.owners(t, key)
	owners[1].kill()
	owners[2].kill()

	_, err := outsider.c.Put(context.Background(), key, bytes.NewReader(randBytes(2*testChunkSize)), cluster.PutOptions{})
	if !errors.Is(err, cluster.ErrQuorum) {
		t.Fatalf("Put error = %v, want cluster.ErrQuorum", err)
	}
	if _, err := outsider.c.Resolve(context.Background(), key); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("Resolve after a refused write = %v, want meta.ErrNotFound", err)
	}
}

// The read side of W < N: a chunk that is on two of three owners is still
// readable when either of the two is the one that survives.
func TestReadSurvivesEachOwnerFailingInTurn(t *testing.T) {
	const key = "read/while/degraded"
	data := randBytes(3 * testChunkSize)

	for i := range cluster.Replicas {
		t.Run(fmt.Sprint("owner", i, "down"), func(t *testing.T) {
			tc := newCluster(t, 5)
			owners, outsider := tc.owners(t, key)
			mustPut(t, outsider, key, data)

			owners[i].kill()
			if got := mustGet(t, outsider, key); !bytes.Equal(got, data) {
				t.Error("object differs from what was written")
			}
		})
	}
}

// With every owner gone the object is unreadable, and that has to surface as an
// error. A short body reported as complete would be silent corruption.
func TestReadWithAllOwnersDownFailsLoudly(t *testing.T) {
	tc := newCluster(t, 5)
	const key = "unreachable/object"
	owners, outsider := tc.owners(t, key)
	mustPut(t, outsider, key, randBytes(2*testChunkSize))
	for _, o := range owners {
		o.kill()
	}

	// The manifest is in etcd, so the object provably exists — the error must
	// say the data is unreachable, not that the object is absent.
	m, err := outsider.c.Resolve(context.Background(), key)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	var got bytes.Buffer
	if err := outsider.c.Stream(context.Background(), m, &got); err == nil {
		t.Fatal("Stream succeeded with every owner down, want an error")
	}
	if got.Len() == int(m.Size) {
		t.Fatal("Stream produced the whole object with every owner down")
	}
}

// A cluster smaller than the write quorum cannot promise W copies. It must then
// require every node it has, rather than silently acknowledging fewer.
func TestUndersizedClusterRequiresEveryNode(t *testing.T) {
	tc := newCluster(t, 2)
	const key = "small/cluster"
	ids := slices.Sorted(maps.Keys(tc.nodes))
	data := randBytes(testChunkSize + 1)

	first := tc.nodes[ids[0]]
	m := mustPut(t, first, key, data)
	if len(m.Nodes) != 2 {
		t.Fatalf("manifest nodes = %v, want both nodes", m.Nodes)
	}
	for _, ref := range m.Chunks {
		for _, id := range ids {
			if !tc.nodes[id].has(ref) {
				t.Errorf("node %s is missing chunk %s", id, ref.ID)
			}
		}
	}

	tc.nodes[ids[1]].kill()
	if _, err := first.c.Put(context.Background(), "second/write", bytes.NewReader(data), cluster.PutOptions{}); !errors.Is(err, cluster.ErrQuorum) {
		t.Fatalf("Put with one of two nodes down = %v, want cluster.ErrQuorum", err)
	}
}

// A node is alive by definition, so it belongs in its own ring whatever etcd
// currently says — including before its registration has landed.
func TestSelfIsAlwaysAMember(t *testing.T) {
	c := cluster.New("n9", "127.0.0.1:1", nil, nil, testChunkSize)
	if got := c.Members(); got["n9"] != "127.0.0.1:1" {
		t.Fatalf("members before any update = %v, want to contain n9", got)
	}

	c.SetMembers(map[string]string{"n1": "127.0.0.1:2"})
	got := c.Members()
	if got["n9"] != "127.0.0.1:1" || got["n1"] != "127.0.0.1:2" {
		t.Fatalf("members = %v, want both n9 and n1", got)
	}
}

// Streaming stays constant-memory across the network too: one chunk is buffered
// so it can be sent to several owners at once, and that is the whole footprint.
func TestReplicationBuffersOneChunkNotTheObject(t *testing.T) {
	if testing.Short() {
		t.Skip("moves 16 MB across five nodes")
	}
	const (
		key = "large/object"
		// Chunks are megabytes in production, and with 1 KB chunks this test
		// would spend all its time on 48,000 round trips instead of memory.
		chunkSize = 1 << 20
		size      = 16 << 20
		// Every byte crosses the network three times through HTTP buffers, so
		// the bound is generous; buffering the object itself would still break it.
		maxAlloc = 8 * size
	)
	tc := newClusterChunked(t, 5, chunkSize)
	_, outsider := tc.owners(t, key)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := outsider.c.Put(context.Background(), key, io.LimitReader(zeroReader{}, size), cluster.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	runtime.ReadMemStats(&after)

	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("allocated %d bytes replicating %d bytes to %d owners", allocated, size, cluster.Replicas)
	if allocated > maxAlloc {
		t.Errorf("allocated %d bytes replicating %d, want <= %d", allocated, size, maxAlloc)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { return len(p), nil }

func sortedIDs(nodes []*node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.id
	}
	slices.Sort(ids)
	return ids
}
