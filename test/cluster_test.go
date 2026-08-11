package test

import (
	"bytes"
	"net/http"
	"slices"
	"testing"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/ring"
)

// clusterSize is one more than N, so that every test has a node which owns
// nothing of the key under test and must therefore go over the network.
const clusterSize = cluster.Replicas + 1

// ownersOf recomputes placement from the ring rather than asking the cluster, so
// a bug that agreed with itself would still be caught.
func ownersOf(nodes []*node, key string) (owners []*node, outsider *node) {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.id
	}
	want := ring.New(ids, ring.DefaultVNodes).Owners(ring.PartitionFor(key), cluster.Replicas)
	for _, n := range nodes {
		if slices.Contains(want, n.id) {
			owners = append(owners, n)
		} else {
			outsider = n
		}
	}
	return owners, outsider
}

// Milestone 4's headline: an object written through any node is readable through
// every node, because its chunks were replicated to the owners of its partition
// and the manifest naming them is in etcd.
func TestObjectIsReadableFromEveryNode(t *testing.T) {
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), testChunkSize, clusterSize)

	const key = "shared/object"
	data := payloadFor(3)
	client := &http.Client{}
	if status, err := nodes[0].put(client, key, data); err != nil || status != http.StatusOK {
		t.Fatalf("PUT = (%d, %v), want (200, nil)", status, err)
	}

	for _, n := range nodes {
		status, got, err := n.get(client, key)
		if err != nil || status != http.StatusOK {
			t.Errorf("GET from %s = (%d, %v), want (200, nil)", n.id, status, err)
			continue
		}
		if !bytes.Equal(got, data) {
			t.Errorf("GET from %s returned %d bytes, want the original %d", n.id, len(got), len(data))
		}
	}
}

// The durability claim under a real crash: SIGKILL an owner and the object is
// still served, correct and complete, by nodes that never saw the client.
func TestObjectSurvivesOwnerCrash(t *testing.T) {
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), testChunkSize, clusterSize)

	const key = "survives/crash"
	data := payloadFor(4)
	client := &http.Client{}
	owners, outsider := ownersOf(nodes, key)
	if status, err := outsider.put(client, key, data); err != nil || status != http.StatusOK {
		t.Fatalf("PUT through the non-owner %s = (%d, %v), want (200, nil)", outsider.id, status, err)
	}

	owners[0].kill()

	for _, n := range append([]*node{outsider}, owners[1:]...) {
		status, got, err := n.get(client, key)
		if err != nil || status != http.StatusOK || !bytes.Equal(got, data) {
			t.Errorf("GET from %s after %s was killed = (%d, %d bytes, %v), want (200, %d, nil)",
				n.id, owners[0].id, status, len(got), err, len(data))
		}
	}
}

// A write that cannot reach W owners must be refused, not acknowledged and lost.
// The client has to be told, and told with a status it can retry on.
func TestWriteIsRefusedWhenTooFewOwnersSurvive(t *testing.T) {
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), testChunkSize, clusterSize)

	const key = "refused/write"
	owners, outsider := ownersOf(nodes, key)
	owners[1].kill()
	owners[2].kill()

	status, err := outsider.put(&http.Client{}, key, payloadFor(2))
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("PUT with %d of %d owners down = %d, want 503",
			cluster.Replicas-1, cluster.Replicas, status)
	}

	// Refusing is only half of it: nothing may be left that a reader can find.
	if status, _, err := outsider.get(&http.Client{}, key); err != nil || status != http.StatusNotFound {
		t.Fatalf("GET after a refused write = (%d, %v), want (404, nil)", status, err)
	}
}
