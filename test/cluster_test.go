package test

import (
	"bytes"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

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

// A write that cannot reach W owners must be refused rather than acknowledged and
// lost — and then, once the cluster has noticed the nodes are gone, the same
// write must succeed on the smaller ring. Refusing forever would make one dead
// node enough to stop accepting data for the keys it owned.
func TestWritesAreRefusedUntilMembershipCatchesUp(t *testing.T) {
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), testChunkSize, clusterSize)

	const key = "refused/then/accepted"
	data := payloadFor(2)
	client := &http.Client{}
	owners, outsider := ownersOf(nodes, key)
	owners[1].kill()
	owners[2].kill()

	// Immediately: the leases have not expired, so the coordinator still counts
	// the dead nodes as owners and cannot reach W.
	status, err := outsider.put(client, key, data)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if status != http.StatusServiceUnavailable {
		t.Fatalf("PUT with %d of %d owners just killed = %d, want 503",
			cluster.Replicas-1, cluster.Replicas, status)
	}
	// Refusing is only half of it: nothing may be left that a reader can find.
	if status, _, err := outsider.get(client, key); err != nil || status != http.StatusNotFound {
		t.Fatalf("GET after a refused write = (%d, %v), want (404, nil)", status, err)
	}

	// Once the leases expire the ring holds only live nodes, and the write has
	// somewhere to go again.
	outsider.waitForMembers(clusterSize - 2)
	if status, err := outsider.put(client, key, data); err != nil || status != http.StatusOK {
		t.Fatalf("PUT after membership converged = (%d, %v), want (200, nil)", status, err)
	}
	status, got, err := outsider.get(client, key)
	if err != nil || status != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET = (%d, %d bytes, %v), want (200, %d, nil)", status, len(got), err, len(data))
	}
}

// Failure detection has to be bounded, or "the cluster notices" is not a
// guarantee. The bound is the lease: a node that stops renewing is gone from
// every other node's view within it, plus etcd's expiry sweep.
func TestFailureIsDetectedWithinTheLease(t *testing.T) {
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), testChunkSize, clusterSize)

	dead, survivors := nodes[0], nodes[1:]
	start := time.Now()
	dead.kill()

	const slack = 3 * time.Second
	for _, n := range survivors {
		gone := func(members map[string]string) bool {
			_, still := members[dead.id]
			return !still
		}
		if !n.awaitMembers(gone, testLeaseTTL+slack) {
			t.Fatalf("%s still lists %s as a member %v after it was killed, want gone within %v",
				n.id, dead.id, time.Since(start), testLeaseTTL+slack)
		}
	}
	t.Logf("every survivor dropped the dead node within %v (lease %v)", time.Since(start), testLeaseTTL)

	// The survivors must also agree on who is left, since disagreement about
	// membership is disagreement about placement.
	want := len(nodes) - 1
	for _, n := range survivors {
		members, err := n.members()
		if err != nil {
			t.Fatalf("members from %s: %v", n.id, err)
		}
		if len(members) != want {
			t.Errorf("%s sees %d members, want %d: %v", n.id, len(members), want, members)
		}
	}
}

// Healing has to happen on its own. Nobody calls repair here: the nodes are
// running with a repair loop, one loses its disk, and redundancy comes back.
func TestALostDiskIsHealedWithoutBeingAskedTo(t *testing.T) {
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), testChunkSize, clusterSize)

	const key = "healed/object"
	data := payloadFor(3)
	client := &http.Client{}
	if status, err := nodes[0].put(client, key, data); err != nil || status != http.StatusOK {
		t.Fatalf("PUT = (%d, %v), want (200, nil)", status, err)
	}

	owners, outsider := ownersOf(nodes, key)
	victim := owners[1]
	held := len(victim.chunkFiles())
	if held == 0 {
		t.Fatalf("%s holds no chunks of an object it owns", victim.id)
	}
	victim.loseChunks()

	if !victim.waitForChunks(held, 15*time.Second) {
		t.Fatalf("%s has %d of %d chunks back, want all of them restored by repair",
			victim.id, len(victim.chunkFiles()), held)
	}

	// Restored, and restored correctly: the object still reads back byte-identical
	// from the node whose copies were rebuilt.
	status, got, err := victim.get(client, key)
	if err != nil || status != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET from the healed node = (%d, %d bytes, %v), want (200, %d, nil)",
			status, len(got), err, len(data))
	}
	if status, got, err := outsider.get(client, key); err != nil || status != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET after healing = (%d, %d bytes, %v), want (200, %d, nil)",
			status, len(got), err, len(data))
	}
}

// Rot has to be found by looking. Nobody asks for a scrub here: a byte is flipped
// under a running node, and the cluster replaces the copy on its own.
func TestBitRotIsFoundAndReplacedWithoutBeingAskedTo(t *testing.T) {
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), testChunkSize, clusterSize)

	const key = "rotted/object"
	data := payloadFor(3)
	client := &http.Client{}
	if status, err := nodes[0].put(client, key, data); err != nil || status != http.StatusOK {
		t.Fatalf("PUT = (%d, %v), want (200, nil)", status, err)
	}

	owners, _ := ownersOf(nodes, key)
	victim := owners[1]
	files := victim.chunkFiles()
	if len(files) == 0 {
		t.Fatalf("%s holds no chunks of an object it owns", victim.id)
	}
	target := files[0]
	want, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	rotted := bytes.Clone(want)
	rotted[len(rotted)/2] ^= 0x01
	if err := os.WriteFile(target, rotted, 0o644); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(got, want) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the rotted chunk was never replaced")
		}
		time.Sleep(50 * time.Millisecond)
	}

	status, got, err := victim.get(client, key)
	if err != nil || status != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET from the scrubbed node = (%d, %d bytes, %v), want (200, %d, nil)",
			status, len(got), err, len(data))
	}
}

// A node joining an existing cluster must be picked up without restarting anyone,
// and must be able to serve objects written before it arrived.
func TestANewNodeJoinsAndServesExistingObjects(t *testing.T) {
	bin := buildKavod(t)
	prefix := clusterPrefix()
	nodes := startCluster(t, bin, prefix, testChunkSize, clusterSize)

	const key = "written/before/the/join"
	data := payloadFor(3)
	client := &http.Client{}
	if status, err := nodes[0].put(client, key, data); err != nil || status != http.StatusOK {
		t.Fatalf("PUT = (%d, %v), want (200, nil)", status, err)
	}

	joiner := launch(t, bin, "joiner", freePort(t), t.TempDir(), prefix, testChunkSize)
	for _, n := range append([]*node{joiner}, nodes...) {
		n.waitForMembers(clusterSize + 1)
	}

	// The joiner holds none of the object's chunks, so serving it means resolving
	// the manifest from etcd and fetching from the owners it names.
	status, got, err := joiner.get(client, key)
	if err != nil || status != http.StatusOK || !bytes.Equal(got, data) {
		t.Fatalf("GET from the new node = (%d, %d bytes, %v), want (200, %d, nil)",
			status, len(got), err, len(data))
	}
}
