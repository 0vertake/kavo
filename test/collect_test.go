package test

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var collectMu sync.Mutex

// withCollect makes collection quick enough to observe inside a test. The grace
// period still has to be longer than the write below takes, which is what it is
// for; the interval decides how long a full sweep of the id space takes, and a
// sweep has to reach every slice before the garbage in one of them is gone.
func withCollect(interval, grace string) func() {
	collectMu.Lock()
	previousInterval, previousGrace := testCollectInterval, testCollectGrace
	testCollectInterval, testCollectGrace = interval, grace
	return func() {
		testCollectInterval, testCollectGrace = previousInterval, previousGrace
		collectMu.Unlock()
	}
}

// An overwrite supersedes the chunks of the version it replaces, and nothing on
// the write path reclaims them. Four real processes, nobody asking for anything:
// the disk has to come back down to one version's worth on its own, and the object
// has to still read afterwards.
func TestAnOverwriteFreesTheOldVersionOnItsOwn(t *testing.T) {
	defer withCollect("100ms", "500ms")()
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 64<<10, 4)

	const key = "collect/overwritten.bin"
	const size = 4 * 64 << 10

	putSigned(t, nodes[0], key, size)
	_, oneVersion := copiesHeld(nodes)
	if oneVersion == 0 {
		t.Fatal("the cluster holds no chunks after a write")
	}

	putSigned(t, nodes[0], key, size)
	if _, both := copiesHeld(nodes); both <= oneVersion {
		t.Fatalf("an overwrite left %d copies on disk, want more than the %d the first version made", both, oneVersion)
	}

	// A full cycle is one pass per slice of the id space, so this waits on the
	// sweep coming round rather than on any single pass.
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, now := copiesHeld(nodes)
		if now == oneVersion {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster still holds %d copies after a minute, want the %d one version needs", now, oneVersion)
		}
		time.Sleep(100 * time.Millisecond)
	}

	getSigned(t, nodes[0], key, size)
}

// The same pass with a fault in the middle of it, because sweeping a disk for
// things nothing points at is only safe if it is also safe while a node is dying,
// coming back, and being repaired. A killed node's copies are still referenced —
// the manifest names it, and it will hold them again — so a sweep that treats a
// membership change as a reason to delete would eat them.
func TestCollectingWhileANodeDiesTakesOnlyGarbage(t *testing.T) {
	defer withCollect("100ms", "500ms")()
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 64<<10, 4)

	const size = 2 * 64 << 10
	keys := make([]string, 6)
	for i := range keys {
		keys[i] = fmt.Sprintf("collect/faulty%d.bin", i)
		putSigned(t, nodes[0], keys[i], size)
	}
	_, oneVersion := copiesHeld(nodes)

	// Half the disk becomes garbage, and then an owner disappears in the middle of
	// the sweep that has to tell the two apart.
	for _, key := range keys {
		putSigned(t, nodes[0], key, size)
	}
	nodes[3].kill()
	time.Sleep(2 * time.Second)
	nodes[3].restart()

	// Repair may put copies back that the dead node's absence cost, so the disk is
	// allowed to settle at one version's worth or more — never less, and never with
	// an object that stopped reading.
	deadline := time.Now().Add(90 * time.Second)
	for {
		_, now := copiesHeld(nodes)
		if now == oneVersion {
			break
		}
		if now < oneVersion {
			t.Fatalf("the cluster holds %d copies, fewer than the %d one version needs", now, oneVersion)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster still holds %d copies after 90s, want the %d one version needs", now, oneVersion)
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, key := range keys {
		getSigned(t, nodes[0], key, size)
	}
}
