package test

import (
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/meta"
)

const remoteMeasurePrefix = "measure-remote"

func skipUnlessRemoteMeasuring(t *testing.T) {
	t.Helper()
	skipUnlessMeasuring(t)
	if !*measureRemote {
		t.Skip("remote measurement: run with -measure.remote")
	}
}

// Milestone 11's heal number over a real network. Same scenario as
// TestMeasureHealTime — fill the cluster, wipe one node's disk, wait for repair —
// but the cluster is already running on named hosts and the wipe goes through
// KAVO_WIPE_CMD rather than a local data directory.
//
// Repair rate is whatever the nodes were started with; for numbers comparable to
// the unthrottled local run, start every node with -repair-rate=0.
func TestMeasureRemoteHealTime(t *testing.T) {
	skipUnlessRemoteMeasuring(t)

	nodes := loadRemoteCluster(t)
	store, err := meta.Open([]string{meta.EndpointFromEnv()}, remoteClusterPrefix())
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

	objects := writeRemoteObjects(t, nodes[0], *measureData, measureChunkSize, remoteMeasurePrefix)

	byID := make(map[string]*remoteNode, len(nodes))
	for _, n := range nodes {
		byID[n.id] = n
	}

	victimID := measureVictimID()
	if victimID == "" {
		victimID = nodes[len(nodes)-1].id
	}
	victim, ok := byID[victimID]
	if !ok {
		t.Fatalf("KAVO_MEASURE_VICTIM=%q is not in the cluster", victimID)
	}

	lost, err := victimHeld(t.Context(), victimID, byID, store)
	if err != nil {
		t.Fatalf("count %s chunks: %v", victimID, err)
	}
	if lost == 0 {
		t.Fatalf("%s holds no chunks, so there is nothing to heal", victimID)
	}
	lostBytes := int64(lost) * measureChunkSize

	_, totalCopies, settled := remoteMissingCopies(t.Context(), byID, store, len(nodes))
	if !settled {
		t.Fatal("cluster not settled before wipe")
	}

	start := time.Now()
	wipeRemoteNode(t, victimID)
	var elapsed time.Duration
	for {
		if holes, _, settled := remoteMissingCopies(t.Context(), byID, store, len(nodes)); settled && len(holes) == 0 {
			elapsed = time.Since(start)
			break
		}
		if time.Since(start) > 10*time.Minute {
			t.Fatal("redundancy did not come back within 10 minutes")
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("remote heal: %d objects, %d chunk copies over %d nodes; %s (%s) lost %d copies (%s)",
		objects, totalCopies, len(nodes), victim.id, victim.addr, lost, bytesOf(lostBytes))
	t.Logf("redundancy restored in %v (%s of copies rebuilt, %s effective)",
		elapsed.Round(10*time.Millisecond), bytesOf(lostBytes), rateOf(lostBytes, elapsed))
}
