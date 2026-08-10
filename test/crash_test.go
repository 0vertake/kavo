package test

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	crashChunkSize = 16 << 10
	uploaders      = 100
	killAfterAcks  = 10
)

// payloadFor derives each object's bytes from its index so the verifier can
// recompute them instead of holding 100 payloads in memory. Sizes straddle the
// chunk size, so most objects span several chunks plus a partial one.
func payloadFor(i int) []byte {
	b := make([]byte, (1+i%8)*12_000)
	for j := range b {
		b[j] = byte(i*31 + j)
	}
	return b
}

func key(i int) string { return fmt.Sprintf("bucket/obj%03d", i) }

// Milestone 2: SIGKILL during 100 concurrent uploads, restart, and assert both
// durability invariants:
//
//  1. every acknowledged write is still readable, byte for byte
//  2. no object is readable in a partial or corrupt state
//
// An unacknowledged write may exist or not — both are legal — but if it exists
// it must be complete and correct.
func TestCrashDuringConcurrentUploads(t *testing.T) {
	bin := buildKavod(t)
	dataDir := t.TempDir()
	n := startNode(t, bin, dataDir, crashChunkSize)

	var (
		acked    [uploaders]bool
		ackCount atomic.Int64
		wg       sync.WaitGroup
	)
	client := &http.Client{}

	// Kill as soon as a handful of writes have been acknowledged, which leaves
	// the rest of the uploads in flight with data half-written.
	killed := make(chan struct{})
	go func() {
		defer close(killed)
		for ackCount.Load() < killAfterAcks {
			time.Sleep(time.Millisecond)
		}
		n.kill()
	}()

	for i := range uploaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, err := n.put(client, key(i), payloadFor(i))
			if err == nil && status == http.StatusOK {
				acked[i] = true
				ackCount.Add(1)
			}
		}()
	}
	wg.Wait()
	<-killed

	interrupted := uploaders - int(ackCount.Load())
	t.Logf("acknowledged %d, interrupted %d", ackCount.Load(), interrupted)
	if interrupted == 0 {
		t.Fatal("every upload finished before the kill landed: the crash path was never exercised")
	}

	// Restart on the same data directory.
	restarted := startNode(t, bin, dataDir, crashChunkSize)
	verifyClient := &http.Client{}

	survived, absent := 0, 0
	for i := range uploaders {
		status, body, err := restarted.get(verifyClient, key(i))
		want := payloadFor(i)

		switch {
		case status == http.StatusOK && err == nil && bytes.Equal(body, want):
			survived++
		case status == http.StatusNotFound:
			absent++
			if acked[i] {
				t.Errorf("%s: acknowledged write lost (404 after restart)", key(i))
			}
		default:
			// Anything else is a torn object: a manifest was visible without
			// all of its chunks intact, which must never happen.
			t.Errorf("%s: torn object after restart (acked=%v status=%d bytes=%d/%d err=%v)",
				key(i), acked[i], status, len(body), len(want), err)
		}
	}
	t.Logf("after restart: %d readable and correct, %d absent", survived, absent)
}
