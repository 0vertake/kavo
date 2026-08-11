package meta

import (
	"context"
	"maps"
	"testing"
	"time"
)

// Short enough to keep these tests quick, long enough that a renewal (sent at a
// third of the TTL) is not racing the test itself.
const testTTL = 2 * time.Second

func TestJoinMakesANodeVisible(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	if err := s.Join(ctx, "n1", "10.0.0.1:8080", testTTL); err != nil {
		t.Fatalf("Join: %v", err)
	}
	if err := s.Join(ctx, "n2", "10.0.0.2:8080", testTTL); err != nil {
		t.Fatalf("Join: %v", err)
	}

	want := map[string]string{"n1": "10.0.0.1:8080", "n2": "10.0.0.2:8080"}
	got, err := s.Members(ctx)
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	if !maps.Equal(got, want) {
		t.Fatalf("Members = %v, want %v", got, want)
	}
}

// A lease TTL below etcd's granularity would be silently rounded to something
// else, so it is refused rather than quietly meaning something different.
func TestJoinRejectsASubSecondLease(t *testing.T) {
	s := newStore(t)
	if err := s.Join(context.Background(), "n1", "10.0.0.1:8080", 100*time.Millisecond); err == nil {
		t.Fatal("Join accepted a 100ms lease, want an error")
	}
}

// The claim milestone 5 has to earn: a node that stops renewing is gone from the
// cluster's view within the lease, without anyone telling the cluster.
//
// Cancelling the context is exactly what a crash does to the renewal loop — it
// stops. Nothing revokes the lease, so only expiry can remove the node.
func TestUnrenewedMembershipExpiresWithinTheLease(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if err := s.Join(ctx, "survivor", "10.0.0.1:8080", testTTL); err != nil {
		t.Fatalf("Join: %v", err)
	}

	crashed, stopRenewing := context.WithCancel(ctx)
	if err := s.Join(crashed, "crashed", "10.0.0.2:8080", testTTL); err != nil {
		t.Fatalf("Join: %v", err)
	}

	updates := s.WatchMembers(ctx)
	if got := <-updates; len(got) != 2 {
		t.Fatalf("first membership = %v, want both nodes", got)
	}

	start := time.Now()
	stopRenewing()

	// The bound is what makes this a failure detector rather than a hope. Some
	// slack over the TTL covers etcd's own expiry sweep.
	const slack = 2 * time.Second
	deadline := time.After(testTTL + slack)
	for {
		select {
		case got, ok := <-updates:
			if !ok {
				t.Fatal("membership watch closed early")
			}
			if _, still := got["crashed"]; still {
				continue
			}
			if _, ok := got["survivor"]; !ok {
				t.Fatalf("membership = %v, want the still-renewing node to remain", got)
			}
			t.Logf("detected the crashed node's departure in %v (lease %v)", time.Since(start), testTTL)
			return
		case <-deadline:
			t.Fatalf("crashed node still a member %v after it stopped renewing, want gone within %v",
				time.Since(start), testTTL+slack)
		}
	}
}

// A node that loses its lease is still running, so it must come back on its own.
// Revoking the lease underneath it is the harshest version of that: etcd drops
// the key while the node keeps serving.
func TestARevokedLeaseIsReclaimed(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Join(ctx, "n1", "10.0.0.1:8080", testTTL); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// Delete the registration behind the node's back. The keepalive it holds is
	// for a lease that no longer has a key, so re-registration is the only way
	// the node reappears.
	leases, err := s.client.Leases(ctx)
	if err != nil {
		t.Fatalf("list leases: %v", err)
	}
	for _, l := range leases.Leases {
		if _, err := s.client.Revoke(ctx, l.ID); err != nil {
			t.Fatalf("revoke lease: %v", err)
		}
	}

	deadline := time.Now().Add(testTTL + 3*time.Second)
	for time.Now().Before(deadline) {
		got, err := s.Members(ctx)
		if err != nil {
			t.Fatalf("Members: %v", err)
		}
		if got["n1"] == "10.0.0.1:8080" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("node never re-registered after its lease was revoked")
}

// The watch has to report a join, not only an expiry, or a new node would never
// be given any data.
func TestWatchReportsAJoin(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Join(ctx, "n1", "10.0.0.1:8080", testTTL); err != nil {
		t.Fatalf("Join: %v", err)
	}
	updates := s.WatchMembers(ctx)
	if got := <-updates; len(got) != 1 {
		t.Fatalf("first membership = %v, want just n1", got)
	}

	if err := s.Join(ctx, "n2", "10.0.0.2:8080", testTTL); err != nil {
		t.Fatalf("Join: %v", err)
	}
	select {
	case got := <-updates:
		if got["n2"] != "10.0.0.2:8080" {
			t.Fatalf("membership after a join = %v, want it to contain n2", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch never reported the new node")
	}
}
