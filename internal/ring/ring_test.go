package ring

import (
	"fmt"
	"math"
	"slices"
	"testing"
)

const replicas = 3 // N=3, the default replication factor

func nodeNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("kavod-%d", i)
	}
	return names
}

// ownershipShare counts how many of the Partitions*replicas ownership slots
// each node holds, and returns the worst deviation from a perfectly even split.
func ownershipShare(r *Ring, nodes []string) (map[string]int, float64) {
	counts := make(map[string]int, len(nodes))
	for _, n := range nodes {
		counts[n] = 0
	}
	for p := range Partitions {
		for _, owner := range r.Owners(p, replicas) {
			counts[owner]++
		}
	}
	ideal := float64(Partitions*replicas) / float64(len(nodes))
	worst := 0.0
	for _, c := range counts {
		worst = math.Max(worst, math.Abs(float64(c)-ideal)/ideal)
	}
	return counts, worst
}

// TestPartitionsAreUniform checks the first hop: object keys must spread evenly
// over the 256 partitions, or no amount of ring tuning will balance the data.
func TestPartitionsAreUniform(t *testing.T) {
	const keys = 200_000
	var counts [Partitions]int
	for i := range keys {
		counts[PartitionFor(fmt.Sprintf("bucket/object-%d", i))]++
	}

	ideal := float64(keys) / Partitions
	worst := 0.0
	for _, c := range counts {
		worst = math.Max(worst, math.Abs(float64(c)-ideal)/ideal)
	}
	t.Logf("%d keys over %d partitions: worst deviation %.1f%%", keys, Partitions, worst*100)
	if worst > 0.10 {
		t.Errorf("partition spread is %.1f%% off ideal, want within 10%%", worst*100)
	}
}

// TestVNodeCountJustifiesDefault is the measurement behind DefaultVNodes: it
// records how evenly ownership spreads as vnodes per node increase, so 128 is a
// number rather than folklore.
//
// Two things the numbers show. A cluster of exactly N nodes is trivially even —
// every node owns every partition — so vnodes cannot be judged there. And past
// roughly one vnode per partition the curve flattens: with only 256 partitions
// to hand out, the residual imbalance is set by partition count, not vnode
// count, so more vnodes just reshuffle the same variance.
func TestVNodeCountJustifiesDefault(t *testing.T) {
	for _, nodeCount := range []int{3, 6, 12} {
		nodes := nodeNames(nodeCount)
		worstBy := map[int]float64{}

		for _, vnodes := range []int{1, 8, 32, 128, 512} {
			_, worst := ownershipShare(New(nodes, vnodes), nodes)
			worstBy[vnodes] = worst
			t.Logf("%2d nodes, %3d vnodes each: worst deviation %5.1f%%", nodeCount, vnodes, worst*100)
		}

		if worstBy[DefaultVNodes] > 0.15 {
			t.Errorf("%d nodes at %d vnodes: %.1f%% off ideal, want within 15%%",
				nodeCount, DefaultVNodes, worstBy[DefaultVNodes]*100)
		}
		if nodeCount > replicas && worstBy[DefaultVNodes] >= worstBy[8] {
			t.Errorf("%d nodes: %d vnodes (%.1f%%) is no better than 8 vnodes (%.1f%%)",
				nodeCount, DefaultVNodes, worstBy[DefaultVNodes]*100, worstBy[8]*100)
		}
	}
}

// TestOwnersAreDistinct guards the invariant that makes replication meaningful:
// N replicas must land on N different nodes.
func TestOwnersAreDistinct(t *testing.T) {
	nodes := nodeNames(6)
	r := New(nodes, DefaultVNodes)
	for p := range Partitions {
		owners := r.Owners(p, replicas)
		if len(owners) != replicas {
			t.Fatalf("partition %d: got %d owners, want %d", p, len(owners), replicas)
		}
		for i, o := range owners {
			if slices.Contains(owners[:i], o) {
				t.Fatalf("partition %d: duplicate owner %s in %v", p, o, owners)
			}
			if !slices.Contains(nodes, o) {
				t.Fatalf("partition %d: unknown owner %s", p, o)
			}
		}
	}
}

// TestPlacementIsDeterministic: two rings built from the same membership must
// agree, whatever order the nodes arrive in. Placement that depended on
// iteration order would silently strand data after a restart.
func TestPlacementIsDeterministic(t *testing.T) {
	nodes := nodeNames(6)
	shuffled := []string{nodes[3], nodes[0], nodes[5], nodes[1], nodes[4], nodes[2]}
	a, b := New(nodes, DefaultVNodes), New(shuffled, DefaultVNodes)

	for p := range Partitions {
		if got, want := b.Owners(p, replicas), a.Owners(p, replicas); !slices.Equal(got, want) {
			t.Fatalf("partition %d: %v != %v", p, got, want)
		}
	}
	if got, want := PartitionFor("bucket/key"), PartitionFor("bucket/key"); got != want {
		t.Fatalf("PartitionFor is not stable: %d != %d", got, want)
	}
}

// TestJoinOnlyMovesDataToNewNode is the consistent-hashing property that makes
// rebalance affordable: a node joining must claim a share of partitions and
// must not cause any reshuffling among the nodes that were already there.
func TestJoinOnlyMovesDataToNewNode(t *testing.T) {
	before := nodeNames(6)
	after := nodeNames(7)
	newNode := after[6]
	rBefore, rAfter := New(before, DefaultVNodes), New(after, DefaultVNodes)

	moved := 0
	for p := range Partitions {
		was := rBefore.Owners(p, replicas)
		now := rAfter.Owners(p, replicas)
		for _, owner := range now {
			if !slices.Contains(was, owner) && owner != newNode {
				t.Fatalf("partition %d: %s took over from %v without joining", p, owner, was)
			}
		}
		if slices.Contains(now, newNode) {
			moved++
		}
	}

	// A 7th node should claim roughly 1/7 of the ownership slots. Measured as a
	// fraction of partitions it appears in, allowing generous slack for the
	// variance of a single ring.
	fraction := float64(moved) / Partitions
	ideal := float64(replicas) / float64(len(after))
	t.Logf("joining node owns %d/%d partitions (%.1f%%), ideal %.1f%%",
		moved, Partitions, fraction*100, ideal*100)
	if fraction < ideal*0.6 || fraction > ideal*1.4 {
		t.Errorf("joining node claimed %.1f%% of partitions, want within 40%% of %.1f%%",
			fraction*100, ideal*100)
	}
}

// TestLeaveOnlyMovesDataFromDepartedNode: when a node leaves, partitions it did
// not own must keep exactly the owners they had, so repair only has to rebuild
// what the departed node held.
func TestLeaveOnlyMovesDataFromDepartedNode(t *testing.T) {
	before := nodeNames(6)
	gone := before[2]
	after := slices.Delete(slices.Clone(before), 2, 3)
	rBefore, rAfter := New(before, DefaultVNodes), New(after, DefaultVNodes)

	affected := 0
	for p := range Partitions {
		was := rBefore.Owners(p, replicas)
		now := rAfter.Owners(p, replicas)
		if !slices.Contains(was, gone) {
			if !slices.Equal(now, was) {
				t.Fatalf("partition %d was not on %s but changed: %v -> %v", p, gone, was, now)
			}
			continue
		}
		affected++
		for _, owner := range was {
			if owner != gone && !slices.Contains(now, owner) {
				t.Fatalf("partition %d: %s dropped an owner it should have kept: %v -> %v",
					p, gone, was, now)
			}
		}
	}
	t.Logf("departure of %s affects %d/%d partitions", gone, affected, Partitions)
	if affected == 0 {
		t.Fatal("removing a node affected no partitions, so the test proved nothing")
	}
}

// TestUndersizedClusterIsVisible: with fewer nodes than replicas the ring must
// return a short list rather than repeating a node, so callers can refuse the
// write instead of quietly storing three copies in one place.
func TestUndersizedClusterIsVisible(t *testing.T) {
	r := New(nodeNames(2), DefaultVNodes)
	owners := r.Owners(0, replicas)
	if len(owners) != 2 {
		t.Fatalf("got %v, want 2 distinct owners", owners)
	}
	if owners[0] == owners[1] {
		t.Fatalf("got duplicate owner in %v", owners)
	}
}

func TestEmptyRingHasNoOwners(t *testing.T) {
	if owners := New(nil, DefaultVNodes).Owners(0, replicas); owners != nil {
		t.Fatalf("got %v, want nil", owners)
	}
}
