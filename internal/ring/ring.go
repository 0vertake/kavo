// Package ring decides which nodes own an object, in two steps: an object key
// maps to one of 256 partitions, and a partition maps to an ordered list of
// owner nodes via a consistent-hash ring of per-node virtual nodes.
//
// The partition indirection is the point. Rebalance tracking, repair queues and
// "% of data moved" are all per-partition — 256 entries — instead of per-object,
// which is what makes them tractable at any object count. Ceph (placement
// groups), MinIO (erasure sets) and Garage (partitions) all do the same.
//
// A ring is immutable: a membership change builds a new one.
package ring

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
)

const (
	// Partitions is how many partitions the key space is divided into. It is
	// fixed for the life of a cluster: changing it remaps every object.
	Partitions = 256

	// DefaultVNodes is how many ring positions each node claims. More vnodes
	// means a more even split at the cost of a bigger ring; ring_test.go
	// measures what this choice actually buys.
	DefaultVNodes = 128

	// partitionShift splits a 64-bit ring position into its partition (the top
	// bits) and the offset within that partition. Deriving both the key's
	// partition and the partition's ring span from one shift keeps them from
	// ever disagreeing.
	partitionShift = 56 // 64 - log2(Partitions)
)

// Ring maps partitions to owner nodes. Build it with New.
type Ring struct {
	vnodes []vnode // sorted by pos
}

type vnode struct {
	pos  uint64
	node string
}

// New builds a ring in which each node claims vnodesPerNode positions. Node IDs
// must be stable across restarts, since a node's positions are derived from its
// ID: reusing an ID means reclaiming exactly the same data.
func New(nodes []string, vnodesPerNode int) *Ring {
	r := &Ring{vnodes: make([]vnode, 0, len(nodes)*vnodesPerNode)}
	for _, node := range nodes {
		for i := range vnodesPerNode {
			r.vnodes = append(r.vnodes, vnode{pos: vnodePos(node, i), node: node})
		}
	}
	slices.SortFunc(r.vnodes, func(a, b vnode) int { return cmp.Compare(a.pos, b.pos) })
	return r
}

// PartitionFor maps an object key to its partition.
func PartitionFor(key string) int {
	return int(hashPos(key) >> partitionShift)
}

// Owners returns the n nodes that own partition p, in preference order: the
// node whose vnode follows the start of the partition's span, then the next
// distinct nodes clockwise. Fewer than n nodes are returned only when the
// cluster is smaller than n, which callers must treat as a placement failure
// rather than silently accepting less redundancy.
func (r *Ring) Owners(p, n int) []string {
	if len(r.vnodes) == 0 || n <= 0 {
		return nil
	}
	start, _ := slices.BinarySearchFunc(r.vnodes, uint64(p)<<partitionShift,
		func(v vnode, target uint64) int { return cmp.Compare(v.pos, target) })

	owners := make([]string, 0, n)
	for i := range r.vnodes { // at most one lap around the ring
		node := r.vnodes[(start+i)%len(r.vnodes)].node
		if !slices.Contains(owners, node) {
			owners = append(owners, node)
			if len(owners) == n {
				break
			}
		}
	}
	return owners
}

// vnodePos places a node's i-th virtual node on the ring.
func vnodePos(node string, i int) uint64 {
	return hashPos(fmt.Sprintf("%s#%d", node, i))
}

func hashPos(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
