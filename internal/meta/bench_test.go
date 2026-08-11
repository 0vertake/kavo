package meta_test

import (
	"context"
	"crypto/rand"
	"strconv"
	"testing"

	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
)

// The fixed cost of every read: resolving the object through its committed
// manifest. Nothing is served without this, so it is the floor under a small GET
// no matter how fast the disks are.
func BenchmarkGet(b *testing.B) {
	s, err := meta.Open([]string{meta.EndpointFromEnv()}, "/kavo-bench/"+rand.Text())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	m := object.Manifest{Size: 1 << 20, Chunks: []object.ChunkRef{
		{ID: "bench000000001", Size: 1 << 20, CRC: 1},
	}}
	ctx := context.Background()
	if err := s.Commit(ctx, "bench/read", m); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.Get(ctx, "bench/read"); err != nil {
			b.Fatal(err)
		}
	}
}

// The other half of the fixed cost of a write: the manifest commit that makes an
// object exist. One etcd Put, which is a raft round and an fsync of etcd's WAL.
func BenchmarkCommit(b *testing.B) {
	s, err := meta.Open([]string{meta.EndpointFromEnv()}, "/kavo-bench/"+rand.Text())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	m := object.Manifest{Size: 1 << 20, Chunks: []object.ChunkRef{
		{ID: "bench000000001", Size: 1 << 20, CRC: 1},
	}}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if err := s.Commit(ctx, "bench/"+strconv.Itoa(i), m); err != nil {
			b.Fatal(err)
		}
	}
}
