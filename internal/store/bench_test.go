package store_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/0vertake/kavo/internal/store"
)

// What one durable chunk costs. Every object in the cluster pays this three
// times over, so if the fixed cost of a small write is a mystery, it is either
// here or in etcd. The fsync pair — file, then parent directory — is the floor:
// it is two disk barriers that cannot be skipped without giving up the
// durability claim.
func BenchmarkWriteChunk(b *testing.B) {
	for _, size := range []int64{4 << 10, 1 << 20, 32 << 20} {
		b.Run(byteSize(size), func(b *testing.B) {
			s, err := store.Open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			data := bytes.Repeat([]byte{0xAB}, int(size))

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				if _, _, err := s.WriteChunk(fmt.Sprintf("bench%012d", i), bytes.NewReader(data)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Reads have no barrier to pay, so this is the number a GET is bounded by once
// the data is in page cache.
func BenchmarkReadChunk(b *testing.B) {
	for _, size := range []int64{4 << 10, 32 << 20} {
		b.Run(byteSize(size), func(b *testing.B) {
			s, err := store.Open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			const id = "benchread001"
			crc, _, err := s.WriteChunk(id, bytes.NewReader(bytes.Repeat([]byte{0xAB}, int(size))))
			if err != nil {
				b.Fatal(err)
			}

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				rc, err := s.ReadChunk(id, crc)
				if err != nil {
					b.Fatal(err)
				}
				// Discarded rather than buffered: buffering would report the
				// benchmark's own memory as the store's.
				if _, err := io.Copy(io.Discard, rc); err != nil {
					b.Fatal(err)
				}
				rc.Close()
			}
		})
	}
}

func byteSize(n int64) string {
	if n >= 1<<20 {
		return fmt.Sprintf("%dMB", n>>20)
	}
	return fmt.Sprintf("%dKB", n>>10)
}
