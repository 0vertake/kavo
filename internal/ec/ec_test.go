package ec

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
)

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.UintN(256))
	}
	return b
}

// The whole promise of a k+m code: any k shards are enough. This walks every
// combination of m losses rather than a sample, because "usually recoverable" is
// not a durability claim.
func TestAnyDataShardsReconstructTheChunk(t *testing.T) {
	for _, s := range []Scheme{{4, 2}, {6, 3}, {2, 1}} {
		t.Run(s.String(), func(t *testing.T) {
			chunk := payload(1 << 16)
			shards, err := s.Encode(chunk)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(shards) != s.Shards() {
				t.Fatalf("got %d shards, want %d", len(shards), s.Shards())
			}

			for _, lost := range combinations(s.Shards(), s.Parity) {
				damaged := clone(shards)
				for _, i := range lost {
					damaged[i] = nil
				}
				got, err := s.Reconstruct(damaged, int64(len(chunk)))
				if err != nil {
					t.Fatalf("losing shards %v: %v", lost, err)
				}
				if !bytes.Equal(got, chunk) {
					t.Fatalf("losing shards %v gave back different bytes", lost)
				}
			}
		})
	}
}

// One loss past the parity count is data loss, and it has to be reported as
// data loss rather than as a chunk of zeros or a partial read.
func TestTooManyLossesIsReportedAsDataLoss(t *testing.T) {
	s := Scheme{4, 2}
	chunk := payload(4096)
	shards, err := s.Encode(chunk)
	if err != nil {
		t.Fatal(err)
	}
	for i := range s.Parity + 1 {
		shards[i] = nil
	}
	got, err := s.Reconstruct(shards, int64(len(chunk)))
	if !errors.Is(err, ErrTooFewShards) {
		t.Fatalf("Reconstruct with %d losses = (%d bytes, %v), want ErrTooFewShards",
			s.Parity+1, len(got), err)
	}
}

// Reconstruct hands back the rebuilt shards too, so repair can push a lost shard
// without re-encoding the chunk it just decoded.
func TestReconstructAlsoRebuildsTheMissingShards(t *testing.T) {
	s := Scheme{4, 2}
	chunk := payload(1 << 15)
	shards, err := s.Encode(chunk)
	if err != nil {
		t.Fatal(err)
	}
	want := clone(shards)

	damaged := clone(shards)
	damaged[1], damaged[5] = nil, nil
	if _, err := s.Reconstruct(damaged, int64(len(chunk))); err != nil {
		t.Fatal(err)
	}
	for i := range damaged {
		if !bytes.Equal(damaged[i], want[i]) {
			t.Errorf("shard %d was not rebuilt to its original bytes", i)
		}
	}
}

// Sizes that do not divide evenly by the data shard count are the normal case,
// since a chunk is whatever the object had left. The padding must never come
// back as part of the chunk.
func TestPaddingIsStrippedAtEverySize(t *testing.T) {
	s := Scheme{6, 3}
	for _, size := range []int{1, 5, 6, 7, 4095, 4096, 4097, 1 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			chunk := payload(size)
			shards, err := s.Encode(chunk)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := s.Reconstruct(shards, int64(size))
			if err != nil {
				t.Fatalf("Reconstruct: %v", err)
			}
			if len(got) != size {
				t.Fatalf("got %d bytes back, want %d", len(got), size)
			}
			if !bytes.Equal(got, chunk) {
				t.Error("bytes differ after a round trip")
			}
		})
	}
}

// A scheme with no parity is striping with the word redundancy attached to it,
// which is worse than no erasure coding at all: one node down loses data.
func TestSchemesWithoutRedundancyAreRefused(t *testing.T) {
	for _, s := range []Scheme{{0, 0}, {6, 0}, {0, 3}, {-1, 3}, {200, 200}} {
		if s.Valid() {
			t.Errorf("%s reported valid", s)
		}
		if _, err := s.Encode(payload(64)); err == nil {
			t.Errorf("Encode with %s succeeded", s)
		}
	}
}

// Shard order is not recoverable from the shards, so a decoder given them in the
// wrong order has no way to notice: it solves the equations it is handed. The
// manifest is what keeps them in order, and this test is the reason it must.
func TestSwappedShardsAreNotDetected(t *testing.T) {
	s := Scheme{4, 2}
	chunk := payload(4096)
	shards, err := s.Encode(chunk)
	if err != nil {
		t.Fatal(err)
	}
	shards[0], shards[1] = shards[1], shards[0]

	got, err := s.Reconstruct(shards, int64(len(chunk)))
	if err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if bytes.Equal(got, chunk) {
		t.Fatal("swapped shards round-tripped correctly, which would make this test pointless")
	}
	// The point: garbage came back and the library was happy. Only the
	// manifest's per-shard checksums catch this.
}

// The wrong number of slots means the caller lost track of which shard is which,
// and guessing would decode garbage.
func TestWrongShardCountIsRefused(t *testing.T) {
	s := Scheme{4, 2}
	shards, err := s.Encode(payload(4096))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Reconstruct(shards[:s.Shards()-1], 4096); err == nil {
		t.Error("Reconstruct accepted too few shard slots")
	}
}

func clone(shards [][]byte) [][]byte {
	out := make([][]byte, len(shards))
	for i, s := range shards {
		out[i] = bytes.Clone(s)
	}
	return out
}

// combinations lists every way to choose k of n indices.
func combinations(n, k int) [][]int {
	if k == 0 {
		return [][]int{{}}
	}
	var out [][]int
	for i := range n {
		for _, rest := range combinations(n-i-1, k-1) {
			pick := []int{i}
			for _, r := range rest {
				pick = append(pick, i+1+r)
			}
			out = append(out, pick)
		}
	}
	return out
}
