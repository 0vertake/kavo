// Package ec erasure-codes a chunk into shards and puts it back together from
// whichever shards survive.
//
// A chunk is split into Data equal shards and Parity more are computed from them,
// so any Data of the Data+Parity shards reconstruct the chunk. Compared with
// keeping three whole copies, 6+3 tolerates the same three losses at 1.5x the
// bytes instead of 3x — paid for on the read side, where a chunk with a missing
// shard has to be recomputed rather than copied.
//
// Shards are fixed-size and padded, so the chunk's true length is not recoverable
// from them. Neither is their order, and Reed–Solomon cannot detect that shards
// were swapped or corrupted — it solves the equations it is given. The manifest
// carries the length, the order, and a checksum per shard, and this package
// refuses to work without them.
package ec

import (
	"errors"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// Scheme is an erasure code: Data shards reconstruct a chunk, and Parity extra
// shards are how many may be lost first.
type Scheme struct {
	Data   int
	Parity int
}

// ErrTooFewShards reports that fewer than Data shards survived, so the chunk
// cannot be rebuilt. This is data loss, not a transient failure.
var ErrTooFewShards = errors.New("ec: too few shards to reconstruct")

// Shards is how many nodes a chunk is spread over.
func (s Scheme) Shards() int { return s.Data + s.Parity }

// Valid reports whether a scheme can actually be used. Parity of zero encodes
// nothing and would quietly turn erasure coding into striping with no
// redundancy at all.
func (s Scheme) Valid() bool {
	return s.Data > 0 && s.Parity > 0 && s.Shards() <= 256
}

func (s Scheme) String() string { return fmt.Sprintf("%d+%d", s.Data, s.Parity) }

// Encode splits chunk into Data shards and computes Parity more.
//
// Every shard has the same length, so the last data shard is zero-padded; the
// caller must keep the chunk's real length to strip it back off. Shards are
// returned in order and must be stored in order, because index is what tells the
// decoder which equation a shard belongs to.
func (s Scheme) Encode(chunk []byte) ([][]byte, error) {
	enc, err := s.encoder()
	if err != nil {
		return nil, err
	}
	shards, err := enc.Split(chunk)
	if err != nil {
		return nil, fmt.Errorf("ec: split %d bytes into %s: %w", len(chunk), s, err)
	}
	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ec: encode %s: %w", s, err)
	}
	return shards, nil
}

// Reconstruct rebuilds the chunk of the given size from the shards that survived.
// A nil entry means that shard is missing; shards must still be in their original
// positions, since position is what identifies them.
//
// It also rebuilds the missing shards themselves, so a caller repairing a lost
// shard gets it from the returned slice rather than by encoding all over again.
func (s Scheme) Reconstruct(shards [][]byte, size int64) ([]byte, error) {
	if len(shards) != s.Shards() {
		return nil, fmt.Errorf("ec: got %d shard slots, want %d for %s", len(shards), s.Shards(), s)
	}
	present := 0
	for _, sh := range shards {
		if sh != nil {
			present++
		}
	}
	if present < s.Data {
		return nil, fmt.Errorf("%w: %d of %d present, need %d", ErrTooFewShards, present, s.Shards(), s.Data)
	}

	enc, err := s.encoder()
	if err != nil {
		return nil, err
	}
	if present < s.Shards() {
		if err := enc.Reconstruct(shards); err != nil {
			return nil, fmt.Errorf("ec: reconstruct %s: %w", s, err)
		}
	}

	// Join concatenates the data shards and cuts the padding back off, which is
	// why size has to come from the manifest and not from the shards.
	chunk := make([]byte, 0, size)
	buf := sliceWriter{chunk}
	if err := enc.Join(&buf, shards, int(size)); err != nil {
		return nil, fmt.Errorf("ec: join %d bytes from %s: %w", size, s, err)
	}
	return buf.b, nil
}

func (s Scheme) encoder() (reedsolomon.Encoder, error) {
	if !s.Valid() {
		return nil, fmt.Errorf("ec: invalid scheme %s", s)
	}
	enc, err := reedsolomon.New(s.Data, s.Parity)
	if err != nil {
		return nil, fmt.Errorf("ec: %s: %w", s, err)
	}
	return enc, nil
}

// sliceWriter collects Join's output in one preallocated buffer. Join wants an
// io.Writer and the destination is memory, so this avoids bytes.Buffer growing
// its way to a size already known.
type sliceWriter struct{ b []byte }

func (w *sliceWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}
