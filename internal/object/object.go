// Package object splits an incoming stream into chunks and reassembles it
// again. Both directions are constant-memory in object size: at most one chunk
// is in flight regardless of how large the object is.
//
// Where chunks are stored is not this package's business. Callers pass a commit
// function to place each chunk and a fetch function to retrieve it, which is
// what lets the same chunking logic serve a local disk and a replicated cluster.
package object

import (
	"crypto/rand"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// DefaultChunkSize is the chunk size used by the data path.
const DefaultChunkSize = 32 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ChunkRef locates one chunk of an object and carries the checksum a reader
// must verify it against.
type ChunkRef struct {
	ID   string
	Size int64
	CRC  uint32
}

// Manifest describes a stored object. It is the unit committed to etcd: an
// object exists only once its manifest is committed.
type Manifest struct {
	Size int64
	// Nodes are the partition owners the chunks were written to. All chunks of
	// an object share a partition, so one list covers them all. A reader tries
	// them in turn, because a chunk that only reached W of N owners is missing
	// from the rest.
	Nodes  []string
	Chunks []ChunkRef
}

// Write splits r into chunks of at most chunkSize, hands each to commit, and
// returns the manifest describing them.
//
// The manifest is not durable — the caller commits it. Until then the chunks are
// written but unreferenced, so a failure leaves garbage rather than a partially
// readable object.
//
// commit receives a buffer that is reused for the next chunk, so it must not
// retain the slice after returning. Buffering one chunk is what allows a chunk
// to be written to several nodes at once from a single pass over the body.
func Write(r io.Reader, chunkSize int64, commit func(ChunkRef, []byte) error) (Manifest, error) {
	if chunkSize <= 0 {
		return Manifest{}, fmt.Errorf("object: chunk size must be positive, got %d", chunkSize)
	}
	// One random prefix per object keeps chunk ids unique across the cluster
	// without a coordinator; the suffix keeps them ordered on disk.
	prefix := rand.Text()

	// One buffer, reused for every chunk, so the footprint of a write is exactly
	// one chunk however large the object is. A small object still pays for the
	// full chunk size here; whether that costs enough to be worth a growing
	// buffer is a question for a benchmark, not a guess.
	buf := make([]byte, chunkSize)

	var m Manifest
	for i := 0; ; i++ {
		n, err := io.ReadFull(r, buf)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return Manifest{}, fmt.Errorf("object: read source: %w", err)
		}
		if n == 0 {
			// Nothing left. An object whose size is an exact multiple of
			// chunkSize must not gain a trailing empty chunk.
			return m, nil
		}

		data := buf[:n]
		ref := ChunkRef{
			ID:   fmt.Sprintf("%s%06d", prefix, i),
			Size: int64(n),
			CRC:  crc32.Checksum(data, castagnoli),
		}
		if err := commit(ref, data); err != nil {
			return Manifest{}, err
		}
		m.Chunks = append(m.Chunks, ref)
		m.Size += ref.Size
	}
}

// Read streams the object described by m into w, obtaining each chunk with
// fetch. The readers fetch returns are expected to verify their own checksums,
// so a corrupt chunk aborts the copy rather than being delivered as complete.
func Read(m Manifest, w io.Writer, fetch func(ChunkRef) (io.ReadCloser, error)) error {
	var total int64
	for _, c := range m.Chunks {
		rc, err := fetch(c)
		if err != nil {
			return err
		}
		n, err := io.Copy(w, rc)
		rc.Close()
		total += n
		if err != nil {
			return fmt.Errorf("object: read chunk %s: %w", c.ID, err)
		}
	}
	// Per-chunk checksums cannot catch a manifest that lost a chunk entry; the
	// byte count can, and a short object must never look complete.
	if total != m.Size {
		return fmt.Errorf("object: short read: %d bytes, manifest says %d", total, m.Size)
	}
	return nil
}
