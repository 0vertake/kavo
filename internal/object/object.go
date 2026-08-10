// Package object splits an incoming stream into store chunks and streams it
// back out again. Both directions are constant-memory: at most one chunk
// buffer is in flight regardless of object size.
package object

import (
	"bufio"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/0vertake/kavo/internal/store"
)

// DefaultChunkSize is the chunk size used by the data path.
const DefaultChunkSize = 32 << 20

// ChunkRef locates one chunk of an object and carries the checksum a reader
// must verify it against.
type ChunkRef struct {
	ID   string
	Size int64
	CRC  uint32
}

// Manifest describes a stored object. It is the unit that later gets committed
// to etcd: an object exists only once its manifest is committed.
type Manifest struct {
	Size   int64
	Chunks []ChunkRef
}

// Write splits r into chunks of at most chunkSize, persists each one, and
// returns the manifest describing them.
//
// The manifest is not durable — the caller commits it. Until then the chunks
// are written but unreferenced, so a crash leaves garbage rather than a
// partially readable object.
func Write(s *store.Store, r io.Reader, chunkSize int64) (Manifest, error) {
	if chunkSize <= 0 {
		return Manifest{}, fmt.Errorf("object: chunk size must be positive, got %d", chunkSize)
	}
	// One random prefix per object keeps chunk IDs unique across the cluster
	// without a coordinator; the suffix keeps them ordered on disk.
	prefix := rand.Text()

	var m Manifest
	br := bufio.NewReader(r)
	for i := 0; ; i++ {
		// Peek before writing so an object whose size is an exact multiple of
		// chunkSize does not leave a trailing empty chunk behind.
		if _, err := br.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				return m, nil
			}
			return Manifest{}, fmt.Errorf("object: read source: %w", err)
		}
		id := fmt.Sprintf("%s%06d", prefix, i)
		crc, n, err := s.WriteChunk(id, io.LimitReader(br, chunkSize))
		if err != nil {
			return Manifest{}, err
		}
		m.Chunks = append(m.Chunks, ChunkRef{ID: id, Size: n, CRC: crc})
		m.Size += n
	}
}

// Read streams the object described by m into w, verifying every chunk against
// its checksum. A corrupt chunk aborts the copy with store.ErrChecksumMismatch
// rather than delivering the object as complete.
func Read(s *store.Store, m Manifest, w io.Writer) error {
	var total int64
	for _, c := range m.Chunks {
		rc, err := s.ReadChunk(c.ID, c.CRC)
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
	// Per-chunk checksums cannot catch a manifest that lost a chunk entry;
	// the byte count can, and a short object must never look complete.
	if total != m.Size {
		return fmt.Errorf("object: short read: %d bytes, manifest says %d", total, m.Size)
	}
	return nil
}
