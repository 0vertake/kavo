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
	"time"

	"github.com/0vertake/kavo/internal/ec"
)

// DefaultChunkSize is the chunk size used by the data path.
const DefaultChunkSize = 32 << 20

// smallObject is where a write's buffer starts. Objects at or below it never
// allocate a full chunk; the cutoff is set at the size where the fixed cost of a
// write — three disk barriers and an etcd commit — stops dominating anyway.
const smallObject = 1 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ChunkRef locates one chunk of an object and carries the checksum a reader
// must verify it against.
type ChunkRef struct {
	ID   string
	Size int64
	CRC  uint32
	// Shards holds one checksum per erasure-coded shard, in shard order, and is
	// empty for a replicated chunk. Position is not decoration: it is what tells
	// the decoder which equation a shard belongs to, and Reed–Solomon cannot
	// notice that two shards were swapped. These checksums are the only thing
	// that can.
	Shards []uint32
}

// ShardID names the i'th shard of a chunk. Shards are derived from the chunk id
// rather than given their own random names, so a reader that has the manifest can
// address every shard without the manifest listing them.
func (c ChunkRef) ShardID(i int) string { return fmt.Sprintf("%ss%02d", c.ID, i) }

// indexWidth is how many trailing digits of a chunk id are the chunk's index within
// its write. Everything before them is the write's own random prefix.
const indexWidth = 6

// WriteID is the id of the write a chunk came from. Every chunk of one write shares
// it, and a shard id extends its chunk's, so it names an upload's whole footprint —
// which is what lets one small record stand for a five-gigabyte upload in flight.
func WriteID(chunkID string) string {
	if len(chunkID) <= indexWidth {
		return ""
	}
	return chunkID[:len(chunkID)-indexWidth]
}

// Manifest describes a stored object. It is the unit committed to etcd: an
// object exists only once its manifest is committed.
type Manifest struct {
	Size int64
	// ETag is the object's MD5, hex-encoded, and for a multipart object the MD5
	// of its parts' MD5s followed by "-<parts>". S3 clients compare it against
	// what they uploaded, so it is the object's identity to a client rather than
	// an internal checksum — integrity is the chunks' CRC32C, which is verified
	// on every read.
	ETag string
	// ContentType and Modified exist because S3 promises them back. Neither
	// affects how the object is stored.
	ContentType string
	Modified    time.Time
	// Nodes are the partition owners the chunks were written to. All chunks of
	// an object share a partition, so one list covers them all.
	//
	// Replicated: a reader tries them in turn, because a chunk that only reached
	// W of N owners is missing from the rest. Erasure-coded: position matters —
	// shard i lives on Nodes[i], and there are as many owners as shards.
	Nodes  []string
	Chunks []ChunkRef
	// Coding is the erasure code the chunks were written with, or the zero
	// Scheme for replication. It is recorded per object rather than read from
	// the node's configuration: an object written as 6+3 must still be readable
	// after the cluster's default changes.
	Coding ec.Scheme
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
// to be written to several nodes at once from a single pass over the body. It
// takes the ref by pointer because an erasure-coding commit has to record a
// checksum per shard, and only the code that made the shards knows them.
// expect is what the caller believes the stream's length is — a Content-Length,
// usually — and sizes the first buffer. It is a hint and nothing else: too small
// and the buffer grows, too large and it is capped at a chunk, absent (zero or
// negative) and the write starts where it used to. Nothing about correctness
// depends on the caller being right, which matters because the caller is
// repeating what a client claimed.
func Write(r io.Reader, chunkSize, expect int64, commit func(*ChunkRef, []byte) error) (Manifest, error) {
	if chunkSize <= 0 {
		return Manifest{}, fmt.Errorf("object: chunk size must be positive, got %d", chunkSize)
	}
	// One random prefix per object keeps chunk ids unique across the cluster
	// without a coordinator; the suffix keeps them ordered on disk.
	prefix := rand.Text()

	// One buffer, reused for every chunk, so the footprint of a write is exactly
	// one chunk however large the object is. It starts at what the caller expects
	// and grows to a full chunk only once the source proves to be bigger:
	// measured, a 4 KB object was allocating 32 MB to store 4 KB, and an 8 MB
	// multipart part 33 MB. The streaming claim is that the footprint stops
	// growing, not that it starts at the maximum.
	start := min(chunkSize, smallObject)
	if expect > 0 {
		start = min(chunkSize, expect)
	}
	buf := make([]byte, start)

	var m Manifest
	for i := 0; ; i++ {
		n, next, err := readChunk(r, buf, chunkSize)
		if err != nil {
			return Manifest{}, fmt.Errorf("object: read source: %w", err)
		}
		buf = next
		if n == 0 {
			// Nothing left. An object whose size is an exact multiple of
			// chunkSize must not gain a trailing empty chunk.
			return m, nil
		}

		data := buf[:n]
		ref := ChunkRef{
			ID:   fmt.Sprintf("%s%0*d", prefix, indexWidth, i),
			Size: int64(n),
			CRC:  crc32.Checksum(data, castagnoli),
		}
		if err := commit(&ref, data); err != nil {
			return Manifest{}, err
		}
		m.Chunks = append(m.Chunks, ref)
		m.Size += ref.Size
	}
}

// readChunk reads up to chunkSize bytes into buf and returns how much it read
// along with the buffer to use for the next chunk — grown to a full chunk if the
// source had more than buf could hold.
func readChunk(r io.Reader, buf []byte, chunkSize int64) (int, []byte, error) {
	n, err := readOrEnd(r, buf)
	if err != nil || int64(n) < int64(len(buf)) || int64(len(buf)) == chunkSize {
		return n, buf, err
	}

	// The buffer is full and might already hold the whole object, so ask for one
	// more byte before allocating a chunk-sized one. Growing on the guess that
	// more is coming is how an object exactly one buffer long ends up paying for
	// a chunk it never uses.
	var probe [1]byte
	more, err := readOrEnd(r, probe[:])
	if err != nil || more == 0 {
		return n, buf, err
	}

	grown := make([]byte, chunkSize)
	copy(grown, buf)
	grown[n] = probe[0]
	n++
	rest, err := readOrEnd(r, grown[n:])
	return n + rest, grown, err
}

// readOrEnd fills p unless the source ends first, which is not an error: the
// last chunk of an object is short unless its size divides evenly.
func readOrEnd(r io.Reader, p []byte) (int, error) {
	n, err := io.ReadFull(r, p)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		err = nil
	}
	return n, err
}

// Read streams the object described by m into w, obtaining each chunk with
// fetch. The readers fetch returns are expected to verify their own checksums,
// so a corrupt chunk aborts the copy rather than being delivered as complete.
func Read(m Manifest, w io.Writer, fetch func(ChunkRef) (io.ReadCloser, error)) error {
	return ReadRange(m, w, 0, m.Size, fetch)
}

// ReadRange streams length bytes of the object, starting at off.
//
// Every chunk the window touches is read in full and the surplus discarded, even
// though only part of it is wanted. A chunk's checksum covers the whole chunk, so
// stopping at the edge of the window would hand back bytes nothing verified: rot
// outside the range would go unnoticed and the client would have no way to know.
// The cost is at most two extra chunks of reading per request.
func ReadRange(m Manifest, w io.Writer, off, length int64, fetch func(ChunkRef) (io.ReadCloser, error)) error {
	if off < 0 || length < 0 || off+length > m.Size {
		return fmt.Errorf("object: range %d+%d is outside an object of %d bytes", off, length, m.Size)
	}

	end, at, total := off+length, int64(0), int64(0)
	for _, c := range m.Chunks {
		next := at + c.Size
		if next <= off || at >= end {
			at = next
			continue
		}
		rc, err := fetch(c)
		if err != nil {
			return err
		}
		skip := max(off-at, 0)
		n, err := readWindow(w, rc, skip, min(next, end)-(at+skip))
		rc.Close()
		total += n
		if err != nil {
			return fmt.Errorf("object: read chunk %s: %w", c.ID, err)
		}
		at = next
	}

	// Per-chunk checksums cannot catch a manifest that lost a chunk entry; the
	// byte count can, and a short object must never look complete.
	if total != length {
		return fmt.Errorf("object: short read: %d bytes, manifest promised %d", total, length)
	}
	return nil
}

// readWindow copies take bytes to w after discarding skip, then drains the rest so
// that the reader checks the chunk it has been verifying all along.
//
// The last byte of the window is held back until that check passes. A chunk's
// checksum is only known once the whole chunk has been read, which is *after* its
// bytes would otherwise be on the wire — and for the final chunk of a response
// there is then nothing left to withhold, so a corrupt chunk would arrive as a
// complete, successful, wrong answer. Keeping one byte back means the client only
// ever receives the length it was promised for a chunk that verified; anything else
// is a transfer that stops short, which every client treats as an error. One byte
// of memory, and invariant 3 stops having a hole in its last chunk.
func readWindow(w io.Writer, rc io.Reader, skip, take int64) (int64, error) {
	if _, err := io.CopyN(io.Discard, rc, skip); err != nil {
		return 0, err
	}
	held := &heldBack{w: w}
	n, err := io.CopyN(held, rc, take)
	if err != nil {
		return n, err
	}
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return n, err
	}
	return n, held.release()
}

// heldBack passes writes through but keeps the most recent byte, releasing it only
// when told to.
//
// last is an array rather than a byte so that releasing it does not allocate: a
// fresh []byte{b} per Write is one allocation per 32 KB copied, which measured as
// 4,000 allocations for a 64 MB read.
type heldBack struct {
	w    io.Writer
	last [1]byte
	held bool
}

func (h *heldBack) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := h.release(); err != nil {
		return 0, err
	}
	if len(p) > 1 {
		if _, err := h.w.Write(p[:len(p)-1]); err != nil {
			return 0, err
		}
	}
	h.last[0], h.held = p[len(p)-1], true
	return len(p), nil
}

func (h *heldBack) release() error {
	if !h.held {
		return nil
	}
	h.held = false
	_, err := h.w.Write(h.last[:])
	return err
}
