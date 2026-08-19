// Package store implements the local chunk store: immutable, checksummed chunk
// files with a crash-safe commit discipline.
//
// Layout under the store root:
//
//	tmp/                 in-flight writes; wiped on Open
//	chunks/<id[:2]>/<id> committed chunks (raw bytes, no header)
//
// Chunk files hold raw data only. Checksums (CRC32C) are computed here but
// persisted by the caller in the object manifest; reads verify against the
// checksum the caller provides.
package store

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	// ErrNotFound is returned when the requested chunk does not exist.
	ErrNotFound = errors.New("store: chunk not found")
	// ErrChecksumMismatch is returned by a chunk reader when the data read
	// does not match the expected CRC32C. It is always returned before a
	// successful EOF, so a streaming caller can abort instead of delivering
	// corrupt data.
	ErrChecksumMismatch = errors.New("store: chunk checksum mismatch")
	// ErrInvalidID reports a chunk id that is malformed or unsafe as a path.
	// Chunk ids arrive from other nodes, so this says the caller sent nonsense,
	// not that this node is unhealthy.
	ErrInvalidID = errors.New("store: invalid chunk id")
	// ErrVerificationFailed is the write-side counterpart: a chunk offered by
	// another node did not match the checksum or length that node declared, so
	// it was rejected instead of committed. It says the sender's bytes are at
	// fault, not this node's disk.
	ErrVerificationFailed = errors.New("store: chunk failed verification")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Store is a local chunk store rooted at a directory. Chunks are immutable, so
// concurrent writes are safe: writing the same ID twice must only ever happen
// with identical content, and the final rename is atomic either way.
type Store struct {
	tmpDir    string
	chunksDir string
}

// Open initializes the store layout under root and discards any leftover
// in-flight writes from a previous crash. Committed chunks are never touched.
func Open(root string) (*Store, error) {
	s := &Store{
		tmpDir:    filepath.Join(root, "tmp"),
		chunksDir: filepath.Join(root, "chunks"),
	}
	// Files in tmp/ were never acknowledged, so dropping them is always safe.
	if err := os.RemoveAll(s.tmpDir); err != nil {
		return nil, fmt.Errorf("store: clean tmp: %w", err)
	}
	for _, dir := range []string{s.tmpDir, s.chunksDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: init %s: %w", dir, err)
		}
	}
	return s, nil
}

// WriteChunk durably persists the contents of r as chunk id and returns the
// CRC32C and size of the data written.
//
// Commit discipline: write to tmp/ → fsync file → rename into chunks/ →
// fsync parent directory. Any error — including fsync errors, which are
// treated as data loss and never retried — leaves no visible chunk behind.
func (s *Store) WriteChunk(id string, r io.Reader) (uint32, int64, error) {
	if err := validateID(id); err != nil {
		return 0, 0, err
	}
	return s.durableWrite(s.chunkPath(id), r, 0, nil)
}

// WriteChunkVerified durably persists r as chunk id, but only if its contents
// match wantCRC and wantSize; otherwise nothing is committed and the mismatch
// is returned.
//
// This is the path for chunks arriving from another node. Committing a chunk
// without checking it would make the write quorum a promise about data nobody
// validated: the coordinator would count the ack, and the corruption would
// surface later as an unreadable object.
func (s *Store) WriteChunkVerified(id string, r io.Reader, wantCRC uint32, wantSize int64) error {
	if err := validateID(id); err != nil {
		return err
	}
	verify := func(crc uint32, size int64) error {
		if size != wantSize {
			return fmt.Errorf("%w: chunk %s is %d bytes, declared %d", ErrVerificationFailed, id, size, wantSize)
		}
		if crc != wantCRC {
			return fmt.Errorf("%w: chunk %s is %08x, declared %08x", ErrVerificationFailed, id, crc, wantCRC)
		}
		return nil
	}
	_, _, err := s.durableWrite(s.chunkPath(id), r, wantSize, verify)
	return err
}

// copySize is how much of a chunk to move per write syscall, given the size the
// caller expects.
//
// io.Copy's own buffer is 32 KB, which for a 32 MB chunk arriving from a peer is
// a thousand write syscalls, and six nodes doing that at once spend most of their
// CPU in the kernel: a 64 MB write across eight clients gained ~25% here. 1 MB
// measured no better than 256 KB and holds four times as much per in-flight
// chunk, and a node under load has one of these per chunk being received.
//
// Bounded below by io.Copy's own default, so a chunk of unknown length is never
// worse than the standard library. Bounded above because expect arrives from
// another node as a declared length: uncapped, one lying Content-Length would be
// an allocation of whatever size that node asked for.
func copySize(expect int64) int64 { return min(max(expect, 32<<10), 256<<10) }

// durableWrite streams r into path via tmp/, returning the CRC32C and size of
// the data written. It is the single implementation of the commit discipline:
// write to tmp/ → fsync file → rename into place → fsync parent directory.
// Any error leaves nothing visible at path.
//
// expect is the length the caller declares, and is only ever a hint: it sizes
// the copy buffer and nothing else. What was actually written is returned.
//
// verify, when non-nil, inspects the staged data before it is committed; if it
// returns an error, nothing is committed.
func (s *Store) durableWrite(path string, r io.Reader, expect int64, verify func(crc uint32, size int64) error) (uint32, int64, error) {
	f, err := os.CreateTemp(s.tmpDir, "w.*")
	if err != nil {
		return 0, 0, fmt.Errorf("store: create temp: %w", err)
	}
	tmpPath := f.Name()
	committed := false
	defer func() {
		if !committed {
			f.Close()
			os.Remove(tmpPath)
		}
	}()

	h := crc32.New(castagnoli)
	// A reader that can write itself lands the whole chunk in one call and never
	// looks at a buffer, so allocating one for it is pure garbage — a megabyte per
	// local write. io.CopyBuffer takes that path before it touches buf.
	var buf []byte
	if _, ok := r.(io.WriterTo); !ok {
		buf = make([]byte, copySize(expect))
	}
	size, err := io.CopyBuffer(io.MultiWriter(f, h), r, buf)
	if err != nil {
		return 0, 0, fmt.Errorf("store: write %s: %w", path, err)
	}
	// Check before fsyncing: data that is about to be discarded is not worth a
	// disk barrier, and nothing is visible at path until the rename either way.
	if verify != nil {
		if err := verify(h.Sum32(), size); err != nil {
			return 0, 0, err
		}
	}
	if err := f.Sync(); err != nil {
		// A failed fsync may have dropped the pages while marking them clean,
		// so the data is gone; never retry and trust it (fsyncgate).
		return 0, 0, fmt.Errorf("store: fsync %s (data lost, not retried): %w", path, err)
	}
	if err := f.Close(); err != nil {
		return 0, 0, fmt.Errorf("store: close %s: %w", path, err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, 0, fmt.Errorf("store: mkdir %s: %w", dir, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return 0, 0, fmt.Errorf("store: commit %s: %w", path, err)
	}
	// The rename is not durable until the directory entry is synced.
	if err := syncDir(dir); err != nil {
		return 0, 0, fmt.Errorf("store: fsync dir %s (data lost, not retried): %w", dir, err)
	}
	committed = true
	return h.Sum32(), size, nil
}

// ReadChunk streams chunk id, verifying its contents against wantCRC. If the
// data does not match, the reader returns ErrChecksumMismatch instead of a
// final EOF; corrupt data is never delivered as a complete, successful read.
func (s *Store) ReadChunk(id string, wantCRC uint32) (io.ReadCloser, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	f, err := os.Open(s.chunkPath(id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return nil, fmt.Errorf("store: open chunk %s: %w", id, err)
	}
	return Verify(f, wantCRC), nil
}

// HasChunk reports whether this node holds chunk id.
//
// Presence only: the checksum is not read, so a chunk that has rotted still
// counts as present. Repair uses this to find missing copies, which is a
// different question from whether the copies that exist are still good.
func (s *Store) HasChunk(id string) (bool, error) {
	if err := validateID(id); err != nil {
		return false, err
	}
	_, err := os.Stat(s.chunkPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: stat chunk %s: %w", id, err)
	}
	return true, nil
}

// HasChunks reports which of the given IDs this node holds. The returned slice
// contains only the IDs that are present; invalid IDs are silently skipped.
// Same presence-not-integrity caveat as HasChunk.
func (s *Store) HasChunks(ids []string) ([]string, error) {
	var have []string
	for _, id := range ids {
		held, err := s.HasChunk(id)
		if errors.Is(err, ErrInvalidID) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if held {
			have = append(have, id)
		}
	}
	return have, nil
}

// ChunkInfo is what a sweep can learn about a chunk without opening it: which
// chunk it is, how much space it occupies, and when it was last written.
//
// The time is the load-bearing field. A chunk is durable before the manifest that
// references it is committed, so at any moment some of what is on disk is
// referenced by nothing and about to be — and telling that apart from real garbage
// is a question about age and nothing else.
type ChunkInfo struct {
	ID       string
	Size     int64
	Modified time.Time
}

// ScanChunks visits every chunk on this node whose id begins with prefix, which
// is how a caller sweeps the disk a slice at a time instead of holding a set of
// every chunk id it holds.
//
// Chunks live in one directory per two-character id prefix, so a one-character
// prefix reads a thirty-second of them and a two-character prefix reads one
// directory. A caller that wants everything passes "".
func (s *Store) ScanChunks(prefix string, visit func(ChunkInfo) error) error {
	dirs, err := os.ReadDir(s.chunksDir)
	if err != nil {
		return fmt.Errorf("store: list chunk directories: %w", err)
	}
	for _, dir := range dirs {
		if !dir.IsDir() || !within(dir.Name(), prefix) {
			continue
		}
		path := filepath.Join(s.chunksDir, dir.Name())
		files, err := os.ReadDir(path)
		if err != nil {
			// A directory that vanished mid-scan is a directory that had nothing
			// left in it, which is not a reason to abandon the rest.
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("store: list chunks in %s: %w", dir.Name(), err)
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasPrefix(f.Name(), prefix) {
				continue
			}
			info, err := f.Info()
			if err != nil {
				// Gone since the directory was read: a chunk being replaced by a
				// repair, or one another pass has already collected.
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return fmt.Errorf("store: stat chunk %s: %w", f.Name(), err)
			}
			if err := visit(ChunkInfo{
				ID:       f.Name(),
				Size:     info.Size(),
				Modified: info.ModTime(),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// within reports whether a two-character chunk directory can hold ids beginning
// with prefix.
func within(dir, prefix string) bool {
	if len(prefix) > len(dir) {
		return strings.HasPrefix(prefix, dir)
	}
	return strings.HasPrefix(dir, prefix)
}

// RemoveChunk deletes this node's copy of a chunk, reporting success if it was
// already gone.
//
// Only for a chunk no manifest points at any more: after a rebalance has placed
// it elsewhere and committed the manifest that says so, or after a garbage
// collection pass has read every manifest in the cluster and found none that
// names it. Deleting a chunk a manifest still references is how an acknowledged
// write is lost, which is why nothing on the read or repair paths calls this.
func (s *Store) RemoveChunk(id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	if err := os.Remove(s.chunkPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("store: remove chunk %s: %w", id, err)
	}
	return nil
}

// Verify wraps r so that reads fail with ErrChecksumMismatch instead of
// returning a final EOF unless the bytes read match want.
//
// Exported for chunks in transit between nodes: TCP's 16-bit checksum is not
// strong enough to be the only thing standing between a flipped bit on the wire
// and a committed replica.
func Verify(r io.ReadCloser, want uint32) io.ReadCloser {
	return &verifyingReader{r: r, want: want}
}

func (s *Store) chunkPath(id string) string {
	return filepath.Join(s.chunksDir, id[:2], id)
}

func validateID(id string) error {
	if len(id) < 2 || strings.ContainsAny(id, `/\.`) {
		return fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// verifyingReader computes CRC32C over everything read and turns the final
// EOF into ErrChecksumMismatch when the checksum does not match.
type verifyingReader struct {
	r    io.ReadCloser
	crc  uint32
	want uint32
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	v.crc = crc32.Update(v.crc, castagnoli, p[:n])
	if errors.Is(err, io.EOF) && v.crc != v.want {
		return n, fmt.Errorf("%w: got %08x, want %08x", ErrChecksumMismatch, v.crc, v.want)
	}
	return n, err
}

func (v *verifyingReader) Close() error { return v.r.Close() }
