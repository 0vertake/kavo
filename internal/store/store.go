// Package store implements the local chunk store: immutable, checksummed chunk
// files with a crash-safe commit discipline.
//
// Layout under the store root:
//
//	tmp/                 in-flight writes; wiped on Open
//	chunks/<id[:2]>/<id> committed chunks (raw bytes, no header)
//	meta/<sha256(key)>   object metadata blobs, keyed by object key
//
// Chunk files hold raw data only. Checksums (CRC32C) are computed here but
// persisted by the caller in the object manifest; reads verify against the
// checksum the caller provides.
package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	// ErrNotFound is returned when the requested chunk does not exist.
	ErrNotFound = errors.New("store: chunk not found")
	// ErrChecksumMismatch is returned by a chunk reader when the data read
	// does not match the expected CRC32C. It is always returned before a
	// successful EOF, so a streaming caller can abort instead of delivering
	// corrupt data.
	ErrChecksumMismatch = errors.New("store: chunk checksum mismatch")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Store is a local chunk store rooted at a directory. Chunks are immutable, so
// concurrent writes are safe: writing the same ID twice must only ever happen
// with identical content, and the final rename is atomic either way.
type Store struct {
	tmpDir    string
	chunksDir string
	metaDir   string
}

// Open initializes the store layout under root and discards any leftover
// in-flight writes from a previous crash. Committed chunks are never touched.
func Open(root string) (*Store, error) {
	s := &Store{
		tmpDir:    filepath.Join(root, "tmp"),
		chunksDir: filepath.Join(root, "chunks"),
		metaDir:   filepath.Join(root, "meta"),
	}
	// Files in tmp/ were never acknowledged, so dropping them is always safe.
	if err := os.RemoveAll(s.tmpDir); err != nil {
		return nil, fmt.Errorf("store: clean tmp: %w", err)
	}
	for _, dir := range []string{s.tmpDir, s.chunksDir, s.metaDir} {
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
	return s.durableWrite(filepath.Join(s.chunksDir, id[:2], id), r)
}

// durableWrite streams r into path via tmp/, returning the CRC32C and size of
// the data written. It is the single implementation of the commit discipline:
// write to tmp/ → fsync file → rename into place → fsync parent directory.
// Any error leaves nothing visible at path.
func (s *Store) durableWrite(path string, r io.Reader) (uint32, int64, error) {
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
	size, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		return 0, 0, fmt.Errorf("store: write %s: %w", path, err)
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
	f, err := os.Open(filepath.Join(s.chunksDir, id[:2], id))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return nil, fmt.Errorf("store: open chunk %s: %w", id, err)
	}
	return &verifyingReader{f: f, want: wantCRC}, nil
}

// PutMeta durably stores a metadata blob for an object key, replacing any
// previous one atomically. This is the local stand-in for the etcd commit: the
// object becomes readable exactly when this returns nil.
func (s *Store) PutMeta(key string, data []byte) error {
	_, _, err := s.durableWrite(s.metaPath(key), bytes.NewReader(data))
	return err
}

// GetMeta returns the metadata blob stored for key, or ErrNotFound.
func (s *Store) GetMeta(key string) ([]byte, error) {
	data, err := os.ReadFile(s.metaPath(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("store: read meta %s: %w", key, err)
	}
	return data, nil
}

// metaPath hashes the key so that any legal object key — slashes, dots,
// unicode, up to 1 KiB — maps to a safe fixed-length filename.
func (s *Store) metaPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.metaDir, hex.EncodeToString(sum[:]))
}

func validateID(id string) error {
	if len(id) < 2 || strings.ContainsAny(id, `/\.`) {
		return fmt.Errorf("store: invalid chunk id %q", id)
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
	f    *os.File
	crc  uint32
	want uint32
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.f.Read(p)
	v.crc = crc32.Update(v.crc, castagnoli, p[:n])
	if errors.Is(err, io.EOF) && v.crc != v.want {
		return n, fmt.Errorf("%w: got %08x, want %08x", ErrChecksumMismatch, v.crc, v.want)
	}
	return n, err
}

func (v *verifyingReader) Close() error {
	return v.f.Close()
}
