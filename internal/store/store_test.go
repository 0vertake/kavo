package store

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustOpen(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func writeChunk(t *testing.T, s *Store, id string, data []byte) uint32 {
	t.Helper()
	crc, size, err := s.WriteChunk(id, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteChunk(%s): %v", id, err)
	}
	if size != int64(len(data)) {
		t.Fatalf("WriteChunk(%s) size = %d, want %d", id, size, len(data))
	}
	return crc
}

func readChunk(t *testing.T, s *Store, id string, crc uint32) ([]byte, error) {
	t.Helper()
	r, err := s.ReadChunk(id, crc)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func TestWriteReadRoundTrip(t *testing.T) {
	s := mustOpen(t)
	data := make([]byte, 4<<20) // spans multiple internal read buffers
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	crc := writeChunk(t, s, "abc123", data)

	got, err := readChunk(t, s, "abc123", crc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("read data differs from written data")
	}
}

func TestEmptyChunk(t *testing.T) {
	s := mustOpen(t)
	crc := writeChunk(t, s, "empty1", nil)

	got, err := readChunk(t, s, "empty1", crc)
	if err != nil || len(got) != 0 {
		t.Fatalf("read = (%d bytes, %v), want (0, nil)", len(got), err)
	}
}

// Invariant: a read returns checksum-valid data or an explicit error, never
// silent corruption. Byte-flipping on disk is what the chaos suite will do.
func TestCorruptionDetected(t *testing.T) {
	s := mustOpen(t)
	data := bytes.Repeat([]byte("kavo"), 1<<16)
	crc := writeChunk(t, s, "corrupt1", data)

	path := filepath.Join(s.chunksDir, "co", "corrupt1")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := readChunk(t, s, "corrupt1", crc); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("read error = %v, want ErrChecksumMismatch", err)
	}
}

// Invariant: a partially written chunk is never visible to a reader.
func TestFailedWriteLeavesNothingVisible(t *testing.T) {
	s := mustOpen(t)
	boom := errors.New("upstream failure")
	src := io.MultiReader(strings.NewReader("partial data"), &failingReader{err: boom})

	if _, _, err := s.WriteChunk("failed1", src); !errors.Is(err, boom) {
		t.Fatalf("WriteChunk error = %v, want %v", err, boom)
	}
	if _, err := s.ReadChunk("failed1", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadChunk after failed write = %v, want ErrNotFound", err)
	}
	entries, err := os.ReadDir(s.tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("tmp dir has %d leftover files after failed write", len(entries))
	}
}

// Restart semantics: committed chunks survive, in-flight writes do not.
func TestReopenAfterCrash(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	crc := writeChunk(t, s, "durable1", []byte("committed"))
	stray := filepath.Join(root, "tmp", "chunk1.12345")
	if err := os.WriteFile(stray, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(root)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	if _, err := os.Stat(stray); !errors.Is(err, os.ErrNotExist) {
		t.Error("stray temp file survived re-Open")
	}
	got, err := readChunk(t, s2, "durable1", crc)
	if err != nil || string(got) != "committed" {
		t.Fatalf("read after re-Open = (%q, %v), want (\"committed\", nil)", got, err)
	}
}

func TestReadMissingChunk(t *testing.T) {
	s := mustOpen(t)
	if _, err := s.ReadChunk("nosuch", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadChunk = %v, want ErrNotFound", err)
	}
}

// Chunk IDs reach the store from network input; a path-traversing id must
// never escape the store root.
func TestInvalidIDs(t *testing.T) {
	s := mustOpen(t)
	for _, id := range []string{"", "a", "../escape", "a/b", `a\b`, "dot.dot"} {
		if _, _, err := s.WriteChunk(id, strings.NewReader("x")); err == nil {
			t.Errorf("WriteChunk(%q) accepted invalid id", id)
		}
		if _, err := s.ReadChunk(id, 0); err == nil {
			t.Errorf("ReadChunk(%q) accepted invalid id", id)
		}
	}
}

type failingReader struct{ err error }

func (f *failingReader) Read([]byte) (int, error) { return 0, f.err }
