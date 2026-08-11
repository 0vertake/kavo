package object

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/0vertake/kavo/internal/store"
)

func mustOpen(t *testing.T) (*store.Store, string) {
	t.Helper()
	root := t.TempDir()
	s, err := store.Open(root)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	return s, root
}

// commitTo and fetchFrom place chunks on a local store, which is the simplest
// backing there is; the cluster supplies replicated versions of the same pair.
func commitTo(s *store.Store) func(ChunkRef, []byte) error {
	return func(ref ChunkRef, data []byte) error {
		return s.WriteChunkVerified(ref.ID, bytes.NewReader(data), ref.CRC, ref.Size)
	}
}

func fetchFrom(s *store.Store) func(ChunkRef) (io.ReadCloser, error) {
	return func(ref ChunkRef) (io.ReadCloser, error) {
		return s.ReadChunk(ref.ID, ref.CRC)
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.UintN(256))
	}
	return b
}

// Chunk-boundary handling is the whole trick in Write: exact multiples must
// not leave a trailing empty chunk, and an empty object must produce none.
func TestRoundTripChunkBoundaries(t *testing.T) {
	const chunkSize = 1024
	for _, tc := range []struct {
		size, wantChunks int
	}{
		{0, 0},
		{1, 1},
		{chunkSize - 1, 1},
		{chunkSize, 1},
		{chunkSize + 1, 2},
		{3 * chunkSize, 3},
		{3*chunkSize + 7, 4},
	} {
		t.Run(fmt.Sprint(tc.size), func(t *testing.T) {
			s, _ := mustOpen(t)
			data := randBytes(tc.size)

			m, err := Write(bytes.NewReader(data), chunkSize, commitTo(s))
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			if len(m.Chunks) != tc.wantChunks {
				t.Errorf("got %d chunks, want %d", len(m.Chunks), tc.wantChunks)
			}
			if m.Size != int64(tc.size) {
				t.Errorf("manifest size = %d, want %d", m.Size, tc.size)
			}

			var got bytes.Buffer
			if err := Read(m, &got, fetchFrom(s)); err != nil {
				t.Fatalf("Read: %v", err)
			}
			if !bytes.Equal(got.Bytes(), data) {
				t.Error("read data differs from written data")
			}
		})
	}
}

// Invariant: a corrupted chunk surfaces as an error, never as a short or
// silently wrong object.
func TestCorruptChunkFailsRead(t *testing.T) {
	s, root := mustOpen(t)
	data := randBytes(4096)

	m, err := Write(bytes.NewReader(data), 1024, commitTo(s))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	id := m.Chunks[2].ID
	path := filepath.Join(root, "chunks", id[:2], id)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := Read(m, io.Discard, fetchFrom(s)); !errors.Is(err, store.ErrChecksumMismatch) {
		t.Fatalf("Read error = %v, want store.ErrChecksumMismatch", err)
	}
}

// Invariant: a failed upload never yields a usable manifest. Returning a
// partial one would make a truncated object committable.
func TestWriteFailsMidStream(t *testing.T) {
	s, _ := mustOpen(t)
	boom := errors.New("client disconnected")
	src := io.MultiReader(bytes.NewReader(randBytes(1500)), failingReader{err: boom})

	m, err := Write(src, 1024, commitTo(s))
	if !errors.Is(err, boom) {
		t.Fatalf("Write error = %v, want %v", err, boom)
	}
	if m.Size != 0 || len(m.Chunks) != 0 {
		t.Fatalf("Write returned usable manifest %+v on failure, want zero value", m)
	}
}

// A manifest that lost a chunk entry has no bad checksum to catch it, so the
// byte count has to.
func TestShortManifestFailsRead(t *testing.T) {
	s, _ := mustOpen(t)
	m, err := Write(bytes.NewReader(randBytes(4096)), 1024, commitTo(s))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	m.Chunks = m.Chunks[:len(m.Chunks)-1] // manifest still claims the full size

	if err := Read(m, io.Discard, fetchFrom(s)); err == nil {
		t.Fatal("Read succeeded on a manifest missing a chunk, want error")
	}
}

func TestMissingChunkFailsRead(t *testing.T) {
	s, _ := mustOpen(t)
	m := Manifest{Size: 1, Chunks: []ChunkRef{{ID: "nosuchchunk", Size: 1}}}

	if err := Read(m, io.Discard, fetchFrom(s)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Read error = %v, want store.ErrNotFound", err)
	}
}

// Milestone 1's headline claim: memory stays flat regardless of object size.
// 64 MB through 1 MB chunks must not grow the heap anywhere near 64 MB.
func TestStreamingIsConstantMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates 64 MB of I/O")
	}
	const (
		chunkSize = 1 << 20
		objSize   = 64 << 20
		maxAlloc  = 8 << 20
	)
	s, _ := mustOpen(t)

	// TotalAlloc is monotonic, so it measures how many bytes the data path
	// allocated in total — buffering the object would show up here even if the
	// GC reclaimed it before the test looked.
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	m, err := Write(io.LimitReader(zeroReader{}, objSize), chunkSize, commitTo(s))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := Read(m, io.Discard, fetchFrom(s)); err != nil {
		t.Fatalf("Read: %v", err)
	}

	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("allocated %d bytes streaming %d bytes in and out", allocated, objSize)
	if allocated > maxAlloc {
		t.Errorf("allocated %d bytes streaming %d bytes, want <= %d", allocated, objSize, maxAlloc)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { return len(p), nil }

type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) { return 0, f.err }
