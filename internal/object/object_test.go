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
func commitTo(s *store.Store) func(*ChunkRef, []byte) error {
	return func(ref *ChunkRef, data []byte) error {
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

// The write buffer starts at one small object and grows to a full chunk only
// when the source turns out to be bigger. That growth is invisible in the
// manifest, which is exactly why it needs its own test: every other test here
// uses a chunk size below the small-object size, so none of them ever reach it.
func TestWriteGrowsItsBufferWithoutChangingTheObject(t *testing.T) {
	const chunkSize = 4 * smallObject
	for _, size := range []int64{
		1,
		smallObject - 1,
		smallObject, // exactly one buffer: must not grow, and must not lose the object
		smallObject + 1,
		chunkSize,
		chunkSize + smallObject,
	} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s, _ := mustOpen(t)
			data := randBytes(int(size))

			m, err := Write(bytes.NewReader(data), chunkSize, commitTo(s))
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			if want := (size + chunkSize - 1) / chunkSize; int64(len(m.Chunks)) != want {
				t.Errorf("got %d chunks, want %d", len(m.Chunks), want)
			}
			if m.Size != size {
				t.Errorf("manifest size = %d, want %d", m.Size, size)
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

// The reason the buffer grows at all: an object far smaller than a chunk must
// not allocate one. Measured before this, a 4 KB object allocated 32 MB.
func TestSmallObjectDoesNotAllocateAChunk(t *testing.T) {
	const chunkSize = 32 * smallObject
	s, _ := mustOpen(t)
	data := randBytes(4 << 10)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := Write(bytes.NewReader(data), chunkSize, commitTo(s)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	runtime.ReadMemStats(&after)

	// One small buffer plus overhead, nowhere near a chunk.
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 4*smallObject {
		t.Errorf("writing 4 KB allocated %d bytes, want well under one %d-byte chunk",
			grew, chunkSize)
	}
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

// The corruption that used to get through: rot in the object's **last** chunk.
// A chunk's checksum is only known once the whole chunk has been read, and for the
// last chunk there is then nothing left to withhold — so every byte had already
// been handed over and the caller received a complete, wrong object with an error
// nobody downstream could see. Found by the chaos suite on a single-chunk object,
// which is the worst version of it: the last chunk is the only chunk.
//
// The reader now has to be short by at least one byte whenever it errors, because
// short is the only remaining way to say "do not trust this".
func TestCorruptFinalChunkIsNotDeliveredWhole(t *testing.T) {
	sizes := []struct {
		name   string
		size   int
		chunks int64
	}{
		{name: "one chunk, which is also the last", size: 900, chunks: 1024},
		{name: "several chunks, rot in the last", size: 4096, chunks: 1024},
	}
	for _, tt := range sizes {
		t.Run(tt.name, func(t *testing.T) {
			s, root := mustOpen(t)
			data := randBytes(tt.size)
			m, err := Write(bytes.NewReader(data), tt.chunks, commitTo(s))
			if err != nil {
				t.Fatalf("Write: %v", err)
			}

			last := m.Chunks[len(m.Chunks)-1].ID
			path := filepath.Join(root, "chunks", last[:2], last)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// The final byte, so that everything before it has been read and
			// delivered before the mismatch is known.
			raw[len(raw)-1] ^= 0xff
			if err := os.WriteFile(path, raw, 0o644); err != nil {
				t.Fatal(err)
			}

			var got bytes.Buffer
			err = Read(m, &got, fetchFrom(s))
			if !errors.Is(err, store.ErrChecksumMismatch) {
				t.Fatalf("Read error = %v, want store.ErrChecksumMismatch", err)
			}
			if got.Len() >= tt.size {
				t.Errorf("delivered %d of %d bytes before failing: a caller that trusts "+
					"Content-Length cannot tell this from success", got.Len(), tt.size)
			}
		})
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
