package store

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
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

// A chunk arriving from a peer is only committed if it matches what the sender
// declared. Anything else must leave no trace: a committed-but-wrong chunk would
// be counted towards a write quorum and only found to be bad much later.
func TestWriteChunkVerified(t *testing.T) {
	data := []byte("chunk from a peer")
	goodCRC := crc32.Checksum(data, castagnoli)
	goodSize := int64(len(data))

	tests := []struct {
		name   string
		crc    uint32
		size   int64
		reject bool
	}{
		{name: "matching", crc: goodCRC, size: goodSize},
		{name: "corrupted in transit", crc: goodCRC ^ 0xffff, size: goodSize, reject: true},
		{name: "truncated in transit", crc: goodCRC, size: goodSize + 10, reject: true},
		{name: "longer than declared", crc: goodCRC, size: goodSize - 1, reject: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := mustOpen(t)
			err := s.WriteChunkVerified("peer01", bytes.NewReader(data), tt.crc, tt.size)

			if !tt.reject {
				if err != nil {
					t.Fatalf("WriteChunkVerified: %v", err)
				}
				got, err := readChunk(t, s, "peer01", goodCRC)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("read = (%q, %v), want (%q, nil)", got, err, data)
				}
				return
			}

			if !errors.Is(err, ErrVerificationFailed) {
				t.Fatalf("WriteChunkVerified error = %v, want ErrVerificationFailed", err)
			}
			// Whatever the reason for rejection, nothing may be readable and no
			// staged file may be left behind.
			if _, err := s.ReadChunk("peer01", goodCRC); !errors.Is(err, ErrNotFound) {
				t.Errorf("chunk is visible after a rejected write: %v", err)
			}
			entries, err := os.ReadDir(s.tmpDir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 0 {
				t.Errorf("tmp dir has %d leftover files after a rejected write", len(entries))
			}
		})
	}
}

func TestVerifyRejectsAlteredStream(t *testing.T) {
	data := []byte("bytes on the wire")
	crc := crc32.Checksum(data, castagnoli)
	altered := append([]byte(nil), data...)
	altered[3] ^= 0xff

	read := func(b []byte) error {
		_, err := io.ReadAll(Verify(io.NopCloser(bytes.NewReader(b)), crc))
		return err
	}
	if err := read(altered); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Verify error = %v, want ErrChecksumMismatch", err)
	}
	if err := read(data); err != nil {
		t.Fatalf("Verify rejected intact data: %v", err)
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
		if _, _, err := s.WriteChunk(id, strings.NewReader("x")); !errors.Is(err, ErrInvalidID) {
			t.Errorf("WriteChunk(%q) error = %v, want ErrInvalidID", id, err)
		}
		if _, err := s.ReadChunk(id, 0); !errors.Is(err, ErrInvalidID) {
			t.Errorf("ReadChunk(%q) error = %v, want ErrInvalidID", id, err)
		}
	}
}

// A chunk arriving from a peer is copied to disk one buffer at a time, and each
// buffer is a write syscall: at io.Copy's own 32 KB that is a thousand syscalls
// for a 32 MB chunk, which halved write throughput under load. This pins the
// property rather than the constant, by recording how much the store asks the
// stream for at once.
func TestStreamedChunksAreWrittenInBigPieces(t *testing.T) {
	s := mustOpen(t)
	data := make([]byte, 4<<20)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	crc := crc32.Checksum(data, castagnoli)

	// Wrapped so it is only an io.Reader: a *bytes.Reader can write itself and
	// would bypass the buffer, which is the case peers do not get.
	r := &readSizes{r: bytes.NewReader(data)}
	if err := s.WriteChunkVerified("stream", r, crc, int64(len(data))); err != nil {
		t.Fatalf("WriteChunkVerified: %v", err)
	}
	if want := 256 << 10; r.largest < want {
		t.Errorf("the largest read from the stream was %d bytes, want at least %d", r.largest, want)
	}
	// The other half of streaming: the buffer is bounded, so it cannot grow into
	// holding a meaningful part of the chunk however big the chunk gets.
	if bound := len(data) / 4; r.largest > bound {
		t.Errorf("a %d-byte chunk was read %d bytes at a time, want at most %d", len(data), r.largest, bound)
	}

	got, err := readChunk(t, s, "stream", crc)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back = (%d bytes, %v), want the %d bytes written", len(got), err, len(data))
	}

	// The declared length comes from another node, so it must not be able to ask
	// this one for an allocation. A terabyte offered as five bytes gets rejected
	// on its contents, but the buffer is bounded long before that.
	liar := &readSizes{r: bytes.NewReader(data)}
	if err := s.WriteChunkVerified("liar", liar, crc, 1<<40); !errors.Is(err, ErrVerificationFailed) {
		t.Fatalf("WriteChunkVerified with a false length = %v, want ErrVerificationFailed", err)
	}
	if liar.largest > 256<<10 {
		t.Errorf("a declared length of 1 TB got a %d-byte buffer", liar.largest)
	}
}

// The buffer is sized to the chunk, so writing a five-byte chunk must not pay a
// megabyte for it.
func TestSmallChunksDoNotGetABigBuffer(t *testing.T) {
	s := mustOpen(t)
	data := []byte("small")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range 8 {
		r := &readSizes{r: bytes.NewReader(data)}
		if _, _, err := s.WriteChunk(fmt.Sprintf("small%02d", i), r); err != nil {
			t.Fatalf("WriteChunk: %v", err)
		}
	}
	runtime.ReadMemStats(&after)

	if grew := after.TotalAlloc - before.TotalAlloc; grew > 1<<20 {
		t.Errorf("8 five-byte writes allocated %d bytes, want nowhere near a 1 MB buffer each", grew)
	}
}

// readSizes is an io.Reader that hides whatever else its source can do, and
// remembers the largest read it was asked for.
type readSizes struct {
	r       io.Reader
	largest int
}

func (s *readSizes) Read(p []byte) (int, error) {
	s.largest = max(s.largest, len(p))
	return s.r.Read(p)
}

type failingReader struct{ err error }

func (f *failingReader) Read([]byte) (int, error) { return 0, f.err }
