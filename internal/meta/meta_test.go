package meta

import (
	"context"
	"crypto/rand"
	"errors"
	"reflect"
	"testing"

	"github.com/0vertake/kavo/internal/object"
)

// newStore gives each test its own prefix so tests cannot see each other's keys.
// Tests here run against a real etcd rather than a fake, because what is being
// tested is exactly what a fake would have to invent: atomic replacement and
// durability.
func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open([]string{EndpointFromEnv()}, "/kavo-test/"+rand.Text())
	if err != nil {
		t.Fatalf("meta.Open (is etcd up? try `make etcd`): %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func manifest(size int64, ids ...string) object.Manifest {
	m := object.Manifest{Size: size}
	for i, id := range ids {
		m.Chunks = append(m.Chunks, object.ChunkRef{ID: id, Size: size, CRC: uint32(i + 1)})
	}
	return m
}

// Object keys are arbitrary bytes from the client. Two different keys must never
// land on the same etcd key, whatever they contain.
func TestCommitGetRoundTrip(t *testing.T) {
	keys := []string{
		"simple",
		"bucket/nested/path/file.name.mp4",
		"trailing/slash/",
		"/leading/slash",
		"double//slash",
		"../not/a/path",
		"unicode/ключ/文字",
		"with spaces and ?query=like#fragment",
	}
	s := newStore(t)
	ctx := context.Background()

	for i, key := range keys {
		want := manifest(int64(i+1)*1024, "chunkA", "chunkB")
		if err := s.Commit(ctx, key, want); err != nil {
			t.Fatalf("Commit(%q): %v", key, err)
		}
		got, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if got.Size != want.Size || len(got.Chunks) != len(want.Chunks) {
			t.Fatalf("Get(%q) = %+v, want %+v", key, got, want)
		}
		for j, c := range got.Chunks {
			if !reflect.DeepEqual(c, want.Chunks[j]) {
				t.Errorf("Get(%q) chunk %d = %+v, want %+v", key, j, c, want.Chunks[j])
			}
		}
	}

	// Distinct keys must have stayed distinct rather than collapsing onto each
	// other: every manifest committed above still has its own size.
	for i, key := range keys {
		got, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if want := int64(i+1) * 1024; got.Size != want {
			t.Errorf("Get(%q).Size = %d, want %d: keys collided", key, got.Size, want)
		}
	}
}

func TestGetUncommittedObject(t *testing.T) {
	if _, err := newStore(t).Get(context.Background(), "never/written"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get error = %v, want ErrNotFound", err)
	}
}

// An overwrite must swap the whole manifest. A reader sees the old object or the
// new one, never chunks from both.
func TestOverwriteReplacesWholeManifest(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	if err := s.Commit(ctx, "k", manifest(4096, "old1", "old2", "old3")); err != nil {
		t.Fatal(err)
	}
	second := manifest(10, "new1")
	if err := s.Commit(ctx, "k", second); err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if got.Size != second.Size || len(got.Chunks) != 1 || got.Chunks[0].ID != "new1" {
		t.Errorf("Get after overwrite = %+v, want exactly %+v", got, second)
	}
}
