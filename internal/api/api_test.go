package api

import (
	"bytes"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/0vertake/kavo/internal/store"
)

const chunkSize = 1024

func newServer(t *testing.T, root string) *httptest.Server {
	t.Helper()
	s, err := store.Open(root)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	srv := httptest.NewServer(New(s, chunkSize))
	t.Cleanup(srv.Close)
	return srv
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.UintN(256))
	}
	return b
}

func put(t *testing.T, srv *httptest.Server, key string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/objects/"+key, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", key, err)
	}
	resp.Body.Close()
	return resp
}

func get(t *testing.T, srv *httptest.Server, key string) (*http.Response, []byte, error) {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + "/objects/" + key)
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp, body, err
}

func TestPutGetRoundTrip(t *testing.T) {
	srv := newServer(t, t.TempDir())
	// Several chunks plus a partial one, and a key with slashes and dots.
	data := randBytes(3*chunkSize + 17)
	key := "bucket/nested/path/file.name.mp4"

	if resp := put(t, srv, key, data); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
	resp, got, err := get(t, srv, key)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	if resp.ContentLength != int64(len(data)) {
		t.Errorf("Content-Length = %d, want %d", resp.ContentLength, len(data))
	}
	if !bytes.Equal(got, data) {
		t.Error("downloaded body differs from uploaded body")
	}
}

func TestGetMissing(t *testing.T) {
	srv := newServer(t, t.TempDir())
	resp, _, _ := get(t, srv, "nope")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", resp.StatusCode)
	}
}

func TestEmptyObject(t *testing.T) {
	srv := newServer(t, t.TempDir())
	put(t, srv, "empty", nil)

	resp, got, err := get(t, srv, "empty")
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK || len(got) != 0 {
		t.Fatalf("GET = (%d, %d bytes), want (200, 0)", resp.StatusCode, len(got))
	}
}

// Overwriting a key must swap the manifest atomically, with no trace of the
// previous object's content in the response.
func TestOverwrite(t *testing.T) {
	srv := newServer(t, t.TempDir())
	put(t, srv, "k", randBytes(3*chunkSize))
	second := randBytes(17)
	put(t, srv, "k", second)

	_, got, err := get(t, srv, "k")
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, second) {
		t.Errorf("got %d bytes, want the %d bytes of the second upload", len(got), len(second))
	}
}

// The manifest commit is the durability point: an acknowledged PUT must still
// be readable by a freshly opened store.
func TestSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	data := randBytes(2 * chunkSize)
	put(t, newServer(t, root), "persisted", data)

	_, got, err := get(t, newServer(t, root), "persisted")
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("object did not survive restart intact")
	}
}

// Invariant end to end: a corrupted chunk must reach the client as a failed
// transfer, never as a complete object with wrong bytes.
func TestCorruptChunkFailsDownload(t *testing.T) {
	root := t.TempDir()
	srv := newServer(t, root)
	data := randBytes(3 * chunkSize)
	put(t, srv, "rotten", data)

	corruptOneChunk(t, filepath.Join(root, "chunks"))

	resp, got, err := get(t, srv, "rotten")
	if err == nil && bytes.Equal(got, data) {
		t.Fatal("corrupt chunk was served as a valid object")
	}
	if err == nil {
		t.Fatalf("GET returned %d and %d bytes with no error, want a failed transfer",
			resp.StatusCode, len(got))
	}
}

func corruptOneChunk(t *testing.T, chunksDir string) {
	t.Helper()
	found := false
	err := filepath.WalkDir(chunksDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || found {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		raw[0] ^= 0xff
		found = true
		return os.WriteFile(path, raw, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("no chunk files found to corrupt")
	}
}
