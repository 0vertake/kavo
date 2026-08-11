package api

import (
	"bufio"
	"bytes"
	"cmp"
	crand "crypto/rand"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/peer"
	"github.com/0vertake/kavo/internal/store"
)

const chunkSize = 1024

// newServer starts a node whose chunks live under root. Manifests go to a real
// etcd under a per-test prefix, so tests are isolated without faking the commit
// point that everything else depends on.
func newServer(t *testing.T, root string) *httptest.Server {
	t.Helper()
	return newServerWithPrefix(t, root, "/kavo-test/"+crand.Text())
}

func newServerWithPrefix(t *testing.T, root, prefix string) *httptest.Server {
	t.Helper()
	s, err := store.Open(root)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	m, err := meta.Open([]string{meta.EndpointFromEnv()}, prefix)
	if err != nil {
		t.Fatalf("meta.Open (is etcd up? try `make etcd`): %v", err)
	}
	t.Cleanup(func() { m.Close() })

	// A cluster of one. Its own address is never dialled, because the
	// coordinator writes local chunks through the store directly.
	c := cluster.New("n1", "127.0.0.1:0", s, m, chunkSize)

	srv := httptest.NewServer(New(c, s))
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

// The manifest commit is the durability point: an acknowledged PUT must still be
// readable by a node that reopens the same chunks and the same cluster prefix.
func TestSurvivesRestart(t *testing.T) {
	root, prefix := t.TempDir(), "/kavo-test/"+crand.Text()
	data := randBytes(2 * chunkSize)
	put(t, newServerWithPrefix(t, root, prefix), "persisted", data)

	_, got, err := get(t, newServerWithPrefix(t, root, prefix), "persisted")
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

// The peer endpoint is the door other nodes push data through, so its rejections
// have to be precise. A 5xx tells a coordinator this node is faulty and may take
// it out of rotation; malformed or short input from the sender must never do that.
func TestPeerChunkPushRejections(t *testing.T) {
	data := []byte("a chunk from a peer")
	crc := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))

	tests := []struct {
		name      string
		id        string // defaults to a valid id
		crcHeader string
		chunked   bool
		want      int
	}{
		{name: "valid", crcHeader: fmt.Sprintf("%08x", crc), want: http.StatusOK},
		{name: "missing checksum header", want: http.StatusBadRequest},
		{name: "malformed checksum header", crcHeader: "not-hex", want: http.StatusBadRequest},
		{name: "wrong checksum", crcHeader: "deadbeef", want: http.StatusBadRequest},
		{name: "unknown length", crcHeader: fmt.Sprintf("%08x", crc), chunked: true, want: http.StatusLengthRequired},
		// A bare ".." is normalised away by the HTTP stack before it arrives;
		// percent-encoded, it reaches the handler and must be rejected as the
		// caller's error rather than as this node being unhealthy.
		{name: "escaped path traversal in id", id: "%2e%2e%2fescape", crcHeader: fmt.Sprintf("%08x", crc), want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := cmp.Or(tt.id, "chunk1")
			srv := newServer(t, t.TempDir())
			req, err := http.NewRequest(http.MethodPut, srv.URL+"/peer/chunks/"+id, bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if tt.crcHeader != "" {
				req.Header.Set(peer.CRCHeader, tt.crcHeader)
			}
			if tt.chunked {
				// An unknown-length body forces chunked encoding, which leaves
				// the receiver with nothing to check the length against.
				req.Body = io.NopCloser(bytes.NewReader(data))
				req.ContentLength = -1
			}

			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatalf("PUT: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("status = %d (%s), want %d", resp.StatusCode, bytes.TrimSpace(body), tt.want)
			}
		})
	}
}

// A transfer cut off partway through must be blamed on the sender, not on this
// node. Go's client refuses to send a body shorter than the length it declares,
// so this speaks HTTP over a raw connection to produce the truncation a dropped
// network link would.
func TestPeerChunkPushTruncatedTransfer(t *testing.T) {
	srv := newServer(t, t.TempDir())
	data := []byte("a chunk from a peer")
	crc := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))

	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	sent := data[:len(data)-8]
	req := fmt.Sprintf("PUT /peer/chunks/chunk1 HTTP/1.1\r\nHost: kavo\r\n"+
		"X-Kavo-Crc32c: %08x\r\nContent-Length: %d\r\n\r\n%s", crc, len(data), sent)
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	// Half-close so the server reads EOF where it expected eight more bytes.
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("status = %d (%s), want 400: a truncated transfer is the sender's fault",
			resp.StatusCode, bytes.TrimSpace(body))
	}

	// And the half-written chunk must not exist at all.
	check, err := http.NewRequest(http.MethodGet, srv.URL+"/peer/chunks/chunk1", nil)
	if err != nil {
		t.Fatal(err)
	}
	check.Header.Set(peer.CRCHeader, fmt.Sprintf("%08x", crc))
	got, err := srv.Client().Do(check)
	if err != nil {
		t.Fatal(err)
	}
	got.Body.Close()
	if got.StatusCode != http.StatusNotFound {
		t.Errorf("GET after a truncated push = %d, want 404", got.StatusCode)
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
