// Package peer_test is external so that these tests can import api, which will
// import peer once writes fan out to owners.
package peer_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/api"
	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/peer"
	"github.com/0vertake/kavo/internal/store"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// newPeer starts a node serving the peer protocol and returns its address and
// data root. It is a whole node, manifest store included, because that is what a
// peer pushes chunks to in production.
func newPeer(t *testing.T) (addr, root string) {
	t.Helper()
	root = t.TempDir()
	s, err := store.Open(root)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	m, err := meta.Open([]string{meta.EndpointFromEnv()}, "/kavo-test/"+rand.Text())
	if err != nil {
		t.Fatalf("meta.Open (is etcd up? try `make etcd`): %v", err)
	}
	t.Cleanup(func() { m.Close() })

	c := cluster.New("n1", "127.0.0.1:0", s, m, 1024)
	srv := httptest.NewServer(api.New(c, s))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://"), root
}

func push(t *testing.T, addr, id string, data []byte, crc uint32, size int64) error {
	t.Helper()
	return peer.PushChunk(context.Background(), addr, id, crc, size, bytes.NewReader(data))
}

func fetch(t *testing.T, addr, id string, crc uint32) ([]byte, error) {
	t.Helper()
	rc, err := peer.FetchChunk(context.Background(), addr, id, crc)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func TestPushFetchRoundTrip(t *testing.T) {
	addr, _ := newPeer(t)
	data := bytes.Repeat([]byte("replicate me"), 5000)
	crc := crc32.Checksum(data, castagnoli)

	if err := push(t, addr, "chunk1", data, crc, int64(len(data))); err != nil {
		t.Fatalf("PushChunk: %v", err)
	}
	got, err := fetch(t, addr, "chunk1", crc)
	if err != nil {
		t.Fatalf("FetchChunk: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("fetched chunk differs from pushed chunk")
	}
}

// A push is only worth counting towards a write quorum if the receiver refuses
// bytes that do not match what was declared, and leaves nothing behind when it
// does refuse.
func TestPushOfBadDataIsRejectedAndStoresNothing(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 4096)
	good := crc32.Checksum(data, castagnoli)

	tests := []struct {
		name string
		crc  uint32
		size int64
	}{
		{name: "checksum does not match", crc: good ^ 0xdead, size: int64(len(data))},
		{name: "length does not match", crc: good, size: int64(len(data)) + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, _ := newPeer(t)
			if err := push(t, addr, "chunk1", data, tt.crc, tt.size); err == nil {
				t.Fatal("PushChunk reported success for a chunk the peer should reject")
			}
			if _, err := fetch(t, addr, "chunk1", good); !errors.Is(err, peer.ErrNotFound) {
				t.Errorf("chunk exists on the peer after a rejected push: %v", err)
			}
		})
	}
}

func TestFetchMissingChunk(t *testing.T) {
	addr, _ := newPeer(t)
	if _, err := fetch(t, addr, "absent", 0); !errors.Is(err, peer.ErrNotFound) {
		t.Fatalf("FetchChunk error = %v, want peer.ErrNotFound", err)
	}
}

// Rot on the sender's disk must surface as a failed transfer. The sender detects
// it while reading and aborts the body; the receiver sees a checksum failure.
func TestFetchDetectsRotOnSendersDisk(t *testing.T) {
	addr, root := newPeer(t)
	data := bytes.Repeat([]byte("rot me"), 4096)
	crc := crc32.Checksum(data, castagnoli)
	if err := push(t, addr, "chunk1", data, crc, int64(len(data))); err != nil {
		t.Fatalf("PushChunk: %v", err)
	}

	flipOneByte(t, filepath.Join(root, "chunks", "ch", "chunk1"))

	got, err := fetch(t, addr, "chunk1", crc)
	if err == nil {
		t.Fatalf("fetched %d bytes with no error from a corrupted chunk", len(got))
	}
	if bytes.Equal(got, data) {
		t.Error("corrupt chunk was delivered as valid data")
	}
}

// The receiving side must verify too. A sender whose disk copy is fine can still
// deliver corrupt bytes if the wire flips one, and TCP's 16-bit checksum is not
// strong enough to be the only defence.
func TestFetchDetectsCorruptionInTransit(t *testing.T) {
	data := bytes.Repeat([]byte("intact"), 4096)
	crc := crc32.Checksum(data, castagnoli)

	// A peer that serves a 200 and bytes that do not match the checksum.
	liar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tampered := append([]byte(nil), data...)
		tampered[len(tampered)/2] ^= 0xff
		w.Write(tampered)
	}))
	t.Cleanup(liar.Close)

	_, err := fetch(t, strings.TrimPrefix(liar.URL, "http://"), "chunk1", crc)
	if !errors.Is(err, store.ErrChecksumMismatch) {
		t.Fatalf("FetchChunk error = %v, want store.ErrChecksumMismatch", err)
	}
}

// A dead peer must surface as an error, not be silently counted as an ack.
func TestPushToUnreachablePeer(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	dead := strings.TrimPrefix(srv.URL, "http://")
	srv.Close()

	if err := push(t, dead, "chunk1", []byte("data"), 0, 4); err == nil {
		t.Fatal("PushChunk to a dead peer reported success")
	}
}

// Connections to a peer are reused across chunks. Not a micro-optimisation: the
// default transport keeps two idle connections per host, so a node doing more
// than two things at once with one peer closes and redials for every chunk after
// that — which under sustained load exhausts the machine's ephemeral ports, and
// did exactly that in the parallel read benchmark.
//
// The count is deliberately loose. What must not happen is one connection per
// chunk; how many the pool settles on is the transport's business.
func TestConnectionsToAPeerAreReused(t *testing.T) {
	root := t.TempDir()
	s, err := store.Open(root)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}

	// The handler is slow so that every fetch of a round is in flight at the same
	// moment and they all return their connections together. That is when the
	// pool's size decides anything: requests that complete instantly barely
	// overlap, and then every transport looks alike.
	inner := api.New(nil, s)
	var conns atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		inner.ServeHTTP(w, r)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")

	data := []byte("a chunk worth fetching")
	crc := crc32.Checksum(data, castagnoli)
	if err := push(t, addr, "reused", data, crc, int64(len(data))); err != nil {
		t.Fatalf("PushChunk: %v", err)
	}

	const workers = 8
	round := func() {
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := fetch(t, addr, "reused", crc); err != nil {
					t.Errorf("FetchChunk: %v", err)
				}
			}()
		}
		wg.Wait()
	}
	round()
	opened := conns.Load()
	round()

	// The second round is the test: its connections already exist. One more is
	// allowed for a connection the pool happens to replace; six more is the
	// default transport throwing away everything past the two it keeps.
	if again := conns.Load() - opened; again > 1 {
		t.Errorf("a second round of %d fetches opened %d more connections, want at most 1 (first round opened %d)",
			workers, again, opened)
	}
}

func flipOneByte(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}
