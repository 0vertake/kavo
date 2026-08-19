// Package peer moves chunks between nodes over plain HTTP with streaming
// bodies. No gRPC: a chunk transfer is one request with a body, which is
// exactly what HTTP is for, and a streaming body keeps memory flat.
//
// Both directions are checksum-verified end to end. A push is only acked once
// the receiving node has verified and fsynced the chunk, and a fetch verifies
// the bytes that came off the wire rather than trusting the sender.
package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/0vertake/kavo/internal/store"
)

// ErrNotFound reports that the peer does not hold the requested chunk. A
// coordinator reading under quorum needs to tell this apart from a peer that is
// broken or unreachable: the first is expected, the second is a fault.
var ErrNotFound = errors.New("peer: chunk not found")

// CRCHeader carries the checksum the far side must verify the chunk against: on
// a push so the receiver rejects bad data instead of committing it, on a fetch
// so the sender checks its own disk copy before streaming it.
const CRCHeader = "X-Kavo-Crc32c"

// client has no timeout on purpose: a chunk transfer is bounded by chunk size
// and link speed, not by a deadline we could pick here. Callers cancel with the
// request context instead.
//
// It does not use the default transport, for one reason: that keeps two idle
// connections per host, and a node talks to the same handful of peers for every
// chunk it ever writes or reads. Past two concurrent requests to one peer the
// default closes connections as soon as they are done and dials again for the
// next chunk, which showed up as ~8% of CPU in a parallel read.
var client = &http.Client{Transport: peerTransport()}

// peerTransport clones the default rather than building one, so proxy, dial and
// TLS behaviour stay whatever the standard library thinks is right.
func peerTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	// A cluster this talks to is small and its members are long-lived, so an idle
	// connection per concurrent request is cheap; redialling one per chunk is not.
	t.MaxIdleConnsPerHost = 64
	t.MaxIdleConns = 0 // no cluster-wide cap: the per-peer one is the real bound
	return t
}

// PushChunk streams size bytes from body to the node at addr, to be stored as
// chunk id. It returns nil only once that node reports the chunk verified
// against crc and fsynced, which is what makes it countable towards a write
// quorum.
func PushChunk(ctx context.Context, addr, id string, crc uint32, size int64, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, chunkURL(addr, id), body)
	if err != nil {
		return fmt.Errorf("peer: push chunk %s to %s: %w", id, addr, err)
	}
	// A declared length is what lets the receiver detect a truncated transfer
	// rather than committing a short chunk with a matching checksum.
	req.ContentLength = size
	req.Header.Set(CRCHeader, fmt.Sprintf("%08x", crc))

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("peer: push chunk %s to %s: %w", id, addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("peer: push chunk %s to %s: %s: %s", id, addr, resp.Status, detail)
	}
	return nil
}

// FetchChunk streams chunk id from the node at addr. The returned reader fails
// with store.ErrChecksumMismatch rather than reaching EOF if the bytes do not
// match crc, so a corrupt or truncated transfer can never be mistaken for a
// complete chunk. It returns ErrNotFound if that node does not hold the chunk.
func FetchChunk(ctx context.Context, addr, id string, crc uint32) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, chunkURL(addr, id), nil)
	if err != nil {
		return nil, fmt.Errorf("peer: fetch chunk %s from %s: %w", id, addr, err)
	}
	req.Header.Set(CRCHeader, fmt.Sprintf("%08x", crc))

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("peer: fetch chunk %s from %s: %w", id, addr, err)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return store.Verify(resp.Body, crc), nil
	case http.StatusNotFound:
		resp.Body.Close()
		return nil, fmt.Errorf("%w: %s on %s", ErrNotFound, id, addr)
	default:
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("peer: fetch chunk %s from %s: %s: %s", id, addr, resp.Status, detail)
	}
}

// HasChunks asks which of the given IDs the node at addr holds, in one round
// trip. It returns a set of the present IDs. Repair uses this to survey a whole
// object page per node instead of one request per chunk copy.
func HasChunks(ctx context.Context, addr string, ids []string) (map[string]bool, error) {
	body, err := json.Marshal(ids)
	if err != nil {
		return nil, fmt.Errorf("peer: check chunks on %s: %w", addr, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+addr+"/peer/chunks/check", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("peer: check chunks on %s: %w", addr, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("peer: check chunks on %s: %w", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("peer: check chunks on %s: %s: %s", addr, resp.Status, detail)
	}
	var have []string
	if err := json.NewDecoder(resp.Body).Decode(&have); err != nil {
		return nil, fmt.Errorf("peer: check chunks on %s: decode: %w", addr, err)
	}
	result := make(map[string]bool, len(have))
	for _, id := range have {
		result[id] = true
	}
	return result, nil
}

func chunkURL(addr, id string) string {
	return "http://" + addr + "/peer/chunks/" + id
}
