// Package api serves objects over HTTP. This is kavo's internal REST dialect;
// the S3-compatible surface arrives in a later milestone.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/peer"
	"github.com/0vertake/kavo/internal/store"
)

// New returns a handler serving objects on /objects/{key...} for clients, and
// chunks on /peer/chunks/{id} for other nodes. Client requests go through the
// coordinator, which spreads them across the cluster; peer requests act on this
// node's own chunk store.
func New(c *cluster.Coordinator, s *store.Store) http.Handler {
	h := &handler{cluster: c, store: s}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /objects/{key...}", h.put)
	mux.HandleFunc("GET /objects/{key...}", h.get)
	mux.HandleFunc("PUT /peer/chunks/{id}", h.putChunk)
	mux.HandleFunc("GET /peer/chunks/{id}", h.getChunk)
	mux.HandleFunc("HEAD /peer/chunks/{id}", h.headChunk)
	mux.HandleFunc("DELETE /peer/chunks/{id}", h.deleteChunk)
	mux.HandleFunc("GET /cluster/members", h.members)
	return mux
}

type handler struct {
	cluster *cluster.Coordinator
	store   *store.Store
}

// members exposes this node's view of the cluster. Failure detection is only a
// claim if it can be observed, and this is what makes the detection time
// measurable from outside the process.
func (h *handler) members(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(h.cluster.Members()); err != nil {
		log.Printf("members: %v", err)
	}
}

// put replicates the body across the partition's owners and commits the
// manifest. The commit is the acknowledgement point: until it returns, the chunks
// on disk are unreferenced and the object does not exist.
func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "empty object key", http.StatusBadRequest)
		return
	}
	m, err := h.cluster.Put(r.Context(), key, r.Body, cluster.PutOptions{Size: r.ContentLength})
	if err != nil {
		log.Printf("put %s: %v", key, err)
		if errors.Is(err, cluster.ErrQuorum) {
			// The cluster cannot promise the write is durable, so it must not
			// claim it is. Retryable once enough nodes are back.
			http.Error(w, "not enough replicas available", http.StatusServiceUnavailable)
			return
		}
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Kavo-Size", strconv.FormatInt(m.Size, 10))
}

// putChunk accepts a chunk from another node. The declared checksum and length
// are verified before anything is committed, so a 200 here means a checked,
// fsynced replica exists and the coordinator may count it towards its quorum.
func (h *handler) putChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	crc, err := declaredCRC(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.ContentLength < 0 {
		http.Error(w, "chunk push requires a Content-Length", http.StatusLengthRequired)
		return
	}
	// Fault attribution matters more than the status text: a coordinator that
	// reads 500 may stop trusting this node, so only this node's own failures
	// may be reported as 5xx. Bad or short input is the sender's problem.
	err = h.store.WriteChunkVerified(id, r.Body, crc, r.ContentLength)
	switch {
	case err == nil:
		return
	case errors.Is(err, store.ErrInvalidID):
		http.Error(w, "invalid chunk id", http.StatusBadRequest)
	case errors.Is(err, store.ErrVerificationFailed):
		log.Printf("peer put chunk %s: rejected: %v", id, err)
		http.Error(w, "chunk failed verification", http.StatusBadRequest)
	case errors.Is(err, io.ErrUnexpectedEOF):
		log.Printf("peer put chunk %s: rejected: %v", id, err)
		http.Error(w, "chunk body ended before Content-Length", http.StatusBadRequest)
	default:
		log.Printf("peer put chunk %s: %v", id, err)
		http.Error(w, "chunk write failed", http.StatusInternalServerError)
	}
}

// getChunk serves a chunk to another node, verifying the local copy against the
// checksum the caller declares. The caller verifies again on arrival: this catches
// rot on this node's disk, that catches corruption in transit.
func (h *handler) getChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	crc, err := declaredCRC(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rc, err := h.store.ReadChunk(id, crc)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
		return
	case errors.Is(err, store.ErrInvalidID):
		http.Error(w, "invalid chunk id", http.StatusBadRequest)
		return
	default:
		log.Printf("peer get chunk %s: %v", id, err)
		http.Error(w, "chunk read failed", http.StatusInternalServerError)
		return
	}
	defer rc.Close()
	if _, err := io.Copy(w, rc); err != nil {
		// Same as the object path: the status is already sent, so aborting the
		// body is the only way to tell the caller not to trust these bytes.
		log.Printf("peer get chunk %s: aborted mid-stream: %v", id, err)
	}
}

// headChunk answers whether this node holds a chunk. Repair surveys every copy of
// every chunk, and asking that by fetching them would move the cluster's data to
// answer a yes-or-no question.
//
// No checksum header is required, because this reports presence and not integrity.
func (h *handler) headChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	held, err := h.store.HasChunk(id)
	switch {
	case errors.Is(err, store.ErrInvalidID):
		http.Error(w, "invalid chunk id", http.StatusBadRequest)
	case err != nil:
		log.Printf("peer head chunk %s: %v", id, err)
		http.Error(w, "chunk probe failed", http.StatusInternalServerError)
	case !held:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// deleteChunk drops a chunk this node no longer owns. Only a rebalance sends
// this, and only after committing a manifest that names the chunk's new homes,
// so no reader can still be sent here for it.
func (h *handler) deleteChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := h.store.RemoveChunk(id)
	switch {
	case errors.Is(err, store.ErrInvalidID):
		http.Error(w, "invalid chunk id", http.StatusBadRequest)
	case err != nil:
		log.Printf("peer delete chunk %s: %v", id, err)
		http.Error(w, "chunk delete failed", http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// declaredCRC reads the checksum the caller asserts the chunk has. It is
// mandatory in both directions: without it neither side can tell good bytes from
// bad, and an unverifiable transfer must not be allowed to look successful.
func declaredCRC(r *http.Request) (uint32, error) {
	raw := r.Header.Get(peer.CRCHeader)
	if raw == "" {
		return 0, fmt.Errorf("missing %s header", peer.CRCHeader)
	}
	crc, err := strconv.ParseUint(raw, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("malformed %s %q", peer.CRCHeader, raw)
	}
	return uint32(crc), nil
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	// The size has to be known before any bytes go out, so the manifest is
	// resolved first and the body is streamed second.
	m, err := h.cluster.Resolve(r.Context(), key)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		log.Printf("get %s: resolve manifest: %v", key, err)
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
	if err := h.cluster.Stream(r.Context(), m, w); err != nil {
		// Bytes are already on the wire, so the status cannot change. Aborting
		// short of the promised Content-Length makes net/http drop the
		// connection, which the client sees as an error rather than as a
		// complete object — never silent corruption.
		log.Printf("get %s: aborted mid-stream: %v", key, err)
	}
}
