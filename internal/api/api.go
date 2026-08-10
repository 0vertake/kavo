// Package api serves objects over HTTP. This is kavo's internal REST dialect;
// the S3-compatible surface arrives in a later milestone.
package api

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/store"
)

// New returns a handler serving PUT and GET on /objects/{key...}.
func New(s *store.Store, chunkSize int64) http.Handler {
	h := &handler{store: s, chunkSize: chunkSize}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /objects/{key...}", h.put)
	mux.HandleFunc("GET /objects/{key...}", h.get)
	return mux
}

type handler struct {
	store     *store.Store
	chunkSize int64
}

// put streams the body into chunks and then commits the manifest. The commit
// is the acknowledgement point: until PutMeta returns, the chunks on disk are
// unreferenced and the object does not exist.
func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		http.Error(w, "empty object key", http.StatusBadRequest)
		return
	}
	m, err := object.Write(h.store, r.Body, h.chunkSize)
	if err != nil {
		log.Printf("put %s: write chunks: %v", key, err)
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	data, err := json.Marshal(m)
	if err != nil {
		log.Printf("put %s: marshal manifest: %v", key, err)
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	if err := h.store.PutMeta(key, data); err != nil {
		log.Printf("put %s: commit manifest: %v", key, err)
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Kavo-Size", strconv.FormatInt(m.Size, 10))
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	data, err := h.store.GetMeta(key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		log.Printf("get %s: read manifest: %v", key, err)
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}
	var m object.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		log.Printf("get %s: corrupt manifest: %v", key, err)
		http.Error(w, "read failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
	if err := object.Read(h.store, m, w); err != nil {
		// Bytes are already on the wire, so the status cannot change. Aborting
		// short of the promised Content-Length makes net/http drop the
		// connection, which the client sees as an error rather than as a
		// complete object — never silent corruption.
		log.Printf("get %s: aborted mid-stream: %v", key, err)
	}
}
