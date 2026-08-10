// Command kavod is the kavo node daemon: it serves objects and stores chunks.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/0vertake/kavo/internal/api"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/store"
	"github.com/0vertake/kavo/internal/version"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dataDir := flag.String("data", "./data", "directory for chunks and metadata")
	chunkSize := flag.Int64("chunk-size", object.DefaultChunkSize, "chunk size in bytes")
	flag.Parse()

	s, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	log.Printf("kavod %s listening on %s (data %s, chunk size %d)",
		version.Version, *addr, *dataDir, *chunkSize)

	// No read/write timeouts: uploads are multi-gigabyte streams, so any
	// wall-clock deadline would kill legitimate transfers.
	if err := http.ListenAndServe(*addr, api.New(s, *chunkSize)); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
