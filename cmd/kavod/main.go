// Command kavod is the kavo node daemon: it serves objects and stores chunks.
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/0vertake/kavo/internal/api"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/store"
	"github.com/0vertake/kavo/internal/version"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	dataDir := flag.String("data", "./data", "directory for chunks")
	chunkSize := flag.Int64("chunk-size", object.DefaultChunkSize, "chunk size in bytes")
	etcd := flag.String("etcd", meta.EndpointFromEnv(), "comma-separated etcd endpoints for manifests")
	prefix := flag.String("cluster", "/kavo", "etcd key prefix identifying this cluster")
	flag.Parse()

	s, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	m, err := meta.Open(strings.Split(*etcd, ","), *prefix)
	if err != nil {
		log.Fatalf("open manifest store: %v", err)
	}
	defer m.Close()

	log.Printf("kavod %s listening on %s (data %s, chunk size %d, etcd %s%s)",
		version.Version, *addr, *dataDir, *chunkSize, *etcd, *prefix)

	// No read/write timeouts: uploads are multi-gigabyte streams, so any
	// wall-clock deadline would kill legitimate transfers.
	if err := http.ListenAndServe(*addr, api.New(s, m, *chunkSize)); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
