// Command kavod is the kavo node daemon: it serves objects and stores chunks.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/0vertake/kavo/internal/api"
	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/store"
	"github.com/0vertake/kavo/internal/version"
)

func main() {
	id := flag.String("id", "n1", "this node's id in the cluster")
	addr := flag.String("addr", ":8080", "address to listen on")
	peerList := flag.String("peers", "", "every node as id=host:port, comma separated, including this one")
	dataDir := flag.String("data", "./data", "directory for chunks")
	chunkSize := flag.Int64("chunk-size", object.DefaultChunkSize, "chunk size in bytes")
	etcd := flag.String("etcd", meta.EndpointFromEnv(), "comma-separated etcd endpoints for manifests")
	prefix := flag.String("cluster", "/kavo", "etcd key prefix identifying this cluster")
	flag.Parse()

	peers, err := parsePeers(*peerList, *id, *addr)
	if err != nil {
		log.Fatalf("peers: %v", err)
	}
	s, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	m, err := meta.Open(strings.Split(*etcd, ","), *prefix)
	if err != nil {
		log.Fatalf("open manifest store: %v", err)
	}
	defer m.Close()

	c, err := cluster.New(*id, peers, s, m, *chunkSize)
	if err != nil {
		log.Fatalf("cluster: %v", err)
	}

	log.Printf("kavod %s node %s listening on %s (data %s, chunk size %d, peers %v, etcd %s%s)",
		version.Version, *id, *addr, *dataDir, *chunkSize, peers, *etcd, *prefix)

	// No read/write timeouts: uploads are multi-gigabyte streams, so any
	// wall-clock deadline would kill legitimate transfers.
	if err := http.ListenAndServe(*addr, api.New(c, s)); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// parsePeers turns id=host:port pairs into a membership map. A node given no
// peers is a cluster of one, which is how a single-node install and the unit
// tests run. Real membership arrives with etcd leases.
func parsePeers(list, self, addr string) (map[string]string, error) {
	if list == "" {
		return map[string]string{self: addr}, nil
	}
	peers := make(map[string]string)
	for entry := range strings.SplitSeq(list, ",") {
		id, hostport, ok := strings.Cut(entry, "=")
		if !ok || id == "" || hostport == "" {
			return nil, fmt.Errorf("bad entry %q, want id=host:port", entry)
		}
		peers[id] = hostport
	}
	return peers, nil
}
