// Command kavod is the kavo node daemon: it serves objects and stores chunks.
package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/0vertake/kavo/internal/api"
	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/ec"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
	"github.com/0vertake/kavo/internal/s3"
	"github.com/0vertake/kavo/internal/sigv4"
	"github.com/0vertake/kavo/internal/store"
	"github.com/0vertake/kavo/internal/version"
)

func main() {
	id := flag.String("id", "n1", "this node's id in the cluster")
	addr := flag.String("addr", ":8080", "address for the internal API: peer chunk transfer and cluster state")
	s3Addr := flag.String("s3", ":9000", "address for the S3 API clients use")
	accessKey := flag.String("access-key", "kavo", "S3 access key id clients sign with")
	secretKey := flag.String("secret-key", "kavosecret", "S3 secret key clients sign with")
	advertise := flag.String("advertise", "", "host:port other nodes should dial (default: -addr)")
	dataDir := flag.String("data", "./data", "directory for chunks")
	chunkSize := flag.Int64("chunk-size", object.DefaultChunkSize, "chunk size in bytes")
	etcd := flag.String("etcd", meta.EndpointFromEnv(), "comma-separated etcd endpoints for manifests")
	prefix := flag.String("cluster", "/kavo", "etcd key prefix identifying this cluster")
	leaseTTL := flag.Duration("lease-ttl", meta.DefaultLeaseTTL, "how long this node may go unheard before the cluster declares it dead")
	repairRate := flag.Int64("repair-rate", cluster.DefaultRepairRate, "bytes per second this node may use to restore missing copies (0 for unlimited)")
	repairInterval := flag.Duration("repair-interval", cluster.DefaultRepairInterval, "pause between repair passes")
	rebalanceInterval := flag.Duration("rebalance-interval", cluster.DefaultRebalanceInterval, "pause between rebalance passes, which move objects onto the nodes that now own them")
	scrubInterval := flag.Duration("scrub-interval", cluster.DefaultScrubInterval, "pause between scrub passes, which re-read this node's chunks to find rot")
	collectInterval := flag.Duration("collect-interval", cluster.DefaultCollectInterval, "pause between collection passes, which reclaim chunks no manifest references")
	collectGrace := flag.Duration("collect-grace", cluster.DefaultCollectGrace, "how long an unreferenced chunk is left alone before it is treated as garbage")
	erasure := flag.String("ec", "", `erasure-code new objects as "data+parity" (for example 6+3) instead of replicating them`)
	flag.Parse()

	// Peers dial this address, so a bind address like ":8080" is not enough:
	// discovering that at the first chunk transfer instead of at startup would
	// mean a node that joins the ring and then fails every write sent to it.
	self := cmp.Or(*advertise, *addr)
	if host, _, err := net.SplitHostPort(self); err != nil || host == "" {
		log.Fatalf("advertise address %q has no host for peers to dial; pass -advertise host:port", self)
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

	// No signal handling: the store is crash-safe by construction, so dying
	// abruptly is a tested path and a graceful shutdown would add nothing except
	// a window where the node is out of the membership but still serving.
	ctx := context.Background()

	c := cluster.New(*id, self, s, m, *chunkSize)
	// Only new writes are affected. Every object records the code it was written
	// with, so switching modes — or getting the flag wrong on one node — cannot
	// strand data that is already stored.
	scheme, err := parseScheme(*erasure)
	if err != nil {
		log.Fatal(err)
	}
	if err := c.EncodeWith(scheme); err != nil {
		log.Fatal(err)
	}
	if err := m.Join(ctx, *id, self, *leaseTTL); err != nil {
		log.Fatalf("join cluster: %v", err)
	}

	// Serving starts only once the real membership is known. Placing data on a
	// view of the cluster that is just this node would acknowledge writes with
	// fewer copies than the cluster could actually make.
	updates := m.WatchMembers(ctx)
	select {
	case peers := <-updates:
		c.SetMembers(peers)
		log.Printf("kavod %s node %s is a member of %v", version.Version, *id, peers)
	case <-time.After(*leaseTTL + 5*time.Second):
		log.Fatalf("no membership from etcd %s within %v", *etcd, *leaseTTL+5*time.Second)
	}
	go func() {
		for peers := range updates {
			c.SetMembers(peers)
			log.Printf("membership changed: %v", peers)
		}
	}()

	// Under-replication is a state to keep converging out of, not an event to
	// react to: nothing announces a write that was acknowledged at W of N or a
	// disk that came back empty.
	go c.RepairLoop(ctx, *repairRate, *repairInterval)

	// Rot answers every survey correctly and only fails when a client reads it,
	// which is far too late for that to be the first time anyone looked.
	go c.ScrubLoop(ctx, *repairRate, *scrubInterval)

	// Repair restores the copies a manifest promises and refuses to put them
	// anywhere else, so a node that leaves for good takes its copy's place with
	// it. This is what moves the place.
	go c.RebalanceLoop(ctx, *repairRate, *rebalanceInterval)

	// An overwrite supersedes the chunks it replaces and a failed write leaves the
	// ones it stored. Neither is reachable and neither frees itself, so without
	// this a store's disk usage only ever goes up.
	go c.CollectLoop(ctx, *collectGrace, *collectInterval)

	redundancy := "replicated"
	if scheme != (ec.Scheme{}) {
		redundancy = "erasure-coded " + scheme.String()
	}
	log.Printf("kavod %s node %s serving S3 on %s, internal API on %s (advertising %s, data %s, chunk size %d, %s, etcd %s%s, lease %v)",
		version.Version, *id, *s3Addr, *addr, self, *dataDir, *chunkSize, redundancy, *etcd, *prefix, *leaseTTL)

	// Two listeners, because the internal API can delete a chunk and read any
	// object without a signature. One port for clients, one for the cluster, so
	// that reaching the first does not imply reaching the second.
	//
	// No read/write timeouts on either: uploads are multi-gigabyte streams, so any
	// wall-clock deadline would kill legitimate transfers.
	go func() {
		if err := http.ListenAndServe(*addr, api.New(c, s)); err != nil {
			log.Fatalf("serve internal API: %v", err)
		}
	}()
	creds := sigv4.Credentials{AccessKey: *accessKey, SecretKey: *secretKey}
	if err := http.ListenAndServe(*s3Addr, s3.New(c, creds)); err != nil {
		log.Fatalf("serve S3: %v", err)
	}
}

// parseScheme reads "data+parity", or "" for replication.
func parseScheme(s string) (ec.Scheme, error) {
	if s == "" {
		return ec.Scheme{}, nil
	}
	var scheme ec.Scheme
	if _, err := fmt.Sscanf(s, "%d+%d", &scheme.Data, &scheme.Parity); err != nil {
		return ec.Scheme{}, fmt.Errorf("erasure code %q is not in data+parity form, for example 6+3", s)
	}
	return scheme, nil
}
