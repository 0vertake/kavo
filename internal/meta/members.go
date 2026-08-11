package meta

import (
	"context"
	"fmt"
	"log"
	"path"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// DefaultLeaseTTL bounds failure detection: a node that stops renewing is
	// gone from the cluster's view within this long. Shorter means faster
	// detection and more false positives under load, since a node busy enough to
	// miss its renewals is declared dead while still serving.
	DefaultLeaseTTL = 5 * time.Second

	// retryInterval is how long to wait before trying etcd again after a failure.
	// Well under any sensible lease TTL, so a blip does not cost a membership.
	retryInterval = 250 * time.Millisecond
)

// Join registers this node as live and keeps the registration alive until ctx is
// done.
//
// The lease is the failure detector. A node that stops renewing — crashed,
// partitioned, or wedged so badly it cannot heartbeat — has its key dropped by
// etcd within the TTL, and every other node learns it left. No separate
// heartbeat protocol, no split-brain about who is up: etcd's own consensus
// decides, and it is the same etcd that decides which manifests are committed.
func (s *Store) Join(ctx context.Context, id, addr string, ttl time.Duration) error {
	if ttl < time.Second {
		return fmt.Errorf("meta: lease ttl %v is below etcd's one-second granularity", ttl)
	}
	// Ids are the last path element of the member key, so a slash would make the
	// node register under a name nobody can address chunks to.
	if id == "" || strings.Contains(id, "/") {
		return fmt.Errorf("meta: node id %q must be non-empty and contain no slashes", id)
	}
	keepalive, err := s.register(ctx, id, addr, ttl)
	if err != nil {
		return err
	}
	go s.stayRegistered(ctx, id, addr, ttl, keepalive)
	return nil
}

func (s *Store) register(ctx context.Context, id, addr string, ttl time.Duration) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	lease, err := s.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("meta: grant membership lease for %s: %w", id, err)
	}
	if _, err := s.client.Put(ctx, s.memberKey(id), addr, clientv3.WithLease(lease.ID)); err != nil {
		return nil, fmt.Errorf("meta: register node %s at %s: %w", id, addr, err)
	}
	keepalive, err := s.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return nil, fmt.Errorf("meta: keep membership lease alive for %s: %w", id, err)
	}
	return keepalive, nil
}

// stayRegistered drains renewals and re-registers if the lease is ever lost.
// Losing a lease is not fatal: the node is up, so it should be a member again as
// soon as it can talk to etcd. Until then the rest of the cluster correctly
// treats it as gone, since a node that cannot reach etcd cannot commit anything.
func (s *Store) stayRegistered(ctx context.Context, id, addr string, ttl time.Duration, keepalive <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		// Renewals arrive until the lease is lost or ctx ends.
		for range keepalive {
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("meta: membership lease for %s lost, re-registering", id)

		for {
			if !sleep(ctx, retryInterval) {
				return
			}
			next, err := s.register(ctx, id, addr, ttl)
			if err == nil {
				keepalive = next
				break
			}
			log.Printf("meta: re-register %s: %v", id, err)
		}
	}
}

// Members returns the live nodes as id -> address.
func (s *Store) Members(ctx context.Context) (map[string]string, error) {
	resp, err := s.client.Get(ctx, s.memberPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("meta: list members: %w", err)
	}
	members := make(map[string]string, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		members[path.Base(string(kv.Key))] = string(kv.Value)
	}
	return members, nil
}

// WatchMembers sends the current membership, then a fresh copy every time it
// changes, until ctx is done. Callers block on the first value before serving
// requests: placing data on a stale view of the cluster is how a node ends up
// acknowledging a write with fewer copies than it thinks.
func (s *Store) WatchMembers(ctx context.Context) <-chan map[string]string {
	out := make(chan map[string]string)
	go func() {
		defer close(out)

		// The watch is established before the first listing, so a change that
		// lands between the two shows up as an event rather than being missed.
		changes := s.client.Watch(ctx, s.memberPrefix(), clientv3.WithPrefix())

		for {
			members, err := s.Members(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// Retry rather than wait for the next change, which may never come.
				log.Printf("meta: %v", err)
				if !sleep(ctx, retryInterval) {
					return
				}
				continue
			}
			select {
			case out <- members:
			case <-ctx.Done():
				return
			}
			select {
			case _, ok := <-changes:
				if !ok {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// sleep waits for d, reporting false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Store) memberPrefix() string { return path.Join(s.prefix, "members") + "/" }

func (s *Store) memberKey(id string) string { return s.memberPrefix() + id }
