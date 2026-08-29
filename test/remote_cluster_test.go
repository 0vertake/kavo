package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/peer"
)

// remoteNode is a kavod process reachable over the network. Unlike *node, there is
// no local data directory — chunk presence is checked through the internal API.
type remoteNode struct {
	id   string
	addr string // host:port for the internal API
}

func (n *remoteNode) url(key string) string {
	return "http://" + n.addr + "/objects/" + key
}

func (n *remoteNode) members() (map[string]string, error) {
	resp, err := http.Get("http://" + n.addr + "/cluster/members")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var members map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
		return nil, err
	}
	return members, nil
}

// loadRemoteCluster reads KAVO_N1_HOST … KAVO_N6_INTERNAL_PORT from the
// environment. The measure-remote script sources cluster.env before running.
func loadRemoteCluster(t *testing.T) []*remoteNode {
	t.Helper()
	var nodes []*remoteNode
	for i := 1; i <= measureCluster; i++ {
		id := fmt.Sprintf("n%d", i)
		host := os.Getenv(fmt.Sprintf("KAVO_N%d_HOST", i))
		port := os.Getenv(fmt.Sprintf("KAVO_N%d_INTERNAL_PORT", i))
		if host == "" || port == "" {
			t.Fatalf("set KAVO_N%d_HOST and KAVO_N%d_INTERNAL_PORT (see deploy/cluster.env.example)", i, i)
		}
		nodes = append(nodes, &remoteNode{id: id, addr: host + ":" + port})
	}
	return nodes
}

func remoteClusterPrefix() string {
	if p := os.Getenv("KAVO_CLUSTER"); p != "" {
		return p
	}
	return "/kavo"
}

func measureVictimID() string {
	return strings.TrimSpace(os.Getenv("KAVO_MEASURE_VICTIM"))
}

func wipeRemoteNode(t *testing.T, victimID string) {
	t.Helper()
	cmd := os.Getenv("KAVO_WIPE_CMD")
	if cmd == "" {
		t.Fatal("set KAVO_WIPE_CMD to wipe the victim's chunks (see deploy/README.md)")
	}
	cmd = strings.ReplaceAll(cmd, "{id}", victimID)
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		t.Fatalf("KAVO_WIPE_CMD %q: %v\n%s", cmd, err, out)
	}
}

// remoteMissingCopies is missingCopies for a cluster reached over the network.
// Chunk presence is batched through POST /peer/chunks/check rather than local disk.
func remoteMissingCopies(ctx context.Context, byID map[string]*remoteNode, store *meta.Store, want int) (holes []string, copies int, settled bool) {
	for id, n := range byID {
		members, err := n.members()
		if err != nil {
			return []string{fmt.Sprintf("%s is not answering: %v", id, err)}, 0, false
		}
		if len(members) != want {
			return []string{fmt.Sprintf("%s sees %d members, want %d", id, len(members), want)}, 0, false
		}
	}

	objects, err := store.ScanObjects(ctx, "", "", 0)
	if err != nil {
		return []string{fmt.Sprintf("scan manifests: %v", err)}, 0, false
	}

	needs := make(map[string][]string, len(byID))
	type placement struct {
		key, node, chunk string
		nodes            []string
	}
	var checks []placement

	for _, o := range objects {
		if wantOwners := wantOwners(o.Manifest, len(byID)); len(o.Manifest.Nodes) < wantOwners {
			holes = append(holes, fmt.Sprintf("%s: placed on %s, want %d owners",
				o.Key, strings.Join(o.Manifest.Nodes, ","), wantOwners))
			continue
		}
		for _, ref := range o.Manifest.Chunks {
			for i, id := range o.Manifest.Nodes {
				copies++
				if _, ok := byID[id]; !ok {
					holes = append(holes, fmt.Sprintf("%s names node %s, which is not in the cluster", o.Key, id))
					continue
				}
				wantChunk := ref.ID
				if o.Manifest.Coding.Valid() {
					wantChunk = ref.ShardID(i)
				}
				needs[id] = append(needs[id], wantChunk)
				checks = append(checks, placement{
					key: o.Key, node: id, chunk: wantChunk, nodes: o.Manifest.Nodes,
				})
			}
		}
	}

	have := make(map[string]map[string]bool, len(byID))
	for id, n := range byID {
		ids := needs[id]
		if len(ids) == 0 {
			have[id] = map[string]bool{}
			continue
		}
		m, err := peer.HasChunks(ctx, n.addr, ids)
		if err != nil {
			return []string{fmt.Sprintf("%s chunk check: %v", id, err)}, copies, false
		}
		have[id] = m
	}

	for _, p := range checks {
		if !have[p.node][p.chunk] {
			holes = append(holes, fmt.Sprintf("%s: %s is missing chunk %s (placed on %s)",
				p.key, p.node, p.chunk, strings.Join(p.nodes, ",")))
		}
	}
	return holes, copies, true
}

// victimHeld counts chunk copies the victim node holds before a wipe, using the
// same manifests repair will rebuild from.
func victimHeld(ctx context.Context, victim string, byID map[string]*remoteNode, store *meta.Store) (copies int, err error) {
	n, ok := byID[victim]
	if !ok {
		return 0, fmt.Errorf("unknown victim %q", victim)
	}
	objects, err := store.ScanObjects(ctx, "", "", 0)
	if err != nil {
		return 0, err
	}
	var ids []string
	for _, o := range objects {
		for _, ref := range o.Manifest.Chunks {
			for i, id := range o.Manifest.Nodes {
				if id != victim {
					continue
				}
				want := ref.ID
				if o.Manifest.Coding.Valid() {
					want = ref.ShardID(i)
				}
				ids = append(ids, want)
			}
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	have, err := peer.HasChunks(ctx, n.addr, ids)
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		if have[id] {
			copies++
		}
	}
	return copies, nil
}

// writeRemoteObjects fills a remote cluster through the internal API.
func writeRemoteObjects(t *testing.T, n *remoteNode, total, each int64, keyPrefix string) int {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Minute}
	count := 0
	for written := int64(0); written < total; written += each {
		key := fmt.Sprintf("%s/obj%04d", keyPrefix, count)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, n.url(key),
			&fill{left: each, seed: byte(count)})
		if err != nil {
			t.Fatal(err)
		}
		req.ContentLength = each
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("PUT %s: %v", key, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT %s: status %d", key, resp.StatusCode)
		}
		count++
	}
	return count
}
