// Package test holds integration tests that run kavod as a real process.
package test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/meta"
)

// node is a kavod process under test.
type node struct {
	t       *testing.T
	id      string
	bin     string
	dataDir string
	addr    string
	cmd     *exec.Cmd
	logs    *bytes.Buffer
}

const (
	// testLeaseTTL is etcd's floor. Detection is then as fast as the design
	// allows, which keeps these tests short; production defaults are longer.
	testLeaseTTL = time.Second

	// Repair runs constantly in every process test, unthrottled. That is both
	// what makes automatic healing observable in seconds and a standing check
	// that a repair loop in the background disturbs nothing else.
	testRepairInterval = 200 * time.Millisecond
)

// clusterPrefix isolates a test's manifests in etcd. It has to be unique per
// run: etcd outlives the test, so a reused prefix would resolve objects whose
// chunks were left behind in a previous run's data directory.
func clusterPrefix() string { return "/kavo-test/" + rand.Text() }

// startNode launches a cluster of one against dataDir. Handing the same dataDir
// and prefix to a later startNode call simulates a restart.
func startNode(t *testing.T, bin, dataDir, prefix string, chunkSize int) *node {
	t.Helper()
	return launch(t, bin, "n1", freePort(t), dataDir, prefix, chunkSize)
}

// startCluster launches n kavod processes into the same cluster, returned in id
// order (n1, n2, ...). They find each other through etcd, so all that makes them
// one cluster is the shared prefix.
func startCluster(t *testing.T, bin, prefix string, chunkSize, n int) []*node {
	t.Helper()
	nodes := make([]*node, n)
	for i := range nodes {
		nodes[i] = launch(t, bin, fmt.Sprintf("n%d", i+1), freePort(t), t.TempDir(), prefix, chunkSize)
	}
	// Every node has to see every other before placement is stable, and until
	// then a write would be spread over a smaller ring than the cluster has.
	for _, n := range nodes {
		n.waitForMembers(len(nodes))
	}
	return nodes
}

func launch(t *testing.T, bin, id, addr, dataDir, prefix string, chunkSize int) *node {
	t.Helper()
	n := &node{
		t:       t,
		id:      id,
		bin:     bin,
		dataDir: dataDir,
		addr:    addr,
		logs:    &bytes.Buffer{},
	}
	n.cmd = exec.Command(bin,
		"-id", id,
		"-addr", n.addr,
		"-data", dataDir,
		"-chunk-size", fmt.Sprint(chunkSize),
		"-etcd", meta.EndpointFromEnv(),
		"-cluster", prefix,
		"-lease-ttl", testLeaseTTL.String(),
		"-repair-interval", testRepairInterval.String(),
		"-repair-rate", "0",
	)
	n.cmd.Stdout = n.logs
	n.cmd.Stderr = n.logs
	if err := n.cmd.Start(); err != nil {
		t.Fatalf("start kavod: %v", err)
	}
	t.Cleanup(n.stop)
	n.waitReady()
	return n
}

func (n *node) waitReady() {
	n.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", n.addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	n.t.Fatalf("kavod never became ready on %s; logs:\n%s", n.addr, n.logs)
}

// kill sends SIGKILL, the only crash worth testing: no signal handler, no
// graceful shutdown, no chance to flush anything.
func (n *node) kill() {
	n.t.Helper()
	if err := n.cmd.Process.Kill(); err != nil {
		n.t.Fatalf("kill kavod: %v", err)
	}
	n.cmd.Wait()
}

func (n *node) stop() {
	if n.cmd.Process != nil {
		n.cmd.Process.Kill()
		n.cmd.Wait()
	}
}

func (n *node) url(key string) string {
	return "http://" + n.addr + "/objects/" + key
}

// chunkFiles lists the chunks on this node's disk.
func (n *node) chunkFiles() []string {
	files, err := filepath.Glob(filepath.Join(n.dataDir, "chunks", "*", "*"))
	if err != nil {
		n.t.Fatalf("list chunks of %s: %v", n.id, err)
	}
	return files
}

// loseChunks deletes everything this node holds, which is what a replaced disk
// looks like: the process is healthy and answering, it simply has nothing.
func (n *node) loseChunks() {
	n.t.Helper()
	for _, f := range n.chunkFiles() {
		if err := os.Remove(f); err != nil {
			n.t.Fatalf("remove %s: %v", f, err)
		}
	}
}

// waitForChunks blocks until this node holds want chunks, reporting whether it
// got there in time.
func (n *node) waitForChunks(want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(n.chunkFiles()) == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// members reports this node's view of the cluster.
func (n *node) members() (map[string]string, error) {
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

// waitForMembers blocks until this node sees want members, or fails the test.
func (n *node) waitForMembers(want int) {
	n.t.Helper()
	if !n.awaitMembers(func(m map[string]string) bool { return len(m) == want }, 15*time.Second) {
		got, _ := n.members()
		n.t.Fatalf("%s sees members %v, want %d of them", n.id, got, want)
	}
}

// awaitMembers polls until this node's view satisfies ok, reporting whether it
// did so within the timeout.
func (n *node) awaitMembers(ok func(map[string]string) bool, timeout time.Duration) bool {
	n.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if members, err := n.members(); err == nil && ok(members) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// put uploads body and reports the status code. A 200 means the write was
// acknowledged and must survive anything that happens next.
func (n *node) put(client *http.Client, key string, body []byte) (int, error) {
	req, err := http.NewRequest(http.MethodPut, n.url(key), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (n *node) get(client *http.Client, key string) (int, []byte, error) {
	resp, err := client.Get(n.url(key))
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
}

func buildKavod(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "kavod")
	out, err := exec.Command("go", "build", "-o", bin, "github.com/0vertake/kavo/cmd/kavod").CombinedOutput()
	if err != nil {
		t.Fatalf("build kavod: %v\n%s", err, out)
	}
	return bin
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}
