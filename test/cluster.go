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
	"syscall"
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/meta"
)

// node is a kavod process under test.
type node struct {
	t         *testing.T
	id        string
	bin       string
	dataDir   string
	prefix    string
	chunkSize int
	erasure   string
	addr      string
	s3Addr    string
	cmd       *exec.Cmd
	logs      *bytes.Buffer
}

const (
	// testLeaseTTL is etcd's floor. Detection is then as fast as the design
	// allows, which keeps these tests short; production defaults are longer.
	testLeaseTTL = time.Second

	// Repair runs constantly in every process test, unthrottled. That is both
	// what makes automatic healing observable in seconds and a standing check
	// that a repair loop in the background disturbs nothing else.
	testRepairInterval = 200 * time.Millisecond

	// The credentials the S3 port expects. Fixed so the CLI test can be told
	// them, and meaningless outside a test.
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// testRepairRate is what nodes are given for -repair-rate. Unlimited, so that a
// test asserting redundancy came back is waiting on the code rather than on a
// throttle. Only the heal measurement changes it, to report what the production
// cap costs.
var testRepairRate = "0"

// clusterPrefix isolates a test's manifests in etcd. It has to be unique per
// run: etcd outlives the test, so a reused prefix would resolve objects whose
// chunks were left behind in a previous run's data directory.
func clusterPrefix() string { return "/kavo-test/" + rand.Text() }

// startNode launches a cluster of one against dataDir. Handing the same dataDir
// and prefix to a later startNode call simulates a restart.
func startNode(t *testing.T, bin, dataDir, prefix string, chunkSize int) *node {
	t.Helper()
	return launch(t, bin, "n1", freePort(t), dataDir, prefix, chunkSize, "")
}

// startCluster launches n kavod processes into the same cluster, returned in id
// order (n1, n2, ...). They find each other through etcd, so all that makes them
// one cluster is the shared prefix.
func startCluster(t *testing.T, bin, prefix string, chunkSize, n int) []*node {
	t.Helper()
	return startClusterCoded(t, bin, prefix, chunkSize, n, "")
}

// startClusterCoded is startCluster with the nodes erasure-coding new writes, so
// that the flag and everything behind it is exercised as an operator would use it.
func startClusterCoded(t *testing.T, bin, prefix string, chunkSize, n int, erasure string) []*node {
	t.Helper()
	nodes := make([]*node, n)
	for i := range nodes {
		nodes[i] = launch(t, bin, fmt.Sprintf("n%d", i+1), freePort(t), t.TempDir(), prefix, chunkSize, erasure)
	}
	// Every node has to see every other before placement is stable, and until
	// then a write would be spread over a smaller ring than the cluster has.
	for _, n := range nodes {
		n.waitForMembers(len(nodes))
	}
	return nodes
}

func launch(t *testing.T, bin, id, addr, dataDir, prefix string, chunkSize int, erasure string) *node {
	t.Helper()
	n := &node{
		t:         t,
		id:        id,
		bin:       bin,
		dataDir:   dataDir,
		prefix:    prefix,
		chunkSize: chunkSize,
		erasure:   erasure,
		addr:      addr,
		// Every node serves S3 too, on its own port: a restart has to be able to
		// bind both, and the CLI test needs a real one to talk to.
		s3Addr: freePort(t),
		logs:   &bytes.Buffer{},
	}
	n.start()
	t.Cleanup(n.stop)
	n.waitReady()
	return n
}

// start runs the process. Everything it needs is on the node, so starting it
// again after a kill is the same call — which is what a restart is.
func (n *node) start() {
	n.t.Helper()
	n.cmd = exec.Command(n.bin,
		"-id", n.id,
		"-addr", n.addr,
		"-s3", n.s3Addr,
		"-access-key", testAccessKey,
		"-secret-key", testSecretKey,
		"-data", n.dataDir,
		"-chunk-size", fmt.Sprint(n.chunkSize),
		"-etcd", meta.EndpointFromEnv(),
		"-cluster", n.prefix,
		"-lease-ttl", testLeaseTTL.String(),
		"-repair-interval", testRepairInterval.String(),
		"-scrub-interval", testRepairInterval.String(),
		// Rebalancing runs as often as repair here. Membership changes constantly
		// in these tests, and the interval is what decides whether "redundancy
		// comes back" is observable in seconds or in minutes.
		"-rebalance-interval", testRepairInterval.String(),
		"-repair-rate", testRepairRate,
	)
	if n.erasure != "" {
		n.cmd.Args = append(n.cmd.Args, "-ec", n.erasure)
	}
	n.cmd.Stdout = n.logs
	n.cmd.Stderr = n.logs
	if err := n.cmd.Start(); err != nil {
		n.t.Fatalf("start kavod %s: %v", n.id, err)
	}
}

// restart is a process coming back from the dead: same id, same addresses, same
// disk, and whatever the crash left on it.
func (n *node) restart() {
	n.t.Helper()
	n.stop()
	n.start()
	n.waitReady()
}

// pause stops the process without killing it: it holds its ports open, answers
// nothing, and renews no lease. That is what a node behind a network partition or
// stuck in a long GC pause looks like to the rest of the cluster, and it is the
// harder case — a killed node's connections are refused immediately, where a
// paused one's hang.
func (n *node) pause() {
	n.t.Helper()
	if err := n.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		n.t.Fatalf("pause %s: %v", n.id, err)
	}
}

func (n *node) resume() {
	n.t.Helper()
	if err := n.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		n.t.Fatalf("resume %s: %v", n.id, err)
	}
}

// waitReady blocks until both listeners are up. Both, because a node whose S3
// port failed to bind would look healthy to the cluster and be unusable to a
// client.
func (n *node) waitReady() {
	n.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, addr := range []string{n.addr, n.s3Addr} {
		for {
			conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if err == nil {
				conn.Close()
				break
			}
			if time.Now().After(deadline) {
				n.t.Fatalf("kavod never became ready on %s; logs:\n%s", addr, n.logs)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
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
		// Already gone is fine: repair may be rewriting this node's chunks while
		// the disk is being wiped, which is exactly what happens in the chaos run.
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			n.t.Fatalf("remove %s: %v", f, err)
		}
	}
}

// hasChunk reports whether this node's disk holds the chunk with this id.
func (n *node) hasChunk(id string) bool {
	_, err := os.Stat(filepath.Join(n.dataDir, "chunks", id[:2], id))
	return err == nil
}

// rotAChunk flips a bit in one chunk this node holds, which is bit rot: the file
// is the right size, in the right place, with the wrong contents. Reports whether
// there was anything to rot.
func (n *node) rotAChunk(pick func(int) int) bool {
	n.t.Helper()
	files := n.chunkFiles()
	if len(files) == 0 {
		return false
	}
	target := files[pick(len(files))]
	data, err := os.ReadFile(target)
	if err != nil || len(data) == 0 {
		// The scrubber or a repair may have replaced the file underneath us.
		return false
	}
	data[pick(len(data))] ^= 1 << pick(8)
	if err := os.WriteFile(target, data, 0o644); err != nil {
		return false
	}
	return true
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
