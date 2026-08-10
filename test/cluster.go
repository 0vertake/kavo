// Package test holds integration tests that run kavod as a real process.
package test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/0vertake/kavo/internal/meta"
)

// node is a kavod process under test.
type node struct {
	t       *testing.T
	bin     string
	dataDir string
	addr    string
	cmd     *exec.Cmd
	logs    *bytes.Buffer
}

// clusterPrefix isolates a test's manifests in etcd. It has to be unique per
// run: etcd outlives the test, so a reused prefix would resolve objects whose
// chunks were left behind in a previous run's data directory.
func clusterPrefix() string { return "/kavo-test/" + rand.Text() }

// startNode builds kavod once per test and launches it against dataDir. Handing
// the same dataDir and prefix to a later startNode call simulates a restart.
func startNode(t *testing.T, bin, dataDir, prefix string, chunkSize int) *node {
	t.Helper()
	n := &node{
		t:       t,
		bin:     bin,
		dataDir: dataDir,
		addr:    freePort(t),
		logs:    &bytes.Buffer{},
	}
	n.cmd = exec.Command(bin,
		"-addr", n.addr,
		"-data", dataDir,
		"-chunk-size", fmt.Sprint(chunkSize),
		"-etcd", meta.EndpointFromEnv(),
		"-cluster", prefix,
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
