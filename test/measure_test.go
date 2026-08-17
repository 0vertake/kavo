package test

// The numbers milestones 6 and 8 promised and never printed: how long a heal
// takes, how much data a join moves, and what a node's memory does while a
// multi-gigabyte object streams through it.
//
// These are measurements, not tests. They print a number and assert only that
// the thing finished, because a wall clock on a laptop is not something to fail
// a build on — `make test` skips them, and `make measure` runs them. What they
// are for is `docs/benchmarks.md`: the file says a heal runs at 349 MB/s, which
// is a rate rather than an answer to "a node died, when is redundancy back".
//
// The three claims they exist to cash:
//
//   - repair is rate-limited by the cap and not by the hardware, so the cap is
//     what decides how much a heal disturbs clients (milestone 6)
//   - a join moves about one node's share and converges, leaving nothing behind
//     (milestone 8)
//   - memory stays flat regardless of object size — measured on the node's own
//     RSS, at a size no buffer could hide (milestone 1, at 1000x the size its
//     unit test uses)
//
// Two more, for the mode the microbenchmarks already compare: heal by decode
// rather than replica fetch, and the same RSS pair against a 4+2 object.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/ring"
)

var (
	measure = flag.Bool("measure", false,
		"run the measurements in measure_test.go, which print numbers rather than asserting them")
	measureObject = flag.Int64("measure.object", 2<<30,
		"how large an object the streaming measurement pushes through a node")
	measureData = flag.Int64("measure.data", 512<<20,
		"how many bytes of objects the heal and rebalance measurements write first")
)

const (
	// measureChunkSize is the production default, because a heal moves chunks
	// and their size is what decides how many round trips that is.
	measureChunkSize = 32 << 20

	// measureCluster is six, matching `make up` and every table in
	// docs/benchmarks.md. The rest of the suite uses four — one more than N, so
	// that some node is always an outsider — but a number quoted in the docs
	// should come from the cluster the docs describe.
	measureCluster = 6
)

func skipUnlessMeasuring(t *testing.T) {
	t.Helper()
	if !*measure {
		t.Skip("measurement, not a test: run with -measure")
	}
}

// fill writes n bytes of a cheap pattern, so that a multi-gigabyte body costs no
// memory to produce and is not a run of zeroes a filesystem could learn to
// compress away.
type fill struct {
	left int64
	seed byte
}

func (f *fill) Read(p []byte) (int, error) {
	if f.left <= 0 {
		return 0, io.EOF
	}
	n := int64(len(p))
	if n > f.left {
		n = f.left
	}
	for i := range p[:n] {
		p[i] = f.seed ^ byte(i*31>>3) ^ byte(i)
	}
	f.left -= n
	return int(n), nil
}

// rssOf reads a process's resident set size in bytes. ps is the one answer that
// means the same thing on macOS and Linux, and this samples it rather than
// asking Go's runtime, because the claim is about the node process and not about
// one package's allocations.
func rssOf(pid int) int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return kb << 10
}

// rssWatcher samples every node's RSS until it is stopped, and reports the
// highest each one reached.
type rssWatcher struct {
	nodes []*node
	peak  []int64
	stop  chan struct{}
	done  chan struct{}
}

func watchRSS(nodes []*node) *rssWatcher {
	w := &rssWatcher{
		nodes: nodes,
		peak:  make([]int64, len(nodes)),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	go func() {
		defer close(w.done)
		for {
			w.sample()
			select {
			case <-w.stop:
				w.sample() // once more, in case the peak was in the last interval
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return w
}

func (w *rssWatcher) sample() {
	for i, n := range w.nodes {
		if n.cmd == nil || n.cmd.Process == nil {
			continue
		}
		if rss := rssOf(n.cmd.Process.Pid); rss > w.peak[i] {
			w.peak[i] = rss
		}
	}
}

func (w *rssWatcher) highest() (int64, string) {
	w.sample()
	var worst int64
	var which string
	for i, rss := range w.peak {
		if rss > worst {
			worst, which = rss, w.nodes[i].id
		}
	}
	return worst, which
}

func (w *rssWatcher) close() (int64, string) {
	close(w.stop)
	<-w.done
	return w.highest()
}

// writeObjects fills the cluster with total bytes of objects through the internal
// API, streaming each one so that the measurement's own memory is not what is
// being measured.
func writeObjects(t *testing.T, n *node, total int64, each int64) int {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Minute}
	count := 0
	for written := int64(0); written < total; written += each {
		key := fmt.Sprintf("measure/obj%04d", count)
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

// copiesHeld is how many chunk files each node is holding, and the total. It is
// the ground truth both measurements below compare against: the manifests say
// where copies belong, and this says where they are.
func copiesHeld(nodes []*node) (perNode map[string]int, total int) {
	perNode = make(map[string]int, len(nodes))
	for _, n := range nodes {
		held := len(n.chunkFiles())
		perNode[n.id] = held
		total += held
	}
	return perNode, total
}

func bytesHeld(n *node) int64 {
	var sum int64
	for _, f := range n.chunkFiles() {
		st, err := os.Stat(f)
		if err != nil {
			continue
		}
		sum += st.Size()
	}
	return sum
}

// Milestone 6's number. A node loses its entire disk while the cluster keeps
// running, and nobody asks for a repair: this reports how long redundancy takes
// to come back, and how much had to move to get there.
//
// Run once at the production rate cap and once unthrottled, because the point of
// the cap is that it — rather than the hardware — decides how long a heal takes
// and how much it disturbs everything else.
func TestMeasureHealTime(t *testing.T) {
	skipUnlessMeasuring(t)

	for _, rate := range []string{strconv.FormatInt(cluster.DefaultRepairRate, 10), "0"} {
		name := "capped at " + rate + " B/s"
		if rate == "0" {
			name = "unthrottled"
		}
		t.Run(name, func(t *testing.T) {
			defer withRepairRate(rate)()

			bin := buildKavod(t)
			prefix := clusterPrefix()
			nodes := startCluster(t, bin, prefix, measureChunkSize, measureCluster)
			store, err := meta.Open([]string{meta.EndpointFromEnv()}, prefix)
			if err != nil {
				t.Fatalf("meta.Open: %v", err)
			}
			defer store.Close()

			objects := writeObjects(t, nodes[0], *measureData, measureChunkSize)
			before, totalCopies := copiesHeld(nodes)

			// The whole disk, not a few chunks: this is the scenario the heal
			// time is quoted for — a node comes back with nothing.
			victim := nodes[len(nodes)-1]
			lost := before[victim.id]
			if lost == 0 {
				t.Fatalf("%s holds no chunks, so there is nothing to heal", victim.id)
			}
			lostBytes := int64(lost) * measureChunkSize

			byID := make(map[string]*node, len(nodes))
			for _, n := range nodes {
				byID[n.id] = n
			}

			start := time.Now()
			victim.loseChunks()
			var elapsed time.Duration
			for {
				if holes, _, settled := missingCopies(t.Context(), byID, store, len(nodes)); settled && len(holes) == 0 {
					elapsed = time.Since(start)
					break
				}
				if time.Since(start) > 10*time.Minute {
					t.Fatal("redundancy did not come back within 10 minutes")
				}
				time.Sleep(100 * time.Millisecond)
			}

			t.Logf("%d objects, %d chunk copies over %d nodes; %s lost %d copies (%s)",
				objects, totalCopies, len(nodes), victim.id, lost, bytesOf(lostBytes))
			t.Logf("redundancy restored in %v (%s of copies rebuilt, %s effective)",
				elapsed.Round(10*time.Millisecond), bytesOf(lostBytes),
				rateOf(lostBytes, elapsed))
		})
	}
}

// The same wipe, against a 4+2 cluster: repair rebuilds the missing shards by
// decoding their siblings rather than fetching a replica. That is a different
// cost — k sources per shard instead of one — and the number the replicated
// measurement cannot speak for.
func TestMeasureHealTimeCoded(t *testing.T) {
	skipUnlessMeasuring(t)
	defer withRepairRate("0")()

	bin := buildKavod(t)
	prefix := clusterPrefix()
	nodes := startClusterCoded(t, bin, prefix, measureChunkSize, measureCluster, "4+2")
	store, err := meta.Open([]string{meta.EndpointFromEnv()}, prefix)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

	objects := writeObjects(t, nodes[0], *measureData, measureChunkSize)
	before, totalShards := copiesHeld(nodes)

	victim := nodes[len(nodes)-1]
	lost := before[victim.id]
	if lost == 0 {
		t.Fatalf("%s holds no shards, so there is nothing to heal", victim.id)
	}
	lostBytes := bytesHeld(victim)

	byID := make(map[string]*node, len(nodes))
	for _, n := range nodes {
		byID[n.id] = n
	}

	start := time.Now()
	victim.loseChunks()
	var elapsed time.Duration
	for {
		if holes, _, settled := missingCopies(t.Context(), byID, store, len(nodes)); settled && len(holes) == 0 {
			elapsed = time.Since(start)
			break
		}
		if time.Since(start) > 10*time.Minute {
			t.Fatal("redundancy did not come back within 10 minutes")
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("%d objects, %d shards over %d nodes; %s lost %d shards (%s)",
		objects, totalShards, len(nodes), victim.id, lost, bytesOf(lostBytes))
	t.Logf("redundancy restored in %v by decode (%s of shards rebuilt, %s effective)",
		elapsed.Round(10*time.Millisecond), bytesOf(lostBytes),
		rateOf(lostBytes, elapsed))
}

// Milestone 8's numbers. A seventh node joins a cluster that already holds data:
// this reports what fraction of the copies move onto it, how long the cluster
// takes to converge, and whether the nodes that gave data up let go of it.
//
// The fraction is the interesting one. Consistent hashing's whole claim is that
// a join moves about one node's share and nothing else — 1/7 here — where
// hashing keys modulo the node count would move most of the data.
func TestMeasureRebalanceOnJoin(t *testing.T) {
	skipUnlessMeasuring(t)

	// A move leaves the copies it superseded for collection, so how long the cluster
	// pays for two placements of the data that moved is part of what a join costs.
	// Measuring that inside a run means turning the collector up; the shipped grace is
	// an hour, which is longer than this measurement. Nothing is written after the
	// join, so a ten-second grace cannot reach a write in flight.
	defer withCollect("1s", "10s")()

	bin := buildKavod(t)
	prefix := clusterPrefix()
	nodes := startCluster(t, bin, prefix, measureChunkSize, measureCluster)
	store, err := meta.Open([]string{meta.EndpointFromEnv()}, prefix)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

	objects := writeObjects(t, nodes[0], *measureData, measureChunkSize)
	_, before := copiesHeld(nodes)

	joiner := launch(t, bin, fmt.Sprintf("n%d", measureCluster+1), freePort(t),
		t.TempDir(), prefix, measureChunkSize, "")
	grown := append(nodes, joiner)
	byID := make(map[string]*node, len(grown))
	for _, n := range grown {
		byID[n.id] = n
	}

	start := time.Now()
	for _, n := range grown {
		n.waitForMembers(len(grown))
	}
	detected := time.Since(start)

	// Converged means the manifests are satisfied and the joiner has stopped
	// receiving: no copy the manifests name is missing, and the count it holds
	// has stood still for three seconds.
	//
	// Deliberately not part of that definition: the total copy count returning to
	// what it was. It cannot have, at this point. A move copies to the new owners and
	// commits, and stops there — nothing but the collection pass deletes a chunk, since
	// a copied object shares its source's chunks. So the cluster holds two placements
	// of everything that moved until a sweep comes past, and how long that takes is
	// measured separately below.
	var converged time.Duration
	var moved int
	stableSince := time.Time{}
	for {
		holes, _, settled := missingCopies(t.Context(), byID, store, len(grown))
		perNode, _ := copiesHeld(grown)
		switch {
		case !settled || len(holes) > 0 || perNode[joiner.id] != moved:
			moved, stableSince = perNode[joiner.id], time.Time{}
		case stableSince.IsZero():
			stableSince = time.Now()
		case time.Since(stableSince) > 3*time.Second:
			converged = time.Since(start)
		}
		if converged != 0 {
			break
		}
		if time.Since(start) > 5*time.Minute {
			t.Fatalf("no convergence within 5 minutes: %d holes, joiner holds %d",
				len(holes), perNode[joiner.id])
		}
		time.Sleep(200 * time.Millisecond)
	}

	// What the ring says should have moved — not a statistical prediction but the
	// exact count, computed from the keys that exist against the seven-node ring.
	// Rebalance is correct exactly when the two numbers are the same one.
	ids := make([]string, len(grown))
	for i, n := range grown {
		ids[i] = n.id
	}
	r := ring.New(ids, ring.DefaultVNodes)
	stored, err := store.ScanObjects(t.Context(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	owed := 0
	for _, o := range stored {
		for _, owner := range r.Owners(ring.PartitionFor(o.Key), cluster.Replicas) {
			if owner == joiner.id {
				owed += len(o.Manifest.Chunks)
			}
		}
	}

	extra, err := misplaced(t.Context(), grown, store)
	if err != nil {
		t.Fatal(err)
	}
	residue := 0
	for _, ids := range extra {
		residue += len(ids)
	}
	_, after := copiesHeld(grown)

	// Now wait for the residue to go, which is the other half of what a join costs:
	// the space is committed to the old placement until a sweep reaches the slices
	// those chunks are in.
	reclaimStart := time.Now()
	var reclaimed time.Duration
	var left int
	for {
		extra, err := misplaced(t.Context(), grown, store)
		if err != nil {
			t.Fatal(err)
		}
		left = 0
		for _, ids := range extra {
			left += len(ids)
		}
		if left == 0 {
			reclaimed = time.Since(reclaimStart)
			break
		}
		if time.Since(reclaimStart) > 5*time.Minute {
			t.Fatalf("%d copies no manifest names were still on disk five minutes after the move", left)
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Logf("%d objects, %d chunk copies over %d nodes before the join", objects, before, len(nodes))
	t.Logf("%s joined: seen by every node in %v, converged in %v",
		joiner.id, detected.Round(10*time.Millisecond), converged.Round(10*time.Millisecond))
	t.Logf("%d of %d copies moved onto it (%.1f%%), and the seven-node ring owes it %d",
		moved, before, 100*float64(moved)/float64(before), owed)
	t.Logf("%d copies once the move committed against %d before, %d of them named by no manifest",
		after, before, residue)
	t.Logf("collection reclaimed all %d in %v after the move, at a 1s interval and a 10s grace",
		residue, reclaimed.Round(100*time.Millisecond))
}

// Milestone 1's claim at a size that cannot be explained away: an object far
// larger than any buffer streams in and out while the node's own resident memory
// is watched from outside the process.
//
// The unit test for this uses 64 MB and Go's allocation counters. This uses
// gigabytes and `ps`, because "flat memory" is a promise about the process an
// operator runs, and because a 2 GB object is the case a reviewer actually
// doubts.
func TestMeasureStreamingALargeObject(t *testing.T) {
	skipUnlessMeasuring(t)

	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), measureChunkSize, measureCluster)

	// Idle first, so the numbers below are a rise over a baseline rather than
	// the size of a Go runtime.
	idle, _ := watchRSS(nodes).close()
	t.Logf("idle RSS, highest of %d nodes: %s", len(nodes), bytesOf(idle))

	// The same object twice over, at two sizes three orders of magnitude apart.
	// One number proves nothing; the pair is what "flat regardless of size" means.
	for _, size := range []int64{64 << 20, *measureObject} {
		t.Run(bytesOf(size), func(t *testing.T) {
			key := "measure/streamed-" + bytesOf(size)
			watcher := watchRSS(nodes)

			start := time.Now()
			putSigned(t, nodes[0], key, size)
			put := time.Since(start)

			start = time.Now()
			getSigned(t, nodes[0], key, size)
			get := time.Since(start)

			peak, which := watcher.close()
			t.Logf("PUT %s in %v (%s), GET in %v (%s)",
				bytesOf(size), put.Round(time.Millisecond), rateOf(size, put),
				get.Round(time.Millisecond), rateOf(size, get))
			t.Logf("peak RSS %s on %s, %s over idle, %.1f%% of the object",
				bytesOf(peak), which, bytesOf(peak-idle), 100*float64(peak)/float64(size))
		})
	}
}

// The same pair of sizes, stored 4+2. A coded read holds a chunk's shards and the
// reconstructed chunk before any of it is valid, which is why a 64 MB GET
// allocates 168 MB in the microbenchmark. This is whether that shows up in the
// process at gigabyte scale, or whether it still stays chunk-shaped.
func TestMeasureStreamingACodedObject(t *testing.T) {
	skipUnlessMeasuring(t)

	bin := buildKavod(t)
	nodes := startClusterCoded(t, bin, clusterPrefix(), measureChunkSize, measureCluster, "4+2")

	idle, _ := watchRSS(nodes).close()
	t.Logf("idle RSS, highest of %d nodes: %s", len(nodes), bytesOf(idle))

	for _, size := range []int64{64 << 20, *measureObject} {
		t.Run(bytesOf(size), func(t *testing.T) {
			key := "measure/coded-" + bytesOf(size)
			watcher := watchRSS(nodes)

			start := time.Now()
			putSigned(t, nodes[0], key, size)
			put := time.Since(start)

			start = time.Now()
			getSigned(t, nodes[0], key, size)
			get := time.Since(start)

			peak, which := watcher.close()
			t.Logf("PUT %s encoded 4+2 in %v (%s), GET in %v (%s)",
				bytesOf(size), put.Round(time.Millisecond), rateOf(size, put),
				get.Round(time.Millisecond), rateOf(size, get))
			t.Logf("peak RSS %s on %s, %s over idle, %.1f%% of the object",
				bytesOf(peak), which, bytesOf(peak-idle), 100*float64(peak)/float64(size))
		})
	}
}

// putSigned streams size bytes to a node's S3 port with an unsigned payload,
// which is the one way to upload something larger than memory to a signed API:
// a hex payload hash would mean reading the object twice, and holding it to hash
// it is the thing being measured.
func putSigned(t *testing.T, n *node, key string, size int64) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		"http://"+n.s3Addr+"/measure/"+key, &fill{left: size, seed: 7})
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = size
	sign(t, req, "UNSIGNED-PAYLOAD")

	resp, err := (&http.Client{Timeout: time.Hour}).Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<12))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT: status %d: %s", resp.StatusCode, body)
	}
}

// getSigned reads the object back and throws it away, counting the bytes. The
// count is the assertion: a short read would make the RSS number meaningless.
func getSigned(t *testing.T, n *node, key string, size int64) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://"+n.s3Addr+"/measure/"+key, nil)
	if err != nil {
		t.Fatal(err)
	}
	sign(t, req, emptyPayloadHash)

	resp, err := (&http.Client{Timeout: time.Hour}).Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<12))
		t.Fatalf("GET: status %d: %s", resp.StatusCode, body)
	}
	read, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		t.Fatalf("GET body: %v", err)
	}
	if read != size {
		t.Fatalf("GET returned %d bytes, want %d", read, size)
	}
}

var emptyPayloadHash = func() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}()

func sign(t *testing.T, req *http.Request, payload string) {
	t.Helper()
	req.Header.Set("X-Amz-Content-Sha256", payload)
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	if err := signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey,
	}, req, payload, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
}

func bytesOf(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.0f MB", float64(n)/(1<<20))
	default:
		return fmt.Sprintf("%.0f KB", float64(n)/(1<<10))
	}
}

func rateOf(n int64, d time.Duration) string {
	if d <= 0 {
		return "instant"
	}
	return fmt.Sprintf("%.0f MB/s", float64(n)/(1<<20)/d.Seconds())
}

// Guard against two measurements changing the harness's repair rate at once.
// They cannot: tests in a package run one at a time unless they ask not to, and
// none of these do.
var repairRateMu sync.Mutex

func withRepairRate(rate string) func() {
	repairRateMu.Lock()
	previous := testRepairRate
	testRepairRate = rate
	return func() {
		testRepairRate = previous
		repairRateMu.Unlock()
	}
}

// misplaced names every chunk file a node is holding that no manifest says it
// should. A rebalance moves a copy and removes the original, so a residue here is
// either a move that did not finish its second half or a copy nobody will ever
// read again — and either way it is storage being paid for and not counted.
func misplaced(ctx context.Context, nodes []*node, store *meta.Store) (map[string][]string, error) {
	objects, err := store.ScanObjects(ctx, "", "", 0)
	if err != nil {
		return nil, err
	}
	// Every (node, chunk) pair the manifests call for.
	wanted := make(map[string]map[string]bool, len(nodes))
	for _, o := range objects {
		for _, ref := range o.Manifest.Chunks {
			for i, id := range o.Manifest.Nodes {
				want := ref.ID
				if o.Manifest.Coding.Valid() {
					want = ref.ShardID(i)
				}
				if wanted[id] == nil {
					wanted[id] = map[string]bool{}
				}
				wanted[id][want] = true
			}
		}
	}

	extra := make(map[string][]string)
	for _, n := range nodes {
		for _, f := range n.chunkFiles() {
			id := filepath.Base(f)
			if !wanted[n.id][id] {
				extra[n.id] = append(extra[n.id], id)
			}
		}
	}
	return extra, nil
}

// copyPartSize is what a client picks for a large copy: big enough that a
// multi-gigabyte object is a handful of calls, and far larger than the chunk size,
// so a part is re-chunked rather than mapped one-to-one.
const copyPartSize = 256 << 20

// What a server-side copy costs, which is the question UploadPartCopy exists to
// answer and the one the S3 surface cannot answer on its own.
//
// Three numbers, one object:
//
//   - CopyObject copies a manifest, so it moves no bytes and takes about as long as
//     an etcd write. The point of measuring it at gigabyte scale is that the number
//     does not change with the size.
//   - a multipart copy does move the bytes, inside the cluster, through the
//     ordinary write path. That is what a client above 8 MB gets, so its rate and
//     its memory are the honest cost of the feature — and the memory is a claim:
//     the part is streamed, so a 256 MB part must not appear in a node's RSS.
//   - the round trip a client would make without it, out to the client and back.
//     The saving is the difference, and it is the argument for the feature.
func TestMeasureCopyingALargeObject(t *testing.T) {
	skipUnlessMeasuring(t)

	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), measureChunkSize, measureCluster)

	idle, _ := watchRSS(nodes).close()
	t.Logf("idle RSS, highest of %d nodes: %s", len(nodes), bytesOf(idle))

	size := *measureObject
	const source = "copy/source"
	putSigned(t, nodes[0], source, size)
	t.Logf("source object: %s", bytesOf(size))

	t.Run("CopyObject", func(t *testing.T) {
		start := time.Now()
		status, body := s3Do(t, nodes[0], http.MethodPut, "copy/whole", nil,
			map[string]string{"X-Amz-Copy-Source": "measure/" + source})
		took := time.Since(start)
		if status != http.StatusOK {
			t.Fatalf("CopyObject: status %d: %s", status, body)
		}
		// Read it back, or "instant" would only prove the request returned.
		getSigned(t, nodes[1], "copy/whole", size)
		t.Logf("copied %s in %v by copying the manifest, moving no bytes",
			bytesOf(size), took.Round(time.Millisecond))
	})

	t.Run("UploadPartCopy", func(t *testing.T) {
		watcher := watchRSS(nodes)
		const dest = "copy/assembled"

		start := time.Now()
		id := createUpload(t, nodes[0], dest)
		var parts []completedPart
		for off, number := int64(0), 1; off < size; number++ {
			end := min(off+copyPartSize, size) - 1
			etag := copyPartRange(t, nodes[number%len(nodes)], dest, id, number,
				"measure/"+source, fmt.Sprintf("bytes=%d-%d", off, end))
			parts = append(parts, completedPart{PartNumber: number, ETag: etag})
			off = end + 1
		}
		completeUpload(t, nodes[0], dest, id, parts)
		took := time.Since(start)

		getSigned(t, nodes[1], dest, size)
		peak, which := watcher.close()
		t.Logf("copied %s as %d parts of %s in %v (%s)",
			bytesOf(size), len(parts), bytesOf(copyPartSize), took.Round(time.Millisecond), rateOf(size, took))
		t.Logf("peak RSS %s on %s, %s over idle: %.1f%% of a part, %.1f%% of the object",
			bytesOf(peak), which, bytesOf(peak-idle),
			100*float64(peak-idle)/float64(copyPartSize), 100*float64(peak-idle)/float64(size))
	})

	t.Run("client round trip", func(t *testing.T) {
		// What the same copy costs a client that has to fetch the object and send
		// it back. Sequential, so this is an upper bound: a client piping one into
		// the other overlaps them, at best halving it. Either way the bytes cross
		// the client's link twice, which is the part a server-side copy removes.
		start := time.Now()
		getSigned(t, nodes[0], source, size)
		down := time.Since(start)
		start = time.Now()
		putSigned(t, nodes[0], "copy/reuploaded", size)
		up := time.Since(start)
		t.Logf("out and back: %v down (%s) + %v up (%s) = %v of client traffic for the same copy",
			down.Round(time.Millisecond), rateOf(size, down),
			up.Round(time.Millisecond), rateOf(size, up),
			(down + up).Round(time.Millisecond))
	})
}

// s3Do signs and sends a request whose body is small enough to hash, which is every
// call in a multipart copy: the bytes being copied never pass through here.
func s3Do(t *testing.T, n *node, method, key string, body []byte, headers map[string]string) (int, []byte) {
	t.Helper()
	sum := sha256.Sum256(body)
	req, err := http.NewRequestWithContext(t.Context(), method,
		"http://"+n.s3Addr+"/measure/"+key, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	sign(t, req, hex.EncodeToString(sum[:]))

	resp, err := (&http.Client{Timeout: time.Hour}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, key, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	return resp.StatusCode, out
}

type completedPart struct {
	PartNumber int
	ETag       string
}

func createUpload(t *testing.T, n *node, key string) string {
	t.Helper()
	status, body := s3Do(t, n, http.MethodPost, key+"?uploads", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("CreateMultipartUpload: status %d: %s", status, body)
	}
	var out struct{ UploadId string }
	if err := xml.Unmarshal(body, &out); err != nil {
		t.Fatalf("CreateMultipartUpload body: %v: %s", err, body)
	}
	return out.UploadId
}

func copyPartRange(t *testing.T, n *node, key, id string, number int, source, byteRange string) string {
	t.Helper()
	status, body := s3Do(t, n, http.MethodPut,
		fmt.Sprintf("%s?partNumber=%d&uploadId=%s", key, number, id), nil,
		map[string]string{"X-Amz-Copy-Source": source, "X-Amz-Copy-Source-Range": byteRange})
	if status != http.StatusOK {
		t.Fatalf("UploadPartCopy %s: status %d: %s", byteRange, status, body)
	}
	var out struct{ ETag string }
	if err := xml.Unmarshal(body, &out); err != nil {
		t.Fatalf("UploadPartCopy body: %v: %s", err, body)
	}
	return out.ETag
}

func completeUpload(t *testing.T, n *node, key, id string, parts []completedPart) {
	t.Helper()
	var b strings.Builder
	b.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", p.PartNumber, p.ETag)
	}
	b.WriteString("</CompleteMultipartUpload>")
	status, body := s3Do(t, n, http.MethodPost, key+"?uploadId="+id, []byte(b.String()), nil)
	if status != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload: status %d: %s", status, body)
	}
}
