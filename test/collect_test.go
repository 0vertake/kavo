package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/0vertake/kavo/internal/cluster"
	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/ring"
)

var collectMu sync.Mutex

// withCollect makes collection quick enough to observe inside a test. The grace
// period still has to be longer than the write below takes, which is what it is
// for; the interval decides how long a full sweep of the id space takes, and a
// sweep has to reach every slice before the garbage in one of them is gone.
func withCollect(interval, grace string) func() {
	collectMu.Lock()
	previousInterval, previousGrace := testCollectInterval, testCollectGrace
	testCollectInterval, testCollectGrace = interval, grace
	return func() {
		testCollectInterval, testCollectGrace = previousInterval, previousGrace
		collectMu.Unlock()
	}
}

// An overwrite supersedes the chunks of the version it replaces, and nothing on
// the write path reclaims them. Four real processes, nobody asking for anything:
// the disk has to come back down to one version's worth on its own, and the object
// has to still read afterwards.
func TestAnOverwriteFreesTheOldVersionOnItsOwn(t *testing.T) {
	defer withCollect("100ms", "500ms")()
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 64<<10, 4)

	const key = "collect/overwritten.bin"
	const size = 4 * 64 << 10

	putSigned(t, nodes[0], key, size)
	_, oneVersion := copiesHeld(nodes)
	if oneVersion == 0 {
		t.Fatal("the cluster holds no chunks after a write")
	}

	putSigned(t, nodes[0], key, size)
	if _, both := copiesHeld(nodes); both <= oneVersion {
		t.Fatalf("an overwrite left %d copies on disk, want more than the %d the first version made", both, oneVersion)
	}

	// A full cycle is one pass per slice of the id space, so this waits on the
	// sweep coming round rather than on any single pass.
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, now := copiesHeld(nodes)
		if now == oneVersion {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster still holds %d copies after a minute, want the %d one version needs", now, oneVersion)
		}
		time.Sleep(100 * time.Millisecond)
	}

	getSigned(t, nodes[0], key, size)
}

// The same pass with a fault in the middle of it, because sweeping a disk for
// things nothing points at is only safe if it is also safe while a node is dying,
// coming back, and being repaired. A killed node's copies are still referenced —
// the manifest names it, and it will hold them again — so a sweep that treats a
// membership change as a reason to delete would eat them.
func TestCollectingWhileANodeDiesTakesOnlyGarbage(t *testing.T) {
	defer withCollect("100ms", "500ms")()
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 64<<10, 4)

	const size = 2 * 64 << 10
	keys := make([]string, 6)
	for i := range keys {
		keys[i] = fmt.Sprintf("collect/faulty%d.bin", i)
		putSigned(t, nodes[0], keys[i], size)
	}
	_, oneVersion := copiesHeld(nodes)

	// Half the disk becomes garbage, and then an owner disappears in the middle of
	// the sweep that has to tell the two apart.
	for _, key := range keys {
		putSigned(t, nodes[0], key, size)
	}
	nodes[3].kill()
	time.Sleep(2 * time.Second)
	nodes[3].restart()

	// Repair may put copies back that the dead node's absence cost, so the disk is
	// allowed to settle at one version's worth or more — never less, and never with
	// an object that stopped reading.
	deadline := time.Now().Add(90 * time.Second)
	for {
		_, now := copiesHeld(nodes)
		if now == oneVersion {
			break
		}
		if now < oneVersion {
			t.Fatalf("the cluster holds %d copies, fewer than the %d one version needs", now, oneVersion)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster still holds %d copies after 90s, want the %d one version needs", now, oneVersion)
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, key := range keys {
		getSigned(t, nodes[0], key, size)
	}
}

// A chunk is durable before the manifest that names it is committed, so for that
// window good data is referenced by nothing at all and only its age protects it.
// This is that window under load: writes arriving continuously from several clients
// at once, against a sweep running as fast as it can go, every one of which has to
// read back exactly what was written. The grace period is short — two seconds
// against writes that take a fraction of one — because a grace of an hour would
// prove nothing about whether the pass consults it.
func TestWritesArrivingDuringSweepsAreAllReadable(t *testing.T) {
	defer withCollect("10ms", "2s")()
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 64<<10, 4)

	const writers = 6
	const each = 8
	const size = 4 * 64 << 10

	type written struct {
		key  string
		body []byte
	}
	acked := make([]written, 0, writers*each)
	var mu sync.Mutex
	var wg sync.WaitGroup
	failed := make(chan error, writers)

	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// A client per writer, spread over the cluster, since which node
			// coordinates decides which node commits and which sweeps.
			client := s3Client(nodes[w%len(nodes)].s3Addr)
			for i := range each {
				key := fmt.Sprintf("collect/racing-%d-%d.bin", w, i)
				body := payload(byte(w*each+i), size)
				if err := putBody(t.Context(), client, key, body); err != nil {
					failed <- fmt.Errorf("PUT %s: %w", key, err)
					return
				}
				mu.Lock()
				acked = append(acked, written{key, body})
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(failed)
	for err := range failed {
		t.Error(err)
	}

	// Read after the writes rather than during them, so that what is being checked
	// is what the sweeps left behind and not what a reader raced.
	client := s3Client(nodes[0].s3Addr)
	for _, w := range acked {
		got, err := getBody(t.Context(), client, w.key)
		if err != nil {
			t.Errorf("GET %s: %v", w.key, err)
			continue
		}
		if !bytes.Equal(got, w.body) {
			t.Errorf("GET %s returned %d bytes of the wrong data, want the %d written", w.key, len(got), len(w.body))
		}
	}
}

// S3 lets a client take days over a multipart upload, so nothing about a part's age
// can be what keeps it: a part's chunks are protected by the part manifest naming
// them, and by the pass reading part manifests as well as object ones. Here the
// upload sits through fifteen full cycles of the id space with a grace period far
// shorter than it has been waiting, and then completes.
func TestAnUploadOutlivingManySweepsStillCompletes(t *testing.T) {
	// Grace long enough to cover a single part upload, which has the same
	// commit window any write has, and nowhere near long enough to cover the wait.
	defer withCollect("10ms", "1s")()
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 64<<10, 4)

	client := s3Client(nodes[0].s3Addr)
	const key = "collect/slow-upload.bin"
	body := payload(0x5b, 6*64<<10)
	split := len(body) / 3

	create, err := client.CreateMultipartUpload(t.Context(), &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(collectBucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("create the upload: %v", err)
	}
	var parts []types.CompletedPart
	for i, part := range [][]byte{body[:split], body[split:]} {
		up, err := client.UploadPart(t.Context(), &awss3.UploadPartInput{
			Bucket: aws.String(collectBucket), Key: aws.String(key), UploadId: create.UploadId,
			PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(part),
		})
		if err != nil {
			t.Fatalf("upload part %d: %v", i+1, err)
		}
		parts = append(parts, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(int32(i + 1))})
	}

	// Fifteen cycles of the whole id space, so every slice the parts live in has
	// been swept several times over while the upload was nothing but parts.
	time.Sleep(5 * time.Second)

	if _, err := client.CompleteMultipartUpload(t.Context(), &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(collectBucket), Key: aws.String(key), UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatalf("complete the upload: %v", err)
	}

	got, err := getBody(t.Context(), client, key)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("the completed object is %d bytes of the wrong data, want the %d uploaded", len(got), len(body))
	}
}

const collectBucket = "collect"

// payload is a body that identifies itself, so that a read returning some other
// object's bytes is a different failure from a read returning nothing.
func payload(seed byte, size int) []byte {
	body := make([]byte, size)
	for i := range body {
		body[i] = seed + byte(i%251)
	}
	return body
}

func putBody(ctx context.Context, client *awss3.Client, key string, body []byte) error {
	_, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(collectBucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	return err
}

func getBody(ctx context.Context, client *awss3.Client, key string) ([]byte, error) {
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(collectBucket), Key: aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// Two background passes, each correct on its own, and the seam between them.
//
// A move copies every chunk to the new owners and only then commits the manifest
// naming them, because until that commit a reader has to find the object where the
// old manifest says it is. The copying is rate-limited. So for the whole length of a
// move — object size over repair rate, which is minutes for a large object and hours
// for a very large one — the destination is holding chunks that no manifest mentions.
// Collection deletes chunks that no manifest mentions.
//
// If the move outlasts the grace period, the sweep takes the copies the move is
// about to promise, the commit promises them anyway, and the old copies are dropped
// on the strength of that promise.
//
// Which is survivable when a move replaces one owner of three: two good copies
// remain and repair puts the third back. It is not survivable when a move replaces
// all three, and a cluster doubling in size does exactly that to some of its
// partitions. So the keys here are chosen for it — every owner different before and
// after — and the move is made slow against a short grace, which is the shape a
// large object has at any grace.
func TestAMoveThatReplacesEveryOwnerKeepsItsData(t *testing.T) {
	defer withCollect("10ms", "500ms")()
	defer withRepairRate("65536")() // 64 KB/s: three copies of 256 KB take twelve seconds
	bin := buildKavod(t)
	prefix := clusterPrefix()
	nodes := startCluster(t, bin, prefix, 64<<10, 3)

	joining := []string{"n4", "n5", "n6"}
	// putSigned writes into the measure bucket, and placement is decided by the
	// stored key, bucket and all.
	keys := keysWhoseOwnersAllChange(t, nodes, joining, "measure/", 2)

	const chunks = 4
	const size = chunks * 64 << 10
	for _, key := range keys {
		putSigned(t, nodes[0], key, size)
	}

	// Three more nodes arrive at once, which is what doubling a cluster looks like,
	// and every copy of these objects has to move.
	grown := nodes
	for _, id := range joining {
		grown = append(grown, launch(t, bin, id, freePort(t), t.TempDir(), prefix, 64<<10, ""))
	}
	for _, n := range grown {
		n.waitForMembers(len(grown))
	}

	// Waiting on the moves rather than on the clock, because a sleep that turns out
	// to be shorter than the moves does not fail this test, it empties it.
	awaitMoved(t, prefix, "measure/", keys, nodes)
	awaitCopies(t, grown, len(keys)*chunks*cluster.Replicas)

	for _, key := range keys {
		getSigned(t, nodes[0], key, size)
	}

	// Reading is the guarantee, but it is not the whole of it: before the sweep
	// consulted the ring, these objects still read, because repair replaced what the
	// sweep took from under the move. Copy, delete, copy again is work the cluster
	// does twice and a durability argument that rests on repair winning a race, so
	// the assertion is that nothing was reclaimed at all. There is no garbage in
	// this test — a move drops what it moved from itself.
	for _, n := range grown {
		if line := firstLineContaining(n.logs.String(), "collect: reclaimed"); line != "" {
			t.Errorf("%s reclaimed chunks while a move was in flight: %s", n.id, line)
		}
	}
}

// awaitMoved waits until every key's manifest names none of the nodes that held it
// before the cluster grew, which is each move having committed.
func awaitMoved(t *testing.T, prefix, bucket string, keys []string, was []*node) {
	t.Helper()
	store, err := meta.Open([]string{meta.EndpointFromEnv()}, prefix)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

	deadline := time.Now().Add(90 * time.Second)
	for _, key := range keys {
		for {
			m, err := store.Get(context.Background(), bucket+key)
			if err != nil {
				t.Fatalf("read the manifest for %s: %v", key, err)
			}
			moved := true
			for _, n := range was {
				moved = moved && !slices.Contains(m.Nodes, n.id)
			}
			if moved {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s still names %v after ninety seconds, so no move ever committed", key, m.Nodes)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// awaitCopies waits for the number of chunks the cluster holds to settle on want,
// which after a move is the drop of what it moved from having happened.
func awaitCopies(t *testing.T, nodes []*node, want int) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, held := copiesHeld(nodes)
		if held == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the cluster holds %d chunks after a minute, want %d", held, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func firstLineContaining(logs, want string) string {
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, want) {
			return line
		}
	}
	return ""
}

// keysWhoseOwnersAllChange finds keys the ring hands to an entirely different set of
// nodes once the cluster grows, which is the case where a move that loses its
// in-flight copies has nothing left to fall back on.
func keysWhoseOwnersAllChange(t *testing.T, nodes []*node, joining []string, bucket string, want int) []string {
	t.Helper()
	before := make([]string, 0, len(nodes))
	for _, n := range nodes {
		before = append(before, n.id)
	}
	after := slices.Sorted(slices.Values(append(slices.Clone(before), joining...)))
	small := ring.New(before, ring.DefaultVNodes)
	large := ring.New(after, ring.DefaultVNodes)

	var keys []string
	for i := 0; len(keys) < want && i < 4000; i++ {
		key := fmt.Sprintf("collect/disjoint%d.bin", i)
		p := ring.PartitionFor(bucket + key)
		was := small.Owners(p, cluster.Replicas)
		now := large.Owners(p, cluster.Replicas)
		if len(was) != cluster.Replicas || len(now) != cluster.Replicas {
			continue
		}
		shared := false
		for _, id := range now {
			shared = shared || slices.Contains(was, id)
		}
		if !shared {
			keys = append(keys, key)
		}
	}
	if len(keys) < want {
		t.Fatalf("found %d keys whose owners all change, want %d", len(keys), want)
	}
	return keys
}
