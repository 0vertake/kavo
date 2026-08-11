package test

// Milestone 10: the chaos suite.
//
// Every other test injects one fault and checks one thing. This one runs a
// concurrent S3 workload against real processes while faults arrive at random —
// crashes, restarts, lost disks, bit rot, and processes frozen mid-request — and
// then asserts the four invariants from the recorded history:
//
//  1. no acknowledged write is lost
//  2. no partially written object is ever readable
//  3. every read returns checksum-valid data or an explicit error
//  4. after healing, redundancy is back to the configured level
//
// The history is what makes this more than a smoke test. Every acknowledgement is
// recorded as it happens, so the final pass knows exactly which objects the store
// promised to keep — and an object nobody was promised is allowed to be missing,
// but is not allowed to be wrong.
//
// The workload speaks S3 with a real signature, because that is the surface a user
// has, and because multipart upload only exists there: an upload spanning a crash
// is a case no single-request test reaches.

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/0vertake/kavo/internal/meta"
)

var (
	chaosDuration = flag.Duration("chaos.duration", 20*time.Second,
		"how long the chaos workload runs before the invariants are checked")
	chaosSeed = flag.Int64("chaos.seed", 0,
		"seed for the fault schedule and workload; 0 picks one and logs it so a failure can be replayed")
)

const (
	chaosBucket  = "chaos"
	chaosWorkers = 8
	// Long enough that most objects span several chunks, short enough that a
	// worker completes many of them inside the run.
	chaosChunkSize = 64 << 10
)

// write is one entry of the recorded history: what was sent, and whether the
// store promised to keep it.
type write struct {
	key       string
	size      int
	fill      byte
	multipart bool
	// acked is the promise. Only a 2xx sets it, and only after the whole request
	// finished — a request whose outcome is unknown stays false, and an object
	// that was never promised is allowed to be absent.
	acked bool
	// A delete has three outcomes, not two, and conflating the last two is how a
	// chaos suite invents a lost write: acknowledged (the object must be gone),
	// never attempted (it must still be there), or attempted and unanswered,
	// which says nothing at all. The response to a delete can be lost after the
	// manifest is already removed — that is a crash between two correct steps,
	// not a violation, and the checker has to allow both outcomes for it.
	deleted, deleteUnknown bool
}

// payload derives an object's bytes from two small numbers so that a history of
// thousands of objects costs nothing to keep, and any byte can be recomputed to
// compare against.
func chaosPayload(fill byte, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = fill ^ byte(i*7>>3) ^ byte(i)
	}
	return b
}

func TestChaos(t *testing.T) {
	seed := *chaosSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	t.Logf("chaos seed %d, duration %v (replay with -chaos.seed=%d)", seed, *chaosDuration, seed)

	bin := buildKavod(t)
	prefix := clusterPrefix()
	// One more node than an object needs, so that a single node being down still
	// leaves W=2 of the object's owners reachable and the workload keeps making
	// progress. Faults are applied to one node at a time for the same reason:
	// refusing writes when quorum is genuinely unreachable is correct but proves
	// nothing that TestWritesAreRefusedUntilMembershipCatchesUp does not.
	nodes := startCluster(t, bin, prefix, chaosChunkSize, clusterSize)
	clients := make([]*awss3.Client, len(nodes))
	for i, n := range nodes {
		clients[i] = s3Client(n.s3Addr)
	}

	var (
		mu      sync.Mutex // guards history and faults
		history []write
		faults  []string
		reads   atomic.Int64
	)

	ctx, stop := context.WithTimeout(t.Context(), *chaosDuration)
	defer stop()

	// The fault schedule, and the one thing it must not do: exceed the redundancy
	// the cluster was configured with. N=3 tolerates two copies of a chunk being
	// lost, not three — wiping a second disk before repair has restored the first
	// destroys data no design claims to keep, and a suite that does it is testing
	// a promise nobody made.
	//
	// So the schedule is one fault at a time, and the next one waits until the
	// cluster is *whole* again: every node a member, every chunk on every owner its
	// manifest names. That barrier is not a convenience — it is invariant 4,
	// asserted after every single fault instead of once at the end.
	store, err := meta.Open([]string{meta.EndpointFromEnv()}, prefix)
	if err != nil {
		t.Fatalf("meta.Open: %v", err)
	}
	defer store.Close()

	var faultsWG sync.WaitGroup
	faultsWG.Add(1)
	go func() {
		defer faultsWG.Done()
		defer stop() // a cluster that cannot heal ends the run
		rng := rand.New(rand.NewPCG(uint64(seed), 0x9e3779b9))
		for ctx.Err() == nil {
			sleep(ctx, time.Duration(200+rng.IntN(800))*time.Millisecond)
			if ctx.Err() != nil {
				return
			}
			victim := nodes[rng.IntN(len(nodes))]
			what := injectFault(ctx, t, victim, rng)
			mu.Lock()
			faults = append(faults, what)
			mu.Unlock()

			// Not the run's context: a barrier that gave up because the clock ran
			// out would report nothing, which is the one way this assertion could
			// quietly stop existing. The run ends a little late instead.
			if holes := waitWhole(context.WithoutCancel(ctx), nodes, store, healStall); len(holes) > 0 {
				t.Errorf("after %q, repair stopped restoring copies for %v with the cluster still short %d; first few:",
					what, healStall, len(holes))
				for _, h := range holes[:min(5, len(holes))] {
					t.Errorf("  %s", h)
				}
				return
			}
		}
	}()

	var work sync.WaitGroup
	for w := range chaosWorkers {
		work.Add(1)
		go func() {
			defer work.Done()
			// Each worker owns its keys, so no two operations can race over one
			// object and leave the history ambiguous. Which node a request goes
			// to is still random: any node coordinates any request.
			rng := rand.New(rand.NewPCG(uint64(seed), uint64(w)+1))
			var mine []write // this worker's acknowledged, still-present objects
			for i := 0; ctx.Err() == nil; i++ {
				client := clients[rng.IntN(len(clients))]
				entry := write{
					key:       fmt.Sprintf("w%d/obj%04d", w, i),
					size:      1 + rng.IntN(3*chaosChunkSize),
					fill:      byte(rng.IntN(256)),
					multipart: rng.IntN(4) == 0,
				}
				if entry.multipart {
					// Parts of uneven size, deliberately not aligned to the
					// chunk size, and at least two of them.
					entry.size = chaosChunkSize + rng.IntN(2*chaosChunkSize)
				}
				entry.acked = chaosPut(ctx, client, entry)

				// Read it back through a different node, which is where a
				// manifest committed but not readable would show up. Then read
				// something older, because a fault that arrives long after a
				// write is the case reading-what-you-just-wrote never reaches:
				// its owner may be frozen, wiped, or halfway through a restart.
				if entry.acked {
					mine = append(mine, entry)
				}
				for _, w := range readTargets(entry, mine, rng) {
					reads.Add(1)
					// During a fault window a read may fail outright — invariant
					// 3 promises an error, not success. What it may never do is
					// return bytes nobody wrote.
					if err := chaosGet(ctx, clients[rng.IntN(len(clients))], w); errors.Is(err, errWrongBytes) {
						t.Errorf("%s: %v", w.key, err)
					}
				}
				// A fraction are deleted again, so the history also covers keys
				// that must be gone at the end rather than present.
				if entry.acked && rng.IntN(5) == 0 {
					_, err := clients[rng.IntN(len(clients))].DeleteObject(ctx, &awss3.DeleteObjectInput{
						Bucket: aws.String(chaosBucket), Key: aws.String(entry.key),
					})
					entry.deleted = err == nil
					entry.deleteUnknown = err != nil
				}
				mu.Lock()
				history = append(history, entry)
				mu.Unlock()
			}
		}()
	}
	work.Wait()
	stop()
	faultsWG.Wait()

	acked, deleted := 0, 0
	for _, w := range history {
		if w.acked {
			acked++
		}
		if w.deleted {
			deleted++
		}
	}
	t.Logf("history: %d writes, %d acknowledged, %d deleted again, %d reads during faults; %d faults injected:\n  %s",
		len(history), acked, deleted, reads.Load(), len(faults), strings.Join(faults, "\n  "))
	if acked == 0 {
		t.Fatal("nothing was ever acknowledged: the workload never worked, so the invariants are vacuous")
	}
	if len(faults) < 3 {
		t.Fatalf("only %d faults were injected; the run is too short to have tested anything", len(faults))
	}

	// Faults stop here. Every node comes back, because invariant 4 is about the
	// cluster healing, not about it healing while being hit.
	for _, n := range nodes {
		if n.cmd.ProcessState != nil {
			n.restart()
		} else {
			// Harmless if it was never paused, and the only way back if the run
			// ended in the middle of a freeze.
			n.resume()
		}
	}
	for _, n := range nodes {
		n.waitForMembers(len(nodes))
	}

	// Invariants 1, 2 and 3: every acknowledged object reads back byte for byte,
	// every deleted one is gone, and an object that was never acknowledged is
	// either absent or perfect — never a torn or corrupt version of itself.
	checkHistory(t, clients[0], history, restartSettle)

	// Invariant 4, one last time now that the workload has stopped.
	if holes := waitWhole(context.WithoutCancel(ctx), nodes, store, healStall); len(holes) > 0 {
		t.Errorf("the run ended and repair then stopped for %v with %d copies still missing; first few:",
			healStall, len(holes))
		for _, h := range holes[:min(10, len(holes))] {
			t.Errorf("  %s", h)
		}
	}
}

func checkHistory(t *testing.T, client *awss3.Client, history []write, settle time.Duration) {
	t.Helper()
	// Reads are retried for as long as healing is allowed to take: a node that
	// was killed a moment ago is still starting, and invariant 1 says the data
	// survives, not that every read during recovery succeeds.
	deadline := time.Now().Add(settle)
	lost, torn, absent, restored := 0, 0, 0, 0
	for _, w := range history {
		var err error
		for {
			err = chaosGet(t.Context(), client, w)
			if err == nil || errors.Is(err, errWrongBytes) || errors.Is(err, errAbsent) ||
				time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		switch {
		case errors.Is(err, errWrongBytes):
			torn++
			t.Errorf("%s: %v (acked=%v deleted=%v)", w.key, err, w.acked, w.deleted)
		case errors.Is(err, errAbsent):
			absent++
			if w.acked && !w.deleted && !w.deleteUnknown {
				lost++
				t.Errorf("%s: acknowledged write is gone (multipart=%v size=%d)", w.key, w.multipart, w.size)
			}
		case err != nil:
			t.Errorf("%s: still unreadable %v after the cluster recovered: %v", w.key, settle, err)
		default:
			restored++
			if w.deleted {
				t.Errorf("%s: readable after a delete that was acknowledged", w.key)
			}
		}
	}
	t.Logf("verified: %d readable and byte-identical, %d absent (%d of them lost, %d torn)",
		restored, absent, lost, torn)
}

// restartSettle is how long a read may be retried while a node that was just
// killed comes back. A restart takes about as long whatever the cluster holds, so
// this one is a constant.
const restartSettle = 45 * time.Second

// healStall is how long the cluster may go without restoring a single copy before
// the barrier calls repair stuck.
//
// It replaced a fixed 45s budget for the whole heal, which was measuring the wrong
// thing: losing a disk destroys however much was on it, so the work grows with the
// run while a constant deadline does not. At a five-minute run that deadline
// expired with 53 of 1,433 copies left to go — a heal 96% finished, failed for
// being big rather than for being broken. Invariant 4 claims redundancy comes
// back, so this waits for progress and fails on the absence of it.
//
// What that gives up is a bound on how long a heal may take: one that is ten times
// slower but still moving now passes. Heal rate is measured in `make bench`, which
// is where a speed claim belongs; this barrier is about completeness.
const healStall = 30 * time.Second

// waitWhole polls until the cluster is whole — every node a member, and every
// chunk of every manifest present on every owner that manifest names — and returns
// the copies still missing if repair stops making progress. Empty means whole.
//
// Manifests are the authority, not the ring: an object is where its manifest says
// it is, so this is the same question repair and rebalance are answering, asked
// from outside.
func waitWhole(ctx context.Context, nodes []*node, store *meta.Store, stall time.Duration) []string {
	byID := make(map[string]*node, len(nodes))
	for _, n := range nodes {
		byID[n.id] = n
	}

	fewest, since := -1, time.Now()
	unsettled := time.Time{}
	for {
		holes, settled := missingCopies(ctx, byID, store, len(nodes))
		switch {
		case len(holes) == 0:
			return holes
		case !settled:
			// Placement is in flux, so the count is not a measurement of anything
			// yet. Start the clock over rather than read a restarting node as a
			// heal that stalled — but not forever: a node that never comes back
			// has to be reported as that, not left to the test's own timeout.
			if unsettled.IsZero() {
				unsettled = time.Now()
			} else if time.Since(unsettled) > restartSettle {
				return holes
			}
			fewest, since = -1, time.Now()
		case fewest < 0 || len(holes) < fewest:
			fewest, since, unsettled = len(holes), time.Now(), time.Time{}
		case time.Since(since) > stall:
			return holes
		default:
			unsettled = time.Time{}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// missingCopies reports the copies a manifest names that are not on the node it
// names, and whether the cluster was settled enough for that to mean anything.
func missingCopies(ctx context.Context, byID map[string]*node, store *meta.Store, want int) (holes []string, settled bool) {
	// Membership first: a node that is not a member owns nothing, so placement is
	// still moving and every answer below would be about a layout in flux.
	for id, n := range byID {
		members, err := n.members()
		if err != nil {
			return []string{fmt.Sprintf("%s is not answering: %v", id, err)}, false
		}
		if len(members) != want {
			return []string{fmt.Sprintf("%s sees %d members, want %d", id, len(members), want)}, false
		}
	}

	objects, err := store.ScanObjects(ctx, "", "", 0)
	if err != nil {
		return []string{fmt.Sprintf("scan manifests: %v", err)}, false
	}

	for _, o := range objects {
		for _, ref := range o.Manifest.Chunks {
			for i, id := range o.Manifest.Nodes {
				owner, ok := byID[id]
				if !ok {
					holes = append(holes, fmt.Sprintf("%s names node %s, which is not in the cluster", o.Key, id))
					continue
				}
				want := ref.ID
				if o.Manifest.Coding.Valid() {
					want = ref.ShardID(i)
				}
				if !owner.hasChunk(want) {
					holes = append(holes, fmt.Sprintf("%s: %s is missing chunk %s", o.Key, id, want))
				}
			}
		}
	}
	return holes, true
}

var (
	// errWrongBytes is the only unacceptable read: a complete response that is not
	// what was written. Everything else is an explicit failure, which invariant 3
	// allows.
	errWrongBytes = errors.New("read returned bytes that were never written")
	errAbsent     = errors.New("no such object")
)

// readTargets is what to read this round: the object just written, plus an older
// one whenever there is a choice. The older read is the one that matters — a fault
// arriving long after a write is only tested by reading something the workload has
// otherwise forgotten about, whose owner may now be frozen, wiped, or restarting.
func readTargets(just write, mine []write, rng *rand.Rand) []write {
	var targets []write
	if just.acked {
		targets = append(targets, just)
	}
	if len(mine) > 1 {
		targets = append(targets, mine[rng.IntN(len(mine)-1)])
	}
	return targets
}

func chaosPut(ctx context.Context, client *awss3.Client, w write) bool {
	body := chaosPayload(w.fill, w.size)
	if !w.multipart {
		_, err := client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String(chaosBucket), Key: aws.String(w.key), Body: bytes.NewReader(body),
		})
		return err == nil
	}

	create, err := client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(chaosBucket), Key: aws.String(w.key),
	})
	if err != nil {
		return false
	}
	// Two parts split at an offset that is not a chunk boundary, so completion has
	// to concatenate chunk lists rather than assume they line up.
	split := w.size / 3
	var parts []types.CompletedPart
	for i, chunk := range [][]byte{body[:split], body[split:]} {
		up, err := client.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket: aws.String(chaosBucket), Key: aws.String(w.key), UploadId: create.UploadId,
			PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(chunk),
		})
		if err != nil {
			return false
		}
		parts = append(parts, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(int32(i + 1))})
	}
	_, err = client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket: aws.String(chaosBucket), Key: aws.String(w.key), UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	return err == nil
}

// chaosGet reads an object and compares it against what was written. It returns
// errAbsent for a 404, errWrongBytes for a response that completed with the wrong
// contents, and any other error as itself.
func chaosGet(ctx context.Context, client *awss3.Client, w write) error {
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(chaosBucket), Key: aws.String(w.key),
	})
	if err != nil {
		var missing *types.NoSuchKey
		if errors.As(err, &missing) {
			return errAbsent
		}
		return err
	}
	defer out.Body.Close()

	got, err := io.ReadAll(out.Body)
	if err != nil {
		// A transfer that stops short of its Content-Length is how a chunk found
		// to be corrupt mid-stream is reported. An error, which is allowed.
		return fmt.Errorf("read body: %w", err)
	}
	if want := chaosPayload(w.fill, w.size); !bytes.Equal(got, want) {
		return fmt.Errorf("%w: %d bytes, want %d: %s", errWrongBytes, len(got), w.size, diagnose(got, w))
	}
	return nil
}

// diagnose says what the wrong bytes look like, because "not equal" does not point
// at a cause and a corruption found under chaos is worth naming precisely.
func diagnose(got []byte, w write) string {
	want := chaosPayload(w.fill, w.size)
	at := 0
	for at < len(got) && at < len(want) && got[at] == want[at] {
		at++
	}
	// Every payload is fill ^ f(i), so the fill of whatever arrived can be read
	// straight off its first byte: if the whole body matches that, these are
	// another object's bytes rather than damaged ones.
	if len(got) > 0 {
		other := got[0] ^ chaosPayload(0, 1)[0]
		if bytes.Equal(got, chaosPayload(other, len(got))) {
			return fmt.Sprintf("first difference at %d; the body is another object's payload (fill %02x, not %02x), multipart=%v",
				at, other, w.fill, w.multipart)
		}
	}
	// Parts concatenated in the wrong order keeps the length and moves the bytes.
	if w.multipart {
		split := w.size / 3
		if bytes.Equal(got, append(append([]byte{}, want[split:]...), want[:split]...)) {
			return fmt.Sprintf("the two parts are concatenated in reverse order (split %d)", split)
		}
	}
	return fmt.Sprintf("first difference at %d of %d, multipart=%v", at, len(want), w.multipart)
}

// injectFault applies one fault to one node and undoes it before returning, so
// that faults never overlap. It returns a description for the run's log, since a
// failure is only useful if it names what was done.
func injectFault(ctx context.Context, t *testing.T, victim *node, rng *rand.Rand) string {
	t.Helper()
	switch rng.IntN(4) {
	case 0:
		// SIGKILL and restart: the crash the fsync discipline exists for.
		victim.kill()
		sleep(ctx, time.Duration(200+rng.IntN(800))*time.Millisecond)
		victim.restart()
		return "killed and restarted " + victim.id

	case 1:
		// Frozen process: ports open, nothing answered, no lease renewed. Callers
		// see hangs rather than refusals, which is the harder failure to handle.
		victim.pause()
		sleep(ctx, time.Duration(200+rng.IntN(1200))*time.Millisecond)
		victim.resume()
		return "froze " + victim.id

	case 2:
		// A replaced disk: the process is healthy and holds nothing.
		before := len(victim.chunkFiles())
		victim.loseChunks()
		return fmt.Sprintf("wiped %d chunks from %s", before, victim.id)

	default:
		// Bit rot, under a running node, with nothing to announce it.
		if !victim.rotAChunk(rng.IntN) {
			return "nothing to rot on " + victim.id
		}
		return "flipped a bit in a chunk on " + victim.id
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func s3Client(addr string) *awss3.Client {
	return awss3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider(testAccessKey, testSecretKey, ""),
	}, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String("http://" + addr)
		o.UsePathStyle = true
	})
}
