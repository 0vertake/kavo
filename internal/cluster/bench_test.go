package cluster_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/0vertake/kavo/internal/object"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
)

// The benchmarks run against a six-node cluster over real HTTP, real etcd and
// real disks, at the production chunk size — the same path a client takes. A
// benchmark of a mocked-out store would measure the mock.
//
// Sizes are chosen for what they expose rather than for coverage: 4 KB is
// dominated by fixed per-object cost (one etcd commit, three chunk round trips),
// 1 MB is a typical object, and 64 MB is two chunks, so it is the only size that
// says anything about how the second chunk overlaps with the first.
const benchNodes = 6

var benchSizes = []int64{4 << 10, 1 << 20, 64 << 20}

func sizeName(n int64) string {
	switch {
	case n >= 1<<20:
		return strconv.FormatInt(n>>20, 10) + "MB"
	default:
		return strconv.FormatInt(n>>10, 10) + "KB"
	}
}

func benchCluster(b *testing.B) *testCluster {
	b.Helper()
	return newClusterChunked(b, benchNodes, object.DefaultChunkSize)
}

// putHTTP writes through the HTTP handler rather than calling the coordinator
// directly, so the number includes everything a client pays for.
func putHTTP(b *testing.B, n *node, key string, data []byte) {
	b.Helper()
	req, err := http.NewRequest(http.MethodPut, n.srv.URL+"/objects/"+key, bytes.NewReader(data))
	if err != nil {
		b.Fatal(err)
	}
	req.ContentLength = int64(len(data))
	resp, err := n.srv.Client().Do(req)
	if err != nil {
		b.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("PUT %s: %s", key, resp.Status)
	}
}

func getHTTP(b *testing.B, n *node, key string) int64 {
	b.Helper()
	resp, err := n.srv.Client().Get(n.srv.URL + "/objects/" + key)
	if err != nil {
		b.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("GET %s: %s", key, resp.Status)
	}
	read, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		b.Fatal(err)
	}
	return read
}

// What one client gets, which is the latency number: every byte is written three
// times over the network and fsynced three times before the PUT returns.
func BenchmarkPut(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			tc := benchCluster(b)
			coordinator := tc.nodes["n1"]
			data := randBytes(int(size))

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				putHTTP(b, coordinator, "bench/"+strconv.Itoa(i), data)
			}
		})
	}
}

// What the cluster gets. A single stream is bounded by round trips it cannot
// avoid, so the interesting question is whether concurrent writers fill the
// disks or contend on something.
func BenchmarkPutParallel(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			tc := benchCluster(b)
			coordinator := tc.nodes["n1"]
			data := randBytes(int(size))
			var seq atomic.Int64

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					putHTTP(b, coordinator, "benchpar/"+strconv.FormatInt(seq.Add(1), 10), data)
				}
			})
		})
	}
}

// Reads are driven from a node that owns nothing, so every chunk crosses the
// network: the honest number for a cluster where any node answers any request.
func BenchmarkGet(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			tc := benchCluster(b)
			const key = "bench/read"
			owners, outsider := tc.owners(b, key)
			putHTTP(b, owners[0], key, randBytes(int(size)))

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if got := getHTTP(b, outsider, key); got != size {
					b.Fatalf("read %d bytes, want %d", got, size)
				}
			}
		})
	}
}

func BenchmarkGetParallel(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			tc := benchCluster(b)
			const key = "bench/readpar"
			owners, outsider := tc.owners(b, key)
			putHTTP(b, owners[0], key, randBytes(int(size)))

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					getHTTP(b, outsider, key)
				}
			})
		})
	}
}

// healable sets up objects that all share one primary owner, since only a
// partition's first owner repairs it: keys spread over six primaries would leave
// the node under measurement seeing most of them as none of its business.
func healable(b *testing.B, tc *testCluster, objects int, data []byte) (*node, []string) {
	b.Helper()
	byPrimary := map[string][]string{}
	for i := 0; ; i++ {
		key := fmt.Sprintf("bench/heal/%d", i)
		owners, _ := tc.owners(b, key)
		id := owners[0].id
		byPrimary[id] = append(byPrimary[id], key)
		if len(byPrimary[id]) < objects {
			continue
		}
		for _, key := range byPrimary[id] {
			putHTTP(b, owners[0], key, data)
		}
		return owners[0], byPrimary[id]
	}
}

// How fast a lost disk comes back: the number that decides whether the window of
// reduced redundancy is minutes or hours. Unrated on purpose — this is the
// ceiling, and the 32 MB/s default cap exists to stay well under it.
func BenchmarkRepairHeal(b *testing.B) {
	tc := newClusterChunked(b, benchNodes, 4<<20)
	repairer, keys := healable(b, tc, 8, randBytes(4<<20))

	var lost int64
	b.ReportAllocs()
	for b.Loop() {
		// Emptying the disks again is setup, not repair, so it is not measured.
		b.StopTimer()
		lost = 0
		for _, key := range keys {
			m, err := repairer.c.Resolve(context.Background(), key)
			if err != nil {
				b.Fatal(err)
			}
			owners, _ := tc.owners(b, key)
			for _, o := range owners[1:] {
				o.loseChunks(b, m.Chunks)
				lost += m.Size
			}
		}
		b.StartTimer()

		if _, err := repairer.c.Repair(context.Background(), 0); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(lost)
}

// What a healthy cluster actually spends its repair budget on: asking every owner
// whether it still holds every chunk, and being told yes. One HEAD per copy, so
// this is the number that decides whether batching the question is worth it.
func BenchmarkRepairSurvey(b *testing.B) {
	tc := newClusterChunked(b, benchNodes, 4<<20)
	repairer, keys := healable(b, tc, 8, randBytes(4<<20))

	b.ReportAllocs()
	b.ResetTimer()
	var copies int
	for b.Loop() {
		st, err := repairer.c.Repair(context.Background(), 0)
		if err != nil {
			b.Fatal(err)
		}
		copies = st.CopiesChecked
	}
	b.StopTimer()
	if copies == 0 {
		b.Fatalf("a survey of %d objects checked no copies", len(keys))
	}
	b.ReportMetric(float64(copies), "copies/pass")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*copies), "ns/copy")
}

// A scrub reads every byte the node holds and checksums it. The number that
// matters is how long a full disk takes, so it is reported per byte read.
func BenchmarkScrub(b *testing.B) {
	tc := newClusterChunked(b, benchNodes, 4<<20)
	data := randBytes(4 << 20)
	for i := range 8 {
		putHTTP(b, tc.nodes["n1"], fmt.Sprintf("bench/scrub/%d", i), data)
	}
	scrubber := tc.nodes["n1"]

	var read int64
	st, err := scrubber.c.Scrub(context.Background(), 0)
	if err != nil {
		b.Fatal(err)
	}
	read = st.BytesRead
	if read == 0 {
		b.Skip("nothing landed on the scrubbing node")
	}

	b.SetBytes(read)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := scrubber.c.Scrub(context.Background(), 0); err != nil {
			b.Fatal(err)
		}
	}
}

// The two redundancy modes side by side, which is the comparison the mode exists
// to win: 4+2 stores 1.5x the object where replication stores 3x, and this is
// what that costs on the read and write paths. Reads are measured with every
// shard present and again with the code's full tolerance missing, since a decode
// that has to solve for missing shards is the case erasure coding is judged on.
func BenchmarkRedundancyModes(b *testing.B) {
	for _, size := range []int64{4 << 10, 1 << 20, 64 << 20} {
		data := randBytes(int(size))

		b.Run("replicated/put/"+sizeName(size), func(b *testing.B) {
			tc := benchCluster(b)
			b.SetBytes(size)
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				putHTTP(b, tc.nodes["n1"], "bench/rep/"+strconv.Itoa(i), data)
			}
		})
		b.Run("coded/put/"+sizeName(size), func(b *testing.B) {
			tc := newECCluster(b, benchNodes, object.DefaultChunkSize)
			b.SetBytes(size)
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				putHTTP(b, tc.nodes["n1"], "bench/ec/"+strconv.Itoa(i), data)
			}
		})

		b.Run("replicated/get/"+sizeName(size), func(b *testing.B) {
			tc := benchCluster(b)
			const key = "bench/rep/read"
			owners, outsider := tc.owners(b, key)
			putHTTP(b, owners[0], key, data)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				getHTTP(b, outsider, key)
			}
		})
		b.Run("coded/get/"+sizeName(size), func(b *testing.B) {
			tc := newECCluster(b, benchNodes, object.DefaultChunkSize)
			const key = "bench/ec/read"
			putHTTP(b, tc.nodes["n1"], key, data)
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				getHTTP(b, tc.nodes["n1"], key)
			}
		})
		b.Run("coded/get-degraded/"+sizeName(size), func(b *testing.B) {
			tc := newECCluster(b, benchNodes, object.DefaultChunkSize)
			const key = "bench/ec/degraded"
			putHTTP(b, tc.nodes["n1"], key, data)
			m, err := tc.nodes["n1"].c.Resolve(context.Background(), key)
			if err != nil {
				b.Fatal(err)
			}
			for i := range testScheme.Parity {
				for _, ref := range m.Chunks {
					tc.nodes[m.Nodes[i]].hide(b, ref.ShardID(i))
				}
			}
			b.SetBytes(size)
			b.ReportAllocs()
			for b.Loop() {
				getHTTP(b, tc.nodes["n1"], key)
			}
		})
	}
}

// Chunking on its own, with no network and no disk, to separate the cost of
// splitting a stream from the cost of storing it. If this is slow, nothing
// downstream can be fast.
func BenchmarkChunking(b *testing.B) {
	for _, size := range []int64{1 << 20, 64 << 20} {
		b.Run(sizeName(size), func(b *testing.B) {
			data := randBytes(int(size))
			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, err := object.Write(bytes.NewReader(data), object.DefaultChunkSize,
					func(*object.ChunkRef, []byte) error { return nil })
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
