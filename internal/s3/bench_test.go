package s3_test

// Benchmarks for the S3 path, which is the path a user actually takes. The
// internal API's numbers are in docs/benchmarks.md; the question here is what the
// gateway adds on top of them — a signature to verify, an MD5 to compute, XML to
// produce — and whether any of it is worth optimising.
//
// Same conditions as those benchmarks so the numbers can sit side by side: six
// nodes over real HTTP, real etcd, real disks, the production chunk size, driven
// by the AWS SDK because a hand-rolled client would not sign the way a real one
// does.

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/0vertake/kavo/internal/object"
)

const benchNodes = 6

// 4 KB is all fixed cost, 1 MB is a typical object, 64 MB is two chunks and the
// only size where per-byte work dominates everything else.
var benchSizes = []int64{4 << 10, 1 << 20, 64 << 20}

func sizeName(n int64) string {
	if n >= 1<<20 {
		return strconv.FormatInt(n>>20, 10) + "MB"
	}
	return strconv.FormatInt(n>>10, 10) + "KB"
}

func benchGateway(b *testing.B) *awss3.Client {
	b.Helper()
	return newGatewaySized(b, benchNodes, object.DefaultChunkSize)
}

func benchPut(b *testing.B, client *awss3.Client, key string, data []byte) {
	b.Helper()
	_, err := client.PutObject(b.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bench"), Key: aws.String(key), Body: bytes.NewReader(data),
	})
	if err != nil {
		b.Fatalf("put %s: %v", key, err)
	}
}

// A signed PUT through the gateway. Against the internal API's number for the same
// size, the difference is what S3 compatibility costs.
func BenchmarkPut(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			client := benchGateway(b)
			data := randBytes(int(size))

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				benchPut(b, client, "put/"+strconv.Itoa(i), data)
			}
		})
	}
}

func BenchmarkPutParallel(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			client := benchGateway(b)
			data := randBytes(int(size))
			var seq atomic.Int64

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					benchPut(b, client, "putpar/"+strconv.FormatInt(seq.Add(1), 10), data)
				}
			})
		})
	}
}

func BenchmarkGet(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			client := benchGateway(b)
			const key = "get/object"
			benchPut(b, client, key, randBytes(int(size)))

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				out, err := client.GetObject(b.Context(), &awss3.GetObjectInput{
					Bucket: aws.String("bench"), Key: aws.String(key),
				})
				if err != nil {
					b.Fatal(err)
				}
				n, err := io.Copy(io.Discard, out.Body)
				out.Body.Close()
				if err != nil || n != size {
					b.Fatalf("read %d bytes: %v", n, err)
				}
			}
		})
	}
}

func BenchmarkGetParallel(b *testing.B) {
	for _, size := range benchSizes {
		b.Run(sizeName(size), func(b *testing.B) {
			client := benchGateway(b)
			const key = "getpar/object"
			benchPut(b, client, key, randBytes(int(size)))

			b.SetBytes(size)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					out, err := client.GetObject(b.Context(), &awss3.GetObjectInput{
						Bucket: aws.String("bench"), Key: aws.String(key),
					})
					if err != nil {
						b.Fatal(err)
					}
					n, err := io.Copy(io.Discard, out.Body)
					out.Body.Close()
					if err != nil || n != size {
						b.Fatalf("read %d bytes: %v", n, err)
					}
				}
			})
		})
	}
}

// What `aws s3 cp` actually does to a large object: 8 MB ranged GETs. Every chunk
// the window touches is read in full, so a window that straddles a chunk boundary
// costs more than its length — this is where that shows up.
func BenchmarkGetRange(b *testing.B) {
	const size = 64 << 20
	const window = 8 << 20
	client := benchGateway(b)
	const key = "range/object"
	benchPut(b, client, key, randBytes(size))

	b.SetBytes(window)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		off := int64(i%(size/window)) * window
		out, err := client.GetObject(b.Context(), &awss3.GetObjectInput{
			Bucket: aws.String("bench"), Key: aws.String(key),
			Range: aws.String(fmt.Sprintf("bytes=%d-%d", off, off+window-1)),
		})
		if err != nil {
			b.Fatal(err)
		}
		n, err := io.Copy(io.Discard, out.Body)
		out.Body.Close()
		if err != nil || n != window {
			b.Fatalf("read %d bytes: %v", n, err)
		}
	}
}

// HEAD is a manifest resolve and a signature check with no data at all: the floor
// for what a request costs before any byte is moved.
func BenchmarkHead(b *testing.B) {
	client := benchGateway(b)
	const key = "head/object"
	benchPut(b, client, key, randBytes(4<<10))

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := client.HeadObject(b.Context(), &awss3.HeadObjectInput{
			Bucket: aws.String("bench"), Key: aws.String(key),
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// The same 64 MB as BenchmarkPut/64MB, sent the way a real client sends it. The
// comparison is the point: multipart trades round trips for concurrency, and this
// says which side wins here.
func BenchmarkMultipartPut(b *testing.B) {
	const size = 64 << 20
	const part = 8 << 20
	client := benchGateway(b)
	data := randBytes(size)

	b.SetBytes(size)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := "multipart/" + strconv.Itoa(i)
		create, err := client.CreateMultipartUpload(b.Context(), &awss3.CreateMultipartUploadInput{
			Bucket: aws.String("bench"), Key: aws.String(key),
		})
		if err != nil {
			b.Fatal(err)
		}
		var parts []types.CompletedPart
		for p := 0; p*part < size; p++ {
			up, err := client.UploadPart(b.Context(), &awss3.UploadPartInput{
				Bucket: aws.String("bench"), Key: aws.String(key), UploadId: create.UploadId,
				PartNumber: aws.Int32(int32(p + 1)),
				Body:       bytes.NewReader(data[p*part : min((p+1)*part, size)]),
			})
			if err != nil {
				b.Fatal(err)
			}
			parts = append(parts, types.CompletedPart{ETag: up.ETag, PartNumber: aws.Int32(int32(p + 1))})
		}
		if _, err := client.CompleteMultipartUpload(b.Context(), &awss3.CompleteMultipartUploadInput{
			Bucket: aws.String("bench"), Key: aws.String(key), UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// Listing is the operation whose cost is per key rather than per byte, and the one
// a client calls before doing anything else. Two shapes: a flat page of 1000, and
// the same keyspace under a delimiter, where a page is mostly grouped prefixes the
// server has to skip past.
func BenchmarkList(b *testing.B) {
	const keys = 1000
	client := newGatewaySized(b, benchNodes, object.DefaultChunkSize)
	body := randBytes(1)
	for i := range keys {
		// Half flat, half nested, so one keyspace answers both questions.
		key := fmt.Sprintf("flat/key%04d", i)
		if i%2 == 0 {
			key = fmt.Sprintf("tree/dir%03d/key%04d", i/2, i)
		}
		benchPut(b, client, key, body)
	}

	b.Run("page of 1000", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			out, err := client.ListObjectsV2(b.Context(), &awss3.ListObjectsV2Input{
				Bucket: aws.String("bench"), MaxKeys: aws.Int32(1000),
			})
			if err != nil {
				b.Fatal(err)
			}
			if len(out.Contents) != 1000 {
				b.Fatalf("listed %d keys, want 1000", len(out.Contents))
			}
		}
	})

	b.Run("delimited", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			out, err := client.ListObjectsV2(b.Context(), &awss3.ListObjectsV2Input{
				Bucket: aws.String("bench"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1000),
			})
			if err != nil {
				b.Fatal(err)
			}
			if len(out.CommonPrefixes) != 2 {
				b.Fatalf("listed %d prefixes, want flat/ and tree/", len(out.CommonPrefixes))
			}
		}
	})
}
