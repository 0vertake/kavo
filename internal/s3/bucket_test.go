package s3_test

// Buckets are prefixes, so what these tests pin down is not storage but the
// answers: a client that creates a bucket must be able to write to it, a client
// that lists buckets must see the ones holding objects, and a client that deletes
// a bucket must be refused while its objects are still readable.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func TestCreateBucketThenWriteToIt(t *testing.T) {
	client := newGateway(t)
	// Twice, because the SDK's first call after a failure is the same call again,
	// and because nothing was created the second one has no reason to fail.
	for range 2 {
		if _, err := client.CreateBucket(t.Context(), &awss3.CreateBucketInput{
			Bucket: aws.String("made-up"),
		}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
	}
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("made-up"),
		Key:    aws.String("k"),
		Body:   strings.NewReader("payload"),
	}); err != nil {
		t.Fatalf("PutObject into a bucket that was just created: %v", err)
	}
}

func TestListBucketsShowsBucketsThatHoldObjects(t *testing.T) {
	client := newGateway(t)
	for _, key := range []string{"alpha/one", "alpha/two", "beta/one", "gamma/deep/nested"} {
		bucket, k, _ := strings.Cut(key, "/")
		if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(k),
			Body:   strings.NewReader("x"),
		}); err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
	}
	// Created but never written to: there is no record of it, so it is not a
	// bucket. Claiming otherwise would mean keeping the record this design does
	// not have.
	if _, err := client.CreateBucket(t.Context(), &awss3.CreateBucketInput{
		Bucket: aws.String("empty-one"),
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	out, err := client.ListBuckets(t.Context(), &awss3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	var got []string
	for _, b := range out.Buckets {
		got = append(got, aws.ToString(b.Name))
	}
	want := []string{"alpha", "beta", "gamma"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("ListBuckets returned %v, want %v", got, want)
	}
}

func TestDeleteBucketRefusesWhileObjectsRemain(t *testing.T) {
	client := newGateway(t)
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("occupied"),
		Key:    aws.String("still-here"),
		Body:   strings.NewReader("payload"),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	_, err := client.DeleteBucket(t.Context(), &awss3.DeleteBucketInput{
		Bucket: aws.String("occupied"),
	})
	if err == nil {
		t.Fatal("DeleteBucket succeeded on a bucket whose object is still readable")
	}
	var api smithy.APIError
	if !errors.As(err, &api) || api.ErrorCode() != "BucketNotEmpty" {
		t.Fatalf("DeleteBucket: got %v, want BucketNotEmpty", err)
	}

	// The object is what was holding it, so removing that is what releases it.
	if _, err := client.DeleteObject(t.Context(), &awss3.DeleteObjectInput{
		Bucket: aws.String("occupied"),
		Key:    aws.String("still-here"),
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := client.DeleteBucket(t.Context(), &awss3.DeleteBucketInput{
		Bucket: aws.String("occupied"),
	}); err != nil {
		t.Fatalf("DeleteBucket on an emptied bucket: %v", err)
	}
}

// Bulk delete, which is how an SDK empties a bucket. The failures matter as much
// as the successes: a client reads the per-key list to decide what to retry, so a
// key reported deleted has to be gone and a key that could not be deleted has to
// say so.
func TestBulkDeleteRemovesEveryKeyItReports(t *testing.T) {
	client := newGateway(t)
	keys := []string{"one", "two", "dir/three"}
	for _, k := range keys {
		if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
			Bucket: aws.String("bulk"), Key: aws.String(k), Body: strings.NewReader(k),
		}); err != nil {
			t.Fatalf("PutObject %s: %v", k, err)
		}
	}

	ids := make([]types.ObjectIdentifier, 0, len(keys)+1)
	for _, k := range keys {
		ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
	}
	// A key that was never written: S3 reports a delete of a missing object as a
	// success, and a client's cleanup loop depends on that.
	ids = append(ids, types.ObjectIdentifier{Key: aws.String("never-existed")})

	out, err := client.DeleteObjects(t.Context(), &awss3.DeleteObjectsInput{
		Bucket: aws.String("bulk"),
		Delete: &types.Delete{Objects: ids},
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	for _, e := range out.Errors {
		t.Errorf("bulk delete reported %s on %s: %s",
			aws.ToString(e.Code), aws.ToString(e.Key), aws.ToString(e.Message))
	}
	if len(out.Deleted) != len(ids) {
		t.Errorf("bulk delete reported %d of %d keys deleted", len(out.Deleted), len(ids))
	}

	// Reported deleted, so nothing may still answer.
	for _, k := range keys {
		if _, err := client.HeadObject(t.Context(), &awss3.HeadObjectInput{
			Bucket: aws.String("bulk"), Key: aws.String(k),
		}); err == nil {
			t.Errorf("%s still exists after a bulk delete reported it gone", k)
		}
	}

	// Quiet is what a cleanup loop asks for: successes are not echoed back.
	if _, err := client.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bulk"), Key: aws.String("quiet"), Body: strings.NewReader("x"),
	}); err != nil {
		t.Fatal(err)
	}
	quiet, err := client.DeleteObjects(t.Context(), &awss3.DeleteObjectsInput{
		Bucket: aws.String("bulk"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{{Key: aws.String("quiet")}},
			Quiet:   aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects quiet: %v", err)
	}
	if len(quiet.Deleted) != 0 || len(quiet.Errors) != 0 {
		t.Errorf("a quiet delete answered with %d deleted and %d errors, want neither",
			len(quiet.Deleted), len(quiet.Errors))
	}
}

// Every bucket subresource is a query on the bucket's own path, and none of them
// exist here. Refusing them is not pedantry: answering PUT ?versioning with a 200
// tells a client that old versions of its objects are being kept, and it will
// overwrite them believing they can be recovered.
func TestBucketSubresourcesAreRefusedRatherThanFaked(t *testing.T) {
	client := newGateway(t)
	endpoint := aws.ToString(client.Options().BaseEndpoint)

	for _, sub := range []string{"versioning", "acl", "tagging", "policy", "lifecycle", "cors", "object-lock"} {
		for _, method := range []string{http.MethodPut, http.MethodDelete} {
			t.Run(method+" ?"+sub, func(t *testing.T) {
				status, body := signedRequest(t, method, endpoint+"/subres?"+sub+"=", nil)
				if status < 400 {
					t.Errorf("%s ?%s answered %d, so a client believes it took effect:\n%s",
						method, sub, status, body)
				}
			})
		}
	}

	// The same paths without a query are the operations that do exist, so the
	// check above must not have refused those too.
	if _, err := client.CreateBucket(t.Context(), &awss3.CreateBucketInput{
		Bucket: aws.String("subres"),
	}); err != nil {
		t.Fatalf("CreateBucket after refusing its subresources: %v", err)
	}
	if _, err := client.DeleteBucket(t.Context(), &awss3.DeleteBucketInput{
		Bucket: aws.String("subres"),
	}); err != nil {
		t.Fatalf("DeleteBucket after refusing its subresources: %v", err)
	}
}

// signedRequest sends one request the SDK has no operation for, signed the way the
// SDK signs, and returns what came back. The bucket subresources need it: an SDK
// will not send PutBucketVersioning to a client that has no such method.
func signedRequest(t *testing.T, method, url string, body []byte) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	if err := signer.SignHTTP(t.Context(), aws.Credentials{
		AccessKeyID: creds.AccessKey, SecretAccessKey: creds.SecretKey,
	}, req, hash, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(out)
}
