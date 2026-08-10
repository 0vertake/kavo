package test

import (
	"net/http"
	"testing"
)

// The point of moving the commit point into etcd: a manifest committed by one
// node is visible to every node in the cluster.
//
// Chunks are still local, so the second node resolves the object and then cannot
// read it. What matters is *how* it fails. It must not answer 404, which would
// claim the object does not exist when it demonstrably does, and it must not
// answer a short body as though it were complete. Replication (milestone 4) is
// what turns this into a successful read.
func TestManifestIsClusterWide(t *testing.T) {
	bin := buildKavod(t)
	prefix := clusterPrefix()
	stored := startNode(t, bin, t.TempDir(), prefix, testChunkSize)
	other := startNode(t, bin, t.TempDir(), prefix, testChunkSize)

	client := &http.Client{}
	data := payloadFor(3)
	if status, err := stored.put(client, "shared/object", data); err != nil || status != http.StatusOK {
		t.Fatalf("PUT = (%d, %v), want (200, nil)", status, err)
	}

	// The node that holds the chunks serves it normally.
	status, got, err := stored.get(client, "shared/object")
	if err != nil || status != http.StatusOK || len(got) != len(data) {
		t.Fatalf("GET from the storing node = (%d, %d bytes, %v), want (200, %d, nil)",
			status, len(got), err, len(data))
	}

	// The other node resolves the same manifest from etcd...
	status, got, err = other.get(client, "shared/object")
	if status == http.StatusNotFound {
		t.Fatal("second node answered 404: the manifest did not reach it through etcd")
	}
	if status != http.StatusOK {
		t.Fatalf("second node answered %d, want 200 followed by a failed transfer", status)
	}
	// ...and fails the transfer rather than inventing a shorter object.
	if err == nil && len(got) == len(data) {
		t.Fatal("second node served the object without holding any of its chunks")
	}
	if err == nil {
		t.Errorf("second node returned %d and %d of %d bytes with no error, want a failed transfer",
			status, len(got), len(data))
	}
}
