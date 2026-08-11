package test

// The real `aws` CLI against real kavod processes. Everything else tests kavo
// against a client this project chose; this tests it against the client users
// actually have, which is the only thing that proves the S3 subset is usable.
//
// Skipped when the CLI is not installed, because a missing tool is not a failure
// of the code under test. It is installed in CI.

import (
	"bytes"
	"crypto/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// aws runs the CLI against a node's S3 port and returns its combined output.
func (n *node) aws(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return n.awsTuned(t, "", args...)
}

// awsTuned is aws with extra settings in the CLI's own config file, which is the
// only way to change how it splits a transfer.
func (n *node) awsTuned(t *testing.T, s3Config string, args ...string) (string, error) {
	t.Helper()
	// The developer's own config is bypassed: it can point at a real account, or
	// set a checksum mode this server does not implement.
	config := filepath.Join(t.TempDir(), "config")
	// "s3 =" with nothing under it is a string rather than a section, and the CLI
	// crashes on it, so the section only appears when it has settings.
	settings := "[default]\n"
	if s3Config != "" {
		settings += "s3 =\n" + s3Config + "\n"
	}
	if err := os.WriteFile(config, []byte(settings), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("aws", append([]string{"--endpoint-url", "http://" + n.s3Addr}, args...)...)
	cmd.Env = append(os.Environ(),
		"AWS_ACCESS_KEY_ID="+testAccessKey,
		"AWS_SECRET_ACCESS_KEY="+testSecretKey,
		"AWS_DEFAULT_REGION=us-east-1",
		"AWS_CONFIG_FILE="+config,
		"AWS_SHARED_CREDENTIALS_FILE="+filepath.Join(t.TempDir(), "credentials"),
		"AWS_EC2_METADATA_DISABLED=true",
	)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func requireAWSCLI(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("aws"); err != nil {
		t.Skip("aws CLI not installed")
	}
}

// The claim the whole S3 milestone rests on: `aws s3 cp` up and down, unchanged,
// through a cluster of real processes.
//
// Uploads are kept to a single PUT by raising the CLI's multipart threshold —
// multipart upload is the next milestone, and pinning the threshold here is what
// keeps this test about the object path rather than about a missing operation.
// Downloads are left at the default, so anything over 8 MB comes back as several
// concurrent ranged GETs. That case is the one worth having: a range served from
// the middle of a chunk has to land at the right offset in the client's file, and
// getting it wrong scrambles the object rather than failing, which a unit test of
// one range in isolation cannot see.
func TestTheAWSCLIRoundTripsObjects(t *testing.T) {
	requireAWSCLI(t)
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 1<<20, clusterSize)

	tests := []struct {
		name string
		key  string
		size int
	}{
		{name: "small", key: "small.bin", size: 1024},
		{name: "empty", key: "empty.bin", size: 0},
		{name: "spanning chunks", key: "some dir/three chunks.bin", size: 3 << 20},
		// Above the CLI's download threshold, so it is fetched as parallel
		// ranged GETs and reassembled.
		{name: "ranged parallel download", key: "big.bin", size: 12 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			src := filepath.Join(dir, "src")
			data := make([]byte, tt.size)
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(src, data, 0o644); err != nil {
				t.Fatal(err)
			}

			// Uploading through one node and downloading through another, since
			// any node coordinates any request and a client may hit either.
			up, err := nodes[0].awsTuned(t, "  multipart_threshold = 1GB",
				"s3", "cp", src, "s3://bucket/"+tt.key)
			if err != nil {
				t.Fatalf("aws s3 cp up: %v\n%s", err, up)
			}
			dst := filepath.Join(dir, "dst")
			down, err := nodes[1].aws(t, "s3", "cp", "s3://bucket/"+tt.key, dst)
			if err != nil {
				t.Fatalf("aws s3 cp down: %v\n%s", err, down)
			}

			got, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Errorf("downloaded %d bytes, want the %d uploaded", len(got), len(data))
			}
		})
	}
}

// The other operations a client leans on: describing an object without reading it,
// and deleting it. `s3api` rather than `s3` so that the failure names the request
// rather than a copy that gave up.
func TestTheAWSCLIDescribesAndDeletes(t *testing.T) {
	requireAWSCLI(t)
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 1<<20, clusterSize)

	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.WriteFile(src, bytes.Repeat([]byte("kavo"), 1024), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := nodes[0].aws(t, "s3", "cp", src, "s3://bucket/described.bin"); err != nil {
		t.Fatalf("cp: %v\n%s", err, out)
	}

	out, err := nodes[1].aws(t, "s3api", "head-object", "--bucket", "bucket", "--key", "described.bin")
	if err != nil {
		t.Fatalf("head-object: %v\n%s", err, out)
	}
	for _, want := range []string{`"ContentLength": 4096`, `"ETag"`, `"LastModified"`} {
		if !strings.Contains(out, want) {
			t.Errorf("head-object output has no %s:\n%s", want, out)
		}
	}

	if out, err := nodes[2].aws(t, "s3", "rm", "s3://bucket/described.bin"); err != nil {
		t.Fatalf("rm: %v\n%s", err, out)
	}
	// The CLI reports a missing object as a failed HEAD, which is the code path
	// a script's `|| exit` depends on.
	if out, err := nodes[0].aws(t, "s3api", "head-object", "--bucket", "bucket", "--key", "described.bin"); err == nil {
		t.Errorf("head-object succeeded after rm:\n%s", out)
	}
}
