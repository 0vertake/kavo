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
	"regexp"
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

// multipartETag matches the "<md5>-<parts>" form in the CLI's JSON output, where
// the ETag's own quotes arrive escaped.
var multipartETag = regexp.MustCompile(`"ETag": "\\"[0-9a-f]{32}-\d+\\""`)

func requireAWSCLI(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("aws"); err != nil {
		t.Skip("aws CLI not installed")
	}
}

// The claim the whole S3 milestone rests on: `aws s3 cp` up and down, unchanged,
// through a cluster of real processes.
//
// Nothing is tuned, so the CLI behaves as it does for a user: anything over 8 MB
// goes up as a concurrent multipart upload and comes back as concurrent ranged
// GETs. Both halves of that are worth having. A part boundary that does not line up
// with a chunk boundary, or a range served from the middle of a chunk landing at the
// wrong offset in the client's file, scrambles the object rather than failing — and
// a unit test of one part or one range in isolation cannot see it.
func TestTheAWSCLIRoundTripsObjects(t *testing.T) {
	requireAWSCLI(t)
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 1<<20, clusterSize)

	tests := []struct {
		name      string
		key       string
		size      int
		multipart bool
	}{
		{name: "small", key: "small.bin", size: 1024},
		{name: "empty", key: "empty.bin", size: 0},
		{name: "spanning chunks", key: "some dir/three chunks.bin", size: 3 << 20},
		// Above the CLI's 8 MB threshold in both directions: uploaded as
		// concurrent parts, downloaded as concurrent ranged GETs.
		{name: "multipart up and ranged down", key: "big.bin", size: 12 << 20, multipart: true},
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
			up, err := nodes[0].aws(t, "s3", "cp", src, "s3://bucket/"+tt.key)
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

			// A multipart ETag ends in the part count, and the CLI's own
			// integrity check compares it against what it computed. Asserting
			// the shape here is what proves the big upload really went up in
			// parts rather than as one PUT the CLI quietly decided to send.
			head, err := nodes[2].aws(t, "s3api", "head-object", "--bucket", "bucket", "--key", tt.key)
			if err != nil {
				t.Fatalf("head-object: %v\n%s", err, head)
			}
			if multipart := multipartETag.MatchString(head); multipart != tt.multipart {
				t.Errorf("etag looks multipart = %v, want %v:\n%s", multipart, tt.multipart, head)
			}
		})
	}
}

// `aws s3 ls` is what a listing is for, and it exercises the parts of one no unit
// test reaches: the CLI asks with a delimiter and encoding-type=url, and renders
// grouped prefixes as directories. `sync` and `rm --recursive` list first too, so
// this is also the check that they can find anything at all.
func TestTheAWSCLIListsAndSyncs(t *testing.T) {
	requireAWSCLI(t)
	bin := buildKavod(t)
	nodes := startCluster(t, bin, clusterPrefix(), 1<<20, clusterSize)

	// A small tree, so that a listing has both a file at the top and directories.
	// The awkward name is deliberate: the CLI asks for encoding-type=url and
	// decodes what comes back, so a key whose raw form is itself a valid escape
	// sequence is the one that catches a server that did not encode.
	local := t.TempDir()
	files := []string{"top.txt", "dir/a.txt", "dir/b.txt", "dir/sub/c.txt", "dir/50%25 off + more.txt"}
	for _, name := range files {
		path := filepath.Join(local, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("contents of "+name), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// sync uploads the tree, and lists to work out what to upload.
	if out, err := nodes[0].aws(t, "s3", "sync", local, "s3://bucket/tree"); err != nil {
		t.Fatalf("sync up: %v\n%s", err, out)
	}

	// The top level shows one file and one directory, which is the delimiter
	// working: without it every key below dir/ would be listed here.
	out, err := nodes[1].aws(t, "s3", "ls", "s3://bucket/tree/")
	if err != nil {
		t.Fatalf("ls: %v\n%s", err, out)
	}
	if !strings.Contains(out, "PRE dir/") {
		t.Errorf("ls did not group dir/ as a prefix:\n%s", out)
	}
	if !strings.Contains(out, "top.txt") {
		t.Errorf("ls did not list top.txt:\n%s", out)
	}
	if strings.Contains(out, "a.txt") {
		t.Errorf("ls listed a key from inside dir/ at the top level:\n%s", out)
	}

	// Recursive listing reaches every key, and only this bucket's keys.
	out, err = nodes[2].aws(t, "s3", "ls", "--recursive", "s3://bucket/")
	if err != nil {
		t.Fatalf("ls --recursive: %v\n%s", err, out)
	}
	for _, name := range files {
		if !strings.Contains(out, "tree/"+name) {
			t.Errorf("ls --recursive did not list tree/%s:\n%s", name, out)
		}
	}

	// And back down again: sync writes the tree to a new directory, which only
	// works if the listing named keys that can be fetched.
	back := t.TempDir()
	if out, err := nodes[0].aws(t, "s3", "sync", "s3://bucket/tree", back); err != nil {
		t.Fatalf("sync down: %v\n%s", err, out)
	}
	for _, name := range files {
		got, err := os.ReadFile(filepath.Join(back, name))
		if err != nil {
			t.Errorf("sync down did not restore %s: %v", name, err)
			continue
		}
		if want := "contents of " + name; string(got) != want {
			t.Errorf("%s came back as %q, want %q", name, got, want)
		}
	}

	// rm --recursive lists, deletes what it found, and leaves an empty bucket.
	if out, err := nodes[1].aws(t, "s3", "rm", "--recursive", "s3://bucket/tree"); err != nil {
		t.Fatalf("rm --recursive: %v\n%s", err, out)
	}
	out, err = nodes[2].aws(t, "s3", "ls", "--recursive", "s3://bucket/")
	if err != nil {
		t.Fatalf("ls after rm: %v\n%s", err, out)
	}
	if strings.TrimSpace(out) != "" {
		t.Errorf("objects survived rm --recursive:\n%s", out)
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
