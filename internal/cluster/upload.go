package cluster

// Multipart upload.
//
// The shape of it: a part is written exactly like a small object, but placed by the
// *final* object's key and its manifest recorded against the upload rather than
// against a key any reader can resolve. Completing the upload concatenates the
// parts' chunk references and commits one manifest — so the object appears whole, at
// one instant, from chunks that were already durable.
//
// This is the same commit point as every other write. Nothing about multipart
// weakens it: before the commit there is no object, and after it every chunk the
// manifest names is fsynced on a quorum.

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/0vertake/kavo/internal/meta"
	"github.com/0vertake/kavo/internal/object"
)

// MaxParts is S3's limit, and the reason completing an upload can read every part
// at once.
const MaxParts = 10000

// UploadMaxAge is how long an unfinished multipart upload is left alone. After
// this, the upload record is dropped and collection reclaims its chunks. S3
// solves the same hole with a lifecycle rule, which is an anti-goal here, so the
// age is a constant rather than a bucket configuration. Seven days is long enough
// for a slow 5 GB upload and short enough that a client that walked away does not
// keep its parts forever.
const UploadMaxAge = 7 * 24 * time.Hour

// ErrNoSuchPart reports that a completion named a part that was never uploaded.
var ErrNoSuchPart = errors.New("cluster: no such part")

// ErrPartMismatch reports that a completion named a part whose ETag is not the one
// the upload produced.
var ErrPartMismatch = errors.New("cluster: part etag does not match")

// ErrPartOrder reports that a completion listed its parts out of order or named one
// twice.
var ErrPartOrder = errors.New("cluster: parts out of order")

// CompletedPart is one entry of a completion request: which part, and the ETag the
// client believes it has.
type CompletedPart struct {
	Number int
	ETag   string
}

// CreateUpload begins a multipart upload and returns its id.
func (c *Coordinator) CreateUpload(ctx context.Context, key, contentType string, metadata map[string]string) (string, error) {
	// Random rather than derived from the key: two clients uploading the same key
	// at once are two independent uploads, and each must be able to abort its own
	// without touching the other's parts.
	id := rand.Text()
	u := meta.Upload{Key: key, ContentType: contentType, Created: time.Now().UTC(), Meta: metadata}
	if err := c.meta.CreateUpload(ctx, id, u); err != nil {
		return "", err
	}
	return id, nil
}

// UploadPart stores one part and returns its ETag. size is the part's declared
// length, used only to size the write buffer.
//
// The part's chunks are placed by the object's key, not the part's, so that
// completion is a single manifest commit rather than a copy: the chunks are already
// on the nodes the object's manifest will name.
func (c *Coordinator) UploadPart(ctx context.Context, id string, number int, body io.Reader, size int64) (string, error) {
	// Part 0 does not exist and part 10001 never will, so this is the same answer
	// as naming a part that was never uploaded.
	if number < 1 || number > MaxParts {
		return "", fmt.Errorf("%w: part number %d is outside 1..%d", ErrNoSuchPart, number, MaxParts)
	}
	u, err := c.meta.Upload(ctx, id)
	if err != nil {
		return "", err
	}

	m, writing, err := c.write(ctx, u.Key, body, size)
	if err != nil {
		return "", err
	}
	if writing == "" {
		err = c.meta.CommitPart(ctx, id, number, m)
	} else {
		err = c.meta.CommitPartWhileWriting(ctx, id, number, m, writing)
	}
	c.stopWriting(ctx, writing)
	if err != nil {
		return "", err
	}
	return m.ETag, nil
}

// CompleteUpload assembles the named parts into the object and returns its
// manifest.
//
// The parts are checked before anything is committed: a completion naming a part
// that does not exist, or one whose ETag does not match, is refused with nothing
// changed. Committing a manifest over a client's mistake would produce an object
// that is readable, checksum-valid, and not what anybody uploaded.
// CopyPart stores a part whose bytes come from a range of an existing object, which
// is UploadPartCopy — how every client copies an object too large to copy in one
// call. The aws CLI switches to it above 8 MB, so without it a server-side copy of a
// large object is not possible at all.
//
// The bytes are read from the source's owners and written to the part's, so a copy
// of this kind moves data inside the cluster where CopyObject moves none. That is
// the reason it was deferred rather than a reason not to have it: the alternative is
// to give the part the source's own chunk references, which holds only when the
// range's boundaries fall exactly on chunk boundaries — and a client choosing an
// 8 MB part size against a 32 MB chunk never lands there. Re-chunking through the
// ordinary write path is honest about the cost and keeps a part indistinguishable
// from an uploaded one, which is what lets completion stay a single manifest commit.
//
// Streaming, so the part's size does not decide the footprint: the pipe hands each
// chunk to the writer as the reader produces it.
func (c *Coordinator) CopyPart(ctx context.Context, id string, number int, src object.Manifest, off, length int64) (string, error) {
	pr, pw := io.Pipe()
	go func() {
		// CloseWithError(nil) closes cleanly, so this reports the end of the range
		// and a failed read the same way, and the writer sees which.
		pw.CloseWithError(c.StreamRange(ctx, src, pw, off, length))
	}()
	// Unblocks the goroutine above if the write side gives up early.
	defer pr.Close()
	return c.UploadPart(ctx, id, number, pr, length)
}

func (c *Coordinator) CompleteUpload(ctx context.Context, id string, parts []CompletedPart) (object.Manifest, error) {
	u, err := c.meta.Upload(ctx, id)
	if errors.Is(err, meta.ErrNotFound) {
		// The upload may have completed already, with the client never seeing the
		// answer: every SDK retries a request whose connection died, and telling
		// it NoSuchUpload says the upload failed while the object is sitting
		// there. What is returned is what this upload produced, which is not
		// necessarily what is at the key now — the answer is about the upload.
		if done, dErr := c.meta.Completion(ctx, id); dErr == nil {
			return object.Manifest{ETag: done.ETag, Modified: done.When}, nil
		}
		return object.Manifest{}, err
	}
	if err != nil {
		return object.Manifest{}, err
	}
	stored, err := c.meta.Parts(ctx, id)
	if err != nil {
		return object.Manifest{}, err
	}
	if len(parts) == 0 {
		return object.Manifest{}, fmt.Errorf("%w: a completion named no parts", ErrNoSuchPart)
	}

	// S3 requires the parts in ascending order and forbids repeats. Sorting them
	// silently would accept a request whose meaning is ambiguous — the client
	// believes it knows the order its bytes go in.
	if !slices.IsSortedFunc(parts, func(a, b CompletedPart) int { return a.Number - b.Number }) {
		return object.Manifest{}, fmt.Errorf("%w: a completion listed its parts descending", ErrPartOrder)
	}

	final := object.Manifest{ContentType: u.ContentType, Meta: u.Meta}
	// The multipart ETag is the MD5 of the parts' MD5s, with the part count after
	// a dash. Clients recompute it to check an upload, so it is not free to be
	// anything simpler.
	sums := md5.New()
	for i, p := range parts {
		if i > 0 && p.Number == parts[i-1].Number {
			return object.Manifest{}, fmt.Errorf("%w: part %d appears twice", ErrPartOrder, p.Number)
		}
		m, ok := stored[p.Number]
		if !ok {
			return object.Manifest{}, fmt.Errorf("%w: part %d of upload %s", ErrNoSuchPart, p.Number, id)
		}
		// The quotes an ETag travels in are stripped; a client may send it either
		// way, and comparing the quoted form never matches.
		if want := strings.Trim(p.ETag, `"`); want != "" && want != m.ETag {
			return object.Manifest{}, fmt.Errorf("%w: part %d is %s, client says %s",
				ErrPartMismatch, p.Number, m.ETag, want)
		}
		// Every part was placed by the object's key, so they agree on owners and
		// on redundancy mode. Anything else would mean chunks the object's
		// manifest cannot describe.
		if i == 0 {
			final.Nodes, final.Coding = m.Nodes, m.Coding
		} else if !slices.Equal(final.Nodes, m.Nodes) || final.Coding != m.Coding {
			return object.Manifest{}, fmt.Errorf(
				"cluster: part %d of upload %s was placed on %v, earlier parts on %v",
				p.Number, id, m.Nodes, final.Nodes)
		}

		raw, err := hex.DecodeString(m.ETag)
		if err != nil {
			return object.Manifest{}, fmt.Errorf("cluster: part %d has etag %q: %w", p.Number, m.ETag, err)
		}
		sums.Write(raw)
		final.Chunks = append(final.Chunks, m.Chunks...)
		final.Size += m.Size
	}
	final.ETag = fmt.Sprintf("%x-%d", sums.Sum(nil), len(parts))
	final.Modified = time.Now().UTC().Truncate(time.Second)

	// The object exists from here. Chunks of parts the completion did not name are
	// leaked, along with those of any part uploaded twice — the same hole overwrites
	// leave, and it closes when chunk GC arrives rather than once per caller.
	if err := c.meta.Commit(ctx, u.Key, final); err != nil {
		return object.Manifest{}, err
	}
	// Before forgetting the upload, so that there is no instant in which the id is
	// neither in flight nor remembered — a retry landing there would be told its
	// upload never existed.
	done := meta.Completion{Key: u.Key, ETag: final.ETag, When: final.Modified, Parts: len(parts)}
	if err := c.meta.FinishUpload(ctx, id, done); err != nil {
		// The object is committed, so this is only about what a retry will be
		// told: NoSuchUpload rather than the answer it missed.
		log.Printf("complete upload %s: remember completion: %v", id, err)
	}
	if err := c.meta.DeleteUpload(ctx, id); err != nil {
		// The object is committed and readable; a leftover upload record is
		// clutter, not corruption.
		log.Printf("complete upload %s: forget upload: %v", id, err)
	}
	return final, nil
}

// AbortUpload discards an upload. Forgetting the parts is what makes the chunks
// they named unreferenced, and collection is what reclaims them. An upload
// nobody aborts is expired after UploadMaxAge — the same forgetting, just later.
//
// This used to drop the parts' chunks itself, which was safe only for as long as no
// two manifests could name the same chunk. It also raced a completion of the same
// upload: parts read here, the object committed there, and then the chunks of a
// committed object deleted. Neither is possible now that nothing deletes a chunk
// except the pass that reads every manifest first.
func (c *Coordinator) AbortUpload(ctx context.Context, id string) error {
	// Read before deleting, so that aborting an id that never existed is an error
	// rather than a success. A client that is told it cleaned up something it made
	// up cannot tell that from having cleaned up the wrong thing, and S3 answers
	// NoSuchUpload.
	if _, err := c.meta.Upload(ctx, id); err != nil {
		return err
	}
	return c.meta.DeleteUpload(ctx, id)
}

// ExpireUploads forgets every in-flight upload older than UploadMaxAge.
// Forgetting the record is what lets collection reclaim the parts.
func (c *Coordinator) ExpireUploads(ctx context.Context) (int, error) {
	return c.meta.ExpireUploads(ctx, time.Now().Add(-UploadMaxAge))
}

// Part is one uploaded part as a listing reports it.
type Part struct {
	Number   int
	ETag     string
	Size     int64
	Modified time.Time
}

// Parts lists an upload's parts in part-number order. An upload has at most
// MaxParts of them, so this is bounded by the protocol rather than by the store.
func (c *Coordinator) Parts(ctx context.Context, id string) ([]Part, error) {
	// So that listing the parts of an upload that does not exist says so, rather
	// than reporting that it has none.
	if _, err := c.meta.Upload(ctx, id); err != nil {
		return nil, err
	}
	stored, err := c.meta.Parts(ctx, id)
	if err != nil {
		return nil, err
	}
	parts := make([]Part, 0, len(stored))
	for number, m := range stored {
		parts = append(parts, Part{Number: number, ETag: m.ETag, Size: m.Size, Modified: m.Modified})
	}
	slices.SortFunc(parts, func(a, b Part) int { return a.Number - b.Number })
	return parts, nil
}

// Uploads lists in-flight uploads under a key prefix. See meta.Store.Uploads for
// the order they come back in and why it is not S3's.
func (c *Coordinator) Uploads(ctx context.Context, prefix, after string, limit int) ([]meta.UploadRef, bool, error) {
	return c.meta.Uploads(ctx, prefix, after, limit)
}
