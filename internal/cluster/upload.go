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
func (c *Coordinator) CreateUpload(ctx context.Context, key, contentType string) (string, error) {
	// Random rather than derived from the key: two clients uploading the same key
	// at once are two independent uploads, and each must be able to abort its own
	// without touching the other's parts.
	id := rand.Text()
	u := meta.Upload{Key: key, ContentType: contentType, Created: time.Now().UTC()}
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

	m, err := c.write(ctx, u.Key, body, size)
	if err != nil {
		return "", err
	}
	if err := c.meta.CommitPart(ctx, id, number, m); err != nil {
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
func (c *Coordinator) CompleteUpload(ctx context.Context, id string, parts []CompletedPart) (object.Manifest, error) {
	u, err := c.meta.Upload(ctx, id)
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

	final := object.Manifest{ContentType: u.ContentType}
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
	if err := c.meta.DeleteUpload(ctx, id); err != nil {
		// The object is committed and readable; a leftover upload record is
		// clutter, not corruption.
		log.Printf("complete upload %s: forget upload: %v", id, err)
	}
	return final, nil
}

// AbortUpload discards an upload. Forgetting the parts is what makes the chunks
// they named unreferenced, and collection is what reclaims them — abandoning an
// upload halfway leaves exactly the same garbage, and there is no second mechanism
// for it.
//
// This used to drop the parts' chunks itself, which was safe only for as long as no
// two manifests could name the same chunk. It also raced a completion of the same
// upload: parts read here, the object committed there, and then the chunks of a
// committed object deleted. Neither is possible now that nothing deletes a chunk
// except the pass that reads every manifest first.
func (c *Coordinator) AbortUpload(ctx context.Context, id string) error {
	return c.meta.DeleteUpload(ctx, id)
}
