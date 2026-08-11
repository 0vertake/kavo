package cluster

import (
	"context"
	"strings"

	"github.com/0vertake/kavo/internal/meta"
)

// ListRequest describes one page of a listing. Keys are absolute — the bucket is
// part of the key, since that is how manifests are stored.
type ListRequest struct {
	Prefix string
	// Delimiter groups keys that share a prefix up to its first occurrence, which
	// is how a flat keyspace is presented as directories.
	Delimiter string
	// From is the key to resume at, inclusive. It comes from a previous page's
	// Next, or from meta.After of a key the client asked to start beyond.
	From string
	// Limit is the most entries to return, counting grouped prefixes.
	Limit int
}

// ListPage is one page of a listing.
type ListPage struct {
	Objects []meta.Entry
	// Prefixes are the groups the delimiter collapsed, in key order.
	Prefixes []string
	// Next is where a following page starts, inclusive, and is empty when the
	// listing is complete. It is a position rather than a key: a page that ended
	// inside a grouped prefix resumes past the whole group, since the group has
	// already been reported and its remaining keys must not be looked at again.
	Next string
}

// scanPageSize is how many keys are read from etcd at a time when a delimiter is
// grouping them. A grouped listing can discard almost all of what it reads — many
// keys under one prefix collapse to one entry — so the page a client asked for is
// not the page etcd is asked for. Without a delimiter the two are the same and
// this does not apply.
const scanPageSize = 256

// List returns one page of a bucket's keys.
//
// Grouping is done here because etcd has no notion of a delimiter. Keys inside a
// group that has already been reported are skipped in the page already fetched,
// and the next fetch starts past the whole group rather than inside it — so a
// directory holding a million objects costs one entry and, at worst, one wasted
// page of reads, not a million.
func (c *Coordinator) List(ctx context.Context, req ListRequest) (ListPage, error) {
	var page ListPage
	if req.Limit <= 0 {
		return page, nil // a page of nothing is complete, and reads nothing to say so
	}
	from := req.From
	// A full page is not a truncated one. The scan goes on until it either finds
	// something reportable past the page — which is what truncated means — or runs
	// out, and only then is the answer known. Stopping at a full page and handing
	// back a resume point instead claims there is more whenever the keyspace ends
	// exactly on the boundary: one wasted round trip, and a page of nothing for a
	// client that trusts IsTruncated.
	full := func() bool { return len(page.Objects)+len(page.Prefixes) == req.Limit }
	for {
		// Without a delimiter every key read becomes an entry, so the page a client
		// asked for is the page to ask etcd for — one round trip rather than four —
		// plus one key, whose existence is the answer to whether more follows. That
		// makes this loop run exactly once when there is no delimiter.
		want := int64(scanPageSize)
		if req.Delimiter == "" {
			want = int64(req.Limit) + 1
		}
		objects, err := c.meta.ScanEntries(ctx, req.Prefix, from, want)
		if err != nil {
			return ListPage{}, err
		}
		if len(objects) == 0 {
			return page, nil // the listing ran out, so it is complete
		}
		// A short read means the range is exhausted, which saves asking again to
		// be told there is nothing left.
		exhausted := int64(len(objects)) < want

		// group holds the prefix just reported, whose remaining keys are inside it
		// and must not be reported again. Keys arrive sorted, so once one falls
		// outside the group nothing later can fall back into it.
		var group string
		for _, o := range objects {
			if group != "" && strings.HasPrefix(o.Key, group) {
				continue
			}
			if full() {
				// Something reportable past the end of the page, so there is a next
				// page, and from has not moved since the page filled.
				page.Next = from
				return page, nil
			}
			var grouped bool
			if group, grouped = groupOf(o.Key, req.Prefix, req.Delimiter); grouped {
				page.Prefixes = append(page.Prefixes, group)
				// Past the group rather than past this key: a following page must
				// not start inside a group it has already been told about.
				from = meta.PastPrefix(group)
			} else {
				page.Objects = append(page.Objects, o)
				from = meta.After(o.Key)
			}
		}
		if exhausted || from == "" {
			return page, nil // nothing left, or nothing sorts past the group just skipped
		}
	}
}

// groupOf returns the prefix a key is grouped under, if the delimiter appears in
// the part of the key after the listing's prefix. The group includes the delimiter,
// as S3 reports it.
func groupOf(key, prefix, delimiter string) (string, bool) {
	if delimiter == "" {
		return "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	i := strings.Index(rest, delimiter)
	if i < 0 {
		return "", false
	}
	return prefix + rest[:i+len(delimiter)], true
}
