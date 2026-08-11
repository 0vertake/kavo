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
	Objects []meta.Object
	// Prefixes are the groups the delimiter collapsed, in key order.
	Prefixes []string
	// Next is where a following page starts, inclusive, and is empty when the
	// listing is complete. It is a position rather than a key: a page that ended
	// inside a grouped prefix resumes past the whole group, since the group has
	// already been reported and its remaining keys must not be looked at again.
	Next string
}

// scanPageSize is how many manifests are read from etcd at a time. A listing with
// a delimiter can discard almost all of them — many keys under one grouped prefix
// collapse to one entry — so the page a client asked for is not the page etcd is
// asked for.
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
	from := req.From
	for {
		objects, err := c.meta.ScanObjects(ctx, req.Prefix, from, scanPageSize)
		if err != nil {
			return ListPage{}, err
		}
		if len(objects) == 0 {
			return page, nil // the listing ran out, so it is complete
		}

		// group holds the prefix just reported, whose remaining keys are inside it
		// and must not be reported again. Keys arrive sorted, so once one falls
		// outside the group nothing later can fall back into it.
		var group string
		for _, o := range objects {
			if group != "" && strings.HasPrefix(o.Key, group) {
				continue
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

			if len(page.Objects)+len(page.Prefixes) == req.Limit {
				// Whether anything follows is not known yet, so the caller gets a
				// resume point and finds out by asking.
				page.Next = from
				return page, nil
			}
		}
		if from == "" {
			return page, nil // nothing sorts past the group just skipped
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
