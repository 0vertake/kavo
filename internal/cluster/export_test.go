package cluster

import (
	"context"

	"github.com/0vertake/kavo/internal/meta"
)

// RebalanceOne runs one pass over a single object, for tests that need to hand
// the pass a manifest the store no longer agrees with. Reaching that state
// through Rebalance would mean winning a race against a client on purpose.
func (c *Coordinator) RebalanceOne(ctx context.Context, o meta.Object) (RebalanceStats, error) {
	var st RebalanceStats
	err := c.rebalanceObject(ctx, o, &st, &pacer{})
	return st, err
}
