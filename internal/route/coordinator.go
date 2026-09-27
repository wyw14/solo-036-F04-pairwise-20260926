package route

import (
	"errors"
	"fmt"
	"github.com/wyw14/solo-036/internal/model"
	"time"
)

// ErrRouteOwned marks a reservation attempt against a route leased to another run.
var ErrRouteOwned = errors.New("route owned")

func (c *Coordinator) Reserve(route, runID string, epoch int64) (model.RouteLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner, ok := c.owners[route]; ok && owner.RunID != runID {
		return model.RouteLease{}, fmt.Errorf("%w by run %s", ErrRouteOwned, owner.RunID)
	}
	lease := model.RouteLease{Route: route, RunID: runID, Epoch: epoch, AcquiredAt: time.Now().UTC()}
	c.owners[route] = lease
	return lease, nil
}
func (c *Coordinator) Release(route, runID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner, ok := c.owners[route]; ok && owner.RunID == runID {
		delete(c.owners, route)
		delete(c.progress, runID)
	}
}
