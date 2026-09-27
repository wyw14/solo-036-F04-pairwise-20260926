package route

import (
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

// ErrRouteBusy indicates the route is currently fenced to another run.
var ErrRouteBusy = errors.New("route is occupied by another run")

// ErrAlreadyQueued indicates a run already holds a place in the route queue.
var ErrAlreadyQueued = errors.New("run is already queued for the route")

// CandidateValid decides whether a queued reservation may still take a route
// when it reaches the front of the queue. A false verdict skips the candidate
// instead of handing the resource over.
type CandidateValid func(model.RouteReservation) (bool, string)

func (c *Coordinator) Reserve(route, runID string, epoch int64) (model.RouteLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner, ok := c.owners[route]; ok && owner.RunID != runID {
		return model.RouteLease{}, fmt.Errorf("%w: owned by %s", ErrRouteBusy, owner.RunID)
	}
	lease := model.RouteLease{Route: route, RunID: runID, Epoch: epoch, AcquiredAt: time.Now().UTC()}
	c.owners[route] = lease
	return lease, nil
}

// Enqueue lines a reservation up for a route. When the route is free, nobody
// is waiting and the reservation passes its validity check, the lease is
// granted immediately and acquired is true. Otherwise the reservation is
// appended to the FIFO queue and its 1-based waiting position is returned.
func (c *Coordinator) Enqueue(res model.RouteReservation, valid CandidateValid) (lease model.RouteLease, acquired bool, position int, err error) {
	if res.Route == "" || res.RunID == "" {
		return model.RouteLease{}, false, 0, errors.New("route reservation identity is incomplete")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if owner, ok := c.owners[res.Route]; ok {
		if owner.RunID == res.RunID {
			return owner, true, 0, nil
		}
	} else if len(c.waiters[res.Route]) == 0 && c.reservationUsable(res, valid) {
		lease := c.grant(res)
		return lease, true, 0, nil
	}
	for _, waiting := range c.waiters[res.Route] {
		if waiting.RunID == res.RunID {
			return model.RouteLease{}, false, 0, fmt.Errorf("%w: run %s on route %s", ErrAlreadyQueued, res.RunID, res.Route)
		}
	}
	c.waiters[res.Route] = append(c.waiters[res.Route], res)
	return model.RouteLease{}, false, len(c.waiters[res.Route]), nil
}

// Withdraw pulls a single run out of a route's queue. It reports whether the
// run was waiting.
func (c *Coordinator) Withdraw(route, runID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	filtered, removed := c.remove(c.waiters[route], runID)
	if !removed {
		return false
	}
	if len(filtered) == 0 {
		delete(c.waiters, route)
	} else {
		c.waiters[route] = filtered
	}
	return true
}

// WithdrawAll pulls a run out of every route queue it is waiting in and
// returns the affected route ids.
func (c *Coordinator) WithdrawAll(runID string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	routes := make([]string, 0)
	for routeID, queue := range c.waiters {
		filtered, removed := c.remove(queue, runID)
		if !removed {
			continue
		}
		routes = append(routes, routeID)
		if len(filtered) == 0 {
			delete(c.waiters, routeID)
		} else {
			c.waiters[routeID] = filtered
		}
	}
	return routes
}

// Release frees a route held by runID. It only drops ownership; handing the
// route to the next valid candidate is PromoteIdle's job, driven by the
// service layer so a failed start can be unwound before the next attempt.
func (c *Coordinator) Release(route, runID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	owner, ok := c.owners[route]
	if !ok || owner.RunID != runID {
		return false
	}
	delete(c.owners, route)
	delete(c.progress, runID)
	return true
}

// PromoteIdle hands an unowned route to the first queued reservation that is
// still a valid candidate, atomically. Expired or rejected reservations are
// dropped from the queue and never acquire the route. It returns the promoted
// reservation, or false when the route is owned or nobody qualified.
func (c *Coordinator) PromoteIdle(route string, valid CandidateValid) (model.RouteReservation, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.owners[route]; ok {
		return model.RouteReservation{}, false
	}
	return c.promoteLocked(route, valid)
}

func (c *Coordinator) promoteLocked(route string, valid CandidateValid) (model.RouteReservation, bool) {
	queue := c.waiters[route]
	survivors := make([]model.RouteReservation, 0, len(queue))
	var promoted model.RouteReservation
	for _, reservation := range queue {
		if !c.reservationUsable(reservation, valid) {
			// Expired or no longer valid at release time: drop it. It must
			// never occupy the route, and it should not keep a place in line
			// ahead of later valid candidates.
			continue
		}
		if promoted.RunID == "" {
			promoted = reservation
			c.grant(reservation)
			continue
		}
		survivors = append(survivors, reservation)
	}
	if len(survivors) == 0 {
		delete(c.waiters, route)
	} else {
		c.waiters[route] = survivors
	}
	return promoted, promoted.RunID != ""
}

func (c *Coordinator) grant(res model.RouteReservation) model.RouteLease {
	lease := model.RouteLease{Route: res.Route, RunID: res.RunID, Epoch: res.Epoch, AcquiredAt: time.Now().UTC()}
	c.owners[res.Route] = lease
	delete(c.progress, res.RunID)
	return lease
}

func (c *Coordinator) reservationUsable(res model.RouteReservation, valid CandidateValid) bool {
	if !res.ExpiresAt.IsZero() && !time.Now().UTC().Before(res.ExpiresAt) {
		return false
	}
	if valid == nil {
		return true
	}
	ok, _ := valid(res)
	return ok
}

func (c *Coordinator) remove(queue []model.RouteReservation, runID string) ([]model.RouteReservation, bool) {
	for i, reservation := range queue {
		if reservation.RunID == runID {
			out := make([]model.RouteReservation, 0, len(queue)-1)
			out = append(out, queue[:i]...)
			out = append(out, queue[i+1:]...)
			return out, true
		}
	}
	return queue, false
}

func (c *Coordinator) Queue(route string) []model.RouteReservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	queue := c.waiters[route]
	out := make([]model.RouteReservation, len(queue))
	copy(out, queue)
	return out
}

func (c *Coordinator) Owner(route string) (model.RouteLease, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	owner, ok := c.owners[route]
	return owner, ok
}

func (c *Coordinator) OwnerOfRun(runID string) (model.RouteLease, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, lease := range c.owners {
		if lease.RunID == runID {
			return lease, true
		}
	}
	return model.RouteLease{}, false
}
