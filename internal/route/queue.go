package route

import (
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

// Enqueue appends a waiting application to the route's FIFO queue.
func (c *Coordinator) Enqueue(entry model.RouteQueueEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queues[entry.RouteID] = append(c.queues[entry.RouteID], entry)
}

// QueueEntry returns the current state of an application by ID.
func (c *Coordinator) QueueEntry(id string) (model.RouteQueueEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entries := range c.queues {
		for _, entry := range entries {
			if entry.ID == id {
				return entry, true
			}
		}
	}
	return model.RouteQueueEntry{}, false
}

// NextWaiting returns the oldest still-waiting application for a route,
// regardless of expiry; callers decide how to handle an expired head.
func (c *Coordinator) NextWaiting(route string) (model.RouteQueueEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.queues[route] {
		if entry.State == model.QueueWaiting {
			return entry, true
		}
	}
	return model.RouteQueueEntry{}, false
}

// Waiting lists the effective candidates for a route in turn order: waiting
// applications whose expiry has not passed.
func (c *Coordinator) Waiting(route string, now time.Time) []model.RouteQueueEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.RouteQueueEntry, 0)
	for _, entry := range c.queues[route] {
		if entry.State == model.QueueWaiting && entry.ExpiresAt.After(now) {
			out = append(out, entry)
		}
	}
	return out
}

// WaitingCount returns the number of effective candidates across all routes.
func (c *Coordinator) WaitingCount(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, entries := range c.queues {
		for _, entry := range entries {
			if entry.State == model.QueueWaiting && entry.ExpiresAt.After(now) {
				count++
			}
		}
	}
	return count
}

// Decide moves a waiting application to a terminal state. It returns false
// when the application is unknown or was already decided.
func (c *Coordinator) Decide(id string, state model.QueueEntryState, reason string) (model.RouteQueueEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for route, entries := range c.queues {
		for i, entry := range entries {
			if entry.ID != id {
				continue
			}
			if entry.State != model.QueueWaiting {
				return entry, false
			}
			entry.State = state
			entry.Reason = reason
			entry.DecidedAt = time.Now().UTC()
			c.queues[route][i] = entry
			return entry, true
		}
	}
	return model.RouteQueueEntry{}, false
}

// Withdraw marks a waiting application on a route as withdrawn. It returns
// false when the application is unknown or no longer waiting.
func (c *Coordinator) Withdraw(route, id string) (model.RouteQueueEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries := c.queues[route]
	for i, entry := range entries {
		if entry.ID != id {
			continue
		}
		if entry.State != model.QueueWaiting {
			return entry, false
		}
		entry.State = model.QueueWithdrawn
		entry.Reason = "withdrawn by duty operator"
		entry.DecidedAt = time.Now().UTC()
		c.queues[route][i] = entry
		return entry, true
	}
	return model.RouteQueueEntry{}, false
}

// WithdrawRun marks every waiting application of a run as withdrawn and
// returns the affected entries.
func (c *Coordinator) WithdrawRun(runID string) []model.RouteQueueEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.RouteQueueEntry, 0)
	for route, entries := range c.queues {
		for i, entry := range entries {
			if entry.RunID == runID && entry.State == model.QueueWaiting {
				entry.State = model.QueueWithdrawn
				entry.Reason = "run is no longer active"
				entry.DecidedAt = time.Now().UTC()
				c.queues[route][i] = entry
				out = append(out, entry)
			}
		}
	}
	return out
}

// Owner returns the run currently holding the route lease.
func (c *Coordinator) Owner(route string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lease, ok := c.owners[route]
	if !ok {
		return "", false
	}
	return lease.RunID, true
}
