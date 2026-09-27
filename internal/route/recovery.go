package route

import "github.com/wyw14/solo-036/internal/model"

func Restore(c *Coordinator, leases []model.RouteLease, queues []model.RouteReservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, lease := range leases {
		if lease.Route != "" && lease.RunID != "" {
			c.owners[lease.Route] = lease
		}
	}
	for _, reservation := range queues {
		if reservation.Route == "" || reservation.RunID == "" {
			continue
		}
		if owner, ok := c.owners[reservation.Route]; ok && owner.RunID == reservation.RunID {
			continue
		}
		duplicate := false
		for _, waiting := range c.waiters[reservation.Route] {
			if waiting.RunID == reservation.RunID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			c.waiters[reservation.Route] = append(c.waiters[reservation.Route], reservation)
		}
	}
}

func Snapshot(c *Coordinator) []model.RouteLease {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.RouteLease, 0, len(c.owners))
	for _, lease := range c.owners {
		out = append(out, lease)
	}
	return out
}

func SnapshotQueues(c *Coordinator) []model.RouteReservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.RouteReservation, 0)
	for _, queue := range c.waiters {
		out = append(out, queue...)
	}
	return out
}
