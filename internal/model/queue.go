package model

import "time"

type QueueEntryState string

const (
	QueueWaiting   QueueEntryState = "waiting"
	QueueGranted   QueueEntryState = "granted"
	QueueWithdrawn QueueEntryState = "withdrawn"
	QueueExpired   QueueEntryState = "expired"
	QueueInvalid   QueueEntryState = "invalid"
)

func (s QueueEntryState) Terminal() bool { return s != QueueWaiting }

// RouteQueueEntry is a duty operator's application to occupy a route once the
// current run releases it. The entry carries the launch parameters so the
// handoff can re-validate conditions at grant time; an entry that expired or
// whose conditions no longer hold must never occupy the route.
type RouteQueueEntry struct {
	ID             string          `json:"id"`
	RouteID        string          `json:"route_id"`
	RunID          string          `json:"run_id"`
	StationID      string          `json:"station_id"`
	TargetPressure float64         `json:"target_pressure"`
	State          QueueEntryState `json:"state"`
	Reason         string          `json:"reason,omitempty"`
	EnqueuedAt     time.Time       `json:"enqueued_at"`
	ExpiresAt      time.Time       `json:"expires_at"`
	DecidedAt      time.Time       `json:"decided_at,omitempty"`
}
