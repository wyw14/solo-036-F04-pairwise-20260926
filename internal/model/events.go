package model

import "time"

type Run struct {
	ID        string
	Station   string
	State     RunState
	Epoch     int64
	CreatedAt time.Time
	UpdatedAt time.Time
}
type Event struct {
	ID      string
	RunID   string
	Kind    string
	Epoch   int64
	Payload string
	At      time.Time
	Durable bool
}
type PassageEvidence struct {
	RunID      string
	Station    string
	SourceID   string
	Generation int64
	Position   float64
	At         time.Time
	Valid      bool
}
type RouteLease struct {
	Route      string
	RunID      string
	Epoch      int64
	AcquiredAt time.Time
}
type RouteReservation struct {
	Route          string    `json:"route"`
	RunID          string    `json:"run_id"`
	Epoch          int64     `json:"epoch"`
	StationID      string    `json:"station_id"`
	TargetPressure float64   `json:"target_pressure"`
	QueuedAt       time.Time `json:"queued_at"`
	ExpiresAt      time.Time `json:"expires_at"`
}
type CommandReceipt struct {
	RunID    string
	Command  string
	Epoch    int64
	Accepted bool
	At       time.Time
}
type Equipment struct {
	Name      string
	Kind      string
	State     string
	UpdatedAt time.Time
}
type Interlock struct {
	Name   string
	Ready  bool
	Reason string
}
type Incident struct {
	ID       string
	RunID    string
	Severity string
	Message  string
	At       time.Time
}
