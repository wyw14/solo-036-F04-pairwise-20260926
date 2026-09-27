package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/route"
)

const (
	defaultQueueTTLSeconds = 1800
	maxQueueTTLSeconds     = 86400
)

type QueueRequest struct {
	StationID        string  `json:"station_id"`
	TargetPressure   float64 `json:"target_pressure"`
	ExpiresInSeconds int64   `json:"expires_in_seconds,omitempty"`
}

type QueuedApplication struct {
	Entry    model.RouteQueueEntry `json:"entry"`
	Position int                   `json:"position"`
}

type RouteQueueView struct {
	RouteID    string              `json:"route_id"`
	OwnerRunID string              `json:"owner_run_id,omitempty"`
	Queue      []QueuedApplication `json:"queue"`
}

// EnqueueRoute registers a launch application for a route. The application is
// validated up front, queued behind earlier candidates, and promoted
// immediately when the route is free.
func (r *Runtime) EnqueueRoute(routeID string, request QueueRequest) (QueuedApplication, error) {
	control := r.ensureControl()
	if _, ok := control.Routes[routeID]; !ok {
		return QueuedApplication{}, fmt.Errorf("unknown route %s", routeID)
	}
	launchRequest := LaunchRequest{StationID: request.StationID, RouteID: routeID, TargetPressure: request.TargetPressure}
	if err := r.ValidateLaunchRequest(launchRequest); err != nil {
		return QueuedApplication{}, fmt.Errorf("application cannot be queued: %w", err)
	}
	ttl := request.ExpiresInSeconds
	if ttl <= 0 {
		ttl = defaultQueueTTLSeconds
	}
	if ttl > maxQueueTTLSeconds {
		ttl = maxQueueTTLSeconds
	}
	created, err := r.CreateOperation(request.StationID)
	if err != nil {
		return QueuedApplication{}, err
	}
	now := time.Now().UTC()
	entry := model.RouteQueueEntry{
		ID:             model.NewRevision(),
		RouteID:        routeID,
		RunID:          created.ID,
		StationID:      request.StationID,
		TargetPressure: request.TargetPressure,
		State:          model.QueueWaiting,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Duration(ttl) * time.Second),
	}
	r.Routes.Enqueue(entry)
	if err := r.appendEvent(model.Event{RunID: created.ID, Kind: "queue_enqueue", Epoch: created.Epoch, Payload: routeID}); err != nil {
		return QueuedApplication{}, err
	}
	r.promoteRoute(routeID)
	return r.queuePosition(routeID, entry.ID)
}

// WithdrawQueueEntry lets duty personnel pull a waiting application off the
// queue; the associated loaded run is cancelled so it never occupies the route.
func (r *Runtime) WithdrawQueueEntry(routeID, entryID string) (model.RouteQueueEntry, error) {
	r.queueMu.Lock()
	entry, ok := r.Routes.Withdraw(routeID, entryID)
	r.queueMu.Unlock()
	if !ok {
		return model.RouteQueueEntry{}, fmt.Errorf("no waiting application %s on route %s", entryID, routeID)
	}
	_ = r.Runs.Cancel(entry.RunID)
	if err := r.appendEvent(model.Event{RunID: entry.RunID, Kind: "queue_withdraw", Payload: routeID}); err != nil {
		return model.RouteQueueEntry{}, err
	}
	return entry, nil
}

// RouteQueue reports the current lease holder and the expected turn order of
// the effective (waiting, unexpired) candidates for a route.
func (r *Runtime) RouteQueue(routeID string) (RouteQueueView, error) {
	control := r.ensureControl()
	if _, ok := control.Routes[routeID]; !ok {
		return RouteQueueView{}, fmt.Errorf("unknown route %s", routeID)
	}
	view := RouteQueueView{RouteID: routeID, Queue: make([]QueuedApplication, 0)}
	if owner, ok := r.Routes.Owner(routeID); ok {
		view.OwnerRunID = owner
	}
	for i, entry := range r.Routes.Waiting(routeID, time.Now().UTC()) {
		view.Queue = append(view.Queue, QueuedApplication{Entry: entry, Position: i + 1})
	}
	return view, nil
}

func (r *Runtime) queuePosition(routeID, entryID string) (QueuedApplication, error) {
	entry, ok := r.Routes.QueueEntry(entryID)
	if !ok {
		return QueuedApplication{}, fmt.Errorf("queued application %s not found", entryID)
	}
	position := 0
	if entry.State == model.QueueWaiting {
		for i, candidate := range r.Routes.Waiting(routeID, time.Now().UTC()) {
			if candidate.ID == entryID {
				position = i + 1
				break
			}
		}
	}
	return QueuedApplication{Entry: entry, Position: position}, nil
}

// promoteRoute hands a released route to the first valid queued application.
// Expired applications and applications whose launch conditions no longer
// hold are decided terminally and never occupy the route; when no valid
// candidate remains the route simply stays free.
//
// Lock ordering: queueMu is always the outermost lock. Callers must not hold
// r.mu when invoking promoteRoute.
func (r *Runtime) promoteRoute(routeID string) {
	r.queueMu.Lock()
	defer r.queueMu.Unlock()
	for {
		if _, owned := r.Routes.Owner(routeID); owned {
			return
		}
		entry, ok := r.Routes.NextWaiting(routeID)
		if !ok {
			return
		}
		if !entry.ExpiresAt.After(time.Now().UTC()) {
			r.decideQueued(entry, model.QueueExpired, "application expired before the route was released", "queue_expire")
			continue
		}
		runState, ok := r.Runs.Get(entry.RunID)
		if !ok || runState.State != model.StateLoaded {
			r.decideQueued(entry, model.QueueInvalid, "run is no longer waiting for the route", "queue_reject")
			continue
		}
		request := LaunchRequest{StationID: entry.StationID, RouteID: routeID, TargetPressure: entry.TargetPressure}
		if err := r.ValidateLaunchRequest(request); err != nil {
			r.decideQueued(entry, model.QueueInvalid, "launch conditions changed: "+err.Error(), "queue_reject")
			continue
		}
		if err := r.executeLaunch(runState, request); err != nil {
			if errors.Is(err, route.ErrRouteOwned) {
				// A concurrent direct launch took the route; the next
				// release will promote the queue again.
				return
			}
			r.decideQueued(entry, model.QueueInvalid, "launch failed at handoff: "+err.Error(), "queue_reject")
			continue
		}
		if _, ok := r.Routes.Decide(entry.ID, model.QueueGranted, "route granted by queue"); ok {
			_ = r.appendEvent(model.Event{RunID: entry.RunID, Kind: "queue_grant", Epoch: runState.Epoch, Payload: routeID})
		}
		return
	}
}

// decideQueued terminally decides a waiting application, cancels its loaded
// run, and records why the route was not handed over.
func (r *Runtime) decideQueued(entry model.RouteQueueEntry, state model.QueueEntryState, reason, eventKind string) {
	decided, ok := r.Routes.Decide(entry.ID, state, reason)
	if !ok {
		return
	}
	_ = r.Runs.Cancel(entry.RunID)
	r.recordIncident(entry.RunID, "warning", fmt.Sprintf("queued application %s on %s: %s", decided.ID, decided.RouteID, reason))
	_ = r.appendEvent(model.Event{RunID: entry.RunID, Kind: eventKind, Payload: decided.RouteID + ": " + reason})
}

// withdrawQueuedRun pulls every waiting application of a run off the queues,
// e.g. when the run is cancelled through the operations API.
func (r *Runtime) withdrawQueuedRun(runID string) {
	r.queueMu.Lock()
	withdrawn := r.Routes.WithdrawRun(runID)
	r.queueMu.Unlock()
	for _, entry := range withdrawn {
		_ = r.appendEvent(model.Event{RunID: runID, Kind: "queue_withdraw", Payload: entry.RouteID + ": run no longer active"})
	}
}
