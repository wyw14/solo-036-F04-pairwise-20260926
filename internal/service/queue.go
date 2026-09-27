package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/route"
)

// defaultQueueTTL bounds how long a lined-up application stays a candidate.
const defaultQueueTTL = 30 * time.Minute

// coreCandidateValid is the cheap fence check executed while the route
// coordinator holds its lock: it must only touch the run store, never the
// control-state mutex. A false verdict makes promotion skip the application
// instead of handing the route over.
func (r *Runtime) coreCandidateValid(res model.RouteReservation) (bool, string) {
	current, ok := r.Runs.Get(res.RunID)
	if !ok {
		return false, "run no longer exists"
	}
	if current.State.Terminal() {
		return false, fmt.Sprintf("run is %s", current.State)
	}
	if current.Epoch != res.Epoch {
		return false, fmt.Sprintf("run epoch advanced from %d to %d", res.Epoch, current.Epoch)
	}
	if current.Station != res.StationID {
		return false, "run station no longer matches the application"
	}
	return true, ""
}

// fullCandidateValid re-checks the launch conditions for a queued
// application: stations, route geometry and pressure window. It runs outside
// the coordinator lock against a consistent snapshot, so an application whose
// conditions changed is rejected even if it reached the front of the queue.
func (r *Runtime) fullCandidateValid(res model.RouteReservation) (bool, string) {
	if ok, reason := r.coreCandidateValid(res); !ok {
		return false, reason
	}
	if !res.ExpiresAt.IsZero() && !time.Now().UTC().Before(res.ExpiresAt) {
		return false, "application has expired"
	}
	snapshot := r.Snapshot()
	station, ok := snapshot.StationIndex()[res.StationID]
	if !ok {
		return false, "launch station no longer exists"
	}
	if station.Role != model.StationLauncher || !station.Enabled {
		return false, "launch station is unavailable"
	}
	var routeState model.RouteState
	for _, candidate := range snapshot.Routes {
		if candidate.ID == res.Route {
			routeState = candidate
			break
		}
	}
	if routeState.ID == "" {
		return false, "route no longer exists"
	}
	if routeState.FromStation != station.ID {
		return false, "route no longer starts at the launch station"
	}
	if res.TargetPressure <= 0 || res.TargetPressure > station.PressureLimit {
		return false, fmt.Sprintf("target pressure is outside 0 and %.2f", station.PressureLimit)
	}
	control := r.ensureControl()
	if _, err := control.Network.Path(routeState.FromStation, routeState.ToStation, res.TargetPressure); err != nil {
		return false, "route no longer supports the requested pressure window"
	}
	return true, ""
}

// Submit lines a run up for a route. A valid application acquires the route
// immediately when it is free; otherwise it waits in FIFO order. When
// allowQueue is false a busy route is rejected before any run is created.
func (r *Runtime) Submit(ctx context.Context, request LaunchRequest, expiresAt time.Time, allowQueue bool) (model.QueueEnrollment, error) {
	if err := r.ValidateLaunchRequest(request); err != nil {
		return model.QueueEnrollment{}, err
	}
	if !allowQueue {
		if lease, ok := r.Routes.Owner(request.RouteID); ok && lease.RunID != "" {
			return model.QueueEnrollment{}, fmt.Errorf("%w: route %s is owned by %s", route.ErrRouteBusy, request.RouteID, lease.RunID)
		}
	}
	created, err := r.CreateOperation(request.StationID)
	if err != nil {
		return model.QueueEnrollment{}, err
	}
	if expiresAt.IsZero() {
		expiresAt = created.CreatedAt.Add(defaultQueueTTL)
	}
	reservation := model.RouteReservation{
		Route:          request.RouteID,
		RunID:          created.ID,
		Epoch:          created.Epoch,
		StationID:      request.StationID,
		TargetPressure: request.TargetPressure,
		QueuedAt:       created.CreatedAt,
		ExpiresAt:      expiresAt.UTC(),
	}
	lease, acquired, position, err := r.Routes.Enqueue(reservation, r.coreCandidateValid)
	if err != nil {
		_ = r.Runs.Cancel(created.ID)
		return model.QueueEnrollment{}, err
	}
	if !acquired && !allowQueue {
		// Lost a race for a free route: retract the just-created application.
		r.Routes.Withdraw(request.RouteID, created.ID)
		_ = r.Runs.Cancel(created.ID)
		r.syncRouteQueue(request.RouteID)
		if holder, ok := r.Routes.Owner(request.RouteID); ok {
			return model.QueueEnrollment{}, fmt.Errorf("%w: route %s is owned by %s", route.ErrRouteBusy, request.RouteID, holder.RunID)
		}
		return model.QueueEnrollment{}, route.ErrRouteBusy
	}
	if acquired {
		record, startErr := r.startRun(created, request, lease, ctx)
		if startErr != nil {
			r.runNext(request.RouteID)
			return model.QueueEnrollment{}, startErr
		}
		return model.QueueEnrollment{Status: "started", RunID: created.ID, RouteID: request.RouteID, Record: &record}, nil
	}
	r.syncRouteQueue(request.RouteID)
	_ = r.appendEvent(model.Event{
		RunID:   created.ID,
		Kind:    "route_queued",
		Epoch:   created.Epoch,
		Payload: request.RouteID,
		At:      reservation.QueuedAt,
	})
	return model.QueueEnrollment{Status: "queued", RunID: created.ID, RouteID: request.RouteID, Position: position}, nil
}

// EnqueueRoute is the attendant-facing queue submission: when the route is
// free the run starts immediately, otherwise it lines up in FIFO order.
func (r *Runtime) EnqueueRoute(ctx context.Context, routeID string, request model.QueueRequest, expiresAt time.Time) (model.QueueEnrollment, error) {
	return r.Submit(ctx, LaunchRequest{
		StationID:      request.StationID,
		RouteID:        routeID,
		TargetPressure: request.TargetPressure,
	}, expiresAt, true)
}

// WithdrawRoute retracts a waiting application and cancels its run. An
// application that has already been promoted to owner must be cancelled as a
// normal operation instead.
func (r *Runtime) WithdrawRoute(routeID, runID string) error {
	if lease, ok := r.Routes.OwnerOfRun(runID); ok && lease.Route == routeID {
		return fmt.Errorf("%w: run %s already holds route %s; cancel the operation instead", route.ErrRouteBusy, runID, routeID)
	}
	if !r.Routes.Withdraw(routeID, runID) {
		return fmt.Errorf("run %s is not queued for route %s", runID, routeID)
	}
	if current, ok := r.Runs.Get(runID); ok && !current.State.Terminal() {
		_ = r.Runs.Cancel(runID)
	}
	r.syncRouteQueue(routeID)
	_ = r.appendEvent(model.Event{
		RunID:   runID,
		Kind:    "route_withdrawn",
		Payload: routeID,
	})
	return nil
}

// RouteQueue reports the owner and predicted turn order for one route,
// annotating every waiting run with its position and whether it is still a
// valid candidate.
func (r *Runtime) RouteQueue(routeID string) (model.RouteQueueStatus, error) {
	if _, ok := r.ensureControl().Routes[routeID]; !ok {
		return model.RouteQueueStatus{}, fmt.Errorf("unknown route %s", routeID)
	}
	return r.routeQueueStatus(routeID), nil
}

func (r *Runtime) RouteQueues() []model.RouteQueueStatus {
	control := r.ensureControl()
	r.mu.Lock()
	routeIDs := make([]string, 0, len(control.Routes))
	for id := range control.Routes {
		routeIDs = append(routeIDs, id)
	}
	r.mu.Unlock()
	out := make([]model.RouteQueueStatus, 0, len(routeIDs))
	for _, id := range routeIDs {
		out = append(out, r.routeQueueStatus(id))
	}
	return out
}

func (r *Runtime) routeQueueStatus(routeID string) model.RouteQueueStatus {
	waiting := r.Routes.Queue(routeID)
	status := model.RouteQueueStatus{
		Route:     routeID,
		Waiting:   make([]model.QueuedRun, 0, len(waiting)),
		UpdatedAt: time.Now().UTC(),
	}
	if lease, ok := r.Routes.Owner(routeID); ok {
		status.Reserved = true
		status.OwnerRunID = lease.RunID
	}
	for i, reservation := range waiting {
		valid, reason := r.fullCandidateValid(reservation)
		entry := model.QueuedRun{
			RunID:          reservation.RunID,
			Position:       i + 1,
			Valid:          valid,
			Reason:         reason,
			StationID:      reservation.StationID,
			TargetPressure: reservation.TargetPressure,
			QueuedAt:       reservation.QueuedAt,
		}
		status.Waiting = append(status.Waiting, entry)
		if valid && status.NextRunID == "" {
			status.NextRunID = reservation.RunID
		}
	}
	return status
}

// runNext hands an idle route to the first valid candidate and starts it,
// skipping applications that are expired or whose conditions no longer hold.
// Each attempt removes at least one queued application, so the loop
// terminates; a failed start releases the route again and tries the next
// candidate instead of leaving the resource stranded.
func (r *Runtime) runNext(routeID string) {
	for {
		before := r.Routes.Queue(routeID)
		reservation, ok := r.Routes.PromoteIdle(routeID, r.coreCandidateValid)
		if !ok {
			r.syncRouteQueue(routeID)
			return
		}
		// PromoteIdle drops expired applications without telling us; cancel
		// the runs that vanished from the queue and were not promoted.
		r.cancelSkipped(before, routeID, reservation.RunID)
		if valid, reason := r.fullCandidateValid(reservation); !valid {
			r.recordIncident(reservation.RunID, "warning", "skip stale route application: "+reason)
			r.Routes.Release(routeID, reservation.RunID)
			_ = r.Runs.Cancel(reservation.RunID)
			continue
		}
		r.syncRouteQueue(routeID)
		request := LaunchRequest{
			StationID:      reservation.StationID,
			RouteID:        reservation.Route,
			TargetPressure: reservation.TargetPressure,
		}
		current, _ := r.Runs.Get(reservation.RunID)
		lease, _ := r.Routes.Owner(routeID)
		if _, err := r.startRun(current, request, lease, context.Background()); err != nil {
			r.recordIncident(reservation.RunID, "warning", "queued run could not start: "+err.Error())
			r.Routes.Release(routeID, reservation.RunID)
			_ = r.Runs.Cancel(reservation.RunID)
			continue
		}
		return
	}
}

// cancelSkipped cancels runs whose applications were dropped during a
// promotion (for example because they had expired); promotedRunID is the
// reservation that acquired the lease and must be left untouched.
func (r *Runtime) cancelSkipped(before []model.RouteReservation, routeID, promotedRunID string) {
	after := r.Routes.Queue(routeID)
	surviving := make(map[string]bool, len(after)+1)
	surviving[promotedRunID] = true
	for _, reservation := range after {
		surviving[reservation.RunID] = true
	}
	for _, reservation := range before {
		if surviving[reservation.RunID] {
			continue
		}
		if current, ok := r.Runs.Get(reservation.RunID); ok && !current.State.Terminal() {
			_ = r.Runs.Cancel(reservation.RunID)
			r.recordIncident(reservation.RunID, "warning", "route application was dropped during promotion")
		}
	}
}

// syncRouteQueue mirrors the coordinator queue into the control snapshot.
func (r *Runtime) syncRouteQueue(routeID string) {
	waiting := r.Routes.Queue(routeID)
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.control.Routes[routeID]
	if !ok {
		return
	}
	state.Queue = waiting
	state.UpdatedAt = time.Now().UTC()
	r.control.Routes[routeID] = state
	r.control.Revision = model.NewRevision()
}

// queuedRunCount reports how many applications are currently waiting across
// all routes.
func (r *Runtime) queuedRunCount() int {
	control := r.ensureControl()
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, state := range control.Routes {
		total += len(state.Queue)
	}
	return total
}
