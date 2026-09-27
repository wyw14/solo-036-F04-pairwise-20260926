package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

const testRouteID = "main-station-a-station-c"

func launchOccupant(t *testing.T, rt *Runtime) model.OperationRecord {
	t.Helper()
	operation, err := rt.Launch(context.Background(), LaunchRequest{
		StationID:      "station-a",
		RouteID:        testRouteID,
		TargetPressure: 10,
	})
	if err != nil {
		t.Fatalf("launch occupant: %v", err)
	}
	return operation
}

func driveToDepressurizing(t *testing.T, rt *Runtime, runID string) {
	t.Helper()
	for _, state := range []model.RunState{
		model.StateTracking,
		model.StateReceiving,
		model.StateDepressurizing,
	} {
		if err := rt.Runs.Transition(runID, state); err != nil {
			t.Fatalf("transition %s to %s: %v", runID, state, err)
		}
	}
}

func TestLaunchConflictHintsAtQueue(t *testing.T) {
	rt := NewRuntime()
	launchOccupant(t, rt)
	_, err := rt.Launch(context.Background(), LaunchRequest{
		StationID:      "station-a",
		RouteID:        testRouteID,
		TargetPressure: 10,
	})
	if err == nil || !strings.Contains(err.Error(), "/api/routes/"+testRouteID+"/queue") {
		t.Fatalf("expected conflict hinting at the queue endpoint, got %v", err)
	}
}

func TestQueueOrderWithdrawAndHandoff(t *testing.T) {
	rt := NewRuntime()
	occupant := launchOccupant(t, rt)

	first, err := rt.EnqueueRoute(testRouteID, QueueRequest{StationID: "station-a", TargetPressure: 10})
	if err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if first.Entry.State != model.QueueWaiting || first.Position != 1 {
		t.Fatalf("first application should wait at position 1, got %+v", first)
	}
	second, err := rt.EnqueueRoute(testRouteID, QueueRequest{StationID: "station-a", TargetPressure: 9.5})
	if err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	if second.Position != 2 {
		t.Fatalf("second application should wait at position 2, got %+v", second)
	}

	view, err := rt.RouteQueue(testRouteID)
	if err != nil {
		t.Fatalf("route queue: %v", err)
	}
	if view.OwnerRunID != occupant.Run.ID {
		t.Fatalf("expected owner %s, got %s", occupant.Run.ID, view.OwnerRunID)
	}
	if len(view.Queue) != 2 || view.Queue[0].Entry.ID != first.Entry.ID || view.Queue[1].Entry.ID != second.Entry.ID {
		t.Fatalf("expected turn order first,second; got %+v", view.Queue)
	}

	withdrawn, err := rt.WithdrawQueueEntry(testRouteID, second.Entry.ID)
	if err != nil {
		t.Fatalf("withdraw second: %v", err)
	}
	if withdrawn.State != model.QueueWithdrawn {
		t.Fatalf("expected withdrawn state, got %s", withdrawn.State)
	}
	if run, _ := rt.Runs.Get(second.Entry.RunID); run.State != model.StateCancelled {
		t.Fatalf("withdrawn run should be cancelled, got %s", run.State)
	}
	if _, err := rt.WithdrawQueueEntry(testRouteID, second.Entry.ID); err == nil {
		t.Fatal("withdrawing a decided application must fail")
	}

	driveToDepressurizing(t, rt, occupant.Run.ID)
	if _, err := rt.Complete(occupant.Run.ID, testRouteID); err != nil {
		t.Fatalf("complete occupant: %v", err)
	}

	entry, ok := rt.Routes.QueueEntry(first.Entry.ID)
	if !ok || entry.State != model.QueueGranted {
		t.Fatalf("first application should be granted, got %+v", entry)
	}
	if run, _ := rt.Runs.Get(first.Entry.RunID); run.State != model.StateLaunched {
		t.Fatalf("granted run should be launched, got %s", run.State)
	}
	if owner, _ := rt.Routes.Owner(testRouteID); owner != first.Entry.RunID {
		t.Fatalf("route should be leased to %s, got %s", first.Entry.RunID, owner)
	}
	view, err = rt.RouteQueue(testRouteID)
	if err != nil {
		t.Fatalf("route queue after handoff: %v", err)
	}
	if len(view.Queue) != 0 {
		t.Fatalf("queue should be drained after handoff, got %+v", view.Queue)
	}
}

func TestEnqueueOnFreeRouteGrantsImmediately(t *testing.T) {
	rt := NewRuntime()
	application, err := rt.EnqueueRoute(testRouteID, QueueRequest{StationID: "station-a", TargetPressure: 10})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if application.Entry.State != model.QueueGranted || application.Position != 0 {
		t.Fatalf("application on a free route should be granted, got %+v", application)
	}
	if owner, _ := rt.Routes.Owner(testRouteID); owner != application.Entry.RunID {
		t.Fatalf("route should be leased to %s, got %s", application.Entry.RunID, owner)
	}
}

func TestPromotionSkipsExpiredApplication(t *testing.T) {
	rt := NewRuntime()
	occupant := launchOccupant(t, rt)

	expiredRun, err := rt.CreateOperation("station-a")
	if err != nil {
		t.Fatalf("create expired run: %v", err)
	}
	now := time.Now().UTC()
	rt.Routes.Enqueue(model.RouteQueueEntry{
		ID:             model.NewRevision(),
		RouteID:        testRouteID,
		RunID:          expiredRun.ID,
		StationID:      "station-a",
		TargetPressure: 10,
		State:          model.QueueWaiting,
		EnqueuedAt:     now.Add(-2 * time.Hour),
		ExpiresAt:      now.Add(-time.Hour),
	})
	expiredID := ""
	if head, ok := rt.Routes.NextWaiting(testRouteID); ok {
		expiredID = head.ID
	}

	viable, err := rt.EnqueueRoute(testRouteID, QueueRequest{StationID: "station-a", TargetPressure: 10})
	if err != nil {
		t.Fatalf("enqueue viable: %v", err)
	}
	if viable.Position != 1 {
		t.Fatalf("expired head must not count toward the expected order, got %+v", viable)
	}

	if err := rt.Cancel(occupant.Run.ID); err != nil {
		t.Fatalf("cancel occupant: %v", err)
	}

	entry, _ := rt.Routes.QueueEntry(expiredID)
	if entry.State != model.QueueExpired {
		t.Fatalf("expired application must be decided expired, got %+v", entry)
	}
	if run, _ := rt.Runs.Get(expiredRun.ID); run.State != model.StateCancelled {
		t.Fatalf("expired run should be cancelled, got %s", run.State)
	}
	entry, _ = rt.Routes.QueueEntry(viable.Entry.ID)
	if entry.State != model.QueueGranted {
		t.Fatalf("viable application should be granted, got %+v", entry)
	}
	if owner, _ := rt.Routes.Owner(testRouteID); owner != viable.Entry.RunID {
		t.Fatalf("route should be leased to %s, got %s", viable.Entry.RunID, owner)
	}
}

func TestPromotionSkipsChangedConditions(t *testing.T) {
	rt := NewRuntime()
	occupant := launchOccupant(t, rt)

	application, err := rt.EnqueueRoute(testRouteID, QueueRequest{StationID: "station-a", TargetPressure: 10})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Conditions change while the application waits: the launcher is disabled.
	rt.mu.Lock()
	station := rt.control.Stations["station-a"]
	station.Enabled = false
	rt.control.Stations["station-a"] = station
	rt.mu.Unlock()

	if err := rt.Cancel(occupant.Run.ID); err != nil {
		t.Fatalf("cancel occupant: %v", err)
	}

	entry, _ := rt.Routes.QueueEntry(application.Entry.ID)
	if entry.State != model.QueueInvalid {
		t.Fatalf("application with changed conditions must be invalid, got %+v", entry)
	}
	if _, owned := rt.Routes.Owner(testRouteID); owned {
		t.Fatal("route must stay free when no valid candidate remains")
	}
	if run, _ := rt.Runs.Get(application.Entry.RunID); run.State != model.StateCancelled {
		t.Fatalf("rejected run should be cancelled, got %s", run.State)
	}
	incidents := rt.Snapshot().Incidents
	if len(incidents) == 0 {
		t.Fatal("expected an incident explaining the rejected handoff")
	}
}

func TestEnqueueRejectsInvalidApplication(t *testing.T) {
	rt := NewRuntime()
	if _, err := rt.EnqueueRoute("no-such-route", QueueRequest{StationID: "station-a", TargetPressure: 10}); err == nil {
		t.Fatal("unknown route must be rejected")
	}
	if _, err := rt.EnqueueRoute(testRouteID, QueueRequest{StationID: "station-c", TargetPressure: 10}); err == nil {
		t.Fatal("non-launcher station must be rejected")
	}
	if _, err := rt.EnqueueRoute(testRouteID, QueueRequest{StationID: "station-a", TargetPressure: 99}); err == nil {
		t.Fatal("over-limit pressure must be rejected")
	}
}
