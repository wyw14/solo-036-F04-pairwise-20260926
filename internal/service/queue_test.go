package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/route"
	"github.com/wyw14/solo-036/internal/tracker"
)

const testRouteID = "main-station-a-station-c"

func testQueueRequest(pressure float64) model.QueueRequest {
	return model.QueueRequest{StationID: "station-a", RouteID: testRouteID, TargetPressure: pressure}
}

// finishRun drives a launched run through tracking, receiving and completion,
// releasing its route at the end.
func finishRun(t *testing.T, r *Runtime, runID string) {
	t.Helper()
	if _, err := r.Track(runID, TrackingRequest{
		StationID:  "station-b",
		SourceID:   "mag-1",
		Kind:       tracker.SourceMagnetic,
		Position:   90,
		ObservedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("track %s: %v", runID, err)
	}
	if _, err := r.Receive(runID, ReceiveRequest{StationID: "station-c", IsolationEpoch: 7}); err != nil {
		t.Fatalf("receive %s: %v", runID, err)
	}
	if _, err := r.Complete(runID, testRouteID); err != nil {
		t.Fatalf("complete %s: %v", runID, err)
	}
}

func enqueue(t *testing.T, r *Runtime, expiresAt time.Time) model.QueueEnrollment {
	t.Helper()
	enrollment, err := r.EnqueueRoute(context.Background(), testRouteID, testQueueRequest(8), expiresAt)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return enrollment
}

func TestQueueFIFOPromotionOnRelease(t *testing.T) {
	r := NewRuntime()

	a := enqueue(t, r, time.Time{})
	if a.Status != "started" {
		t.Fatalf("first application should start immediately, got %s", a.Status)
	}
	b := enqueue(t, r, time.Time{})
	c := enqueue(t, r, time.Time{})
	if b.Status != "queued" || b.Position != 1 || c.Status != "queued" || c.Position != 2 {
		t.Fatalf("expected B at position 1 and C at 2, got b=%+v c=%+v", b, c)
	}

	status, err := r.RouteQueue(testRouteID)
	if err != nil {
		t.Fatal(err)
	}
	if status.OwnerRunID != a.RunID || status.NextRunID != b.RunID || len(status.Waiting) != 2 {
		t.Fatalf("unexpected queue status: %+v", status)
	}
	for _, waiting := range status.Waiting {
		if !waiting.Valid {
			t.Fatalf("waiting run %s should be valid: %s", waiting.RunID, waiting.Reason)
		}
	}

	finishRun(t, r, a.RunID)

	status, _ = r.RouteQueue(testRouteID)
	if status.OwnerRunID != b.RunID {
		t.Fatalf("after release the route must pass to B, owner=%s", status.OwnerRunID)
	}
	if len(status.Waiting) != 1 || status.Waiting[0].RunID != c.RunID || status.NextRunID != c.RunID {
		t.Fatalf("C should be next, got %+v", status.Waiting)
	}
	runB, _ := r.Runs.Get(b.RunID)
	if runB.State != model.StateLaunched {
		t.Fatalf("B should be launched after promotion, got %s", runB.State)
	}

	// Cancelling the promoted owner hands the route further down the line.
	if err := r.Cancel(b.RunID); err != nil {
		t.Fatal(err)
	}
	status, _ = r.RouteQueue(testRouteID)
	if status.OwnerRunID != c.RunID {
		t.Fatalf("after cancelling B the route must pass to C, owner=%s", status.OwnerRunID)
	}
	runC, _ := r.Runs.Get(c.RunID)
	if runC.State != model.StateLaunched {
		t.Fatalf("C should be launched, got %s", runC.State)
	}
}

func TestPromotionSkipsCancelledApplication(t *testing.T) {
	r := NewRuntime()
	a := enqueue(t, r, time.Time{})
	b := enqueue(t, r, time.Time{})
	c := enqueue(t, r, time.Time{})

	// B's run ends without going through the queue withdrawal: its
	// application is stale and must not occupy the route on release.
	if err := r.Runs.Cancel(b.RunID); err != nil {
		t.Fatal(err)
	}
	status, _ := r.RouteQueue(testRouteID)
	var bEntry *model.QueuedRun
	for i := range status.Waiting {
		if status.Waiting[i].RunID == b.RunID {
			bEntry = &status.Waiting[i]
		}
	}
	if bEntry == nil || bEntry.Valid || bEntry.Reason == "" {
		t.Fatalf("B should be reported invalid, got %+v", bEntry)
	}

	finishRun(t, r, a.RunID)

	status, _ = r.RouteQueue(testRouteID)
	if status.OwnerRunID != c.RunID {
		t.Fatalf("stale B must be skipped in favour of C, owner=%s", status.OwnerRunID)
	}
	for _, waiting := range status.Waiting {
		if waiting.RunID == b.RunID {
			t.Fatal("stale application must be removed from the queue")
		}
	}
}

func TestPromotionSkipsExpiredApplication(t *testing.T) {
	r := NewRuntime()
	a := enqueue(t, r, time.Time{})
	b := enqueue(t, r, time.Now().UTC().Add(-time.Minute))
	c := enqueue(t, r, time.Time{})

	status, _ := r.RouteQueue(testRouteID)
	if status.NextRunID != c.RunID {
		t.Fatalf("expired B cannot be next candidate, next=%s", status.NextRunID)
	}

	finishRun(t, r, a.RunID)

	status, _ = r.RouteQueue(testRouteID)
	if status.OwnerRunID != c.RunID {
		t.Fatalf("expired application must never own the route, owner=%s", status.OwnerRunID)
	}
	runB, _ := r.Runs.Get(b.RunID)
	if !runB.State.Terminal() {
		t.Fatalf("skipped expired run should be cancelled, got %s", runB.State)
	}
}

func TestWithdrawRoute(t *testing.T) {
	r := NewRuntime()
	a := enqueue(t, r, time.Time{})
	b := enqueue(t, r, time.Time{})

	if err := r.WithdrawRoute(testRouteID, b.RunID); err != nil {
		t.Fatalf("withdraw B: %v", err)
	}
	runB, _ := r.Runs.Get(b.RunID)
	if runB.State != model.StateCancelled {
		t.Fatalf("withdrawn run should be cancelled, got %s", runB.State)
	}
	if err := r.WithdrawRoute(testRouteID, b.RunID); err == nil {
		t.Fatal("withdrawing again must fail")
	}
	if err := r.WithdrawRoute(testRouteID, a.RunID); !errors.Is(err, route.ErrRouteBusy) {
		t.Fatalf("withdrawing the owner must return ErrRouteBusy, got %v", err)
	}

	status, _ := r.RouteQueue(testRouteID)
	if len(status.Waiting) != 0 {
		t.Fatalf("queue should be empty after withdrawal, got %+v", status.Waiting)
	}

	finishRun(t, r, a.RunID)
	status, _ = r.RouteQueue(testRouteID)
	if status.Reserved || status.OwnerRunID != "" {
		t.Fatalf("route should stay free with no candidates, got %+v", status)
	}
}

func TestDirectLaunchRejectsBusyRoute(t *testing.T) {
	r := NewRuntime()
	a := enqueue(t, r, time.Time{})
	if a.Status != "started" {
		t.Fatalf("first application should start, got %s", a.Status)
	}
	if _, err := r.Launch(context.Background(), LaunchRequest{
		StationID: "station-a", RouteID: testRouteID, TargetPressure: 8,
	}); !errors.Is(err, route.ErrRouteBusy) {
		t.Fatalf("second direct launch must fail with ErrRouteBusy, got %v", err)
	}
}
