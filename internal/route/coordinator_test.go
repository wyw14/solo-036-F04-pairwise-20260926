package route

import (
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

func res(route, run string, epoch int64, expiresAt time.Time) model.RouteReservation {
	return model.RouteReservation{
		Route:          route,
		RunID:          run,
		Epoch:          epoch,
		StationID:      "station-a",
		TargetPressure: 8,
		QueuedAt:       time.Now().UTC(),
		ExpiresAt:      expiresAt,
	}
}

func alwaysValid(model.RouteReservation) (bool, string) { return true, "" }

func TestEnqueueFIFOAndPromotion(t *testing.T) {
	c := NewCoordinator()
	if _, err := c.Reserve("r1", "run-a", 1); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, acquired, pos, err := c.Enqueue(res("r1", "run-b", 1, time.Time{}), alwaysValid); err != nil || acquired || pos != 1 {
		t.Fatalf("enqueue b: acquired=%v pos=%d err=%v", acquired, pos, err)
	}
	if _, acquired, pos, err := c.Enqueue(res("r1", "run-c", 1, time.Time{}), alwaysValid); err != nil || acquired || pos != 2 {
		t.Fatalf("enqueue c: acquired=%v pos=%d err=%v", acquired, pos, err)
	}

	if !c.Release("r1", "run-a") {
		t.Fatal("release a failed")
	}
	promoted, ok := c.PromoteIdle("r1", alwaysValid)
	if !ok || promoted.RunID != "run-b" {
		t.Fatalf("expected run-b promoted, got %+v ok=%v", promoted, ok)
	}
	if owner, ok := c.Owner("r1"); !ok || owner.RunID != "run-b" {
		t.Fatalf("owner should be run-b, got %+v", owner)
	}
	queue := c.Queue("r1")
	if len(queue) != 1 || queue[0].RunID != "run-c" {
		t.Fatalf("run-c should remain waiting, got %+v", queue)
	}
}

func TestPromotionSkipsInvalidCandidates(t *testing.T) {
	c := NewCoordinator()
	if _, err := c.Reserve("r1", "run-a", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := c.Enqueue(res("r1", "run-stale", 1, time.Time{}), alwaysValid); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := c.Enqueue(res("r1", "run-good", 1, time.Time{}), alwaysValid); err != nil {
		t.Fatal(err)
	}
	c.Release("r1", "run-a")

	rejectStale := func(r model.RouteReservation) (bool, string) {
		if r.RunID == "run-stale" {
			return false, "conditions changed"
		}
		return true, ""
	}
	promoted, ok := c.PromoteIdle("r1", rejectStale)
	if !ok || promoted.RunID != "run-good" {
		t.Fatalf("expected run-good to overtake the stale entry, got %+v ok=%v", promoted, ok)
	}
	for _, waiting := range c.Queue("r1") {
		if waiting.RunID == "run-stale" {
			t.Fatal("rejected candidate must be dropped from the queue")
		}
	}
}

func TestExpiredReservationNeverAcquires(t *testing.T) {
	c := NewCoordinator()
	expired := res("r1", "run-old", 1, time.Now().UTC().Add(-time.Minute))
	// Free route: an expired application must not acquire immediately, but
	// it still lines up and is dropped when promotion is attempted.
	_, acquired, pos, err := c.Enqueue(expired, alwaysValid)
	if err != nil || acquired || pos != 1 {
		t.Fatalf("expired enqueue: acquired=%v pos=%d err=%v", acquired, pos, err)
	}
	if _, ok := c.PromoteIdle("r1", alwaysValid); ok {
		t.Fatal("expired application must not be promoted")
	}
	if _, owned := c.Owner("r1"); owned {
		t.Fatal("route must remain free after expired promotion")
	}
	if len(c.Queue("r1")) != 0 {
		t.Fatal("expired application must be dropped")
	}
}

func TestWithdrawAndWithdrawAll(t *testing.T) {
	c := NewCoordinator()
	c.Reserve("r1", "run-a", 1)
	c.Enqueue(res("r1", "run-b", 1, time.Time{}), alwaysValid)
	c.Enqueue(res("r1", "run-c", 1, time.Time{}), alwaysValid)
	if !c.Withdraw("r1", "run-b") {
		t.Fatal("withdraw should report true")
	}
	if c.Withdraw("r1", "run-b") {
		t.Fatal("second withdraw should report false")
	}
	queue := c.Queue("r1")
	if len(queue) != 1 || queue[0].RunID != "run-c" {
		t.Fatalf("queue after withdraw = %+v", queue)
	}

	c.Reserve("r2", "run-z", 1)
	c.Enqueue(res("r2", "run-c", 1, time.Time{}), alwaysValid)
	routes := c.WithdrawAll("run-c")
	if len(routes) != 2 {
		t.Fatalf("WithdrawAll should touch 2 routes, got %v", routes)
	}
	if len(c.Queue("r1")) != 0 || len(c.Queue("r2")) != 0 {
		t.Fatal("run-c should be removed from every queue")
	}
}

func TestFreeRouteAcquiresImmediately(t *testing.T) {
	c := NewCoordinator()
	lease, acquired, _, err := c.Enqueue(res("r1", "run-a", 1, time.Time{}), alwaysValid)
	if err != nil || !acquired {
		t.Fatalf("free route should be acquired immediately: lease=%+v err=%v", lease, err)
	}
	if owner, ok := c.Owner("r1"); !ok || owner.RunID != "run-a" {
		t.Fatalf("unexpected owner %+v", owner)
	}
}

func TestDuplicateEnqueueRejected(t *testing.T) {
	c := NewCoordinator()
	c.Reserve("r1", "run-a", 1)
	c.Enqueue(res("r1", "run-b", 1, time.Time{}), alwaysValid)
	if _, _, _, err := c.Enqueue(res("r1", "run-b", 1, time.Time{}), alwaysValid); err == nil {
		t.Fatal("duplicate enqueue must fail")
	}
}
