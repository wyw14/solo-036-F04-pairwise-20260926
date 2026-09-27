package route

import (
	"sync"

	"github.com/wyw14/solo-036/internal/model"
)

type Coordinator struct {
	mu       sync.Mutex
	owners   map[string]model.RouteLease
	progress map[string]float64
	waiters  map[string][]model.RouteReservation
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		owners:   make(map[string]model.RouteLease),
		progress: make(map[string]float64),
		waiters:  make(map[string][]model.RouteReservation),
	}
}
