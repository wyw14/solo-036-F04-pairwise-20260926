package route

import (
	"github.com/wyw14/solo-036/internal/model"
	"sync"
)

type Coordinator struct {
	mu       sync.Mutex
	owners   map[string]model.RouteLease
	progress map[string]float64
	queues   map[string][]model.RouteQueueEntry
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		owners:   make(map[string]model.RouteLease),
		progress: make(map[string]float64),
		queues:   make(map[string][]model.RouteQueueEntry),
	}
}
