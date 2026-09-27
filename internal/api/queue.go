package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/solo-036/internal/service"
)

type enqueueRequest struct {
	StationID        string  `json:"station_id"`
	TargetPressure   float64 `json:"target_pressure"`
	ExpiresInSeconds int64   `json:"expires_in_seconds"`
}

func (h *Handler) EnqueueRoute(w http.ResponseWriter, r *http.Request) {
	var request enqueueRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	application, err := h.runtime.EnqueueRoute(chi.URLParam(r, "routeID"), service.QueueRequest{
		StationID:        request.StationID,
		TargetPressure:   request.TargetPressure,
		ExpiresInSeconds: request.ExpiresInSeconds,
	})
	if err != nil {
		writeError(w, r, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusCreated, application)
}

func (h *Handler) RouteQueue(w http.ResponseWriter, r *http.Request) {
	view, err := h.runtime.RouteQueue(chi.URLParam(r, "routeID"))
	if err != nil {
		writeError(w, r, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (h *Handler) WithdrawQueueEntry(w http.ResponseWriter, r *http.Request) {
	entry, err := h.runtime.WithdrawQueueEntry(chi.URLParam(r, "routeID"), chi.URLParam(r, "entryID"))
	if err != nil {
		writeError(w, r, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, entry)
}
