package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/solo-036/internal/model"
	"github.com/wyw14/solo-036/internal/route"
)

type queueEnqueueRequest struct {
	StationID      string  `json:"station_id"`
	TargetPressure float64 `json:"target_pressure"`
	ExpiresAt      string  `json:"expires_at"`
}

func (h *Handler) Routes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.runtime.RouteQueues())
}

func (h *Handler) RouteQueue(w http.ResponseWriter, r *http.Request) {
	status, err := h.runtime.RouteQueue(chi.URLParam(r, "routeID"))
	if err != nil {
		writeError(w, r, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *Handler) EnqueueRoute(w http.ResponseWriter, r *http.Request) {
	routeID := chi.URLParam(r, "routeID")
	var request queueEnqueueRequest
	if err := decodeBody(w, r, &request); err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	var expiresAt time.Time
	if strings.TrimSpace(request.ExpiresAt) != "" {
		parsed, err := time.Parse(time.RFC3339Nano, request.ExpiresAt)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, errors.New("expires_at must use RFC3339 format"))
			return
		}
		expiresAt = parsed.UTC()
	}
	enrollment, err := h.runtime.EnqueueRoute(r.Context(), routeID, model.QueueRequest{
		StationID:      request.StationID,
		RouteID:        routeID,
		TargetPressure: request.TargetPressure,
	}, expiresAt)
	if err != nil {
		if errors.Is(err, route.ErrAlreadyQueued) {
			writeError(w, r, http.StatusConflict, err)
			return
		}
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	if enrollment.Status == "started" {
		writeJSON(w, http.StatusCreated, enrollment)
		return
	}
	writeJSON(w, http.StatusAccepted, enrollment)
}

func (h *Handler) WithdrawRoute(w http.ResponseWriter, r *http.Request) {
	routeID := chi.URLParam(r, "routeID")
	runID := chi.URLParam(r, "runID")
	if err := h.runtime.WithdrawRoute(routeID, runID); err != nil {
		if errors.Is(err, route.ErrRouteBusy) {
			writeError(w, r, http.StatusConflict, err)
			return
		}
		writeError(w, r, http.StatusNotFound, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
