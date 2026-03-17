package main

import (
	"context"
	"net/http"
	"time"

	"github.com/pgvillage-tools/stolon/internal/logging"
)

func (h *Handlers) HealthRoutes() []Route {
	return []Route{
		{"GET /healthz", h.HealthzHandler},
		{"GET /readyz", h.ReadyzHandler},
	}
}

// Health endpoints
func (h *Handlers) HealthzHandler(w http.ResponseWriter, r *http.Request) {
	ctx, logger := logging.GetLogComponent(context.Background(), logging.WebApiComponent)
	var (
		status = http.StatusOK
		msg    = []byte("OK")
	)
	ctx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(time.Second))
	defer cancelFunc()

	if err := h.client.Healthy(ctx); err != nil {
		logger.Error().AnErr("error", err).Msg("store is not healthy")
		status = http.StatusInternalServerError
		msg = []byte("ERROR")
	}
	w.WriteHeader(status)
	if _, err := w.Write(msg); err != nil {
		logger.Error().AnErr("error", err).Msg("unable to return healthz response")
	}
}

func (h *Handlers) ReadyzHandler(w http.ResponseWriter, r *http.Request) {
	_, logger := logging.GetLogComponent(context.Background(), logging.WebApiComponent)
	if !h.Ready.Load() {
		http.Error(w, "Not Ready", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("Ready"))

	if err != nil {
		logger.Error().AnErr("error", err).Msg("unable to return readyz response")
	}
}
