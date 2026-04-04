package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/pgvillage-tools/stolon/internal/logging"
	"github.com/pgvillage-tools/stolon/internal/store"
)

type Route struct {
	Pattern string
	Handler http.HandlerFunc
}

type Handlers struct {
	client store.Store
	Ready  *atomic.Bool
	nextID int
}

func NewHandlers(client store.Store, ready *atomic.Bool) *Handlers {
	return &Handlers{
		client: client,
		Ready:  ready,
		nextID: 1,
	}
}

func (h *Handlers) Routes() []Route {
	var routes []Route
	routes = append(routes, h.HealthRoutes()...)
	routes = append(routes, h.StatusRoutes()...)
	routes = append(routes, h.UpdateRoutes()...)
	return routes
}

// Helper method
func (h *Handlers) writeJSON(ctx context.Context, w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		_, logger := logging.GetLogComponent(ctx, logging.WebApiComponent)
		logger.Error().AnErr("err", err).Msg("error while writing json")
	}
}
