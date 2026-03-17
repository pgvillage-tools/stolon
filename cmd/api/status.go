package main

import (
	"context"
	"net/http"
	"time"

	"github.com/pgvillage-tools/stolon/internal/logging"
)

func (h *Handlers) StatusRoutes() []Route {
	return []Route{
		{"GET /cluster/status", h.ClusterStatusHandler},
		{"GET /proxy/status", h.ProxyStatusHandler},
		{"GET /sentinel/status", h.SentinelStatusHandler},
	}
}

// ClusterStatusHandler endpoint
func (h *Handlers) ClusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx, logger := logging.GetLogComponent(context.Background(), logging.WebApiComponent)
	ctx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(time.Second))
	defer cancelFunc()

	if cd, _, err := h.client.GetClusterData(ctx); err != nil {
		logger.Error().AnErr("error", err).Msg("failed to get cluster data")
		if _, err := w.Write([]byte("ERROR")); err != nil {
			logger.Error().AnErr("error", err).Msg("unable to return cluster data")
		}
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		h.writeJSON(ctx, w, cd)
	}
}

// ProxyStatusHandler endpoint
func (h *Handlers) ProxyStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx, logger := logging.GetLogComponent(context.Background(), logging.WebApiComponent)
	ctx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(time.Second))
	defer cancelFunc()

	if proxiesInfo, err := h.client.GetProxiesInfo(ctx); err != nil {
		logger.Error().AnErr("error", err).Msg("failed to get cluster data")
		if _, err := w.Write([]byte("ERROR")); err != nil {
			logger.Error().AnErr("error", err).Msg("unable to return cluster data")
		}
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		h.writeJSON(ctx, w, proxiesInfo)
	}
}

// SentinelStatusHandler endpoint
func (h *Handlers) SentinelStatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx, logger := logging.GetLogComponent(context.Background(), logging.WebApiComponent)
	ctx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(time.Second))
	defer cancelFunc()

	if sentinelsInfo, err := h.client.GetSentinelsInfo(ctx); err != nil {
		logger.Error().AnErr("error", err).Msg("failed to get cluster data")
		if _, err := w.Write([]byte("ERROR")); err != nil {
			logger.Error().AnErr("error", err).Msg("unable to return cluster data")
		}
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		h.writeJSON(ctx, w, sentinelsInfo)
	}
}
