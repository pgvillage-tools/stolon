package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	v1 "github.com/pgvillage-tools/stolon/api/v1"
	cmdcommon "github.com/pgvillage-tools/stolon/cmd"
	"github.com/pgvillage-tools/stolon/internal/logging"
	"github.com/pgvillage-tools/stolon/internal/store"
)

func (h *Handlers) StatusRoutes() []Route {
	return []Route{
		{"GET /status", h.StatusHandler},
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

func (h *Handlers) sentinelInfo(ctx context.Context) (is v1.InfoSentinels, err error) {
	ctx, logger := logging.GetLogComponent(ctx, logging.WebApiComponent)
	var ssi v1.SentinelsInfo
	election, err := cmdcommon.NewElection(ctx, &cfg.CommonConfig, "")
	if err != nil {
		logger.Error().AnErr("error", err).Msg("failed to get election info")
		return nil, err
	}
	lsid, err := election.Leader()
	if err != nil && err != store.ErrElectionNoLeader {
		logger.Error().AnErr("error", err).Msg("failed to get leader info")
		return nil, err
	}
	if ssi, err = h.client.GetSentinelsInfo(ctx); err != nil {
		logger.Error().AnErr("error", err).Msg("failed to get sentinels info")
		return nil, err
	}
	for _, si := range ssi {
		leader := lsid != "" && si.UID == lsid
		is = append(is, v1.InfoSentinel{
			UID:    si.UID,
			Leader: leader,
		})
	}
	return is, nil
}

func (h *Handlers) proxyInfo(ctx context.Context) (ip v1.InfoProxies, err error) {
	ctx, logger := logging.GetLogComponent(ctx, logging.WebApiComponent)
	proxiesInfo, err := h.client.GetProxiesInfo(context.TODO())
	if err != nil {
		logger.Error().AnErr("error", err).Msg("failed to get proxies info")
		return nil, err
	}
	proxiesInfoSlice := proxiesInfo.ToSlice()

	sort.Sort(proxiesInfoSlice)
	for _, pi := range proxiesInfoSlice {
		ip = append(ip, v1.InfoProxy{UID: pi.UID, Generation: pi.Generation})
	}
	return ip, nil
}

func (h *Handlers) keepersInfo(ctx context.Context, cd *v1.Data) (ik v1.InfoKeepers, err error) {
	kssKeys := cd.Keepers.SortedKeys()
	for _, kuid := range kssKeys {
		k := cd.Keepers[kuid]
		db := cd.FindDB(k)
		dbListenAddress := "(no db assigned)"
		var (
			pgHealthy           bool
			pgCurrentGeneration int64
			pgWantedGeneration  int64
		)
		if db != nil {
			pgHealthy = db.Status.Healthy
			pgCurrentGeneration = db.Status.CurrentGeneration
			pgWantedGeneration = db.Generation

			dbListenAddress = "(unknown)"
			if db.Status.ListenAddress != "" {
				dbListenAddress = fmt.Sprintf("%s:%s", db.Status.ListenAddress, db.Status.Port)
			}
		}
		ik = append(ik, v1.InfoKeeper{
			UID:                 kuid,
			ListenAddress:       dbListenAddress,
			Healthy:             k.Status.Healthy,
			PgHealthy:           pgHealthy,
			PgWantedGeneration:  pgWantedGeneration,
			PgCurrentGeneration: pgCurrentGeneration,
		})
	}
	return ik, err
}

func (h *Handlers) clusterInfo(ctx context.Context) (v1.InfoCluster, error) {
	var (
		cd  *v1.Data
		p   v1.InfoProxies
		s   v1.InfoSentinels
		k   v1.InfoKeepers
		err error
	)
	cd, _, err = h.client.GetClusterData(ctx)
	k, err = h.keepersInfo(ctx, cd)
	if err != nil {
		return v1.InfoCluster{}, err
	}
	p, err = h.proxyInfo(ctx)
	if err != nil {
		return v1.InfoCluster{}, err
	}
	s, err = h.sentinelInfo(ctx)

	var clusterStatus v1.InfoGenericStatus

	if cd.Cluster == nil || cd.DBs == nil {
		clusterStatus = v1.InfoGenericStatus{"available": false}
	} else if cd.Cluster.Status.Master == "" {
		clusterStatus = v1.InfoGenericStatus{"available": true}
	} else {
		primary := cd.Cluster.Status.Master
		primaryKeeper := cd.DBs[primary].Spec.KeeperUID
		clusterStatus = v1.InfoGenericStatus{
			"available":     true,
			"primaryDB":     cd.DBs[primary].UID,
			"primaryKeeper": cd.Keepers[primaryKeeper].UID,
		}
	}

	return v1.InfoCluster{
		Keepers:   k,
		Proxies:   p,
		Sentinels: s,
		Status:    clusterStatus,
	}, nil
}

func (h *Handlers) StatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx, logger := logging.GetLogComponent(context.Background(), logging.WebApiComponent)
	ctx, cancelFunc := context.WithDeadline(ctx, time.Now().Add(time.Second))
	defer cancelFunc()

	ic, err := h.clusterInfo(ctx)
	if err != nil {
		logger.Error().AnErr("error", err).Msg("")
		if _, err := w.Write([]byte("ERROR")); err != nil {
			logger.Error().AnErr("error", err).Msg("unable to return ERROR message")
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.writeJSON(ctx, w, ic)

}
