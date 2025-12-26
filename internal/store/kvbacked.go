// Copyright 2018 Sorint.lab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	etcdclientv3 "github.com/coreos/etcd/clientv3"
	"github.com/docker/leadership"
	"github.com/docker/libkv"
	libkvstore "github.com/docker/libkv/store"
	"github.com/sorintlab/stolon/internal/cluster"
	"github.com/sorintlab/stolon/internal/common"
)

// BackendType represents a type of KV Store BackendType
type BackendType string

const (
	// CONSUL means that consul is used as backend
	CONSUL BackendType = "consul"
	// ETCDV2 means that etcd is used as backend and that the v2 api is used
	ETCDV2 BackendType = "etcdv2"
	// ETCDV3 means that etcd is used as backend and that the v3 api is used
	ETCDV3 BackendType = "etcdv3"
)

const (
	keepersInfoDir   = "/keepers/info/"
	clusterDataFile  = "clusterdata"
	sentinelsInfoDir = "/sentinels/info/"
	proxiesInfoDir   = "/proxies/info/"
)

const (
	// DefaultEtcdEndpoints defines the default endpoints when using etcd
	DefaultEtcdEndpoints = "http://127.0.0.1:2379"
	// DefaultConsulEndpoints defines the default endpoints when using consul
	DefaultConsulEndpoints = "http://127.0.0.1:8500"
)

// TODO(sgotti) fix this in libkv?
// consul min ttl is 10s and libkv divides this by 2
const minTTL = 20 * time.Second

var urlSchemeRegexp = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9+-.]*)://`)

// Config defines the config to be used for the endpoint
type Config struct {
	Backend       BackendType
	Endpoints     string
	Timeout       time.Duration
	BasePath      string
	CertFile      string
	KeyFile       string
	CAFile        string
	SkipTLSVerify bool
}

// KVPair represents {Key, Value, Lastindex} tuple
type KVPair struct {
	Key       string
	Value     []byte
	LastIndex uint64
}

// WriteOptions defines options to be used when writing to the backend
type WriteOptions struct {
	TTL time.Duration
}

// KVStore is an interface representing a backend (e.a. etcd, k8s en Consul)
type KVStore interface {
	// Put a value at the specified key
	Put(ctx context.Context, key string, value []byte, options *WriteOptions) error

	// Get a value given its key
	Get(ctx context.Context, key string) (*KVPair, error)

	// List the content of a given prefix
	List(ctx context.Context, directory string) ([]*KVPair, error)

	// Atomic CAS operation on a single value.
	// Pass previous = nil to create a new key.
	AtomicPut(ctx context.Context, key string, value []byte, previous *KVPair, options *WriteOptions) (*KVPair, error)

	Delete(ctx context.Context, key string) error

	// Close the store connection
	Close() error
}

// NewKVStore retruns a freshly initialized KVStore
func NewKVStore(cfg Config) (KVStore, error) {
	var kvBackend libkvstore.Backend
	switch cfg.Backend {
	case CONSUL:
		kvBackend = libkvstore.CONSUL
	case ETCDV2:
		kvBackend = libkvstore.ETCD
	case ETCDV3:
	default:
		return nil, fmt.Errorf("Unknown store backend: %q", cfg.Backend)
	}

	endpointsStr := cfg.Endpoints
	if endpointsStr == "" {
		switch cfg.Backend {
		case CONSUL:
			endpointsStr = DefaultConsulEndpoints
		case ETCDV2, ETCDV3:
			endpointsStr = DefaultEtcdEndpoints
		}
	}
	endpoints := strings.Split(endpointsStr, ",")

	// 1) since libkv wants endpoints as a list of IP and not URLs but we
	// want to also support them then parse and strip them
	// 2) since libkv will enable TLS for all endpoints when config.TLS
	// isn't nil we have to check that all the endpoints have the same
	// scheme
	addrs := []string{}
	var scheme string
	for _, e := range endpoints {
		var curscheme, addr string
		if urlSchemeRegexp.Match([]byte(e)) {
			u, err := url.Parse(e)
			if err != nil {
				return nil, fmt.Errorf("cannot parse endpoint %q: %v", e, err)
			}
			curscheme = u.Scheme
			addr = u.Host
		} else {
			// Assume it's a schemeless endpoint
			curscheme = "http"
			addr = e
		}
		if scheme == "" {
			scheme = curscheme
		}
		if scheme != curscheme {
			return nil, fmt.Errorf("all the endpoints must have the same scheme")
		}
		addrs = append(addrs, addr)
	}

	var tlsConfig *tls.Config
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("endpoints scheme must be http or https")
	}
	if scheme == "https" {
		var err error
		tlsConfig, err = common.NewTLSConfig(cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.SkipTLSVerify)
		if err != nil {
			return nil, fmt.Errorf("cannot create store tls config: %v", err)
		}
	}

	switch cfg.Backend {
	case CONSUL, ETCDV2:
		config := &libkvstore.Config{
			TLS:               tlsConfig,
			ConnectionTimeout: cfg.Timeout,
		}

		store, err := libkv.NewStore(kvBackend, addrs, config)
		if err != nil {
			return nil, err
		}
		return &libKVStore{store: store}, nil
	case ETCDV3:
		config := etcdclientv3.Config{
			Endpoints:            addrs,
			TLS:                  tlsConfig,
			DialTimeout:          20 * time.Second,
			DialKeepAliveTime:    1 * time.Second,
			DialKeepAliveTimeout: cfg.Timeout,
		}

		c, err := etcdclientv3.New(config)
		if err != nil {
			return nil, err
		}
		return &etcdV3Store{c: c, requestTimeout: cfg.Timeout}, nil
	default:
		return nil, fmt.Errorf("Unknown store backend: %q", cfg.Backend)
	}
}

// KVBackedStore defines a config store backed by a KV backend
type KVBackedStore struct {
	clusterPath string
	store       KVStore
}

// NewKVBackedStore returns a freshly initialized KVBackedStore
func NewKVBackedStore(kvStore KVStore, path string) *KVBackedStore {
	return &KVBackedStore{
		clusterPath: path,
		store:       kvStore,
	}
}

// AtomicPutClusterData is an atomic way to store CLusterData to a kv store
func (s *KVBackedStore) AtomicPutClusterData(ctx context.Context, cd *cluster.Data, previous *KVPair) (*KVPair, error) {
	cdj, err := json.Marshal(cd)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(s.clusterPath, clusterDataFile)
	// Skip prev Value since LastIndex is enough for a CAS and it gives
	// problem with etcd v2 api with big prev values.
	var prev *KVPair
	if previous != nil {
		prev = &KVPair{
			Key:       previous.Key,
			LastIndex: previous.LastIndex,
		}
	}
	return s.store.AtomicPut(ctx, path, cdj, prev, nil)
}

// PutClusterData stores ClusterData to a kv store
func (s *KVBackedStore) PutClusterData(ctx context.Context, cd *cluster.Data) error {
	cdj, err := json.Marshal(cd)
	if err != nil {
		return err
	}
	path := filepath.Join(s.clusterPath, clusterDataFile)
	return s.store.Put(ctx, path, cdj, nil)
}

// GetClusterData retrieves ClusterData from a kv store
func (s *KVBackedStore) GetClusterData(ctx context.Context) (*cluster.Data, *KVPair, error) {
	var cd *cluster.Data
	path := filepath.Join(s.clusterPath, clusterDataFile)
	pair, err := s.store.Get(ctx, path)
	if err != nil {
		if err != ErrKeyNotFound {
			return nil, nil, err
		}
		return nil, nil, nil
	}
	if err := json.Unmarshal(pair.Value, &cd); err != nil {
		return nil, nil, err
	}
	return cd, pair, nil
}

// SetKeeperInfo stores keeper info to a kv store
func (s *KVBackedStore) SetKeeperInfo(ctx context.Context, id string, ms *cluster.KeeperInfo, ttl time.Duration) error {
	msj, err := json.Marshal(ms)
	if err != nil {
		return err
	}
	if ttl < minTTL {
		ttl = minTTL
	}
	return s.store.Put(ctx, filepath.Join(s.clusterPath, keepersInfoDir, id), msj, &WriteOptions{TTL: ttl})
}

// GetKeepersInfo retrieves all keeper info from a kv store
func (s *KVBackedStore) GetKeepersInfo(ctx context.Context) (cluster.KeepersInfo, error) {
	keepers := cluster.KeepersInfo{}
	pairs, err := s.store.List(ctx, filepath.Join(s.clusterPath, keepersInfoDir))
	if err != nil {
		if err != ErrKeyNotFound {
			return nil, err
		}
		return keepers, nil
	}
	for _, pair := range pairs {
		var ki cluster.KeeperInfo
		err = json.Unmarshal(pair.Value, &ki)
		if err != nil {
			return nil, err
		}
		keepers[ki.UID] = &ki
	}
	return keepers, nil
}

// SetSentinelInfo stores info on a sentinel to the kv store
func (s *KVBackedStore) SetSentinelInfo(ctx context.Context, si *cluster.SentinelInfo, ttl time.Duration) error {
	sij, err := json.Marshal(si)
	if err != nil {
		return err
	}
	if ttl < minTTL {
		ttl = minTTL
	}
	return s.store.Put(ctx, filepath.Join(s.clusterPath, sentinelsInfoDir, si.UID), sij, &WriteOptions{TTL: ttl})
}

// GetSentinelsInfo retrieves all sentinel info from a kv store
func (s *KVBackedStore) GetSentinelsInfo(ctx context.Context) (cluster.SentinelsInfo, error) {
	ssi := cluster.SentinelsInfo{}
	pairs, err := s.store.List(ctx, filepath.Join(s.clusterPath, sentinelsInfoDir))
	if err != nil {
		if err != ErrKeyNotFound {
			return nil, err
		}
		return ssi, nil
	}
	for _, pair := range pairs {
		var si cluster.SentinelInfo
		err = json.Unmarshal(pair.Value, &si)
		if err != nil {
			return nil, err
		}
		ssi = append(ssi, &si)
	}
	return ssi, nil
}

// SetProxyInfo stores info on a proxy to the kv store
func (s *KVBackedStore) SetProxyInfo(ctx context.Context, pi *cluster.ProxyInfo, ttl time.Duration) error {
	pij, err := json.Marshal(pi)
	if err != nil {
		return err
	}
	if ttl < minTTL {
		ttl = minTTL
	}
	return s.store.Put(ctx, filepath.Join(s.clusterPath, proxiesInfoDir, pi.UID), pij, &WriteOptions{TTL: ttl})
}

// GetProxiesInfo retrieves all proxy info from a kv store
func (s *KVBackedStore) GetProxiesInfo(ctx context.Context) (cluster.ProxiesInfo, error) {
	psi := cluster.ProxiesInfo{}
	pairs, err := s.store.List(ctx, filepath.Join(s.clusterPath, proxiesInfoDir))
	if err != nil {
		if err != ErrKeyNotFound {
			return nil, err
		}
		return psi, nil
	}
	for _, pair := range pairs {
		var pi cluster.ProxyInfo
		err = json.Unmarshal(pair.Value, &pi)
		if err != nil {
			return nil, err
		}
		psi[pi.UID] = &pi
	}
	return psi, nil
}

// NewKVBackedElection starts a campaign for getting elected with a kv backed store
func NewKVBackedElection(kvStore KVStore, path, candidateUID string, timeout time.Duration) Election {
	switch kvStore := kvStore.(type) {
	case *libKVStore:
		s := kvStore
		candidate := leadership.NewCandidate(s.store, path, candidateUID, minTTL)
		return &libkvElection{store: s, path: path, candidate: candidate}
	case *etcdV3Store:
		etcdV3Store := kvStore
		return &etcdv3Election{
			c:              etcdV3Store.c,
			path:           path,
			candidateUID:   candidateUID,
			ttl:            minTTL,
			requestTimeout: timeout,
		}
	default:
		panic("unknown kvstore")
	}
}
