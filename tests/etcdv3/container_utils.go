package etcdv3_test

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/etcd"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	etcdImage  = "quay.io/coreos/etcd:v3.6.7"
	proxyPort  = 25432
	keeperPort = 5432
	pgUser     = "postgres"
	pgDatabase = "postgres"
)

func runEtcd(
	ctx context.Context,
	etcdImage string,
	nw *testcontainers.DockerNetwork,
) (
	cnt *etcd.EtcdContainer,
	ep string,
	err error,
) {
	// setup etcd
	etcdContainer, startErr := etcd.Run(ctx,
		etcdImage,
		network.WithNetwork([]string{"etcd"}, nw),
	)
	if startErr != nil {
		return nil, "", startErr
	}
	ips, ipErr := etcdContainer.ContainerIPs(ctx)
	if ipErr != nil {
		return nil, "", ipErr
	}
	return etcdContainer, fmt.Sprintf("http://%s:2379", ips[0]), nil
}

func runStolonCtl(
	ctx context.Context,
	etcdEndpoints string,
	nw *testcontainers.DockerNetwork,
	command ...string,
) (testcontainers.Container, error) {
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: command,
				Env: map[string]string{
					"STOLONCTL_STORE_ENDPOINTS": etcdEndpoints,
					"STOLONCTL_LOG_LEVEL":       "debug",
				},
				Networks: []string{nw.Name},
				Image:    "stolonctl",
			},
			Started: true,
		})
}

func runKeeper(
	ctx context.Context,
	etcdEndpoints string,
	nw *testcontainers.DockerNetwork,
	aliasses map[string][]string,
	settings map[string]string,
) (testcontainers.Container, error) {
	pgVersion := os.Getenv("PGVERSION")
	if pgVersion == "" {
		pgVersion = "19"
	}
	envSettings := map[string]string{
		"STKEEPER_STORE_ENDPOINTS": etcdEndpoints,
		"STKEEPER_LOG_LEVEL":       "debug",
	}
	for k, v := range settings {
		k = fmt.Sprintf("STKEEPER_%s",
			strings.ReplaceAll(strings.ToUpper(k), "-", "_"))
		envSettings[k] = v
	}
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env:            envSettings,
				Image:          fmt.Sprintf("keeper-%s", pgVersion),
				Networks:       []string{nw.Name},
				NetworkAliases: aliasses,
				WaitingFor: wait.ForLog(
					"postgres hba entries not changed"),
			},
			Started: true,
		})
}

func runSentinel(
	ctx context.Context,
	etcdEndpoints string,
	nw *testcontainers.DockerNetwork,
) (testcontainers.Container, error) {
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: map[string]string{
					"STSENTINEL_STORE_ENDPOINTS": etcdEndpoints,
					"STSENTINEL_LOG_LEVEL":       "debug",
				},
				Networks:   []string{nw.Name},
				ExtraHosts: []string{},
				Image:      "sentinel",
			},
			Started: true,
		})
}

func runProxy(
	ctx context.Context,
	etcdEndpoints string,
	nw *testcontainers.DockerNetwork,
	aliasses map[string][]string,
) (testcontainers.Container, error) {
	envSettings := map[string]string{
		"STPROXY_STORE_ENDPOINTS": etcdEndpoints,
		"STPROXY_LOG_LEVEL":       "debug",
	}
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env:            envSettings,
				Image:          "proxy",
				Networks:       []string{nw.Name},
				NetworkAliases: aliasses,
				WaitingFor: wait.ForLog(
					"proxying to master address"),
				// WaitingFor: wait.ForListeningPort(
				//     nat.Port(fmt.Sprintf("%d/tcp", proxyPort))),
			},
			Started: true,
		})
}
