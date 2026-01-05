package etcdv3_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/etcd"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	etcdImage = "quay.io/coreos/etcd:v3.6.7"
)

func setupEtcd(
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
				},
				Networks: []string{nw.Name},
				FromDockerfile: testcontainers.FromDockerfile{
					Context:    "../../",
					Dockerfile: "Dockerfile.stolonctl",
				},
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
	envSettings := map[string]string{"STKEEPER_STORE_ENDPOINTS": etcdEndpoints}
	for k, v := range settings {
		k = fmt.Sprintf("STKEEPER_%s",
			strings.ReplaceAll(strings.ToUpper(k), "-", "_"))
		envSettings[k] = v
	}
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: envSettings,
				FromDockerfile: testcontainers.FromDockerfile{
					Context:    "../../",
					Dockerfile: "Dockerfile.keeper",
				},
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
				},
				Networks:   []string{nw.Name},
				ExtraHosts: []string{},
				FromDockerfile: testcontainers.FromDockerfile{
					Context:    "../../",
					Dockerfile: "Dockerfile.sentinel",
				},
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
	envSettings := map[string]string{"STPROXY_STORE_ENDPOINTS": etcdEndpoints}
	return testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: envSettings,
				FromDockerfile: testcontainers.FromDockerfile{
					Context:    "../../",
					Dockerfile: "Dockerfile.proxy",
				},
				Networks:       []string{nw.Name},
				NetworkAliases: aliasses,
				WaitingFor: wait.ForLog(
					"proxying to master address"),
			},
			Started: true,
		})
}
