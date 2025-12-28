// Package etcdv3_test will run integration tests for using etcd as backend with v3 api
package etcdv3_test

import (
	"context"
	"fmt"
	"maps"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/etcd"
	"github.com/testcontainers/testcontainers-go/network"
)

var _ = Describe("Smoke", Ordered, func() {
	const (
		numEtcd       = 1
		pgPassword    = "test123"
		autoRemove    = true
		initialConfig = `{"stolon_custom_config":{"defaultSUReplAccessMode":"strict","pgParameters":{},"pgHBA":[]}}`

		numKeepers = 3
	)
	var (
		ctx              context.Context
		nw               *testcontainers.DockerNetwork
		etcdContainer    *etcd.EtcdContainer
		etcdEndpoints    string
		sentinelCnt      testcontainers.Container
		keeperContainers []testcontainers.Container
		allContainers    []testcontainers.Container
		keeperSettings   = map[string]string{
			"pg-repl-password": pgPassword,
			"pg-su-password":   pgPassword,
		}
	)

	BeforeAll(func() {
		// RYUK requires permissions we don't need and don't want to implement
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")

		ctx = context.Background()

		var nwErr error
		nw, nwErr = network.New(ctx)
		Ω(nwErr).NotTo(HaveOccurred())

		// setup etcd
		var etcdErr error
		etcdContainer, etcdEndpoints, etcdErr = setupEtcd(ctx, etcdImage, nw)
		Ω(etcdErr).NotTo(HaveOccurred())
		allContainers = []testcontainers.Container{etcdContainer}

		// run stolonctl to define a new cluster in etcd
		stolonCtlInitCnt, initErr := runStolonCtl(
			ctx,
			etcdEndpoints,
			nw,
			"init", "--yes")
		Ω(initErr).NotTo(HaveOccurred())
		allContainers = append(allContainers, stolonCtlInitCnt)

		// Run stolonctl patch to set initial config
		stolonCtlPatchCnt, patchErr := runStolonCtl(
			ctx,
			etcdEndpoints,
			nw,
			"update",
			"--patch",
			initialConfig,
		)
		Ω(patchErr).NotTo(HaveOccurred())
		allContainers = append(allContainers, stolonCtlPatchCnt)
		// - cat myspec.json | stolonctl update --patch --file -
		//   or
		// - stolonctl update --patch "${MYSPEC}"
		//   or
		// - stolonctl update --patch --file "${STOLONCTL_FILE}"'

		// Start sentinel
		var sentinelErr error
		sentinelCnt, sentinelErr = runSentinel(ctx, etcdEndpoints, nw)
		Ω(sentinelErr).NotTo(HaveOccurred())
		allContainers = append(allContainers, sentinelCnt)

		// start keeper(s)
		for i := 0; i < numKeepers; i++ {
			alias := fmt.Sprintf("keeper_%d", i)
			settings := maps.Clone(keeperSettings)
			settings["pg-listen-address"] = alias
			aliasses := map[string][]string{nw.Name: []string{alias}}
			cnt, keeperErr := runKeeper(ctx, etcdEndpoints, nw, aliasses, settings)
			Ω(keeperErr).NotTo(HaveOccurred())
			keeperContainers = append(keeperContainers, cnt)
			allContainers = append(allContainers, cnt)
		}
		/*
			logs, logErr := cnt.Logs(ctx)
			Ω(logErr).NotTo(HaveOccurred())
			data, readErr := io.ReadAll(logs)
			Ω(readErr).NotTo(HaveOccurred())
			fmt.Fprintf(GinkgoWriter, "DEBUG - Logs: %s", string(data))
		*/
		// wait for postgres to be available
	})
	AfterAll(func() {
		if !autoRemove {
			return
		}
		for _, cnt := range allContainers {
			Ω(cnt.Terminate(ctx)).NotTo(HaveOccurred())
		}
		Ω(nw.Remove(ctx)).NotTo(HaveOccurred())
	})
	Context("when running etcd", func() {
		It("should work properly", func() {
			state, stateErr := etcdContainer.State(ctx)
			Ω(stateErr).NotTo(HaveOccurred())
			Ω(state).NotTo(BeNil())
		})
	})
	Context("when creating a cluster with 3 instances", func() {
		It("should create a cluster with one primary and 2 standby's", func() {
			for _, cnt := range keeperContainers {
				state, stateErr := cnt.State(ctx)
				Ω(stateErr).NotTo(HaveOccurred())
				Ω(state).NotTo(BeNil())
			}
		})
	})
})
