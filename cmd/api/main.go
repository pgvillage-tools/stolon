// Copyright 2026 PgVillage
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

// Package cmd holds all CLI code for the keeper
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"

	cmdcommon "github.com/pgvillage-tools/stolon/cmd"
	"github.com/pgvillage-tools/stolon/internal/flagutil"
	"github.com/pgvillage-tools/stolon/internal/logging"

	"github.com/spf13/cobra"
)

type config struct {
	cmdcommon.CommonConfig

	baseURL string
	port    uint
	token   string
	debug   bool
}

var cfg config

func init() {
	cmdcommon.AddCommonFlags(CmdWebApi, &cfg.CommonConfig)
	// revive:disable
	CmdWebApi.PersistentFlags().StringVar(&cfg.baseURL, "baseurl", "0.0.0.0",
		"baseurl for the webserver to run on.")
	CmdWebApi.PersistentFlags().StringVar(&cfg.token, "token", "",
		"token for authentication.")
	CmdWebApi.PersistentFlags().UintVar(&cfg.port, "port", defaultPort,
		"port for webserver")
	CmdWebApi.PersistentFlags().BoolVar(&cfg.debug, "debug", false,
		"enable debug logging")
	// revive:enable
}

var ready atomic.Bool
var logLevel = new(slog.LevelVar)

const (
	defaultPort = uint(8080)
)

// CmdWebApi exports the main webapi process
var CmdWebApi = &cobra.Command{
	Use:     "api",
	Run:     api,
	Version: cmdcommon.Version,
}

func api(c *cobra.Command, _ []string) {
	ctx, logger := logging.GetLogComponent(context.Background(), logging.WebApiComponent)

	if cfg.debug {
		logging.SetStaticLevel("debug")
	}
	if cmdcommon.IsColorLoggerEnable(c, &cfg.CommonConfig) {
		logging.EnableColor()
	}

	if err := cmdcommon.CheckCommonConfig(&cfg.CommonConfig); err != nil {
		logger.Fatal().AnErr("err", err).Msg("")
	}
	cl, err := cmdcommon.NewStore(ctx, &cfg.CommonConfig)
	if err != nil {
		logger.Fatal().AnErr("error", err).Msg("")
	}

	h := NewHandlers(cl, &ready)
	mux := http.NewServeMux()

	logger.Debug().Msg("registering routes")
	for _, route := range h.Routes() {
		mux.HandleFunc(route.Pattern, route.Handler)
		logger.Debug().Str("pattern", route.Pattern).Msg("registered route")
	}

	logger.Info().Str("host", cfg.baseURL).Uint("port", cfg.port).Msg("starting server")
	ready.Store(true)
	logger.Info().Bool("ready", ready.Load()).Msg("server ready")
	err = http.ListenAndServe(fmt.Sprintf("%s:%d", cfg.baseURL, cfg.port), mux)
	if err != nil {
		logger.Error().AnErr("error", err).Msg("server failing")
		os.Exit(-1)
	}
}

func main() {

	_, logger := logging.GetLogComponent(context.Background(), logging.SentinelComponent)
	if err := flagutil.SetFlagsFromEnv(CmdWebApi.PersistentFlags(), "STAPI"); err != nil {
		logger.Fatal().AnErr("err", err).Msg("")
	}

	if err := CmdWebApi.Execute(); err != nil {
		logger.Fatal().AnErr("err", err).Msg("")
	}
}
