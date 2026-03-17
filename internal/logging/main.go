/*
Copyright 2023, Tax Administration of The Netherlands.
Licensed under the EUPL 1.2.
See LICENSE.md for details.
*/

package logging

import (
	"context"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	defaultLevel = zerolog.InfoLevel
)

var (
	// Commandline args will use this to enable all debug logging
	staticLevel = defaultLevel
	// Commandline args can use this to enable logging for a component
	staticComponents = Components{}
	// Commandline args will use this to enable all debug logging
	dynamicLevel = defaultLevel
	// Commandline args can use this to enable logging for a component
	dynamicComponents = Components{}
	// The output logger to be used
	output zerolog.ConsoleWriter
	// sToLevel is a map to easilly convert between string and zerolog level
	sToLevel = map[string]zerolog.Level{
		"debug":   zerolog.DebugLevel,
		"info":    zerolog.InfoLevel,
		"error":   zerolog.ErrorLevel,
		"warn":    zerolog.WarnLevel,
		"warning": zerolog.WarnLevel,
	}
)

func init() {
	output = zerolog.ConsoleWriter{
		Out:        os.Stdout,
		NoColor:    !doColor(),
		TimeFormat: time.RFC3339,
	}
	l := zerolog.New(output).With().Timestamp().Logger()

	log.Logger = l
	zerolog.DefaultContextLogger = &l
}

func doColor() bool {
	if os.Getenv("NO_COLOR") != "" {
		return true
	}
	return isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
}

// SetStaticLevel configures global debugging level from commandline argument
func SetStaticLevel(level string) {
	staticLevel = defaultLevel
	if lvl, ok := sToLevel[level]; ok {
		staticLevel = lvl
	}
}

// SetStaticComponents configures component debugging from commandline argument
func SetStaticComponents(components Components) {
	if components == nil {
		components = Components{}
	}
	staticComponents = components
}

// SetDynamicLoggingConfig configures global debugging and component debugging
func SetDynamicLoggingConfig(level string, components Components) {
	dynamicLevel = defaultLevel
	if lvl, ok := sToLevel[level]; ok {
		dynamicLevel = lvl
	}
	if components == nil {
		components = Components{}
	}
	dynamicComponents = components
}

func getComponentLevel(name Component) zerolog.Level {
	if level, exists := dynamicComponents[name]; exists {
		return level
	}
	if level, ok := staticComponents[name]; ok {
		return level
	}
	if staticLevel < dynamicLevel {
		return staticLevel
	}
	return dynamicLevel
}

// GetID can be used to get the ID (uuid) from the context.
// If there is no ID, it will be generated and added
func GetID(inCtx context.Context) (id string, outCtx context.Context) {
	var ctxId = inCtx.Value("ID")
	outCtx = inCtx
	if ctxId == nil {
		id = uuid.NewString()
		outCtx = context.WithValue(inCtx, "ID", id)
	} else if id, ok := ctxId.(string); ok {
		id = uuid.NewString()
		outCtx = context.WithValue(inCtx, "ID", id)
	}
	return id, outCtx
}

// GetLogComponent gets the logger for a component from a context.
func GetLogComponent(ctx context.Context, comp Component) (context.Context, *zerolog.Logger) {
	logger := log.Ctx(ctx)
	level := getComponentLevel(comp)

	if logger.GetLevel() != level {
		id, ctx := GetID(ctx)
		ll := logger.
			Output(output).
			Level(level).
			With().
			Str("ID", id).
			Str("component", componentToString(comp)).
			Logger()
		logger = &ll
		ctx = logger.WithContext(ctx)
	}
	return ctx, logger
}

// EnableColor will force logging with colors
// (default depends on if we run in a terminal)
func EnableColor() {
	output.NoColor = false
}

// DisableColor will force logging with colors
// (default depends on if we run in a terminal)
func DisableColor() {
	output.NoColor = true
}
