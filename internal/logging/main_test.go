package logging

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

const (
	defaultLevelString = "info"
	enabledLevelString = "debug"
)

type logSink struct {
	logs []string
}

func (l *logSink) Write(p []byte) (n int, err error) {
	l.logs = append(l.logs, string(p))
	return len(p), nil
}

func (l *logSink) Index(i int) string {
	if i >= 0 && i < len(l.logs) {
		return l.logs[i]
	}
	return ""
}

func TestDebuggingStatic(t *testing.T) {
	const comp1 = TestComponent
	SetDynamicLoggingConfig(defaultLevelString, nil)
	ctx := context.TODO()
	// debug false
	SetStaticLevel(defaultLevelString)
	_, noDebugLogger := GetLogComponent(ctx, comp1)
	assert.Equal(t, zerolog.InfoLevel, noDebugLogger.GetLevel())
	// debug true
	SetStaticLevel(enabledLevelString)
	_, allDebugLogger := GetLogComponent(ctx, comp1)
	assert.Equal(t, zerolog.DebugLevel, allDebugLogger.GetLevel())
	// debug component
	SetStaticComponents(Components{comp1: zerolog.DebugLevel})
	_, componentDebugLogger := GetLogComponent(ctx, comp1)
	assert.Equal(t, zerolog.DebugLevel, componentDebugLogger.GetLevel())
}

func TestDebuggingDynamic(t *testing.T) {
	ctx := context.TODO()
	const comp1 = TestComponent
	SetStaticLevel(defaultLevelString)
	SetStaticComponents(nil)
	// debug false
	SetDynamicLoggingConfig(defaultLevelString, nil)
	_, noDebugLogger := GetLogComponent(ctx, comp1)
	assert.Equal(t, zerolog.InfoLevel, noDebugLogger.GetLevel())
	// debug true
	SetDynamicLoggingConfig(enabledLevelString, nil)
	_, allDebugLogger := GetLogComponent(ctx, comp1)
	assert.Equal(t, zerolog.DebugLevel, allDebugLogger.GetLevel())
	// debug component on
	SetDynamicLoggingConfig(defaultLevelString, Components{comp1: zerolog.DebugLevel})
	_, componentDebugLogger := GetLogComponent(ctx, comp1)
	assert.Equal(t, zerolog.DebugLevel, componentDebugLogger.GetLevel())

	// debug component off
	SetDynamicLoggingConfig(defaultLevelString, Components{})
	_, componentNoDebugLogger := GetLogComponent(ctx, comp1)
	assert.Equal(t, zerolog.InfoLevel, componentNoDebugLogger.GetLevel())
}
